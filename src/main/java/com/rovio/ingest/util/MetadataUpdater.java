/*
 * Copyright 2021 Rovio Entertainment Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rovio.ingest.util;

import com.google.common.base.Preconditions;
import com.rovio.ingest.WriterContext;
import org.apache.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorConfig;
import org.apache.druid.timeline.DataSegment;
import org.skife.jdbi.v2.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.util.List;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;
import static java.lang.String.format;

public class MetadataUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdater.class);
    private static final int DEFAULT_MAX_RETRIES = 5;

    private static final String MARK_ALL_OLDER_SEGMENTS_AS_UNUSED_SQL =
            "UPDATE %1$s SET used = false" +
                    " WHERE dataSource=:dataSource AND version != :version AND used = true";

    private final String dataSource;
    private final String version;
    private final boolean initDataSource;
    private final SQLMetadataConnector sqlConnector;
    private final String segmentsTable;
    private final SQLMetadataStorageUpdaterJobHandler metadataSegmentUpdaterJobHandler;

    public MetadataUpdater(WriterContext param) {
        Preconditions.checkNotNull(param);
        this.dataSource = param.getDataSource();
        this.version = param.getVersion();
        this.initDataSource = param.isInitDataSource();
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig() {
            @Override
            public String getConnectURI() {
                return param.getMetadataDbUri();
            }

            @Override
            public String getUser() {
                return param.getMetadataDbUser();
            }

            @Override
            public String getPassword() {
                return param.getMetadataDbPass();
            }
        };

        MetadataStorageTablesConfig metadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase(param.getMetadataDbTableBase());
        this.segmentsTable = metadataStorageTablesConfig.getSegmentsTable();
        this.sqlConnector = new MySQLConnector(() -> metadataStorageConnectorConfig,
                () -> metadataStorageTablesConfig,
                new MySQLConnectorConfig());
        this.metadataSegmentUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(sqlConnector);
        testDbConnection();
    }

    private void testDbConnection() {
        boolean tableExists = this.sqlConnector.retryWithHandle(h -> this.sqlConnector.tableExists(h, this.segmentsTable));
        if (!tableExists) {
            throw new IllegalStateException(format("Required druid segment table \"%s\" does not exists", this.segmentsTable));
        }
    }

    /**
     * Updates segments in Metadata database using {@link org.apache.druid.indexer.SQLMetadataStorageUpdaterJobHandler#publishSegments},
     * with additional handling for (re-)init.
     */
    public void publishSegments(List<DataSegment> dataSegments) {
        if (dataSegments.isEmpty()) {
            LOG.warn("No segments created, skipping metadata update.");
            return;
        }

        try {
            RetryUtils.Task<Void> task = () -> {
                metadataSegmentUpdaterJobHandler.publishSegments(segmentsTable, dataSegments, MAPPER);
                return null;
            };

            RetryUtils.retry(
                    task,
                    this::isLockTimeoutException,
                    0,
                    DEFAULT_MAX_RETRIES,
                    null,
                    String.format("Lock wait timeout exception while publishing segments for %s", dataSource));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish segments with retry", e);
        }

        if (initDataSource) {
            // Mark older version segments as unused as an additional step.
            // Druid Coordinator already does this on every refreshInterval but this is done here just to speed up things.
            try {
                RetryUtils.Task<Integer> task = () -> this.sqlConnector.getDBI().withHandle(handle -> {
                    Update updateStatement = handle.createStatement(format(MARK_ALL_OLDER_SEGMENTS_AS_UNUSED_SQL, this.segmentsTable))
                            .bind("dataSource", this.dataSource)
                            .bind("version", this.version);
                    return updateStatement.execute();
                });

                RetryUtils.retry(
                        task,
                        this::isLockTimeoutException,
                        0,
                        DEFAULT_MAX_RETRIES,
                        null,
                        String.format("Lock wait timeout exception while marking old segments as unused with init for %s", dataSource));
            } catch (Exception e) {
                LOG.warn("Failed to mark old segments as unused with init after retries; ignored", e);
            }
        }
      }

    private boolean isLockTimeoutException(Throwable e) {
        return e != null &&
                ((e instanceof BatchUpdateException &&
                        e.getMessage() != null && e.getMessage().contains("Lock wait timeout exceeded")) ||
                        isLockTimeoutException(e.getCause())
                );
    }
}
