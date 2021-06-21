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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;
import static java.lang.String.format;

public class MetadataUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdater.class);

    private static final String MARK_ALL_OLDER_SEGMENTS_AS_UNUSED_SQL =
            "UPDATE %1$s SET used = false" +
                    " WHERE dataSource=:dataSource AND version != :version AND used = true";

    private final String dataSource;
    private final String version;
    private final boolean initDataSource;
    private final SQLMetadataConnector sqlConnector;
    private final String segmentsTable;
    private final SQLMetadataStorageUpdaterJobHandler metadataStorageUpdaterJobHandler;

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
        this.metadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(sqlConnector);

        testDbConnection();
    }

    private void testDbConnection() {
        boolean tableExists = this.sqlConnector.retryWithHandle(h -> this.sqlConnector.tableExists(h, this.segmentsTable));
        if (!tableExists) {
            throw new IllegalStateException(format("Required druid segment table \"%s\" does not exists", this.segmentsTable));
        }
    }

    /**
     * Updates segments in Metadata segment table using {@link org.apache.druid.indexer.SQLMetadataStorageUpdaterJobHandler#publishSegments},
     * with additional handling for (re-)init.
     */
    public void publishSegments(List<DataSegment> dataSegments) {
        if (dataSegments.isEmpty()) {
            LOG.warn("No segments created, skipping metadata update.");
            return;
        }

        metadataStorageUpdaterJobHandler.publishSegments(segmentsTable, dataSegments, MAPPER);
        LOG.info("All segments published");

        this.sqlConnector.getDBI().withHandle(handle -> {
            handle.getConnection().setAutoCommit(false);
            handle.begin();

            Update updateStatement;
            if (initDataSource) {
                updateStatement = handle.createStatement(format(MARK_ALL_OLDER_SEGMENTS_AS_UNUSED_SQL, this.segmentsTable))
                        .bind("dataSource", this.dataSource)
                        .bind("version", this.version);
                updateStatement.execute();
            }

            handle.commit();
            return null;
        });
    }
}
