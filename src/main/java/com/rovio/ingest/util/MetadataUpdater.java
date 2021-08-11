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
import com.google.common.collect.ImmutableMap;
import com.rovio.ingest.WriterContext;
import com.rovio.ingest.model.DbType;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorConfig;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnector;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnectorConfig;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLTablesConfig;
import org.apache.druid.timeline.DataSegment;
import org.skife.jdbi.v2.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;
import static java.lang.String.format;

public class MetadataUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdater.class);

    private static final String SELECT_UNUSED_OLD_SEGMENTS =
            "SELECT id FROM %1$s WHERE dataSource = :dataSource AND version < :version AND used = true AND id NOT IN (:ids)";
    private static final String MARK_SEGMENT_AS_UNUSED_BY_ID =
            "UPDATE %1$s SET used=false WHERE id = :id";

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

        final DbType dbType = DbType.from(param.getMetadataDbType());
        this.sqlConnector = this.makeSqlConnector(dbType, metadataStorageConnectorConfig, metadataStorageTablesConfig);
        this.metadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(sqlConnector);

        testDbConnection();
    }

    private SQLMetadataConnector makeSqlConnector(DbType dbType,
                                                  MetadataStorageConnectorConfig metadataStorageConnectorConfig,
                                                  MetadataStorageTablesConfig metadataStorageTablesConfig) {
        switch (dbType) {
            case Mysql:
                return new MySQLConnector(() -> metadataStorageConnectorConfig,
                        () -> metadataStorageTablesConfig,
                        new MySQLConnectorConfig());
            case Postgres:
                return new PostgreSQLConnector(() -> metadataStorageConnectorConfig,
                        () -> metadataStorageTablesConfig,
                        new PostgreSQLConnectorConfig(),
                        new PostgreSQLTablesConfig());
            default:
                throw new IllegalArgumentException("Failed to instantiate the SQL connector");
        }
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

        if (initDataSource) {
            // Mark old segments as unused for the datasource.
            // This done in 2 steps:
            // 1. List all used old segments for the datasource
            // 2. Update these segments as unused.
            // This is done this way to avoid accidentally locking the entire segment table.

            sqlConnector.getDBI().withHandle(handle -> {
                List<String> oldSegmentIds = handle
                        .createQuery(String.format(SELECT_UNUSED_OLD_SEGMENTS, segmentsTable))
                        .bind("dataSource", dataSource)
                        .bind("version", version)
                        .bind("ids",
                                dataSegments
                                        .stream()
                                        .map(d -> StringUtils.wrap(d.getId().toString(), "'"))
                                        .collect(Collectors.joining(",")))
                        .list()
                        .stream()
                        .map(m -> m.get("id").toString())
                        .collect(Collectors.toList());

                if (!oldSegmentIds.isEmpty()) {
                    PreparedBatch batch = handle.prepareBatch(String.format(MARK_SEGMENT_AS_UNUSED_BY_ID, segmentsTable));
                    for (String id : oldSegmentIds) {
                        batch.add(new ImmutableMap.Builder<String, Object>()
                                .put("id", id)
                                .build());
                    }

                    LOG.info(String.format("Marking %s old segments as ununsed", oldSegmentIds.size()));
                    batch.execute();
                }

                return null;
            });

        }
    }
}
