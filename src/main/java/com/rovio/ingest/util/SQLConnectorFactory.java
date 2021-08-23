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

import com.rovio.ingest.model.DbType;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorConfig;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnector;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnectorConfig;

public class SQLConnectorFactory {

    public SQLMetadataConnector makeSqlConnector(DbType dbType,
                                          MetadataStorageConnectorConfig metadataStorageConnectorConfig,
                                          MetadataStorageTablesConfig metadataStorageTablesConfig) {
        switch (dbType) {
            case Mysql:
                return new MySQLConnector(() -> metadataStorageConnectorConfig,
                        ()  -> metadataStorageTablesConfig,
                        new MySQLConnectorConfig());
            case Postgres:
                return new PostgreSQLConnector(() -> metadataStorageConnectorConfig,
                        () -> metadataStorageTablesConfig,
                        new PostgreSQLConnectorConfig());
            default:
                throw new IllegalArgumentException("Failed to instantiate the SQL connector");
        }
    }
}
