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
package com.rovio.ingest;

import com.rovio.ingest.model.DbType;
import com.rovio.ingest.util.SQLConnectorFactory;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.storage.mysql.MySQLConnector;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLConnector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SQLConnectorFactoryTest {

    public static String dbUser = "user";
    public static String dbPass = "pass";
    public static String dbConnectionUri = "jdbc:mysql//localhost";
    private final SQLConnectorFactory sqlConnectorFactory = new SQLConnectorFactory();

    @Test
    public void shouldMakeTheCorrectSqlConnector() {
        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new MetadataStorageConnectorConfig() {
            @Override
            public String getConnectURI() {
                return dbConnectionUri;
            }

            @Override
            public String getUser() {
                return dbUser;
            }

            @Override
            public String getPassword() {
                return dbPass;
            }
        };

        assertTrue(this.sqlConnectorFactory.makeSqlConnector(DbType.Mysql, metadataStorageConnectorConfig, null) instanceof MySQLConnector);
        assertTrue(this.sqlConnectorFactory.makeSqlConnector(DbType.Postgres, metadataStorageConnectorConfig, null) instanceof PostgreSQLConnector);
    }

}
