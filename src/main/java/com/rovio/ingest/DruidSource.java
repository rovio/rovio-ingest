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

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class DruidSource implements TableProvider {
    public static final String FORMAT = DruidSource.class.getCanonicalName();

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return new StructType();
    }

    @Override
    public Table getTable(StructType structType, Transform[] transforms, Map<String, String> map) {
        return new DruidTable(map);
    }

    public boolean supportsExternalMetadata() {
        return false;
    }
}
