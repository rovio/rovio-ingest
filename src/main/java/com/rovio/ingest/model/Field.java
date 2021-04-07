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
package com.rovio.ingest.model;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;

public class Field implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final DataType sqlType;
    private final AggregatorType aggregatorType;
    private final int ordinal;

    private Field(String name, DataType dataType, int ordinal) {
        this.name = name;
        this.sqlType = dataType;
        this.aggregatorType = AggregatorType.from(dataType);
        this.ordinal = ordinal;
    }

    static Field from(StructField field, int oridinal) {
        return new Field(field.name(), field.dataType(), oridinal);
    }

    public String getName() {
        return name;
    }

    public DataType getSqlType() {
        return this.sqlType;
    }

    public AggregatorType getAggregatorType() {
        return aggregatorType;
    }

    public int getOrdinal() {
        return ordinal;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", dataType=" + sqlType +
                ", type=" + aggregatorType +
                ", ordinal=" + ordinal +
                '}';
    }
}
