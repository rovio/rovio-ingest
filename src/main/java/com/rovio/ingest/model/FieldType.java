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
import org.apache.spark.sql.types.DataTypes;

import java.util.Objects;

public enum FieldType {
    TIMESTAMP,
    DOUBLE,
    LONG,
    STRING,
    ARRAY_OF_DOUBLE,
    ARRAY_OF_LONG,
    ARRAY_OF_STRING;

    public static FieldType from(DataType dataType) {
        if (isNumericType(dataType)) {
            return LONG;
        }

        if (isDateTimeType(dataType)) {
            return TIMESTAMP;
        }

        if (dataType == DataTypes.DoubleType || dataType == DataTypes.FloatType) {
            return DOUBLE;
        }

        if (dataType == DataTypes.StringType || dataType == DataTypes.BooleanType) {
            return STRING;
        }

        if (isArrayOfNumericType(dataType)) {
            return ARRAY_OF_LONG;
        }

        if (isArrayOfDoubleType(dataType)) {
            return ARRAY_OF_DOUBLE;
        }

        if (isArrayOfStringType(dataType)) {
            return ARRAY_OF_STRING;
        }

        throw new IllegalArgumentException("Unsupported Type " + dataType);
    }

    private static boolean isDateTimeType(DataType dataType) {
        return dataType == DataTypes.DateType || dataType == DataTypes.TimestampType;
    }

    private static boolean isNumericType(DataType dataType) {
        return dataType == DataTypes.LongType
                || dataType == DataTypes.IntegerType
                || dataType == DataTypes.ShortType
                || dataType == DataTypes.ByteType;
    }

    private static boolean isArrayOfNumericType(DataType dataType) {
        return Objects.equals(dataType, DataTypes.createArrayType(DataTypes.LongType))
                || Objects.equals(dataType, DataTypes.createArrayType(DataTypes.IntegerType))
                || Objects.equals(dataType, DataTypes.createArrayType(DataTypes.ShortType))
                || Objects.equals(dataType, DataTypes.createArrayType(DataTypes.ByteType));
    }

    private static boolean isArrayOfDoubleType(DataType dataType) {
        return Objects.equals(dataType, DataTypes.createArrayType(DataTypes.DoubleType))
                || Objects.equals(dataType, DataTypes.createArrayType(DataTypes.FloatType));
    }
    private static boolean isArrayOfStringType(DataType dataType) {
        return Objects.equals(dataType, DataTypes.createArrayType(DataTypes.StringType))
                || Objects.equals(dataType, DataTypes.createArrayType(DataTypes.BooleanType));
    }

}
