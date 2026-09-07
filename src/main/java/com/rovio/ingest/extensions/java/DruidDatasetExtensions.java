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
package com.rovio.ingest.extensions.java;

import com.rovio.ingest.util.NormalizeTimeColumnUDF;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.unix_timestamp;

public class DruidDatasetExtensions {

    private static final DataType[] METRIC_TYPES = new DataType[]{
            DataTypes.FloatType,
            DataTypes.DoubleType,
            DataTypes.IntegerType,
            DataTypes.LongType,
            DataTypes.ShortType,
            DataTypes.ByteType,
            DataTypes.createArrayType(DataTypes.LongType),
            DataTypes.createArrayType(DataTypes.DoubleType)
    };
    private static final DataType[] DIMENSION_TYPES = new DataType[]{
            DataTypes.StringType,
            DataTypes.DateType,
            DataTypes.TimestampType,
            DataTypes.BooleanType,
            DataTypes.createArrayType(DataTypes.StringType)
    };
    private static final Logger LOG = LoggerFactory.getLogger(DruidDatasetExtensions.class);

    public static Dataset<Row> repartitionByDruidSegmentSize(Dataset<Row> dataset,
                                                              String timeColumn) {
        return repartitionByDruidSegmentSize(dataset, timeColumn, "DAY", 5000000, false);
    }

    public static Dataset<Row> repartitionByDruidSegmentSize(Dataset<Row> dataset,
                                                              String timeColumn,
                                                              String granularityString,
                                                              Integer rowsPerSegment,
                                                              boolean excludeColumnsWithUnknownTypes) {
        if (rowsPerSegment <= 0) {
            throw new AssertionError("rowsPerSegment must be greater than 0");
        }

        UserDefinedFunction normalizeUdf = udf(new NormalizeTimeColumnUDF(), DataTypes.LongType);
        Dataset<Row> validatedDataset = validate(dataset, timeColumn, excludeColumnsWithUnknownTypes);

        if (GranularityType.DAY.name().equals(granularityString)
                && DataTypes.DateType.equals(dataset.schema().apply(timeColumn).dataType())) {
            validatedDataset = validatedDataset.withColumn(
                    "__PARTITION_TIME__", col(timeColumn).cast(DataTypes.TimestampType));
        } else {
            validatedDataset = validatedDataset.withColumn(
                    "__PARTITION_TIME__",
                    normalizeUdf.apply(
                                    unix_timestamp(column(timeColumn))
                                            .multiply(1000)
                                            .cast(DataTypes.LongType),
                                    lit(granularityString))
                            .divide(1000)
                            .cast(DataTypes.TimestampType));
        }

        Column[] sortColumns = Arrays.stream(validatedDataset.columns())
                .map(org.apache.spark.sql.functions::col)
                .toArray(Column[]::new);

        return validatedDataset
                .withColumn(
                        "__num_rows__",
                        row_number().over(
                                Window.partitionBy(column("__PARTITION_TIME__"))
                                        .orderBy(sortColumns)))
                .withColumn(
                        "__PARTITION_NUM__",
                        col("__num_rows__")
                                .minus(1)
                                .divide(lit(rowsPerSegment))
                                .cast(DataTypes.IntegerType))
                .repartition(column("__PARTITION_TIME__"), column("__PARTITION_NUM__"))
                .drop(new String[]{"__num_rows__"});
    }

    private static Dataset<Row> validate(Dataset<Row> dataset,
                                         String timeColumn,
                                         boolean excludeColumnsWithUnknownTypes) {
        if (Arrays.stream(dataset.columns()).noneMatch(timeColumn::equals)) {
            throw new IllegalArgumentException(timeColumn + " not found in dataset");
        }

        List<DataType> metricTypes = Arrays.asList(METRIC_TYPES);
        List<DataType> dimensionTypes = Arrays.asList(DIMENSION_TYPES);
        List<StructField> excludedDimensions = new ArrayList<>();
        Dataset<Row> validatedDataset = dataset;

        for (StructField field : dataset.schema().fields()) {
            if (dimensionTypes.contains(field.dataType())) {
                if (!field.name().equals(timeColumn)
                        && (DataTypes.TimestampType.equals(field.dataType())
                        || DataTypes.DateType.equals(field.dataType()))) {
                    validatedDataset = validatedDataset.withColumn(
                            field.name(), column(field.name()).cast(DataTypes.StringType));
                }
            } else if (!metricTypes.contains(field.dataType())) {
                excludedDimensions.add(field);
            }
        }

        if (!excludedDimensions.isEmpty()) {
            String unsupportedDimensions = String.join(", ", excludedDimensions.stream()
                    .map(StructField::toString)
                    .toArray(String[]::new));
            if (excludeColumnsWithUnknownTypes) {
                LOG.warn("Dimensions excluded because of unsupported types: {}", unsupportedDimensions);
            } else {
                throw new IllegalArgumentException(
                        "Dimensions with unsupported data types, "
                                + "set excludeColumnsWithUnknownTypes to true to exclude: "
                                + unsupportedDimensions);
            }
        }

        String[] excludedNames = excludedDimensions.stream()
                .map(StructField::name)
                .toArray(String[]::new);
        return validatedDataset.drop(excludedNames);
    }

}
