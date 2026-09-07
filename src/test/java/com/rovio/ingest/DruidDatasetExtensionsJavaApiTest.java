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

import com.rovio.ingest.extensions.java.DruidDatasetExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DruidDatasetExtensionsJavaApiTest {

    private static final StructType KPI_SCHEMA = new StructType()
            .add("date", DataTypes.TimestampType, false)
            .add("country", DataTypes.StringType, false)
            .add("dau", DataTypes.IntegerType, false)
            .add("revenue", DataTypes.DoubleType, false)
            .add("is_segmented", DataTypes.BooleanType, false);

    private SparkSession spark;

    @BeforeAll
    public void startSpark() {
        spark = SparkSession.builder()
                .appName(getClass().getName())
                .master("local[2]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    public void stopSpark() {
        spark.stop();
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
    }

    @ParameterizedTest
    @CsvSource({
            "DAY,2019-10-16 00:00:00",
            "WEEK,2019-10-14 00:00:00",
            "HOUR,2019-10-16 12:00:00",
            "MINUTE,2019-10-16 12:34:00",
            "SECOND,2019-10-16 12:34:56"
    })
    public void normalizesSupportedGranularities(String granularity, String expectedPartitionTime) {
        Dataset<Row> input = timestampDataset(Collections.singletonList(
                kpiRow("2019-10-16T12:34:56Z", "US")));

        Row partition = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(input, "date", granularity, 2, false)
                .select(
                        col("__PARTITION_TIME__").cast(DataTypes.StringType),
                        col("__PARTITION_NUM__"))
                .head();

        assertEquals(expectedPartitionTime, partition.getString(0));
        assertEquals(0, partition.getInt(1));
    }

    @Test
    public void assignsPartitionsDeterministically() {
        Dataset<Row> input = timestampDataset(Arrays.asList(
                kpiRow("2019-10-14T00:00:00Z", "US"),
                kpiRow("2019-10-14T00:00:00Z", "GB"),
                kpiRow("2019-10-14T00:00:00Z", "FI"),
                kpiRow("2019-10-14T00:00:00Z", "DE")));

        Dataset<Row> result = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(input, "date", "WEEK", 2, false);

        assertEquals(
                Arrays.asList("DE:0", "FI:0", "GB:1", "US:1"),
                partitionAssignments(result));
    }

    @Test
    public void dropsUnsupportedColumnsWhenRequested() {
        Dataset<Row> result = DruidDatasetExtensions.repartitionByDruidSegmentSize(
                datasetWithUnsupportedColumn(), "date", "DAY", 2, true);

        assertArrayEquals(
                new String[]{"date", "country", "dau", "__PARTITION_TIME__", "__PARTITION_NUM__"},
                result.columns());
        assertEquals(1, result.count());
    }

    @Test
    public void rejectsUnsupportedColumnsByDefault() {
        assertThrows(
                IllegalArgumentException.class,
                () -> DruidDatasetExtensions.repartitionByDruidSegmentSize(
                        datasetWithUnsupportedColumn(), "date"));
    }

    @Test
    public void twoArgumentOverloadUsesDocumentedDefaults() {
        StructType schema = new StructType()
                .add("date", DataTypes.DateType, false)
                .add("country", DataTypes.StringType, false)
                .add("dau", DataTypes.IntegerType, false);
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                RowFactory.create(Date.valueOf("2019-10-16"), "US", 50),
                RowFactory.create(Date.valueOf("2019-10-16"), "GB", 20)), schema);

        Dataset<Row> shorthand = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(input, "date");
        Dataset<Row> explicit = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(input, "date", "DAY", 5000000, false);

        assertEquals(partitionRows(explicit), partitionRows(shorthand));
        assertEquals(
                Arrays.asList(
                        "GB:2019-10-16 00:00:00:0",
                        "US:2019-10-16 00:00:00:0"),
                partitionRows(shorthand));
    }

    private Dataset<Row> timestampDataset(List<Row> rows) {
        return spark.createDataFrame(rows, KPI_SCHEMA);
    }

    private Row kpiRow(String instant, String country) {
        return RowFactory.create(Timestamp.from(Instant.parse(instant)), country, 20, 20.0, false);
    }

    private Dataset<Row> datasetWithUnsupportedColumn() {
        StructType schema = new StructType()
                .add("date", DataTypes.TimestampType, false)
                .add("country", DataTypes.StringType, false)
                .add("dau", DataTypes.IntegerType, false)
                .add("values", DataTypes.createMapType(
                        DataTypes.StringType, DataTypes.LongType, false), false);
        return spark.createDataFrame(
                Collections.singletonList(RowFactory.create(
                        Timestamp.from(Instant.parse("2019-10-17T00:00:00Z")),
                        "US",
                        50,
                        Collections.singletonMap("value", 1L))),
                schema);
    }

    private List<String> partitionAssignments(Dataset<Row> dataset) {
        return dataset
                .select("country", "__PARTITION_NUM__")
                .orderBy("country")
                .collectAsList()
                .stream()
                .map(row -> row.getString(0) + ":" + row.getInt(1))
                .toList();
    }

    private List<String> partitionRows(Dataset<Row> dataset) {
        return dataset
                .select(
                        col("country"),
                        col("__PARTITION_TIME__").cast(DataTypes.StringType),
                        col("__PARTITION_NUM__"))
                .orderBy("country")
                .collectAsList()
                .stream()
                .map(row -> row.getString(0) + ":" + row.getString(1) + ":" + row.getInt(2))
                .toList();
    }
}
