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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.rovio.ingest.extensions.java.DruidDatasetExtensions;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchHolder;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import static com.rovio.ingest.WriterContext.ConfKeys.DATASOURCE_INIT;
import static com.rovio.ingest.WriterContext.ConfKeys.METRICS_SPEC;
import static com.rovio.ingest.WriterContext.ConfKeys.QUERY_GRANULARITY;
import static com.rovio.ingest.WriterContext.ConfKeys.SEGMENT_GRANULARITY;
import static org.apache.spark.sql.functions.column;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DruidDatasetExtensionsTest extends DruidSourceBaseTest {

    @AfterEach
    public void after() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @Test
    public void saveWithPartitionedByTime() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = DruidDatasetExtensions.repartitionByDruidSegmentSize(dataset, "date", "DAY", 5000000, false);
        dataset.show(false);

        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, false);
        verifySegmentTable(interval, version, true, 2);
    }

    @Test
    public void saveWithMonthGranularity() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "MONTH", 5000000, false);
        dataset.show(false);

        options.put(SEGMENT_GRANULARITY, "MONTH");
        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-01T00:00:00Z"), DateTime.parse("2019-11-01T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        verifySegmentTable(interval, version, true, 1);
    }

    @Test
    public void saveWithDayGranularityWithMaxRows() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "DAY", 1, false);
        dataset.show(false);

        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 4, false);
        verifySegmentTable(interval, version, true, 8);
    }

    @Test
    public void saveWithPartiallyOverWrittenSegments() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "DAY", 5000000, false);
        dataset.show(false);

        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String firstVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, firstVersion, 1, false);
        verifySegmentTable(interval, firstVersion, true, 2);

        Dataset<Row> dataset2 = loadCsv(spark, "/data2.csv");
        dataset2.show(false);
        dataset2 = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset2, "date", "DAY", 5000000, false);
        dataset2.show(false);

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);
        dataset2.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, firstVersion, 1, false);
        verifySegmentTable(interval, firstVersion, true, 2);

        interval = new Interval(DateTime.parse("2019-10-17T00:00:00Z"), DateTime.parse("2019-10-19T00:00:00Z"));
        String secondVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, secondVersion, 1, false);
        verifySegmentTable(interval, secondVersion, true, 2);
    }

    @Test
    public void saveWithIncrementalAndGranularityChanges() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "DAY", 5000000, false);
        dataset.show(false);

        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String firstVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, firstVersion, 1, false);
        verifySegmentTable(interval, firstVersion, true, 2);

        Dataset<Row> dataset2 = loadCsv(spark, "/data2.csv");
        dataset2.show(false);
        dataset2 = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset2, "date", "MONTH", 5000000, false);
        dataset2.show(false);

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);
        options.put(SEGMENT_GRANULARITY, "MONTH");

        dataset2.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        verifySegmentTable(interval, firstVersion, true, 2);

        String secondVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        interval = new Interval(DateTime.parse("2019-10-01T00:00:00Z"), DateTime.parse("2019-11-01T00:00:00Z"));
        verifySegmentTable(interval, secondVersion, true, 1);
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, secondVersion, 1, true);
    }

    @Test
    public void saveOverwritesWhenGranularityChangesWithInit() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "DAY", 5000000, false);
        dataset.show(false);

        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String firstVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, firstVersion, 1, false);
        verifySegmentTable(interval, firstVersion, true, 2);

        Dataset<Row> dataset2 = loadCsv(spark, "/data2.csv");
        dataset2 = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset2, "date", "MONTH", 5000000, false);
        dataset2.show(false);

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);
        options.put(SEGMENT_GRANULARITY, "MONTH");
        options.put(DATASOURCE_INIT, "true");

        dataset2.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        verifySegmentTable(interval, firstVersion, false, 2);

        String secondVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        interval = new Interval(DateTime.parse("2019-10-01T00:00:00Z"), DateTime.parse("2019-11-01T00:00:00Z"));
        verifySegmentTable(interval, secondVersion, true, 1);
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, secondVersion, 1, true);
    }

    @Test
    public void shouldWriteDataSegmentsWithCorrectValues() throws IOException {
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy-MM-dd")
                .load(DruidSourceBaseTest.class.getResource("/data3.csv").getPath())
                .withColumn("date", column("date").cast(DataTypes.TimestampType));
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "MONTH", 5000000, false);
        dataset.show(false);

        options.put(SEGMENT_GRANULARITY, "MONTH");
        options.put(QUERY_GRANULARITY, "SECOND");
        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-01T00:00:00Z"), DateTime.parse("2019-11-01T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        verifySegmentTable(interval, version, true, 1);

        Table<Integer, ImmutableMap<String, Object>, ImmutableMap<String, Object>> parsed = readSegmentData(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        assertEquals(2, parsed.size());
        assertTrue(parsed.containsRow(0));
        ImmutableMap<String, Object> dimensions = ImmutableMap.<String, Object>builder()
                .put("string_column", "US")
                .put("__time", DateTime.parse("2019-10-16T00:01:00Z"))
                .put("string_date_column", "2019-10-16")
                .put("boolean_column", "true")
                .build();
        Map<String, Object> data = parsed.get(0, dimensions);
        assertEquals(10L, data.get("long_column"));
        assertEquals(100.0, data.get("double_column"));

        dimensions = ImmutableMap.<String, Object>builder()
                .put("string_column", "US")
                .put("__time", DateTime.parse("2019-10-16T00:02:00Z"))
                .put("string_date_column", "2019-10-16")
                .put("boolean_column", "false")
                .build();
        data = parsed.get(0, dimensions);
        assertEquals(-1L, data.get("long_column"));
        assertEquals(-1.0, data.get("double_column"));
    }

    @Test
    public void shouldWriteDataSegmentsWithCorrectValuesUsingPartialMetricSpec() throws IOException {
        String metricsSpec = "[" +
                "{\n" +
                "   \"type\": \"longSum\",\n" +
                "   \"name\": \"long_column\",\n" +
                "   \"fieldName\": \"long_column\",\n" +
                "   \"expression\": null\n" +
                "}]";
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy-MM-dd")
                .load(DruidSourceBaseTest.class.getResource("/data3.csv").getPath())
                .withColumn("date", column("date").cast(DataTypes.TimestampType));
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "MONTH", 5000000, false);
        dataset.show(false);

        options.put(SEGMENT_GRANULARITY, "MONTH");
        options.put(QUERY_GRANULARITY, "SECOND");
        options.put(METRICS_SPEC, metricsSpec);
        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-01T00:00:00Z"), DateTime.parse("2019-11-01T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        verifySegmentTable(interval, version, true, 1);
        Table<Integer, ImmutableMap<String, Object>, ImmutableMap<String, Object>> parsed = readSegmentData(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        assertEquals(2, parsed.size());
        assertTrue(parsed.containsRow(0));
        ImmutableMap<String, Object> dimensions = ImmutableMap.<String, Object>builder()
                .put("string_column", "US")
                .put("__time", DateTime.parse("2019-10-16T00:01:00Z"))
                .put("string_date_column", "2019-10-16")
                .put("boolean_column", "true")
                .put("double_column", "100.0")
                .build();
        Map<String, Object> data = parsed.get(0, dimensions);
        assertNotNull(data);
        assertEquals(10L, data.get("long_column"));
        assertNull(data.get("double_column"));

        dimensions = ImmutableMap.<String, Object>builder()
                .put("string_column", "US")
                .put("__time", DateTime.parse("2019-10-16T00:02:00Z"))
                .put("string_date_column", "2019-10-16")
                .put("boolean_column", "false")
                .put("double_column", "-1.0")
                .build();
        data = parsed.get(0, dimensions);
        assertNotNull(data);
        assertEquals(-1L, data.get("long_column"));
        assertNull(data.get("double_column"));
    }


    @Test
    public void shouldWriteDataSegmentsWithSketchBuild() throws IOException {
        String metricsSpec = "[" +
                "{\n" +
                "   \"type\": \"HLLSketchBuild\",\n" +
                "   \"name\": \"string_column_hll\",\n" +
                "   \"fieldName\": \"string_column\"\n" +
                "}," +
                "{\n" +
                "   \"type\": \"thetaSketch\",\n" +
                "   \"name\": \"string_column_theta\",\n" +
                "   \"fieldName\": \"string_column\",\n" +
                "   \"isInputThetaSketch\": false,\n" +
                "   \"size\": 4096\n" +
                "}]";
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy-MM-dd")
                .load(DruidSourceBaseTest.class.getResource("/data3.csv").getPath())
                .withColumn("date", column("date").cast(DataTypes.TimestampType));
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "MONTH", 5000000, false);
        dataset.show(false);

        options.put(SEGMENT_GRANULARITY, "MONTH");
        options.put(QUERY_GRANULARITY, "SECOND");
        options.put(METRICS_SPEC, metricsSpec);
        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-01T00:00:00Z"), DateTime.parse("2019-11-01T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        verifySegmentTable(interval, version, true, 1);
        Table<Integer, ImmutableMap<String, Object>, ImmutableMap<String, Object>> parsed = readSegmentData(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        assertEquals(2, parsed.size());
        assertTrue(parsed.containsRow(0));
        // String column is automatically excluded from dimensions as it is used for sketch aggregation.
        ImmutableMap<String, Object> dimensions = ImmutableMap.<String, Object>builder()
                .put("__time", DateTime.parse("2019-10-16T00:01:00Z"))
                .put("string_date_column", "2019-10-16")
                .put("boolean_column", "true")
                .put("long_column", "10")
                .put("double_column", "100.0")
                .build();
        Map<String, Object> data = parsed.get(0, dimensions);
        assertNotNull(data);
        assertEquals(1.0, ((HllSketchHolder) data.get("string_column_hll")).getEstimate());
        assertEquals(1.0, ((SketchHolder) data.get("string_column_theta")).getEstimate());

        // String column is automatically excluded from dimensions as it is used for sketch aggregation.
        dimensions = ImmutableMap.<String, Object>builder()
                .put("__time", DateTime.parse("2019-10-16T00:02:00Z"))
                .put("string_date_column", "2019-10-16")
                .put("boolean_column", "false")
                .put("long_column", "-1")
                .put("double_column", "-1.0")
                .build();
        data = parsed.get(0, dimensions);
        assertNotNull(data);
        assertEquals(1.0, ((HllSketchHolder) data.get("string_column_hll")).getEstimate());
        assertEquals(1.0, ((SketchHolder) data.get("string_column_theta")).getEstimate());
    }

    @Test
    public void shouldWriteDataSegmentsWithThetaSketchAsInputColumn() throws IOException {
        // string_column_theta is base64 encoded theta sketch.
        String metricsSpec = "[" +
                "{\n" +
                "   \"type\": \"thetaSketch\",\n" +
                "   \"name\": \"string_column_theta\",\n" +
                "   \"fieldName\": \"string_column_theta\",\n" +
                "   \"isInputThetaSketch\": true,\n" +
                "   \"size\": 4096\n" +
                "}]";
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy-MM-dd")
                .load(DruidSourceBaseTest.class.getResource("/data4.csv").getPath())
                .withColumn("date", column("date").cast(DataTypes.TimestampType));
        dataset = DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset, "date", "MONTH", 5000000, false);
        dataset.show(false);

        options.put(SEGMENT_GRANULARITY, "MONTH");
        options.put(QUERY_GRANULARITY, "SECOND");
        options.put(METRICS_SPEC, metricsSpec);
        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-01T00:00:00Z"), DateTime.parse("2019-11-01T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        verifySegmentTable(interval, version, true, 1);
        Table<Integer, ImmutableMap<String, Object>, ImmutableMap<String, Object>> parsed = readSegmentData(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        assertEquals(2, parsed.size());
        assertTrue(parsed.containsRow(0));
        ImmutableMap<String, Object> dimensions = ImmutableMap.<String, Object>builder()
                .put("__time", DateTime.parse("2019-10-16T00:01:00Z"))
                .put("string_date_column", "2019-10-16")
                .put("boolean_column", "true")
                .put("long_column", "10")
                .put("double_column", "100.0")
                .build();
        Map<String, Object> data = parsed.get(0, dimensions);
        assertNotNull(data);
        assertEquals(4.0, ((SketchHolder) data.get("string_column_theta")).getEstimate());

        dimensions = ImmutableMap.<String, Object>builder()
                .put("__time", DateTime.parse("2019-10-16T00:02:00Z"))
                .put("string_date_column", "2019-10-16")
                .put("boolean_column", "false")
                .put("long_column", "-1")
                .put("double_column", "-1.0")
                .build();
        data = parsed.get(0, dimensions);
        assertNotNull(data);
        assertEquals(2.0, ((SketchHolder) data.get("string_column_theta")).getEstimate());
    }
}
