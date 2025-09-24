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

import com.google.common.collect.Lists;
import org.apache.druid.timeline.SegmentId;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import static com.rovio.ingest.WriterContext.ConfKeys.DATASOURCE_INIT;
import static com.rovio.ingest.WriterContext.ConfKeys.EXCLUDED_DIMENSIONS;
import static com.rovio.ingest.WriterContext.ConfKeys.PARTITION_NUM_START;
import static com.rovio.ingest.WriterContext.ConfKeys.SEGMENT_GRANULARITY;
import static com.rovio.ingest.WriterContext.ConfKeys.SEGMENT_MAX_ROWS;
import static com.rovio.ingest.WriterContext.ConfKeys.SEGMENT_VERSION;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DruidSourceTest extends DruidSourceBaseTest {

    @Test
    public void failWithoutDatasourceOption() {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> dataset.write().format("com.rovio.ingest.DruidSource").mode(SaveMode.Append).save());
        assertThat(thrown.getMessage(), containsString("Missing mandatory \"druid.datasource\" option"));
    }

    @Test
    public void failForInitDatasourceWithAppendWriteMode() {
        options.put(DATASOURCE_INIT, "true");
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> loadCsv(spark, "/data.csv")
                        .write()
                        .format("com.rovio.ingest.DruidSource")
                        .mode(SaveMode.Append)
                        .options(options)
                        .save());

        assertThat(thrown.getMessage(), containsString("Init database with Append write mode is not supported"));
    }

    @Test
    public void failForInvalidVersionOption() {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        options.put(SEGMENT_VERSION, "invalid-version-format");
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> dataset.write().format("com.rovio.ingest.DruidSource").mode(SaveMode.Overwrite).options(options).save());
        assertThat(thrown.getMessage(),
                containsString("\"druid.segment_version\" should be timestamp in ISO8601 format (yyyy-MM-ddTHH:mm:ss.SSSZZ)"));
    }

    @Test
    public void failForInvalidPartitionNumStartOption() {
        Executable runJob = () -> loadCsv(spark, "/data.csv")
                .write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        options.put(PARTITION_NUM_START, "-1");
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, runJob);
        assertThat(thrown.getMessage(), containsString("\"druid.partition_num_start\" should be >= 0"));
    }

    @Test
    public void failForNullTimestamp() {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv")
                .drop("date")
                .withColumn("date", lit(null).cast(DataTypes.TimestampType))
                .repartition(column("country"));
        dataset.show(false);
        SparkException thrown = assertThrows(SparkException.class,
                () -> dataset.write()
                        .format("com.rovio.ingest.DruidSource")
                        .mode(SaveMode.Overwrite)
                        .options(options)
                        .save());
        assertThat(thrown.getCause().getMessage(), containsString(
                "Null value for column 'date'."));
    }

    @Test
    public void passWhenPartitionedByTime() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = dataset.repartition(column("date"));
        dataset.show(false);

        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, false);
        List<SegmentId> segmentIds = verifySegmentTable(interval, version, true, 2);
        Assertions.assertEquals("2019-10-16T00:00:00.000Z", segmentIds.get(0).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(0).getIntervalEnd().toString());
        Assertions.assertEquals(0, segmentIds.get(0).getPartitionNum());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(1).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-18T00:00:00.000Z", segmentIds.get(1).getIntervalEnd().toString());
        Assertions.assertEquals(0, segmentIds.get(1).getPartitionNum());

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);

        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, false);
        segmentIds = verifySegmentTable(interval, version, true, 2);
        Assertions.assertEquals("2019-10-16T00:00:00.000Z", segmentIds.get(0).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(0).getIntervalEnd().toString());
        Assertions.assertEquals(0, segmentIds.get(0).getPartitionNum());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(1).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-18T00:00:00.000Z", segmentIds.get(1).getIntervalEnd().toString());
        Assertions.assertEquals(0, segmentIds.get(1).getPartitionNum());
    }

    @Test
    public void passWhenPartitionedByTimeWithAppendWriteMode() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = dataset.repartition(column("date"));
        dataset.show(false);

        String version = DateTime.now(ISOChronology.getInstanceUTC()).plusMinutes(10).toString();
        options.put(SEGMENT_VERSION, version);

        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, false);
        List<SegmentId> segmentIds = verifySegmentTable(interval, version, true, 2);
        Assertions.assertEquals(version, segmentIds.get(0).getVersion());
        Assertions.assertEquals("2019-10-16T00:00:00.000Z", segmentIds.get(0).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(0).getIntervalEnd().toString());
        Assertions.assertEquals(0, segmentIds.get(0).getPartitionNum());
        Assertions.assertEquals(version, segmentIds.get(1).getVersion());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(1).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-18T00:00:00.000Z", segmentIds.get(1).getIntervalEnd().toString());
        Assertions.assertEquals(0, segmentIds.get(1).getPartitionNum());

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);

        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 2, false);
        segmentIds = verifySegmentTable(interval, version, true, 4);
        Assertions.assertEquals(0, segmentIds.get(0).getPartitionNum());
        Assertions.assertEquals(version, segmentIds.get(1).getVersion());
        Assertions.assertEquals("2019-10-16T00:00:00.000Z", segmentIds.get(1).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(1).getIntervalEnd().toString());
        Assertions.assertEquals(1, segmentIds.get(1).getPartitionNum());
        Assertions.assertEquals(0, segmentIds.get(2).getPartitionNum());
        Assertions.assertEquals(version, segmentIds.get(3).getVersion());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(3).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-18T00:00:00.000Z", segmentIds.get(3).getIntervalEnd().toString());
        Assertions.assertEquals(1, segmentIds.get(3).getPartitionNum());
    }

    @Test
    public void passWhenPartitionedByTimeWithAppendWriteModeAndPartitionNumStart() throws IOException {
        options.put(PARTITION_NUM_START, "1000");

        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        dataset = dataset.repartition(column("date"));
        dataset.show(false);

        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, false);
        List<SegmentId> segmentIds = verifySegmentTable(interval, version, true, 2);
        Assertions.assertEquals("2019-10-16T00:00:00.000Z", segmentIds.get(0).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(0).getIntervalEnd().toString());
        Assertions.assertEquals(1000, segmentIds.get(0).getPartitionNum());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(1).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-18T00:00:00.000Z", segmentIds.get(1).getIntervalEnd().toString());
        Assertions.assertEquals(1000, segmentIds.get(1).getPartitionNum());

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);

        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 2, false);
        segmentIds = verifySegmentTable(interval, version, true, 4);
        Assertions.assertEquals(1000, segmentIds.get(0).getPartitionNum());
        Assertions.assertEquals("2019-10-16T00:00:00.000Z", segmentIds.get(1).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(1).getIntervalEnd().toString());
        Assertions.assertEquals(1001, segmentIds.get(1).getPartitionNum());
        Assertions.assertEquals(1000, segmentIds.get(2).getPartitionNum());
        Assertions.assertEquals("2019-10-17T00:00:00.000Z", segmentIds.get(3).getIntervalStart().toString());
        Assertions.assertEquals("2019-10-18T00:00:00.000Z", segmentIds.get(3).getIntervalEnd().toString());
        Assertions.assertEquals(1001, segmentIds.get(3).getPartitionNum());
    }

    @Test
    public void passWhenPartitionedByDate() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv")
                // Convert TimestampType -> DateType
                .withColumn("date", column("date").cast(DataTypes.DateType));
        dataset = dataset.repartition(column("date"));
        dataset.show(false);

        String version = DateTime.now(ISOChronology.getInstanceUTC()).plusMinutes(15).toString();
        options.put(SEGMENT_VERSION, version);

        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, false);
        verifySegmentTable(interval, version, true, 2);
    }

    @Test
    public void passWhenPartitionedByTransitiveTimeDimension() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        List<Column> columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("DAY"));
        dataset = dataset.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        dataset = dataset.repartition(column("__partition"));
        dataset.show(false);

        options.put(EXCLUDED_DIMENSIONS, "__partition");
        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, false);
        verifySegmentTable(interval, version, true, 2);
    }

    @Test
    public void passForMonthGranularity() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        List<Column> columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("MONTH"));
        dataset = dataset.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        dataset = dataset.repartition(column("__partition"));
        dataset.show(false);

        options.put(EXCLUDED_DIMENSIONS, "__partition");
        options.put(SEGMENT_GRANULARITY, "MONTH");
        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-01T00:00:00Z"), DateTime.parse("2019-11-01T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 1, true);
        verifySegmentTable(interval, version, true, 1);
    }

    @Test
    public void passForDayGranularityWithMaxRows() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        List<Column> columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("DAY"));
        dataset = dataset.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        dataset = dataset.repartition(column("__partition"));
        dataset.show(false);

        options.put(EXCLUDED_DIMENSIONS, "__partition");
        options.put(SEGMENT_MAX_ROWS, String.valueOf(1));
        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 4, false);
        verifySegmentTable(interval, version, true, 8);
    }

    @Test
    public void passForDayGranularityWithPartitionTimeAndNum() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        List<Column> columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("DAY"));
        dataset = dataset.withColumn("__PARTITION_TIME__",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq())
                        .divide(1000)
                        .cast(DataTypes.TimestampType));

        dataset = dataset.withColumn("__PARTITION_NUM__", functions.row_number()
                .over(Window.partitionBy("__PARTITION_TIME__").orderBy("__PARTITION_TIME__")).mod(2));
        dataset = dataset.repartition(column("__PARTITION_TIME__"), column("__PARTITION_NUM__"));
        dataset.show(false);

        options.put(SEGMENT_MAX_ROWS, String.valueOf(1));
        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, version, 2, false);
        verifySegmentTable(interval, version, true, 4);
    }

    @Test
    public void passForPartiallyOverWrittenSegments() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        List<Column> columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("DAY"));
        dataset = dataset.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        dataset = dataset.repartition(column("__partition"));
        dataset.show(false);

        options.put(EXCLUDED_DIMENSIONS, "__partition");
        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String firstVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, firstVersion, 1, false);
        verifySegmentTable(interval, firstVersion, true, 2);

        Dataset<Row> dataset2 = loadCsv(spark, "/data2.csv");
        dataset2.show(false);
        columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("DAY"));
        dataset2 = dataset2.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        dataset2 = dataset2.repartition(dataset2.col("__partition"));
        dataset2.show(false);

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);
        dataset2.write()
                .format("com.rovio.ingest.DruidSource")
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
    public void shouldSaveWhenIncrementalAndGranularityChanges() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        List<Column> columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("DAY"));
        dataset = dataset.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        dataset = dataset.repartition(column("__partition"));
        dataset.show(false);

        options.put(EXCLUDED_DIMENSIONS, "__partition");
        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String firstVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, firstVersion, 1, false);
        verifySegmentTable(interval, firstVersion, true, 2);

        dataset = loadCsv(spark, "/data2.csv");
        dataset.show(false);
        columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("MONTH"));
        dataset = dataset.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        Dataset<Row> dataset2 = dataset.repartition(dataset.col("__partition"));
        dataset2.show(false);

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);
        options.put(SEGMENT_GRANULARITY, "MONTH");
        dataset2.write()
                .format("com.rovio.ingest.DruidSource")
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
    public void shouldOverwriteWhenGranularityChangesWithInit() throws IOException {
        Dataset<Row> dataset = loadCsv(spark, "/data.csv");
        List<Column> columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("DAY"));
        dataset = dataset.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        dataset = dataset.repartition(column("__partition"));
        dataset.show(false);

        options.put(EXCLUDED_DIMENSIONS, "__partition");
        dataset.write()
                .format("com.rovio.ingest.DruidSource")
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(DateTime.parse("2019-10-16T00:00:00Z"), DateTime.parse("2019-10-18T00:00:00Z"));
        String firstVersion = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentPath(Paths.get(testFolder.toString(), DATA_SOURCE), interval, firstVersion, 1, false);
        verifySegmentTable(interval, firstVersion, true, 2);

        Dataset<Row> dataset2 = loadCsv(spark, "/data2.csv");
        dataset2.show(false);
        columns = Lists.newArrayList(unix_timestamp(column("date")).multiply(1000), lit("MONTH"));
        dataset2 = dataset2.withColumn("__partition",
                callUDF("normalizeTimeColumn",
                        JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq()));

        dataset2 = dataset2.repartition(dataset2.col("__partition"));
        dataset2.show(false);

        DateTimeUtils.setCurrentMillisFixed(VERSION_TIME_MILLIS + 60_000);
        options.put(SEGMENT_GRANULARITY, "MONTH");
        options.put(DATASOURCE_INIT, "true");

        dataset2.write()
                .format("com.rovio.ingest.DruidSource")
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
}
