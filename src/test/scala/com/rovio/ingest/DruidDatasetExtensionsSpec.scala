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
package com.rovio.ingest

import org.scalatest._
import com.rovio.ingest.WriterContext.ConfKeys
import com.rovio.ingest.model.DbType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

// must define classes outside of the actual test methods, otherwise spark can't find them
case class KpiRow(date: String, country: String, dau: Integer, revenue: Double, is_segmented: Boolean)
case class RowWithUnsupportedType(date: String, country: String, dau: Integer, labels: Array[String])
case class PartitionedRow(date: String, country: String, dau: Integer, revenue: Double, `__PARTITION_NUM__`: Integer)
case class ExpectedRow(`__PARTITION_TIME__`: String, `__PARTITION_NUM__`: Integer, count: Integer)

// This is needed for mvn test. It wouldn't find this test otherwise.
@RunWith(classOf[JUnitRunner])
class DruidDatasetExtensionsSpec extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterEach {

  before {
    DruidSourceBaseTest.MYSQL.start()
    DruidSourceBaseTest.prepareDatabase()
  }

  after {
    DruidSourceBaseTest.MYSQL.stop()
  }

  // Could instead try assertSmallDataFrameEquality from
  //  https://github.com/MrPowers/spark-fast-tests
  // for now avoid the dependency and collect as an array & use toSet to ignore order
  def assertEqual[T](expected: Dataset[T], actual: Dataset[T]): Assertion =
    actual.collect().toSet should be (expected.collect().toSet)

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("Spark/MLeap Parity Tests")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[2]")
      .getOrCreate()
  }

  // import Seq().toDS
  import spark.implicits._

  // import the implicit classes – those provide the functions being tested!
  import com.rovio.ingest.extensions.DruidDatasetExtensions._

  "repartitionByDruidSegmentSize" should "prepare dataset for ingestion with DAY segment granularity" in {

    val result = Seq(
      // same as content of data.csv
      KpiRow(date="2019-10-17 00:00:00", country="US", dau=50, revenue=100.0, is_segmented=true),
      KpiRow(date="2019-10-17 00:01:00", country="GB", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-17 01:00:00", country="DE", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:00:00", country="US", dau=50, revenue=100.0, is_segmented=false),
      KpiRow(date="2019-10-16 00:01:00", country="FI", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 01:00:00", country="GB", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 01:01:00", country="DE", dau=20, revenue=20.0, is_segmented=false)
    ).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))
      // note how we can call .repartitionByDruidSegmentSize directly on Dataset[Row]
      // the nice thing is this allows continuous method chaining on Dataset without breaking the chain
      .repartitionByDruidSegmentSize("date", "DAY", 2)
      // group & count
      // because we can't know which exact rows end up in each partition within the same date
      // however we know how many partitions there should be for each date
      .groupBy('__PARTITION_TIME__, '__PARTITION_NUM__).count()

    val expected = Seq(
      ExpectedRow(`__PARTITION_TIME__`="2019-10-17 00:00:00", `__PARTITION_NUM__`=0, count=2),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:00", `__PARTITION_NUM__`=1, count=2),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-17 00:00:00", `__PARTITION_NUM__`=1, count=1),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:00", `__PARTITION_NUM__`=0, count=2)
    ).toDS
      .withColumn("__PARTITION_TIME__", '__PARTITION_TIME__.cast(DataTypes.TimestampType))

    assertEqual(expected, result)
  }

  it should "prepare dataset for ingestion with WEEK segment granularity" in {

    val result = Seq(
      // same as content of data.csv
      KpiRow(date="2019-10-17 00:00:00", country="US", dau=50, revenue=100.0, is_segmented=true),
      KpiRow(date="2019-10-17 00:01:00", country="GB", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-17 01:00:00", country="DE", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:00:00", country="US", dau=50, revenue=100.0, is_segmented=false),
      KpiRow(date="2019-10-16 00:01:00", country="FI", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 01:00:00", country="GB", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 01:01:00", country="DE", dau=20, revenue=20.0, is_segmented=false)
    ).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))
      // note how we can call .repartitionByDruidSegmentSize directly on Dataset[Row]
      // the nice thing is this allows continuous method chaining on Dataset without breaking the chain
      .repartitionByDruidSegmentSize("date", "WEEK", 4)
      // group & count
      // because we can't know which exact rows end up in each partition within the same date
      // however we know how many partitions there should be for each date
      .groupBy('__PARTITION_TIME__, '__PARTITION_NUM__).count()

    val expected = Seq(
      ExpectedRow(`__PARTITION_TIME__`="2019-10-14 00:00:00", `__PARTITION_NUM__`=0, count=4),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-14 00:00:00", `__PARTITION_NUM__`=1, count=3)
    ).toDS
      .withColumn("__PARTITION_TIME__", '__PARTITION_TIME__.cast(DataTypes.TimestampType))

    assertEqual(expected, result)
  }

  it should "prepare dataset for ingestion with HOUR segment granularity" in {

    val result = Seq(
      // same as content of data.csv
      KpiRow(date="2019-10-17 00:00:00", country="US", dau=50, revenue=100.0, is_segmented=true),
      KpiRow(date="2019-10-17 00:01:00", country="GB", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-17 01:00:00", country="DE", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:00:00", country="US", dau=50, revenue=100.0, is_segmented=false),
      KpiRow(date="2019-10-16 00:01:00", country="FI", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 01:00:00", country="GB", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 01:01:00", country="DE", dau=20, revenue=20.0, is_segmented=false)
    ).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))
      // note how we can call .repartitionByDruidSegmentSize directly on Dataset[Row]
      // the nice thing is this allows continuous method chaining on Dataset without breaking the chain
      .repartitionByDruidSegmentSize("date", "HOUR", 2)
      // group & count
      // because we can't know which exact rows end up in each partition within the same date
      // however we know how many partitions there should be for each date
      .groupBy('__PARTITION_TIME__, '__PARTITION_NUM__).count()

    val expected = Seq(
      ExpectedRow(`__PARTITION_TIME__`="2019-10-17 00:00:00", `__PARTITION_NUM__`=0, count=2),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:00", `__PARTITION_NUM__`=0, count=2),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-17 01:00:00", `__PARTITION_NUM__`=0, count=1),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 01:00:00", `__PARTITION_NUM__`=0, count=2)
    ).toDS
      .withColumn("__PARTITION_TIME__", '__PARTITION_TIME__.cast(DataTypes.TimestampType))

    assertEqual(expected, result)
  }

  it should "prepare dataset for ingestion with MINUTE segment granularity" in {

    val result = Seq(
      KpiRow(date="2019-10-16 00:00:00", country="US", dau=50, revenue=100.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:01:00", country="GB", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:00:00", country="DE", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:01:00", country="US", dau=50, revenue=100.0, is_segmented=false),
      KpiRow(date="2019-10-16 00:00:00", country="FI", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 01:00:00", country="GB", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 01:01:00", country="DE", dau=20, revenue=20.0, is_segmented=false)
    ).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))
      .repartitionByDruidSegmentSize("date", "MINUTE", 2)
      // group & count
      // because we can't know which exact rows end up in each partition within the same date
      // however we know how many partitions there should be for each date
      .groupBy('__PARTITION_TIME__, '__PARTITION_NUM__).count()

    val expected = Seq(
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:00", `__PARTITION_NUM__`=0, count=2),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:00", `__PARTITION_NUM__`=1, count=1),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:01:00", `__PARTITION_NUM__`=0, count=2),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 01:00:00", `__PARTITION_NUM__`=0, count=1),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 01:01:00", `__PARTITION_NUM__`=0, count=1)
    ).toDS
      .withColumn("__PARTITION_TIME__", '__PARTITION_TIME__.cast(DataTypes.TimestampType))

    assertEqual(expected, result)
  }

  it should "prepare dataset for ingestion with SECOND segment granularity" in {

    val result = Seq(
      KpiRow(date="2019-10-16 00:00:00", country="US", dau=50, revenue=100.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:00:00", country="GB", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:00:00", country="DE", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-16 00:00:01", country="US", dau=50, revenue=100.0, is_segmented=false),
      KpiRow(date="2019-10-16 00:00:01", country="FI", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 00:00:02", country="GB", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16 00:00:03", country="DE", dau=20, revenue=20.0, is_segmented=false)
    ).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))
      .repartitionByDruidSegmentSize("date", "SECOND", 2)
      // group & count
      // because we can't know which exact rows end up in each partition within the same date
      // however we know how many partitions there should be for each date
      .groupBy('__PARTITION_TIME__, '__PARTITION_NUM__).count()

    val expected = Seq(
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:00", `__PARTITION_NUM__`=0, count=2),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:00", `__PARTITION_NUM__`=1, count=1),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:01", `__PARTITION_NUM__`=0, count=2),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:02", `__PARTITION_NUM__`=0, count=1),
      ExpectedRow(`__PARTITION_TIME__`="2019-10-16 00:00:03", `__PARTITION_NUM__`=0, count=1)
    ).toDS
      .withColumn("__PARTITION_TIME__", '__PARTITION_TIME__.cast(DataTypes.TimestampType))

    assertEqual(expected, result)
  }

  it should "exclude columns with unsupported types" in {
    val ds = Seq(RowWithUnsupportedType(
      date="2019-10-17", country="US", dau=50, labels=Array("A", "B"))).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))
    val result = ds.repartitionByDruidSegmentSize("date", "DAY", 2, excludeColumnsWithUnknownTypes = true)

    result.schema should be (StructType(Seq(
      StructField("date", TimestampType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("dau", IntegerType, nullable = true),
      StructField("__PARTITION_TIME__", TimestampType, nullable = true),
      StructField("__PARTITION_NUM__", IntegerType, nullable = true))
      // the Array type col "labels" expected to have been dropped
    ))
  }

  it should "throw exception from unsupported types by default" in {
    val ds = Seq(RowWithUnsupportedType(
      date="2019-10-17", country="US", dau=50, labels=Array("A", "B"))).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))
    assertThrows[IllegalArgumentException] {
      ds.repartitionByDruidSegmentSize("date", "DAY", 2)
    }
  }

  it should "save the dataset to druid" in {

    DruidSourceBaseTest.setUpDb()

    // create Data source options
    val options = Map[String, String](
      ConfKeys.DEEP_STORAGE_LOCAL_DIRECTORY -> "/tmp/local_segments",
      ConfKeys.METADATA_DB_TYPE -> DbType.Mysql.name(),
      ConfKeys.METADATA_DB_URI -> DruidSourceBaseTest.connectionString,
      ConfKeys.METADATA_DB_USERNAME -> DruidSourceBaseTest.dbUser,
      ConfKeys.METADATA_DB_PASSWORD -> DruidSourceBaseTest.dbPass,
      ConfKeys.DEEP_STORAGE_TYPE -> "local",

      // note: these are ignored when DEEP_STORAGE_TYPE = local
      ConfKeys.DEEP_STORAGE_S3_BUCKET-> "my-bucket",
      ConfKeys.DEEP_STORAGE_S3_BASE_KEY -> "druid/prod/segments"
    )

    val ds = Seq(
      // same as content of data.csv
      KpiRow(date="2019-10-17", country="US", dau=50, revenue=100.0, is_segmented=true),
      KpiRow(date="2019-10-17", country="GB", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-17", country="DE", dau=20, revenue=20.0, is_segmented=true),
      KpiRow(date="2019-10-16", country="US", dau=50, revenue=100.0, is_segmented=false),
      KpiRow(date="2019-10-16", country="FI", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16", country="GB", dau=20, revenue=20.0, is_segmented=false),
      KpiRow(date="2019-10-16", country="DE", dau=20, revenue=20.0, is_segmented=false)
    ).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))

    // note how we can call .repartitionByDruidSegmentSize directly on Dataset[Row]
    // the nice thing is this allows continuous method chaining on Dataset without breaking the chain
    ds.repartitionByDruidSegmentSize("date", rowsPerSegment=2)
      .write
      .mode(SaveMode.Overwrite)
      .options(options)
      .druid("target-datasource-name-in-druid", timeColumn="date")

  }

}
