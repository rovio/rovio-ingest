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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._

// This is needed for mvn test. It wouldn't find this test otherwise.
@RunWith(classOf[JUnitRunner])
class DruidDatasetExtensionsAzureSpec extends AnyFlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterEach {

  before {
    DruidDeepStorageAzureTest.AZURE.start()
    DruidSourceBaseTest.MYSQL.start()
    DruidSourceBaseTest.prepareDatabase(DruidSourceBaseTest.MYSQL)
  }

  after {
    DruidSourceBaseTest.MYSQL.stop()
    DruidDeepStorageAzureTest.AZURE.stop()
  }

  // Could instead try assertSmallDataFrameEquality from
  //  https://github.com/MrPowers/spark-fast-tests
  // for now avoid the dependency and collect as an array & use toSet to ignore order
  def assertEqual[T](expected: Dataset[T], actual: Dataset[T]): Assertion =
    actual.collect().toSet should be(expected.collect().toSet)

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("Spark/MLeap Parity Tests")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .master("local[2]")
      .getOrCreate()
  }

  import spark.implicits._

  // import the implicit classes – those provide the functions being tested!
  import com.rovio.ingest.extensions.DruidDatasetExtensions._

  it should "save the dataset to druid on Azure" in {

    DruidSourceBaseTest.setUpDb(DruidSourceBaseTest.MYSQL)

    // create Data source options
    val options: Map[String, String] = DruidSourceBaseTest.getDataSourceOptions(DruidSourceBaseTest.MYSQL).asScala.toMap ++
      DruidDeepStorageAzureTest.getAzureOptions.asScala.toMap

    val ds = Seq(
      // same as content of data.csv
      KpiRow(date = "2019-10-17", country = "US", dau = 50, revenue = 100.0, is_segmented = true),
      KpiRow(date = "2019-10-17", country = "GB", dau = 20, revenue = 20.0, is_segmented = true),
      KpiRow(date = "2019-10-17", country = "DE", dau = 20, revenue = 20.0, is_segmented = true),
      KpiRow(date = "2019-10-16", country = "US", dau = 50, revenue = 100.0, is_segmented = false),
      KpiRow(date = "2019-10-16", country = "FI", dau = 20, revenue = 20.0, is_segmented = false),
      KpiRow(date = "2019-10-16", country = "GB", dau = 20, revenue = 20.0, is_segmented = false),
      KpiRow(date = "2019-10-16", country = "DE", dau = 20, revenue = 20.0, is_segmented = false)
    ).toDS
      .withColumn("date", 'date.cast(DataTypes.TimestampType))

    // note how we can call .repartitionByDruidSegmentSize directly on Dataset[Row]
    // the nice thing is this allows continuous method chaining on Dataset without breaking the chain
    ds.repartitionByDruidSegmentSize("date", rowsPerSegment = 2)
      .write
      .mode(SaveMode.Overwrite)
      .options(options)
      .druid("target-datasource-name-in-druid-on-azure", timeColumn = "date")

  }
}
