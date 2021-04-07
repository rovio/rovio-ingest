/**
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
package com.rovio.ingest.extensions

import com.rovio.ingest.DruidSource
import com.rovio.ingest.WriterContext.ConfKeys.{DATA_SOURCE, TIME_COLUMN}
import com.rovio.ingest.util.NormalizeTimeColumnUDF
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{column, unix_timestamp}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, functions}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

/**
 * DataSet[Row] extension to prepare a Dataset for ingestion by [[DruidSource]].
 */
object DruidDatasetExtensions {

  @SerialVersionUID(1L)
  implicit class DruidDataFrameWriter[T](writer: DataFrameWriter[T]) extends Serializable {
    def druid(dataSource: String, timeColumn: String): Unit =
      writer
        .option(DATA_SOURCE, dataSource)
        .option(TIME_COLUMN, timeColumn)
        .format(classOf[DruidSource].getCanonicalName)
        .save()
  }

  /**
   *
   * @param dataset input Dataset
   */
  @SerialVersionUID(1L)
  implicit class DruidDataset(dataset: Dataset[Row]) extends Serializable {
    private val METRIC_TYPES = Array(FloatType, DoubleType, IntegerType, LongType, ShortType, ByteType)
    private val DIMENSION_TYPES = Array(StringType, DateType, TimestampType, BooleanType)
    private val log = LoggerFactory.getLogger(classOf[DruidDataset])

    /**
     * This method performs the following validations:
     * <ul>
     * <li>Type of `time_column` is `Date` or `Timestamp`</li>
     * <li>The dataset has one or more metric columns</li>
     * <li>The dataset has one or more dimension columns</li>
     * <li>The Dataset has no columns with unknown types, unless `excludeColumsWithUnknownTypes` is set to true</li>
     * </ul>
     * <p>
     * The method performs the following transformations:
     * <ul>
     * <li>Drops all columns of complex datatypes such as `StructType`, `MapType` or `ArrayType` as they are not
     * supported by `DruidSource`. This is only done if `excludeColumnsWithUnknownTypes` is set to true,
     * otherwise validation has already failed.</li>
     * <li>Adds a new column `__PARTITION_TIME__` whose value is based on `time_column` column and the given segment
     * granularity (applies [[NormalizeTimeColumnUDF]])</li>
     * <li>Adds a new column `__PARTITION_NUM__` to denote partition shard which is
     * `(row_number() - 1) / rows_per_segment` for each partition</li>
     * <li>Partitions the dataset with columns `__PARTITION_TIME__` & `__PARTITION_NUM__`</li>
     * </ul>
     *
     * @param timeColumn         Time column in the dataset
     * @param granularityString  Segment granularity must be one of
     *                           [[org.apache.druid.java.util.common.granularity.PeriodGranularity]], default is `DAY`.
     * @param rowsPerSegment     Number rows in each Druid segment, 5000000 rows by default.
     * @param excludeColumnsWithUnknownTypes If `false`, validation throws an IllegalArgumentException from fields
     *                                       with unknown data types. If `true`, fields with unknown data types are
     *                                       excluded and a warning is logged. Default is `false`.
     * @return Dataset           A newly partitioned dataset with columns `__PARTITION_TIME__` & `__PARTITION_NUM__`
     *                           added.
     */
    def repartitionByDruidSegmentSize(timeColumn: String,
                                      granularityString: String = "DAY",
                                      rowsPerSegment: Int = 5000000,
                                      excludeColumnsWithUnknownTypes: Boolean = false): Dataset[Row] = {
      assert(rowsPerSegment > 0, "rowsPerSegment must be greater than 0")

      def normalize(instant: Long): Long = NormalizeTimeColumnUDF.normalize(instant, granularityString)

      val normalize_udf = functions.udf(normalize _)
      validate(timeColumn, excludeColumnsWithUnknownTypes)
        .withColumn("__PARTITION_TIME__",
          normalize_udf(unix_timestamp(column(timeColumn))
            .multiply(1000)
            .cast(DataTypes.LongType))
            .divide(1000)
            .cast(DataTypes.TimestampType))
        .withColumn("__num_rows__",
          functions.row_number()
            .over(Window.partitionBy("__PARTITION_TIME__")
              .orderBy("__PARTITION_TIME__")))
        .withColumn("__PARTITION_NUM__",
          ((functions.col("__num_rows__") - 1) / functions.lit(rowsPerSegment))
            .cast(DataTypes.IntegerType))
        .repartition(column("__PARTITION_TIME__"), column("__PARTITION_NUM__"))
        .drop("__num_rows__")
    }

    private def validate(timeColumn: String,
                         excludeColumnsWithUnknownTypes: Boolean): DataFrame = {
      val excludedDimensions = new ListBuffer[StructField]()
      var validatedDataset = dataset
      if (!dataset.dtypes.exists(_._1 == timeColumn)) {
        throw new IllegalArgumentException(timeColumn + " not found in dataset")
      }

      for (field <- dataset.schema.fields) {
        if (DIMENSION_TYPES.contains(field.dataType)) {
          // Extra logic for time_column casting to time_stamp type if isn't by default
          if (field.name != timeColumn && (field.dataType == TimestampType || field.dataType == DateType)) {
            // Druid dataSource expects only one timestamp/date column, cast other timestamp/date columns to String
            validatedDataset = validatedDataset.withColumn(field.name, column(field.name).cast(DataTypes.StringType))
          }
        } else if (!METRIC_TYPES.contains(field.dataType)) {
          excludedDimensions += field
        }
      }

      if (excludedDimensions.nonEmpty) {
        if (excludeColumnsWithUnknownTypes) {
          log.warn(
            f"Dimensions excluded because of unsupported types: ${excludedDimensions.mkString(", ")}")
        } else {
          throw new IllegalArgumentException(
            "Dimensions with unsupported data types, " +
              f"set excludeColumnsWithUnknownTypes to true to exclude: ${excludedDimensions.mkString(", ")}")
        }
      }
      validatedDataset.drop(excludedDimensions.map(_.name): _*)
    }
  }

}
