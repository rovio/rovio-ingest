#
# Copyright 2021 Rovio Entertainment Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from datetime import datetime

from py4j.compat import long
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class ConfKeys:
    DATASOURCE_INIT = "druid.datasource.init"
    # Segment config
    DATA_SOURCE = "druid.datasource"
    TIME_COLUMN = "druid.time_column"
    METRICS_SPEC = "druid.metrics_spec"
    SEGMENT_GRANULARITY = "druid.segment_granularity"
    QUERY_GRANULARITY = "druid.query_granularity"
    BITMAP_FACTORY = "druid.bitmap_factory"
    EXCLUDED_DIMENSIONS = "druid.exclude_dimensions"
    SEGMENT_MAX_ROWS = "druid.segment.max_rows"
    SEGMENT_ROLLUP = "druid.segment.rollup"
    # Metadata config
    METADATA_DB_URI = "druid.metastore.db.uri"
    METADATA_DB_USERNAME = "druid.metastore.db.username"
    METADATA_DB_PASSWORD = "druid.metastore.db.password"
    METADATA_DB_TABLE_BASE = "druid.metastore.db.table.base"
    # Deep Storage config
    DEEP_STORAGE_TYPE = "druid.segment_storage.type"
    # S3 config
    DEEP_STORAGE_S3_BUCKET = "druid.segment_storage.s3.bucket"
    DEEP_STORAGE_S3_BASE_KEY = "druid.segment_storage.s3.basekey"
    # Local config (only for testing)
    DEEP_STORAGE_LOCAL_DIRECTORY = "druid.segment_storage.local.dir"


def repartition_by_druid_segment_size(self, time_col_name, segment_granularity='DAY', rows_per_segment=5000000,
                                      exclude_columns_with_unknown_types=False):
    _jdf = self.sql_ctx._sc._jvm.com.rovio.ingest.extensions.java.DruidDatasetExtensions \
        .repartitionByDruidSegmentSize(self._jdf, time_col_name, segment_granularity, rows_per_segment,
                                       exclude_columns_with_unknown_types)
    return DataFrame(_jdf, self.sql_ctx)


def normalize_date(spark: SparkSession, value: datetime, granularity: str) -> datetime:
    instant: long = long(value.timestamp() * 1000)
    normalized: long = spark.sparkContext._jvm.com.rovio.ingest.util \
        .NormalizeTimeColumnUDF.normalize(instant, granularity)
    return datetime.fromtimestamp(normalized / 1000)


def add_dataframe_druid_extension():
    DataFrame.repartition_by_druid_segment_size = repartition_by_druid_segment_size
