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

from assertions import assert_df
from rovio_ingest.extensions.dataframe_extension import add_dataframe_druid_extension, normalize_date
import pyspark.sql.types as t
from pyspark.sql import DataFrame
from test_helper import row, get_df


def to_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d')


def test_repartition_by_druid_segment_size(spark):
    add_dataframe_druid_extension()

    schema = t.StructType([
        t.StructField('date', t.DateType()),
        t.StructField('country', t.StringType()),
        t.StructField('dau', t.IntegerType()),
        t.StructField('revenue', t.DoubleType()),
    ])

    rows = [
        row(date=to_date("2019-10-17"), country="US", dau=50, revenue=100.0),
        row(date=to_date("2019-10-17"), country="GB", dau=20, revenue=20.0),
        row(date=to_date("2019-10-17"), country="DE", dau=20, revenue=20.0),
        row(date=to_date("2019-10-16"), country="US", dau=50, revenue=100.0),
        row(date=to_date("2019-10-16"), country="FI", dau=20, revenue=20.0),
        row(date=to_date("2019-10-16"), country="GB", dau=20, revenue=20.0),
        row(date=to_date("2019-10-16"), country="DE", dau=20, revenue=20.0)
    ]

    df: DataFrame = get_df(spark, rows, schema)

    # note how we can call .repartitionByDruidSegmentSize directly on Dataset[Row]
    # the nice thing is this allows continuous method chaining on Dataset without braking the chain
    df = df.repartition_by_druid_segment_size('date', segment_granularity='DAY', rows_per_segment=2)

    # group & count
    # because we can't know which exact rows end up in each partition within the same date
    # however we know how many partitions there should be for each date
    df = df.groupBy('__PARTITION_TIME__', '__PARTITION_NUM__').count()

    expected: DataFrame = get_df(spark, [
        row(__PARTITION_TIME__=to_date("2019-10-17"), __PARTITION_NUM__=0, count=2),
        row(__PARTITION_TIME__=to_date("2019-10-16"), __PARTITION_NUM__=1, count=2),
        row(__PARTITION_TIME__=to_date("2019-10-17"), __PARTITION_NUM__=1, count=1),
        row(__PARTITION_TIME__=to_date("2019-10-16"), __PARTITION_NUM__=0, count=2),
    ], t.StructType([
        t.StructField('__PARTITION_TIME__', t.TimestampType()),
        t.StructField('__PARTITION_NUM__', t.IntegerType()),
        t.StructField('count', t.LongType()),
    ]))

    assert_df(df, expected)


def test_normalize_date(spark):
    value = datetime(2020, 5, 6, 19, 46, 00)
    # WEEK: so 6th (Wednesday) -> 4th (Monday)
    assert normalize_date(spark, value, 'WEEK') == datetime(2020, 5, 4)
