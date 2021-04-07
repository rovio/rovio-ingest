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

from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as t


def row(**key_value_pairs):
    """Converts kwargs into a dict. Why use this? Because syntax highlighting for kwargs is nicer than for dicts."""
    return key_value_pairs


def get_df(spark: SparkSession,
           rows: List[Dict],
           schema: t.StructType,
           pad_missing_cols: bool = False) -> DataFrame:
    """
    Creates a DataFrame with the given schema from the given data. Validates that all field names in the dict exist in
    the schema.

    :param spark: SparkSession
    :param rows: data rows
    :param schema: schema for the df
    :param pad_missing_cols: if False, fails if any of the schema fields doesn't exist in any of the row dicts.
    If True, sets a NULL value instead.
    :return: DataFrame
    """

    columns = spark.createDataFrame([], schema).columns

    def validate_and_get_row(row_dict):

        # validate that there are no extra cols
        for key in row_dict:
            if key not in columns:
                raise KeyError('column "' + key + '" is not in schema')

        # create row in the order specified by schema
        if pad_missing_cols:
            return [row_dict.get(col_name, None) for col_name in columns]
        else:
            return [row_dict[col_name] for col_name in columns]

    data = [validate_and_get_row(row_dict) for row_dict in rows]
    return spark.createDataFrame(data, schema)
