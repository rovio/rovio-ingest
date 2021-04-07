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

import os
import pytest
from pyspark.sql import SparkSession
from spark_session import get_or_create_spark_session


def set_env():
    # fix local timezone issues
    os.environ['TZ'] = 'UTC'
    # spark configuration
    os.environ['PYSPARK_PYTHON'] = 'python3'
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'


set_env()


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return get_or_create_spark_session()
