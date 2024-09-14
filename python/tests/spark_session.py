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
from pyspark.sql import SparkSession


def get_or_create_spark_session() -> SparkSession:
    """
    Minimize parallelism etc. to speed up execution.
    Test data is small & local, so just avoid all kind of overhead.
    """
    builder = SparkSession.builder \
        .config('spark.sql.shuffle.partitions', 1) \
        .config('spark.default.parallelism', 1) \
        .config('spark.shuffle.compress', False) \
        .config('spark.rdd.compress', False)

    # spark.jars doesn't seem to support classes folders. hence using the two extraClassPath properties.
    # difference is that spark.jars takes a comma-separated list of jars,
    #   while extraClassPath requires file paths with a platform-specific separator
    classpath = _rovio_ingest_classpath()
    return builder \
        .config('spark.driver.extraClassPath', classpath) \
        .config('spark.executor.extraClassPath', classpath) \
        .getOrCreate()


def _rovio_ingest_classpath() -> str:
    """
    Read classpath of the local rovio-ingest project, including the locally compiled classes.

    Note that pyspark 2.4.4 comes with spark jars with specific scala version, for example:

        rovio-ingest/python/venv/lib/python3.9/site-packages/pyspark/jars/spark-core_2.11-2.4.4.jar

    Thus, pyspark is incompatible with scala 2.12 and only works with scala 2.11.

    This means that these pyspark wrappers have not been tested against scala 2.12. However, they may still work as long
    as all jars are with the same scala version.

    Classpath file can be refreshed manually by running `mvn generate-sources`.
    However, that's only needed if making changes to dependencies, and `mvn compile` includes it as well.
    """
    project_root = os.path.join(os.path.dirname(__file__), '..', '..')
    classes_folder = os.path.join(project_root, 'target', 'classes')
    classpath_file = os.path.join(project_root, 'target', 'runtime_classpath.txt')
    assert os.path.exists(classpath_file), 'runtime_classpath.txt is missing. Run mvn compile first to generate it.'
    with open(classpath_file, 'r') as f:
        runtime_classpath = f.read()
    # add target/classes to the beginning of the classpath
    return f'{classes_folder}:{runtime_classpath}'
