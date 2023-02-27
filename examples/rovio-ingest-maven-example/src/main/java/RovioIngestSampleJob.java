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
import com.rovio.ingest.DruidSource;
import com.rovio.ingest.WriterContext.ConfKeys;
import com.rovio.ingest.extensions.java.DruidDatasetExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class RovioIngestSampleJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();

        Dataset<Row> dataset = spark.emptyDataFrame();

        Map<String, String> options = new HashMap<>();

        options.put(ConfKeys.DATA_SOURCE, "target-datasource-name-in-druid");
        options.put(ConfKeys.TIME_COLUMN, "date");
        //options.put(ConfKeys.METADATA_DB_TYPE, "mysql");
        options.put(ConfKeys.METADATA_DB_URI, "jdbc:mysql://localhost:3306/druid");
        options.put(ConfKeys.METADATA_DB_USERNAME, "username");
        options.put(ConfKeys.METADATA_DB_PASSWORD, "password");
        options.put(ConfKeys.DEEP_STORAGE_S3_BUCKET, "my-bucket");
        options.put(ConfKeys.DEEP_STORAGE_S3_BASE_KEY, "druid/prod/segments");

        DruidDatasetExtensions
                .repartitionByDruidSegmentSize(dataset,"date", "DAY", 5000000, false)
                .write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();
    }

}
