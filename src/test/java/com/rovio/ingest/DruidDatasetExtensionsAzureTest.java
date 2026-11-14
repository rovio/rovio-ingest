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

import com.rovio.ingest.extensions.java.DruidDatasetExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static com.rovio.ingest.WriterContext.ConfKeys.DEEP_STORAGE_TYPE;

@Testcontainers
public class DruidDatasetExtensionsAzureTest extends DruidSourceBaseTest {

    @Container
    public static final GenericContainer<?> AZURE = DruidDeepStorageAzureTest.AZURE;

    @Test
    public void savesDatasetToAzureDeepStorage() {
        options.putAll(DruidDeepStorageAzureTest.getAzureOptions());
        options.put(DEEP_STORAGE_TYPE, "azure");

        Dataset<Row> dataset = DruidDatasetExtensions.repartitionByDruidSegmentSize(
                loadCsv(spark, "/data.csv"), "date");

        dataset.write()
                .format(DruidSource.FORMAT)
                .mode(SaveMode.Overwrite)
                .options(options)
                .save();

        Interval interval = new Interval(
                DateTime.parse("2019-10-16T00:00:00Z"),
                DateTime.parse("2019-10-18T00:00:00Z"));
        String version = DateTime.now(ISOChronology.getInstanceUTC()).toString();
        verifySegmentTable(interval, version, true, 2);
    }
}
