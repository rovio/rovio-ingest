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
package com.rovio.ingest.extensions.java;

import com.rovio.ingest.extensions.DruidDatasetExtensions.DruidDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DruidDatasetExtensions {

  // this static method is just to make the call from python wrapper as simple as possible
  // (no need to call 'new' in python code)
  public static Dataset<Row> repartitionByDruidSegmentSize(Dataset<Row> dataset,
                                                           String timeColumn,
                                                           String granularityString,
                                                           Integer rowsPerSegment,
                                                           boolean excludeColumnsWithUnknownTypes) {
    return new DruidDataset(dataset)
            .repartitionByDruidSegmentSize(timeColumn, granularityString, rowsPerSegment,
                    excludeColumnsWithUnknownTypes);
  }

}
