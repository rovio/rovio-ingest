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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.Set;

public class DruidTable implements SupportsWrite {
  private final WriterContext param;

  public DruidTable(Map<String, String> map) {
    this.param = WriterContext.from(new CaseInsensitiveStringMap(map), DateTimes.nowUtc().toString());
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    return new DruidDataSourceWriterBuilder(logicalWriteInfo.schema(), param);
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public StructType schema() {
    // Return empty StructType as this value is not really used, because library doesn't support reading from Druid.
    // However, it cannot return null as Spark fails with NPE.
    return new StructType();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(
            // Druid allows different schemas for segments and combines them.
            TableCapability.ACCEPT_ANY_SCHEMA,
            TableCapability.BATCH_WRITE,
            TableCapability.TRUNCATE);
  }
}
