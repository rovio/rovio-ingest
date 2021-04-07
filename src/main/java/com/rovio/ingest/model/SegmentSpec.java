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
package com.rovio.ingest.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;

public class SegmentSpec implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String PARTITION_TIME_COLUMN_NAME = "__PARTITION_TIME__";
    private static final String PARTITION_NUM_COLUMN_NAME = "__PARTITION_NUM__";
    public static final String TIME_DIMENSION = "__time";

    private final String dataSource;
    private final String timeColumn;
    private final String segmentGranularity;
    private final String queryGranularity;
    private final List<Field> fields;
    private final Field partitionTime;
    private final Field partitionNum;
    private final boolean rollup;

    private SegmentSpec(String dataSource, String timeColumn, String segmentGranularity, String queryGranularity, List<Field> fields, Field partitionTime, Field partitionNum, boolean rollup) {
        this.dataSource = dataSource;
        this.timeColumn = timeColumn;
        this.segmentGranularity = segmentGranularity;
        this.queryGranularity = queryGranularity;
        this.fields = fields;
        this.partitionTime = partitionTime;
        this.partitionNum = partitionNum;
        this.rollup = rollup;
    }

    public static SegmentSpec from(String datasource, String timeColumn, List<String> excludedDimensions,
                                   String segmentGranularity, String queryGranularity, StructType schema, boolean rollup) {
        Preconditions.checkNotNull(datasource);
        Preconditions.checkNotNull(timeColumn);
        Preconditions.checkNotNull(excludedDimensions);
        Preconditions.checkNotNull(segmentGranularity);
        Preconditions.checkNotNull(queryGranularity);
        Preconditions.checkNotNull(schema);

        Field partitionTime = null;
        Field partitionNum = null;
        List<Field> fields = new ArrayList<>(schema.fields().length);
        for (int i = 0; i < schema.fields().length; i++) {
            StructField field = schema.fields()[i];
            if (field.name().equals(PARTITION_TIME_COLUMN_NAME)) {
                partitionTime = Field.from(field, i);
            } else if (field.name().equals(PARTITION_NUM_COLUMN_NAME)) {
                partitionNum = Field.from(field, i);
            } else if (!excludedDimensions.contains(field.name())) {
                fields.add(Field.from(field, i));
            }
        }

        Preconditions.checkArgument(
                fields.stream().anyMatch(f -> f.getName().equals(timeColumn) && f.getAggregatorType() == AggregatorType.TIMESTAMP),
                String.format("Schema does not have field with name \"%s\" of DateTime type", timeColumn));

        Preconditions.checkArgument(
                fields.stream().noneMatch(f -> f.getAggregatorType() == AggregatorType.TIMESTAMP && !f.getName().equals(timeColumn) && !f.getName().equals(PARTITION_TIME_COLUMN_NAME)),
                String.format("Schema has another timestamp field other than \"%s\"", timeColumn));

        Preconditions.checkArgument(fields.stream().anyMatch(f -> f.getAggregatorType() == AggregatorType.STRING),
                "Schema has no dimensions");

        Preconditions.checkArgument(fields.stream().anyMatch(f -> f.getAggregatorType() == AggregatorType.LONG || f.getAggregatorType() == AggregatorType.DOUBLE),
                "Schema has no metrics");

        if (partitionTime != null) {
            Preconditions.checkArgument(partitionTime.getAggregatorType() == AggregatorType.TIMESTAMP,
                    String.format("Field with name \"%s\" should be DateTime type, current type is %s", PARTITION_TIME_COLUMN_NAME, partitionTime.getAggregatorType().name()));
        }

        if (partitionNum != null) {
            Preconditions.checkArgument(partitionNum.getAggregatorType() == AggregatorType.LONG,
                    String.format("Field with name \"%s\" should be long/int type, current type is %s", PARTITION_NUM_COLUMN_NAME, partitionNum.getAggregatorType().name()));
        }

        return new SegmentSpec(datasource, timeColumn, segmentGranularity, queryGranularity, fields, partitionTime, partitionNum, rollup);
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    public List<Field> getFields() {
        return fields;
    }

    public Field getPartitionTime() {
        return partitionTime;
    }

    public Field getPartitionNum() {
        return partitionNum;
    }

    public DataSchema getDataSchema() {
        return new DataSchema(
                dataSource,
                getInputRowParser(),
                getAggregators(),
                getGranularitySpec(),
                null,
                MAPPER
        );
    }

    @Override
    public String toString() {
        return "SegmentSpec{" +
                "dataSource='" + dataSource + '\'' +
                ", timeColumn='" + timeColumn + '\'' +
                ", segmentGranularity='" + segmentGranularity + '\'' +
                ", queryGranularity='" + queryGranularity + '\'' +
                ", fields=" + fields +
                '}';
    }

    private GranularitySpec getGranularitySpec() {
        return new UniformGranularitySpec(
                Granularity.fromString(segmentGranularity),
                Granularity.fromString(queryGranularity),
                rollup,
                null);
    }

    private AggregatorFactory[] getAggregators() {
        ImmutableList.Builder<AggregatorFactory> builder = ImmutableList.builder();
        for (Field field : fields) {
            String fieldName = field.getName();
            AggregatorType dataAggregatorType = field.getAggregatorType();
            if (dataAggregatorType == AggregatorType.DOUBLE) {
                builder.add(new DoubleSumAggregatorFactory(fieldName, fieldName));
            } else if (dataAggregatorType == AggregatorType.LONG) {
                builder.add(new LongSumAggregatorFactory(fieldName, fieldName));
            }
        }

        return builder.build().toArray(new AggregatorFactory[0]);
    }

    private Map<String, Object> getInputRowParser() {
        ImmutableList.Builder<DimensionSchema> builder = ImmutableList.builder();
        for (Field field : fields) {
            String fieldName = field.getName();
            if (field.getAggregatorType() == AggregatorType.STRING) {
                builder.add(StringDimensionSchema.create(fieldName));
            }
        }

        InputRowParser inputRowParser = new MapInputRowParser(new TimeAndDimsParseSpec(
                new TimestampSpec(TIME_DIMENSION, "auto", null),
                new DimensionsSpec(builder.build(), null, null)));
        return MAPPER.convertValue(inputRowParser, new TypeReference<Map<String, Object>>() {
        });
    }
}
