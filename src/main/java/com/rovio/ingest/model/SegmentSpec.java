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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;

@SuppressWarnings("Convert2Diamond")
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
    private final String dimensionsSpec;
    private final String metricsSpec;
    private final String transformSpec;
    private final Set<String> complexMetricColumns;
    private final Set<String> complexDimensionColumns;

    private SegmentSpec(String dataSource, String timeColumn, String segmentGranularity, String queryGranularity,
                        List<Field> fields, Field partitionTime, Field partitionNum, boolean rollup,
                        String dimensionsSpec, String metricsSpec, String transformSpec) {
        this.dataSource = dataSource;
        this.timeColumn = timeColumn;
        this.segmentGranularity = segmentGranularity;
        this.queryGranularity = queryGranularity;
        this.fields = fields;
        this.partitionTime = partitionTime;
        this.partitionNum = partitionNum;
        this.rollup = rollup;
        this.dimensionsSpec = dimensionsSpec;
        this.metricsSpec = metricsSpec;
        this.transformSpec = transformSpec;
        this.complexMetricColumns = Arrays
                .stream(getAggregators())
                .filter(aggregatorFactory -> aggregatorFactory.getIntermediateType().is(ValueType.COMPLEX))
                .flatMap((AggregatorFactory aggregatorFactory) -> aggregatorFactory.requiredFields().stream())
                .collect(Collectors.toSet());
        this.complexDimensionColumns = getDimensionsSpec().getDimensions()
                .stream()
                .filter(dimensionSchema -> dimensionSchema.getColumnType() == ColumnType.NESTED_DATA)
                .map(DimensionSchema::getName)
                .collect(Collectors.toSet());
    }

    public static SegmentSpec from(String datasource, String timeColumn, List<String> excludedDimensions,
            String segmentGranularity, String queryGranularity, StructType schema, boolean rollup, String metricsSpec) {
        return from(datasource, timeColumn, excludedDimensions, segmentGranularity, queryGranularity, schema, rollup, null, metricsSpec, null);
    }

    public static SegmentSpec from(String datasource, String timeColumn, List<String> excludedDimensions,
                                   String segmentGranularity, String queryGranularity, StructType schema, boolean rollup,
                                   String dimensionsSpec, String metricsSpec, String transformSpec) {
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
                fields.stream().anyMatch(f -> f.getName().equals(timeColumn) && f.getFieldType() == FieldType.TIMESTAMP),
                String.format("Schema does not have field with name \"%s\" of DateTime type", timeColumn));

        Preconditions.checkArgument(
                fields.stream().noneMatch(f -> f.getFieldType() == FieldType.TIMESTAMP && !f.getName().equals(timeColumn) && !f.getName().equals(PARTITION_TIME_COLUMN_NAME)),
                String.format("Schema has another timestamp field other than \"%s\"", timeColumn));

        Preconditions.checkArgument(fields.stream().anyMatch(f -> f.getFieldType() == FieldType.STRING || f.getFieldType() == FieldType.ARRAY_OF_STRING),
                "Schema has no dimensions");

        Preconditions.checkArgument(!rollup || fields.stream().anyMatch(f -> f.getFieldType() == FieldType.LONG || f.getFieldType() == FieldType.DOUBLE),
                "Schema has rollup enabled but has no metrics");

        if (partitionTime != null) {
            Preconditions.checkArgument(partitionTime.getFieldType() == FieldType.TIMESTAMP,
                    String.format("Field with name \"%s\" should be DateTime type, current type is %s", PARTITION_TIME_COLUMN_NAME, partitionTime.getFieldType().name()));
        }

        if (partitionNum != null) {
            Preconditions.checkArgument(partitionNum.getFieldType() == FieldType.LONG,
                    String.format("Field with name \"%s\" should be long/int type, current type is %s", PARTITION_NUM_COLUMN_NAME, partitionNum.getFieldType().name()));
        }

        return new SegmentSpec(datasource, timeColumn, segmentGranularity, queryGranularity, fields, partitionTime, partitionNum, rollup, dimensionsSpec, metricsSpec, transformSpec);
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
                new TimestampSpec(TIME_DIMENSION, "auto", null),
                getDimensionsSpec(),
                getAggregators(),
                getGranularitySpec(),
                getTransformSpec()
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

    private DimensionsSpec getDimensionsSpec() {
        if (StringUtils.isNotBlank(dimensionsSpec)) {
            try {
                return MAPPER.readValue(dimensionsSpec, new TypeReference<DimensionsSpec>() {});
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(String.format("Failed to deserialize from dimensionsSpec=%s", dimensionsSpec), e);
            }
        }
        else {
            return new DimensionsSpec(getDimensionSchemas());
        }
    }

    private ImmutableList<DimensionSchema> getDimensionSchemas() {
        ImmutableList.Builder<DimensionSchema> builder = ImmutableList.builder();
        AggregatorFactory[] aggregators = getAggregators();
        Set<String> aggregatorFields = new LinkedHashSet<>();

        for (AggregatorFactory aggregator : aggregators) {
            aggregatorFields.addAll(aggregator.requiredFields());
        }

        for (Field field : fields) {
            String fieldName = field.getName();
            if (!getTimeColumn().equals(fieldName) && !aggregatorFields.contains(fieldName)) {
                if (field.getFieldType() == FieldType.STRING) {
                    builder.add(StringDimensionSchema.create(fieldName));
                } else if (field.getFieldType() == FieldType.LONG) {
                    builder.add(new LongDimensionSchema(fieldName));
                } else if (field.getFieldType() == FieldType.DOUBLE) {
                    builder.add(new DoubleDimensionSchema(fieldName));
                } else if (field.getFieldType() == FieldType.TIMESTAMP) {
                    builder.add(new LongDimensionSchema(fieldName));
                } else if (field.getFieldType() == FieldType.ARRAY_OF_STRING) {
                    builder.add(new AutoTypeColumnSchema(fieldName, ColumnType.STRING_ARRAY));
                } else if (field.getFieldType() == FieldType.ARRAY_OF_DOUBLE) {
                    builder.add(new AutoTypeColumnSchema(fieldName,  ColumnType.DOUBLE_ARRAY));
                } else if (field.getFieldType() == FieldType.ARRAY_OF_LONG) {
                    builder.add(new AutoTypeColumnSchema(fieldName,  ColumnType.LONG_ARRAY));
                }
            }
        }
        return builder.build();
    }

    private TransformSpec getTransformSpec() {
        if (StringUtils.isNotBlank(transformSpec)) {
            try {
                return MAPPER.readValue(transformSpec, new TypeReference<TransformSpec>() {});
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(String.format("Failed to deserialize from transformSpec=%s", transformSpec), e);
            }
        }

        return TransformSpec.NONE;
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
        if (StringUtils.isNotBlank(metricsSpec)) {
            try {
                builder.addAll(MAPPER.readValue(metricsSpec, new TypeReference<List<AggregatorFactory>>() {}));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(String.format("Failed to deserialize from metricsSpec=%s", metricsSpec), e);
            }
        } else {
            for (Field field : fields) {
                String fieldName = field.getName();
                FieldType dataFieldType = field.getFieldType();
                if (dataFieldType == FieldType.DOUBLE) {
                    builder.add(new DoubleSumAggregatorFactory(fieldName, fieldName));
                } else if (dataFieldType == FieldType.LONG) {
                    builder.add(new LongSumAggregatorFactory(fieldName, fieldName));
                }
            }
        }

        return builder.build().toArray(new AggregatorFactory[0]);
    }

    public Set<String> getComplexMetricColumns() {
        return complexMetricColumns;
    }

    public Set<String> getComplexDimensionColumns() {
        return complexDimensionColumns;
    }
}
