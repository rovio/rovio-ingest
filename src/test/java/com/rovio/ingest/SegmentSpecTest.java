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

import com.rovio.ingest.model.SegmentSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.column.ValueType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SegmentSpecTest {

    @Test
    public void shouldThrowErrorForMissingDataSource() {
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", new StructType(), true, true, null));
    }

    @Test
    public void shouldFailForNoTimeDimension() {
        StructType schema = new StructType()
                .add("country", DataTypes.StringType)
                .add("metric1", DataTypes.LongType);
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, true, null));
    }

    @Test
    public void shouldFailWhenMoreThanOneTimestampColumn() {
        StructType schema = new StructType()
                .add("__time", DataTypes.TimestampType)
                .add("updateTime", DataTypes.TimestampType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("metric1", DataTypes.LongType)
                .add("metric2", DataTypes.DoubleType);
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, true, null));
    }

    @Test
    public void shouldFailForNoMetricsWhenRollupEnabled() {
        StructType schema = new StructType()
                .add("__time", DataTypes.TimestampType)
                .add("country", DataTypes.StringType);
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, true, null));
    }

    @Test
    public void shouldNotFailForNoMetricsWhenRollupDisabled() {
        StructType schema = new StructType()
                .add("__time", DataTypes.TimestampType)
                .add("country", DataTypes.StringType);
        assertDoesNotThrow(() -> SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, false, false, null));
    }

    @Test
    public void shouldFailForNoDimensions() {
        StructType schema = new StructType()
                .add("__time", DataTypes.TimestampType)
                .add("metric", DataTypes.LongType);
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, true, null));
    }

    @Test
    public void shouldFailForUnSupportedDataTypes() {
        StructType schema = new StructType()
                .add("city", DataTypes.CalendarIntervalType);
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, true, null));
    }

    @Test
    public void shouldFailForNestedStructTypes() {
        StructType schema = new StructType()
                .add("complex", new StructType().add("id", DataTypes.StringType));
        assertThrows(IllegalArgumentException.class,
                () -> SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, true, null));
    }

    @Test
    public void shouldCreateDataSchema() {
        StructType schema = new StructType()
                .add("__time", DataTypes.TimestampType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("metric1", DataTypes.LongType)
                .add("metric2", DataTypes.DoubleType);
        String metricsSpec = "[" +
                "{\n" +
                "   \"type\": \"longSum\",\n" +
                "   \"name\": \"metric1\",\n" +
                "   \"fieldName\": \"metric1\",\n" +
                "   \"expression\": null\n" +
                "},\n" +
                "{\n" +
                "   \"type\": \"doubleSum\",\n" +
                "   \"name\": \"metric2\",\n" +
                "   \"fieldName\": \"metric2\",\n" +
                "   \"expression\": null\n" +
                "}\n" +
            "]";
        SegmentSpec spec = SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, true, metricsSpec);

        assertEquals("temp", spec.getDataSchema().getDataSource());
        assertEquals("__time", spec.getTimeColumn());

        assertEquals(2, spec.getDataSchema().getAggregators().length);
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof LongSumAggregatorFactory));
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof DoubleSumAggregatorFactory));

        List<DimensionSchema> dimensions = spec.getDataSchema().getDimensionsSpec().getDimensions();
        assertEquals(2, dimensions.size());
        List<String> expected = Arrays.asList("country", "city");
        assertTrue(dimensions.stream().allMatch(d -> expected.contains(d.getName())));
        assertTrue(dimensions.stream().allMatch(d -> ValueType.STRING == d.getValueType()));

        assertTrue(spec.getDataSchema().getGranularitySpec().isRollup());

        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getSegmentGranularity());
        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getQueryGranularity());
    }

    @Test
    public void shouldExcludeDimensions() {
        StructType schema = new StructType()
                .add("__time", DataTypes.TimestampType)
                .add("updateTime", DataTypes.TimestampType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("metric1", DataTypes.LongType)
                .add("metric2", DataTypes.DoubleType);
        String metricsSpec = "[" +
                "{\n" +
                "   \"type\": \"longSum\",\n" +
                "   \"name\": \"metric1\",\n" +
                "   \"fieldName\": \"metric1\",\n" +
                "   \"expression\": null\n" +
                "},\n" +
                "{\n" +
                "   \"type\": \"doubleSum\",\n" +
                "   \"name\": \"metric2\",\n" +
                "   \"fieldName\": \"metric2\",\n" +
                "   \"expression\": null\n" +
                "}\n" +
                "]";

        SegmentSpec spec = SegmentSpec.from("temp", "__time", Collections.singletonList("updateTime"), "DAY", "DAY", schema, true, true, metricsSpec);

        assertEquals("temp", spec.getDataSchema().getDataSource());
        assertEquals("__time", spec.getTimeColumn());

        assertEquals(2, spec.getDataSchema().getAggregators().length);
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof LongSumAggregatorFactory));
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof DoubleSumAggregatorFactory));

        List<DimensionSchema> dimensions = spec.getDataSchema().getDimensionsSpec().getDimensions();
        assertEquals(2, dimensions.size());
        List<String> expected = Arrays.asList("country", "city");
        assertTrue(dimensions.stream().allMatch(d -> expected.contains(d.getName())));
        assertTrue(dimensions.stream().allMatch(d -> ValueType.STRING == d.getValueType()));

        assertTrue(spec.getDataSchema().getGranularitySpec().isRollup());
        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getSegmentGranularity());
        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getQueryGranularity());
    }

    @Test
    public void shouldSupportTimeColumnSubstitution() {
        StructType schema = new StructType()
                .add("updateTime", DataTypes.TimestampType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("metric1", DataTypes.LongType)
                .add("metric2", DataTypes.DoubleType);
        SegmentSpec spec = SegmentSpec.from("temp", "updateTime",  Collections.emptyList(), "DAY", "DAY", schema, true, true, null);

        assertEquals("temp", spec.getDataSchema().getDataSource());
        assertEquals("updateTime", spec.getTimeColumn());

        assertEquals(2, spec.getDataSchema().getAggregators().length);
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof LongSumAggregatorFactory));
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof DoubleSumAggregatorFactory));

        List<DimensionSchema> dimensions = spec.getDataSchema().getDimensionsSpec().getDimensions();
        assertEquals(2, dimensions.size());
        List<String> expected = Arrays.asList("country", "city");
        assertTrue(dimensions.stream().allMatch(d -> expected.contains(d.getName())));
        assertTrue(dimensions.stream().allMatch(d -> ValueType.STRING == d.getValueType()));

        assertTrue(spec.getDataSchema().getGranularitySpec().isRollup());

        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getSegmentGranularity());
        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getQueryGranularity());
    }

    @Test
    public void shouldNotRollup() {
        StructType schema = new StructType()
                .add("__time", DataTypes.TimestampType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("metric1", DataTypes.LongType)
                .add("metric2", DataTypes.DoubleType);
        SegmentSpec spec = SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, false, null);

        assertEquals("temp", spec.getDataSchema().getDataSource());
        assertEquals("__time", spec.getTimeColumn());

        assertEquals(2, spec.getDataSchema().getAggregators().length);
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof LongSumAggregatorFactory));
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof DoubleSumAggregatorFactory));

        List<DimensionSchema> dimensions = spec.getDataSchema().getDimensionsSpec().getDimensions();
        assertEquals(2, dimensions.size());
        List<String> expected = Arrays.asList("country", "city");
        assertTrue(dimensions.stream().allMatch(d -> expected.contains(d.getName())));
        assertTrue(dimensions.stream().allMatch(d -> ValueType.STRING == d.getValueType()));

        assertFalse(spec.getDataSchema().getGranularitySpec().isRollup());

        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getSegmentGranularity());
        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getQueryGranularity());
    }

    @Test
    public void shouldFailWhenMetricsSpecIsInvalidJson() {
        StructType schema = new StructType()
                .add("__time", DataTypes.TimestampType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("metric1", DataTypes.LongType)
                .add("metric2", DataTypes.DoubleType);
        SegmentSpec spec = SegmentSpec.from("temp", "__time", Collections.emptyList(), "DAY", "DAY", schema, true, true, "{}");
        assertThrows(IllegalArgumentException.class,
                spec::getDataSchema);
    }

    @Test
    public void shouldSupportMetricsSpecAsJson() {
        StructType schema = new StructType()
                .add("updateTime", DataTypes.TimestampType)
                .add("country", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("metric1", DataTypes.LongType)
                .add("metric2", DataTypes.DoubleType);
        String metricsSpec = "[" +
                    "{\n" +
                    "   \"type\": \"longSum\",\n" +
                    "   \"name\": \"metric1\",\n" +
                    "   \"fieldName\": \"metric1\",\n" +
                    "   \"expression\": null\n" +
                    "},\n" +
                    "{\n" +
                    "   \"type\": \"doubleSum\",\n" +
                    "   \"name\": \"metric2\",\n" +
                    "   \"fieldName\": \"metric2\",\n" +
                    "   \"expression\": null\n" +
                    "},\n" +
                    "{\n" +
                    "   \"type\": \"longMax\",\n" +
                    "   \"name\": \"metric1_max\",\n" +
                    "   \"fieldName\": \"metric1\",\n" +
                    "   \"expression\": null\n" +
                    "},\n" +
                    "{\n" +
                    "   \"type\": \"doubleMin\",\n" +
                    "   \"name\": \"metric2_min\",\n" +
                    "   \"fieldName\": \"metric2\",\n" +
                    "   \"expression\": null\n" +
                    "}" +
                "]";
        SegmentSpec spec = SegmentSpec.from("temp", "updateTime",  Collections.emptyList(), "DAY", "DAY", schema, true, true, metricsSpec);

        assertEquals("temp", spec.getDataSchema().getDataSource());
        assertEquals("updateTime", spec.getTimeColumn());

        assertEquals(4, spec.getDataSchema().getAggregators().length);
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof LongSumAggregatorFactory && f.getName().equals("metric1") && Objects.equals(((LongSumAggregatorFactory) f).getFieldName(), "metric1")));
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof DoubleSumAggregatorFactory && f.getName().equals("metric2") && Objects.equals(((DoubleSumAggregatorFactory) f).getFieldName(), "metric2")));
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof LongMaxAggregatorFactory && f.getName().equals("metric1_max") && Objects.equals(((LongMaxAggregatorFactory) f).getFieldName(), "metric1")));
        assertTrue(Arrays.stream(spec.getDataSchema().getAggregators()).anyMatch(f -> f instanceof DoubleMinAggregatorFactory && f.getName().equals("metric2_min") && Objects.equals(((DoubleMinAggregatorFactory) f).getFieldName(), "metric2")));

        List<DimensionSchema> dimensions = spec.getDataSchema().getDimensionsSpec().getDimensions();
        assertEquals(2, dimensions.size());
        List<String> expected = Arrays.asList("country", "city");
        assertTrue(dimensions.stream().allMatch(d -> expected.contains(d.getName())));
        assertTrue(dimensions.stream().allMatch(d -> ValueType.STRING == d.getValueType()));

        assertTrue(spec.getDataSchema().getGranularitySpec().isRollup());

        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getSegmentGranularity());
        assertEquals(Granularity.fromString("DAY"), spec.getDataSchema().getGranularitySpec().getQueryGranularity());
    }
}
