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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.kll.KllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchBuildComplexMetricSerde;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchMergeComplexMetricSerde;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;


public class DataSegmentCommitMessage implements WriterCommitMessage {
    private static final long serialVersionUID = 1L;
    public static final ObjectMapper MAPPER = new DefaultObjectMapper();

    static {
        InjectableValues.Std
                injectableValues =
                new InjectableValues.Std()
                        // ExpressionMacroTable is injected in AggregatorFactories.
                        // However, ExprMacro are not actually required as the DataSource is write-only.
                        .addValue(ExprMacroTable.class, ExprMacroTable.nil())
                        .addValue(DefaultColumnFormatConfig.class, new DefaultColumnFormatConfig(null))
                        // PruneLoadSpecHolder are injected in DataSegment.
                        .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);

        MAPPER.setInjectableValues(injectableValues);
        // Register LinearShardSpec as a sub type to be used by the mapper for deserializing DataSegment from json.
        MAPPER.registerSubtypes(new NamedType(LinearShardSpec.class, "linear"));

        MAPPER.setTimeZone(TimeZone.getTimeZone("UTC"));

        new NestedDataModule().getJacksonModules().forEach(MAPPER::registerModule);
        new SketchModule().getJacksonModules().forEach(MAPPER::registerModule);
        new HllSketchModule().getJacksonModules().forEach(MAPPER::registerModule);
        new KllSketchModule().getJacksonModules().forEach(MAPPER::registerModule);
        new DoublesSketchModule().getJacksonModules().forEach(MAPPER::registerModule);
        new ArrayOfDoublesSketchModule().getJacksonModules().forEach(MAPPER::registerModule);

        NestedDataModule.registerHandlersAndSerde();
        HllSketchModule.registerSerde();
        KllSketchModule.registerSerde();
        DoublesSketchModule.registerSerde();
        SketchModule.registerSerde();
        // ArrayOfDoublesSketchModule doesn't expose registerSerde() method, so we have to register it manually.
        ComplexMetrics.registerSerde("arrayOfDoublesSketch", new ArrayOfDoublesSketchMergeComplexMetricSerde());
        ComplexMetrics.registerSerde("arrayOfDoublesSketchMerge", new ArrayOfDoublesSketchMergeComplexMetricSerde());
        ComplexMetrics.registerSerde("arrayOfDoublesSketchBuild", new ArrayOfDoublesSketchBuildComplexMetricSerde());
        ComplexMetrics.registerSerde(NestedDataComplexTypeSerde.TYPE_NAME, NestedDataComplexTypeSerde.INSTANCE);
    }


    private final String json;

    private DataSegmentCommitMessage(String json) {
        this.json = json;
    }

    static DataSegmentCommitMessage getInstance(Collection<DataSegment> segments) throws JsonProcessingException {
        return new DataSegmentCommitMessage(MAPPER.writeValueAsString(segments));
    }


    Collection<DataSegment> getSegments() throws IOException {
        if (json != null) {
            return MAPPER.readValue(json, new TypeReference<List<DataSegment>>() {
            });
        } else {
            return Collections.emptyList();
        }
    }
}
