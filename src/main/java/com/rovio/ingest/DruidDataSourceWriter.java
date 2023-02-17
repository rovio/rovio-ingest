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
import com.rovio.ingest.util.MetadataUpdater;
import com.rovio.ingest.util.SegmentStorageUpdater;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.timeline.DataSegment;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class DruidDataSourceWriter implements BatchWrite {

    private final WriterContext param;
    private final SegmentSpec segmentSpec;
    private final MetadataUpdater metadataUpdater;

    DruidDataSourceWriter(StructType schema, WriterContext param) {
        this.param = param;
        this.segmentSpec = SegmentSpec.from(param.getDataSource(),param.getTimeColumn(), param.getExcludedDimensions(),
                param.getSegmentGranularity(), param.getQueryGranularity(), schema, param.isRollup(),
                param.getDimensionsSpec(), param.getMetricsSpec(), param.getTransformSpec());
        this.metadataUpdater = new MetadataUpdater(param);
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new TaskWriterFactory(param, segmentSpec);
    }

    @Override
    public final boolean useCommitCoordinator() {
        return true;
    }

    @Override
    public final void onDataWriterCommit(WriterCommitMessage message) {
        // ignored.
    }

    @Override
    public final void commit(WriterCommitMessage[] messages) {
        List<DataSegment> dataSegments = toDataSegments(messages);
        if (metadataUpdater != null) {
            metadataUpdater.publishSegments(dataSegments);
        }
    }

    @Override
    public final void abort(WriterCommitMessage[] messages) {
        List<DataSegment> dataSegments = toDataSegments(messages);
        DataSegmentKiller segmentKiller = SegmentStorageUpdater.createKiller(param);
        dataSegments.forEach(segmentKiller::killQuietly);
    }

    private List<DataSegment> toDataSegments(WriterCommitMessage[] messages) {
        try {
            List<DataSegment> segments = new ArrayList<>();
            for (WriterCommitMessage message : messages) {
                DataSegmentCommitMessage segmentCommitMessage = (DataSegmentCommitMessage) message;
                if (segmentCommitMessage != null) {
                    segments.addAll(segmentCommitMessage.getSegments());
                }
            }
            return segments;
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize data segments", e);
        }
    }

    private static class TaskWriterFactory implements DataWriterFactory {

        private final WriterContext params;
        private final SegmentSpec segmentSpec;

        TaskWriterFactory(WriterContext params, SegmentSpec segmentSpec) {
            this.params = params;
            this.segmentSpec = segmentSpec;
        }

        @Override
        public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
            return new TaskDataWriter(taskId, params, segmentSpec);
        }
    }
}
