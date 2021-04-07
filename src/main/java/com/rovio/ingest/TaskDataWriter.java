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

import com.rovio.ingest.model.Field;
import com.rovio.ingest.model.SegmentSpec;
import com.rovio.ingest.util.SegmentStorageUpdater;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.segment.realtime.appenderator.SegmentNotWritableException;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.segment.realtime.plumber.Committers;
import org.apache.druid.segment.realtime.plumber.CustomVersioningPolicy;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;

class TaskDataWriter implements DataWriter<InternalRow> {
    private static final IndexIO INDEX_IO = new IndexIO(MAPPER, () -> 0);
    private static final IndexMerger INDEX_MERGER_V_9 = new IndexMergerV9(MAPPER, INDEX_IO, TmpFileSegmentWriteOutMediumFactory.instance());
    private static final Logger LOG = LoggerFactory.getLogger(TaskDataWriter.class);

    private final long taskId;
    private final SegmentSpec segmentSpec;
    private final int maxRows;
    private final DataSchema dataSchema;
    private final RealtimeTuningConfig tuningConfig;
    private final Appenderator appenderator;
    private final DataSegmentKiller segmentKiller;
    private final Supplier<Committer> committerSupplier;
    private final Set<DataSegment> pushedSegments;

    private SegmentIdentifier current;

    TaskDataWriter(long taskId, WriterContext context, SegmentSpec segmentSpec) {
        this.taskId = taskId;
        this.segmentSpec = segmentSpec;
        this.maxRows = context.getSegmentMaxRows();
        this.dataSchema = segmentSpec.getDataSchema();
        this.tuningConfig = getTuningConfig(context);
        DataSegmentPusher segmentPusher = SegmentStorageUpdater.createPusher(context);
        File basePersistDirectory = new File(tuningConfig.getBasePersistDirectory(), UUID.randomUUID().toString());
        this.segmentKiller = SegmentStorageUpdater.createKiller(context);
        this.appenderator = Appenderators.createOffline(
                dataSchema,
                tuningConfig.withBasePersistDirectory(basePersistDirectory),
                new FireDepartmentMetrics(),
                segmentPusher,
                MAPPER,
                INDEX_IO,
                INDEX_MERGER_V_9);
        this.committerSupplier = Committers::nil;
        this.pushedSegments = new HashSet<>();
        this.appenderator.startJob();
    }

    @Override
    public void write(InternalRow record) throws IOException {
        try {
            Map<String, Object> map = parse(record);
            // Adjust as spark returns long value with milliseconds.
            long timestamp = (long) map.get(SegmentSpec.TIME_DIMENSION) / 1000;
            InputRow inputRow = new MapBasedInputRow(timestamp,
                    dataSchema.getParser().getParseSpec().getDimensionsSpec().getDimensionNames(), map);

            long bucketStartEpoch = segmentSpec.getPartitionTime() != null
                    // Adjust as spark returns long value with milliseconds.
                    ? record.getLong(segmentSpec.getPartitionTime().getOrdinal()) / 1000
                    : timestamp;
            DateTime bucketStart = dataSchema.getGranularitySpec().getSegmentGranularity().bucketStart(DateTimes.utc(bucketStartEpoch));
            final Interval interval = new Interval(bucketStart, dataSchema.getGranularitySpec().getSegmentGranularity().increment(bucketStart));

            if (segmentSpec.getPartitionTime() != null && segmentSpec.getPartitionNum() != null) {
                int partitionNum = record.getInt(segmentSpec.getPartitionNum().getOrdinal());
                current = newSegmentIdentifier(interval, partitionNum);
            } else {
                if (current != null && !current.getInterval().equals(interval)) {
                    // Check if any open segment has the same interval.
                    LOG.debug("TaskId={}, currentId={}, new segment interval = {}", taskId, current, interval);
                    current = appenderator.getSegments()
                            .stream()
                            .filter(s -> s.getInterval().equals(interval))
                            .findAny()
                            .orElse(null);
                }

                if (current == null) {
                    // New interval.
                    current = newSegmentIdentifier(interval, 0);
                }

                if (appenderator.getSegments().contains(current) && appenderator.getRowCount(current) >= maxRows) {
                    LOG.debug("TaskId={}, New segment interval = {} has {} rows", taskId, interval, appenderator.getRowCount(current));
                    pushSegments(Collections.singletonList(current));
                    int partition = current.getShardSpec().getPartitionNum();
                    current = newSegmentIdentifier(interval, partition + 1);
                }
            }

            appenderator.add(current, inputRow, committerSupplier::get, true);
        } catch (SegmentNotWritableException e) {
            throw new IOException("Failed to write segment", e);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        try {
            List<SegmentIdentifier> toPush = appenderator.getSegments();
            pushSegments(toPush);
            LOG.info("Commit taskId = {}, pushedSegments={}", taskId, pushedSegments.size());
            return DataSegmentCommitMessage.getInstance(pushedSegments);
        } finally {
            appenderator.close();
        }
    }

    @Override
    public void abort() throws IOException {
        try {
            for (DataSegment segment : pushedSegments) {
                segmentKiller.killQuietly(segment);
            }
            // Note: this does not clear segments from deep storage
            appenderator.clear();
            LOG.info("Abort taskId = {}, pushedSegments={}", taskId, pushedSegments.size());
        } catch (InterruptedException e) {
            throw new IOException("Interrupted during abort", e);
        } finally {
            appenderator.close();
        }
    }

    private Map<String, Object> parse(InternalRow record) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : segmentSpec.getFields()) {
            String columnName = field.getName();
            if (segmentSpec.getTimeColumn().equals(columnName)) {
                // the configured time columnName is mapped to __time
                map.put(SegmentSpec.TIME_DIMENSION, record.get(field.getOrdinal(), DataTypes.LongType));
            } else {
                map.put(columnName, record.get(field.getOrdinal(), field.getSqlType()));
            }
        }
        // build the full map first and validate then, so that map can be used in error message for convenience.
        // (toString of InternalRow doesn't produce human-readable output)
        if (map.get(SegmentSpec.TIME_DIMENSION) == null) {
            throw new IllegalStateException(
                    String.format("Null value for column '%s'. Row: %s", segmentSpec.getTimeColumn(), map));
        }
        return map;
    }

    private SegmentIdentifier newSegmentIdentifier(Interval interval, int partitionNum) {
        return new SegmentIdentifier(
                dataSchema.getDataSource(),
                interval,
                tuningConfig.getVersioningPolicy().getVersion(interval),
                new LinearShardSpec(partitionNum)
        );
    }

    private void pushSegments(List<SegmentIdentifier> toPush) throws IOException {
        try {
            SegmentsAndMetadata segmentsAndMetadata = appenderator.push(toPush, committerSupplier.get(), false).get();
            List<DataSegment> updated = segmentsAndMetadata.getSegments();
            if (updated.size() != toPush.size()) {
                LOG.error("Failed to push all segments, taskId={}, toPush={}, updated={}", taskId, toPush.size(), updated.size());
                throw new IOException("Failed to push all segments");
            }

            for (SegmentIdentifier identifier : toPush) {
                LOG.debug("TaskId={} pushed segment segmentId={}", taskId, identifier);
                appenderator.drop(identifier).get();
            }
            pushedSegments.addAll(updated);
            LOG.info("TaskId={}, pushed segments {}", taskId, pushedSegments.size());
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to push segment", e);
        }
    }

    private RealtimeTuningConfig getTuningConfig(WriterContext context) {
        return new RealtimeTuningConfig(
                context.getMaxRowsInMemory(),
                -1L,
                null,
                null,
                null,
                new CustomVersioningPolicy(context.getVersion()),
                null,
                null,
                null,
                new IndexSpec(getBitmapSerdeFactory(context), null, null, null),
                true,
                0,
                0,
                true,
                0L,
                0L,
                null,
                null
        );
    }

    private BitmapSerdeFactory getBitmapSerdeFactory(WriterContext context) {
        switch (context.getBitmapFactory()) {
            case "concise":
                return new ConciseBitmapSerdeFactory();
            case "roaring":
                return new RoaringBitmapSerdeFactory(true);
        }
        throw new IllegalArgumentException(
                "Unknown bitmap factory: '" + context.getBitmapFactory() + "'");
    }

    @Override
    public void close() throws IOException {

    }
}
