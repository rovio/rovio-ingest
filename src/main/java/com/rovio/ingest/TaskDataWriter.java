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
import com.rovio.ingest.util.ReflectionUtils;
import com.rovio.ingest.util.SegmentStorageUpdater;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.config.NullValueHandlingConfig;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
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
import org.apache.druid.segment.realtime.appenderator.DefaultOfflineAppenderatorFactory;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentNotWritableException;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.plumber.Committers;
import org.apache.druid.segment.realtime.plumber.CustomVersioningPolicy;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.types.UTF8String;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final File basePersistDirectory;

    private SegmentIdWithShardSpec current;

    TaskDataWriter(long taskId, WriterContext context, SegmentSpec segmentSpec) {
        this.taskId = taskId;
        this.segmentSpec = segmentSpec;
        this.maxRows = context.getSegmentMaxRows();
        this.dataSchema = segmentSpec.getDataSchema();
        this.tuningConfig = getTuningConfig(context);
        DataSegmentPusher segmentPusher = SegmentStorageUpdater.createPusher(context);
        // Similar code for creating a basePersistDirectory was removed in https://github.com/apache/druid/pull/13040
        // with a mention that basePersistDirectory "is always overridden in production using withBasePersistDirectory given some subdirectory of the task work directory".
        // Creating a temp folder manually here, as appenderator.startJob() requires it, but there doesn't seem to be an easy way to get it provided at this point(?).
        // To clean up, this dir is deleted in #close
        this.basePersistDirectory = FileUtils.createTempDir("rovio-ingest-persist-");
        this.segmentKiller = SegmentStorageUpdater.createKiller(context);
        this.appenderator = new DefaultOfflineAppenderatorFactory(segmentPusher, MAPPER, INDEX_IO, INDEX_MERGER_V_9)
                .build(dataSchema, tuningConfig.withBasePersistDirectory(basePersistDirectory), new FireDepartmentMetrics());
        this.committerSupplier = Committers::nil;
        this.pushedSegments = new HashSet<>();
        this.appenderator.startJob();

        try {
            ReflectionUtils.setStaticFieldValue(NullHandling.class, "INSTANCE", new NullValueHandlingConfig(context.isUseDefaultValueForNull(), null));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Unable to set null handling!!", e);
        }
    }

    @Override
    public void write(InternalRow record) throws IOException {
        try {
            Map<String, Object> map = parse(record);
            long timestamp = (long) map.get(SegmentSpec.TIME_DIMENSION);
            InputRow inputRow = new MapBasedInputRow(timestamp,
                    dataSchema.getDimensionsSpec().getDimensionNames(), map);

            long bucketStartEpoch = segmentSpec.getPartitionTime() != null
                    // Adjust as spark returns long value with milliseconds.
                    ? record.getLong(segmentSpec.getPartitionTime().getOrdinal()) / 1000
                    : timestamp;
            DateTime bucketStart = dataSchema.getGranularitySpec().getSegmentGranularity().bucketStart(DateTimes.utc(bucketStartEpoch));
            final Interval interval = new Interval(bucketStart, dataSchema.getGranularitySpec().getSegmentGranularity().increment(bucketStart));

            if (segmentSpec.getPartitionTime() != null && segmentSpec.getPartitionNum() != null) {
                int partitionNum = record.getInt(segmentSpec.getPartitionNum().getOrdinal());
                current = newSegmentId(interval, partitionNum);
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
                    current = newSegmentId(interval, 0);
                }

                if (appenderator.getSegments().contains(current) && appenderator.getRowCount(current) >= maxRows) {
                    LOG.debug("TaskId={}, New segment interval = {} has {} rows", taskId, interval, appenderator.getRowCount(current));
                    pushSegments(Collections.singletonList(current));
                    int partition = current.getShardSpec().getPartitionNum();
                    current = newSegmentId(interval, partition + 1);
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
            List<SegmentIdWithShardSpec> toPush = appenderator.getSegments();
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
                Long tsVal = (Long) record.get(field.getOrdinal(), DataTypes.TimestampType);
                if (tsVal != null && field.getSqlType() == DataTypes.DateType) {
                    // date is stored as days since epoch
                    tsVal = LocalDate.ofEpochDay(tsVal).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                } else if (tsVal != null) {
                    // Adjust to millis as spark returns long value with microseconds.
                    tsVal = tsVal / 1000;
                }
                // the configured time columnName is mapped to __time
                map.put(SegmentSpec.TIME_DIMENSION, tsVal);
            } else {
                DataType sqlType = field.getSqlType();
                Object value = record.get(field.getOrdinal(), sqlType);
                if (sqlType == DataTypes.StringType && value instanceof UTF8String) {
                    // Convert to String as Spark return UTF8String which is not compatible with Druid sketches.
                    value = new String(((UTF8String) value).getBytes(), StandardCharsets.UTF_8);
                }
                map.put(columnName, value);
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

    private SegmentIdWithShardSpec newSegmentId(Interval interval, int partitionNum) {
        return new SegmentIdWithShardSpec(
                dataSchema.getDataSource(),
                interval,
                tuningConfig.getVersioningPolicy().getVersion(interval),
                new LinearShardSpec(partitionNum)
        );
    }

    private void pushSegments(List<SegmentIdWithShardSpec> toPush) throws IOException {
        try {
            SegmentsAndCommitMetadata segmentsAndMetadata = appenderator.push(toPush, committerSupplier.get(), false).get();
            List<DataSegment> updated = segmentsAndMetadata.getSegments();
            if (updated.size() != toPush.size()) {
                LOG.error("Failed to push all segments, taskId={}, toPush={}, updated={}", taskId, toPush.size(), updated.size());
                throw new IOException("Failed to push all segments");
            }

            for (SegmentIdWithShardSpec identifier : toPush) {
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
                null,
                context.getMaxRowsInMemory(),
                -1L,
                null,
                null,
                null,
                null,
                new CustomVersioningPolicy(context.getVersion()),
                null,
                null,
                null,
                new IndexSpec(getBitmapSerdeFactory(context), null, null, null),
                null,
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
        if (this.basePersistDirectory != null) {
            FileUtils.deleteDirectory(basePersistDirectory);
        }
    }
}
