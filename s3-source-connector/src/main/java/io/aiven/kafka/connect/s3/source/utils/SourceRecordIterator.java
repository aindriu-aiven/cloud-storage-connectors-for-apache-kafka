/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.s3.source.utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.SchemaAndValue;

import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.utils.FilePatternUtils;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.source.task.DistributionStrategy;
import io.aiven.kafka.connect.common.source.task.enums.ObjectDistributionStrategy;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Iterator that processes S3 files and creates Kafka source records. Supports different output formats (Avro, JSON,
 * Parquet).
 */
public final class SourceRecordIterator implements Iterator<S3SourceRecord> {
    /** The OffsetManager that we are using */
    private final OffsetManager<S3OffsetManagerEntry> offsetManager;

    /** The configuration for this S3 source */
    private final S3SourceConfig s3SourceConfig;
    /** The transformer for the data conversions */
    private final Transformer transformer;
    /** The AWS client that provides the S3Objects */
    private final AWSV2SourceClient sourceClient;

    /** The task ID associated with this iterator */
    private int taskId;

    /** The S3 bucket we are processing */
    private final String bucket;

    /** The inner iterator to provides S3Object that have been filtered potentially had data extracted */
    private final Iterator<S3SourceRecord> inner;
    /** The outer iterator that provides S3SourceRecords */
    private Iterator<S3SourceRecord> outer;

    final FileMatching fileMatching;

    final Predicate<Optional<S3SourceRecord>> taskAssignment;
    private FilePatternUtils<S3Key> filePattern;

    public SourceRecordIterator(final S3SourceConfig s3SourceConfig,
            final OffsetManager<S3OffsetManagerEntry> offsetManager, final Transformer transformer,
            final AWSV2SourceClient sourceClient) {

        super();
        this.s3SourceConfig = s3SourceConfig;
        this.offsetManager = offsetManager;
        this.bucket = s3SourceConfig.getAwsS3BucketName();
        this.transformer = transformer;
        this.sourceClient = sourceClient;
        this.taskAssignment = new TaskAssignment(initializeObjectDistributionStrategy(), filePattern);
        this.taskId = s3SourceConfig.getTaskId();
        this.fileMatching = new FileMatching(filePattern);

        final Stream<S3SourceRecord> s3SourceRecordStream = sourceClient.getS3ObjectStream(null)
                .map(fileMatching)
                .filter(taskAssignment)
                .map(Optional::get);

        inner = s3SourceRecordStream.iterator();
        outer = Collections.emptyIterator();
    }

    @Override
    public boolean hasNext() {
        while (!outer.hasNext() && inner.hasNext()) {
            outer = convert(inner.next()).iterator();
        }
        return outer.hasNext();
    }

    @Override
    public S3SourceRecord next() {
        return outer.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator is unmodifiable");
    }

    /**
     * Converts the S3Object into stream of S3SourceRecords.
     *
     * @param s3SourceRecord
     *            the SourceRecord that drives the creation of source records with values.
     * @return a stream of S3SourceRecords created from the input stream of the S3Object.
     */
    private Stream<S3SourceRecord> convert(final S3SourceRecord s3SourceRecord) {

        s3SourceRecord.setKeyData(
                transformer.getKeyData(s3SourceRecord.getObjectKey(), s3SourceRecord.getTopic(), s3SourceConfig));

        return transformer
                .getRecords(sourceClient.getObject(s3SourceRecord.getObjectKey()), s3SourceRecord.getTopic(),
                        s3SourceRecord.getPartition(), s3SourceConfig, s3SourceRecord.getRecordCount())
                .map(new Mapper(s3SourceRecord));

    }

    private DistributionStrategy initializeObjectDistributionStrategy() {
        final ObjectDistributionStrategy objectDistributionStrategy = s3SourceConfig.getObjectDistributionStrategy();
        final int maxTasks = s3SourceConfig.getMaxTasks();
        this.taskId = s3SourceConfig.getTaskId() % maxTasks;
        this.filePattern = new FilePatternUtils<>(
                s3SourceConfig.getS3FileNameFragment().getFilenameTemplate().toString(),
                s3SourceConfig.getTargetTopics());
        return objectDistributionStrategy.getDistributionStrategy(maxTasks);
    }

    /**
     * maps the data from the @{link Transformer} stream to an S3SourceRecord given all the additional data required.
     */
    static class Mapper implements Function<SchemaAndValue, S3SourceRecord> {
        /**
         * The S3SourceRecord that produceces the values.
         */
        private final S3SourceRecord sourceRecord;

        public Mapper(final S3SourceRecord sourceRecord) {
            // TODO this is the point where the global S3OffsetManagerEntry becomes local and we can do a lookahead type
            // operation within the Transformer
            // to see if there are more records.
            this.sourceRecord = sourceRecord.clone();
        }

        @Override
        public S3SourceRecord apply(final SchemaAndValue valueData) {
            sourceRecord.incrementRecordCount();
            final S3SourceRecord result = sourceRecord.clone();
            result.setValueData(valueData);
            return result;
        }
    }

    class TaskAssignment implements Predicate<Optional<S3SourceRecord>> {
        final DistributionStrategy distributionStrategy;
        final FilePatternUtils<S3Key> pattern;

        TaskAssignment(final DistributionStrategy distributionStrategy, final FilePatternUtils<S3Key> utils) {
            this.distributionStrategy = distributionStrategy;
            this.pattern = utils;
        }

        @Override
        public boolean test(final Optional<S3SourceRecord> s3SourceRecord) {
            if (s3SourceRecord.isPresent()) {
                final S3SourceRecord record = s3SourceRecord.get();
                final Optional<Context<S3Key>> ctx = pattern.process(new S3Key(record.getObjectKey()));
                if (ctx.isPresent()) {
                    return taskId == distributionStrategy.getTaskFor(ctx.get());
                }
            }
            return false;
        }
    }

    class FileMatching implements Function<S3Object, Optional<S3SourceRecord>> {

        final FilePatternUtils<S3Key> utils;
        FileMatching(final FilePatternUtils<S3Key> utils) {
            this.utils = utils;
        }

        @Override
        public Optional<S3SourceRecord> apply(final S3Object s3Object) {

            final Optional<Context<S3Key>> context = utils.process(new S3Key(s3Object.key()));
            if (context.isPresent()) {
                final S3SourceRecord s3SourceRecord = new S3SourceRecord(s3Object);
                S3OffsetManagerEntry offsetManagerEntry = new S3OffsetManagerEntry(bucket, s3Object.key(),
                        context.get().getTopic().orElseThrow(), context.get().getPartition().orElseGet(null));
                offsetManagerEntry = offsetManager
                        .getEntry(offsetManagerEntry.getManagerKey(), offsetManagerEntry::fromProperties)
                        .orElse(offsetManagerEntry);
                s3SourceRecord.setOffsetManagerEntry(offsetManagerEntry);
                return Optional.of(s3SourceRecord);
            }
            return Optional.empty();
        }
    }

}
