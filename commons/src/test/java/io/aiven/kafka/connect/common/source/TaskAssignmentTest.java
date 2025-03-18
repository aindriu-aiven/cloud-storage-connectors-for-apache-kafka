package io.aiven.kafka.connect.common.source;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.impl.NativeClient;
import io.aiven.kafka.connect.common.source.impl.OffsetManagerEntry;
import io.aiven.kafka.connect.common.source.impl.SourceRecord;
import io.aiven.kafka.connect.common.source.impl.SourceRecordIterator;
import io.aiven.kafka.connect.common.source.input.InputFormat;
import io.aiven.kafka.connect.common.source.input.Transformer;
import io.aiven.kafka.connect.common.source.input.TransformerFactory;
import io.aiven.kafka.connect.common.source.task.Context;
import io.aiven.kafka.connect.common.source.task.DistributionType;
import io.aiven.kafka.connect.common.templating.Template;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Optional;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskAssignmentTest {


    // private final String fileName = "topic-00001-1741965423180.txt";
    /** The file pattern for the file name */
   // private final String filePattern = "{{topic}}-{{partition}}-{{start_offset}}";

    public static SourceCommonConfig configureMockConfig(final int taskId, final int maxTasks, DistributionType distributionType) {
        SourceCommonConfig mockConfig = mock(SourceCommonConfig.class);
        when(mockConfig.getDistributionType()).thenReturn(distributionType);
        when(mockConfig.getTaskId()).thenReturn(taskId);
        when(mockConfig.getMaxTasks()).thenReturn(maxTasks);
        when(mockConfig.getTargetTopic()).thenReturn("topic");
        when(mockConfig.getTransformerMaxBufferSize()).thenReturn(4096);
        when(mockConfig.getFilenameTemplate()).thenReturn(Template.of( "{{topic}}-{{partition}}-{{start_offset}}"));
        return mockConfig;
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "0"})
    void testThatMatchingHashedKeysAreDetected(final int taskId) {
        int maxTasks = 4;

        String[] keys = {"topic-00001-1741965423183.txt", "topic-00001-1741965423180.txt", "topic-00001-1741965423181.txt", "topic-00001-1741965423182.txt"};
        String objectKey = keys[taskId];

        OffsetManager<OffsetManagerEntry> offsetManager = new OffsetManager<>(null);
        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        SourceCommonConfig config = configureMockConfig(taskId, maxTasks, DistributionType.OBJECT_HASH);
        NativeClient nativeClient = mock(NativeClient.class);
        final SourceRecordIterator iterator = new SourceRecordIterator(config, offsetManager, transformer, 4096,
                nativeClient);

        Predicate<Optional<SourceRecord>> pred = iterator.taskAssignment;
        SourceRecord record = mock(SourceRecord.class);
        for (int i = 0; i < maxTasks; i++) {
            Context<String> context = new Context<>(keys[i]);
            when(record.getContext()).thenReturn(context);
            if (i == taskId) {
                assertThat(pred.test(Optional.of(record))).isTrue();
            } else {
                assertThat(pred.test(Optional.of(record))).isFalse();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"1", "2", "3", "0"})
    void testThatMatchingPartitionKeysAreDetected(final int taskId) {
        int maxTasks = 4;

        String[] keys = {"topic-00001-1741965423183.txt", "topic-00002-1741965423183.txt", "topic-00003-1741965423183.txt", "topic-00004-1741965423183.txt"};
        String objectKey = keys[taskId];

        OffsetManager<OffsetManagerEntry> offsetManager = new OffsetManager<>(null);
        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        SourceCommonConfig config = configureMockConfig(taskId, maxTasks, DistributionType.PARTITION);
        NativeClient nativeClient = mock(NativeClient.class);
        final SourceRecordIterator iterator = new SourceRecordIterator(config, offsetManager, transformer, 4096,
                nativeClient);

        Predicate<Optional<SourceRecord>> pred = iterator.taskAssignment;
        SourceRecord record = mock(SourceRecord.class);
        for (int i = 0; i < maxTasks; i++) {
            Context<String> context = new Context<>(keys[i]);
            context.setPartition(i);
            when(record.getContext()).thenReturn(context);
            if (i == taskId) {
                assertThat(pred.test(Optional.of(record))).isTrue();
            } else {
                assertThat(pred.test(Optional.of(record))).isFalse();
            }
        }
    }

    @Test
    void testThatNullKeysAreHandled() {
        int maxTasks = 4;

        OffsetManager<OffsetManagerEntry> offsetManager = new OffsetManager<>(null);
        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        NativeClient nativeClient = mock(NativeClient.class);

        SourceRecord record = mock(SourceRecord.class);
        for (int taskId = 0; taskId < maxTasks; taskId++) {
            SourceCommonConfig config = configureMockConfig(taskId, maxTasks, DistributionType.OBJECT_HASH);
            final SourceRecordIterator iterator = new SourceRecordIterator(config, offsetManager, transformer, 4096,
                    nativeClient);
            Predicate<Optional<SourceRecord>> pred = iterator.taskAssignment;

            Context<String> context = new Context<>(null);
            when(record.getContext()).thenReturn(context);
            assertThat(pred.test(Optional.of(record))).isFalse();
        }
    }

    @Test
    void testThatNullObjectsAreHandled() {
        int maxTasks = 4;

        OffsetManager<OffsetManagerEntry> offsetManager = new OffsetManager<>(null);
        Transformer transformer = TransformerFactory.getTransformer(InputFormat.BYTES);
        NativeClient nativeClient = mock(NativeClient.class);

        for (int taskId = 0; taskId < maxTasks; taskId++) {
            SourceCommonConfig config = configureMockConfig(taskId, maxTasks, DistributionType.PARTITION);
            final SourceRecordIterator iterator = new SourceRecordIterator(config, offsetManager, transformer, 4096,
                    nativeClient);
            Predicate<Optional<SourceRecord>> pred = iterator.taskAssignment;

            assertThat(pred.test(Optional.empty())).isFalse();
        }
    }

}
