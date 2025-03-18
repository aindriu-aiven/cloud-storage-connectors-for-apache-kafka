package io.aiven.kafka.connect.common.source;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.impl.NativeClient;
import io.aiven.kafka.connect.common.source.impl.NativeObject;
import io.aiven.kafka.connect.common.source.impl.SourceRecordIterator;
import io.aiven.kafka.connect.common.source.impl.SourceRecord;
import io.aiven.kafka.connect.common.source.impl.OffsetManagerEntry;
import io.aiven.kafka.connect.common.source.input.Transformer;
import org.apache.kafka.common.config.ConfigDef;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to verify AbstraactSourceRecordIterator works.
 */
public class SourceRecordIteratorTest extends AbstractSourceRecordIteratorTest<NativeObject, String, OffsetManagerEntry, SourceRecord> {

    NativeClient nativeClient;


    @Override
    protected String createKFrom(String key) {
        return key;
    }

    @Override
    protected SourceRecordIterator createSourceRecordIterator(SourceCommonConfig mockConfig, OffsetManager<OffsetManagerEntry> mockOffsetManager, Transformer transformer) {
        return new SourceRecordIterator(mockConfig, mockOffsetManager, transformer, 4096, nativeClient);
    }

    @Override
    protected Mutator createClientMutator() {
        return new Mutator();
    }

    @Override
    protected SourceCommonConfig createMockedConfig() {
        return mock(SourceCommonConfig.class);
    }

    public class Mutator extends ClientMutator<NativeObject, String, Mutator> {

        @Override
        protected NativeObject createObject(String key, ByteBuffer data) {
            return new NativeObject(key, data);
        }

        /**
         * Create a list of NativeObjects from a single block.
         * @return A list of NativeObjects from a single block.
         */
        private List<NativeObject> dequeueData() {
            // Dequeue a block.  Sets the objects.
            dequeueBlock();
            return objects;
        }

        @Override
        public void build() {
            nativeClient = mock(NativeClient.class);

            // when a listObjectV2 is requests deququ the answer from the blocks.
            when(nativeClient.listObjects()).thenAnswer(env -> dequeueData());
            // when an objectRequest is sent retrieve the response data.
            when(nativeClient.getObjectAsBytes(anyString()))
                    .thenAnswer(env -> getData(env.getArgument(0)));
        }
    }


}
