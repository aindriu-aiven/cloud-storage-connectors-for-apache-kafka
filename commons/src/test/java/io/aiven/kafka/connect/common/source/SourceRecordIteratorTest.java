/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.connect.common.source;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.impl.NativeClient;
import io.aiven.kafka.connect.common.source.impl.NativeObject;
import io.aiven.kafka.connect.common.source.impl.OffsetManagerEntry;
import io.aiven.kafka.connect.common.source.impl.SourceRecord;
import io.aiven.kafka.connect.common.source.impl.SourceRecordIterator;
import io.aiven.kafka.connect.common.source.input.Transformer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Test to verify AbstraactSourceRecordIterator works.
 */
public class SourceRecordIteratorTest
        extends
            AbstractSourceRecordIteratorTest<NativeObject, String, OffsetManagerEntry, SourceRecord> { // NOPMD
                                                                                                       // TestClassWithoutTestCases

    NativeClient nativeClient;

    @Override
    protected String createKFrom(final String key) {
        return key;
    }

    @Override
    protected SourceRecordIterator createSourceRecordIterator(final SourceCommonConfig mockConfig,
            final OffsetManager<OffsetManagerEntry> mockOffsetManager, final Transformer transformer) {
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

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "stores mutable fields in offset manager to be reviewed before release")
    public class Mutator extends ClientMutator<NativeObject, String, Mutator> {

        @Override
        protected NativeObject createObject(final String key, final ByteBuffer data) {
            return new NativeObject(key, data);
        }

        /**
         * Create a list of NativeObjects from a single block.
         *
         * @return A list of NativeObjects from a single block.
         */
        private List<NativeObject> dequeueData() {
            // Dequeue a block. Sets the objects.
            dequeueBlock();
            return objects;
        }

        @Override
        public void build() {
            nativeClient = mock(NativeClient.class);

            // when a listObjectV2 is requests deququ the answer from the blocks.
            when(nativeClient.listObjects()).thenAnswer(env -> dequeueData());
            // when an objectRequest is sent retrieve the response data.
            when(nativeClient.getObjectAsBytes(anyString())).thenAnswer(env -> getData(env.getArgument(0)));
        }
    }

}
