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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.aiven.kafka.connect.common.source.impl.NativeObject;
import io.aiven.kafka.connect.common.source.impl.OffsetManagerEntry;
import io.aiven.kafka.connect.common.source.impl.SourceRecord;

/**
 * A test class to verify the AbstractSourceRecordTest works.
 */
public class SourceRecordTest extends AbstractSourceRecordTest<NativeObject, String, OffsetManagerEntry, SourceRecord> { // NOPMD
                                                                                                                         // TestClassWithoutTestCases

    @Override
    protected String createKFrom(final String key) {
        return key;
    }

    @Override
    protected OffsetManagerEntry createOffsetManagerEntry(final String key) {
        return new OffsetManagerEntry(key, "two", "three");
    }

    @Override
    protected SourceRecord createSourceRecord() {
        return new SourceRecord(
                new NativeObject("key", ByteBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8))));
    }

}
