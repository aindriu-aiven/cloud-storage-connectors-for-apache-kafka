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

package io.aiven.kafka.connect.common.source.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.stream.Stream;

import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;

import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AbstractSourceRecordIterator implementation for the AbstractSourceRecord implementation.
 */
final public class SourceRecordIterator
        extends
            AbstractSourceRecordIterator<NativeObject, String, OffsetManagerEntry, SourceRecord> {
    private final Logger log = LoggerFactory.getLogger(SourceRecordIterator.class);

    private final NativeClient nativeClient;

    public SourceRecordIterator(final SourceCommonConfig sourceConfig,
            final OffsetManager<OffsetManagerEntry> offsetManager, final Transformer transformer, final int bufferSize,
            final NativeClient nativeClient) {
        super(sourceConfig, offsetManager, transformer, bufferSize);
        this.nativeClient = nativeClient;
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    protected Stream<NativeObject> getNativeItemStream(final String offset) {
        return nativeClient.listObjects().stream();
    }

    @Override
    protected IOSupplier<InputStream> getInputStream(final SourceRecord sourceRecord) {
        return () -> new ByteArrayInputStream(sourceRecord.getNativeItem().data.array());
    }

    @Override
    protected String getNativeKey(final NativeObject nativeObject) {
        return nativeObject.key;
    }

    @Override
    protected SourceRecord createSourceRecord(final NativeObject nativeObject) {
        return new SourceRecord(nativeObject);
    }

    @Override
    protected OffsetManagerEntry createOffsetManagerEntry(final NativeObject nativeObject) {
        return new OffsetManagerEntry(nativeObject.key, "two", "three");
    }

    @Override
    protected OffsetManager.OffsetManagerKey getOffsetManagerKey() {
        return new OffsetManagerEntry(getLastSeenNativeKey(), "two", "three").getManagerKey();
    }
}
