package io.aiven.kafka.connect.common.source;

import io.aiven.kafka.connect.common.source.impl.NativeObject;
import io.aiven.kafka.connect.common.source.impl.SourceRecord;
import io.aiven.kafka.connect.common.source.impl.OffsetManagerEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A test class to verify the AbstractSourceRecordTest works.
 */
public class SourceRecordTest extends AbstractSourceRecordTest<NativeObject, String, OffsetManagerEntry, SourceRecord> {

    @Override
    protected String createKFrom(final String key) {
        return key;
    }

    @Override
    protected OffsetManagerEntry createOffsetManagerEntry(final String key) {
        return new OffsetManagerEntry(key, "two",  "three");
    }

    @Override
    protected SourceRecord createSourceRecord() {
        NativeObject nativeObject = new NativeObject("key", ByteBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8)));
        return new SourceRecord(nativeObject);
    }

}


