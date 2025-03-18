package io.aiven.kafka.connect.common.source.impl;

import io.aiven.kafka.connect.common.source.AbstractSourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AbstractSourceRecord implementation for the NativeObject.
 */
final public class SourceRecord extends AbstractSourceRecord<NativeObject, String, OffsetManagerEntry, SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRecord.class);

    /**
     * Constructor.
     * @param nativeObject The native object
     */
    public SourceRecord(final NativeObject nativeObject) {
        super(LOGGER, new NativeInfo<NativeObject, String>() {
            @Override
            public NativeObject getNativeItem() {
                return nativeObject;
            }

            @Override
            public String getNativeKey() {
                return nativeObject.key;
            }

            @Override
            public long getNativeItemSize() {
                return nativeObject.data.capacity();
            }
        });
    }

    /**
     * A copy constructor.
     * @param source the source record to copy.
     */
    public SourceRecord(final SourceRecord source) {
        super(source);;
    }

    @Override
    public SourceRecord duplicate() {
        return new SourceRecord(this);
    }
}
