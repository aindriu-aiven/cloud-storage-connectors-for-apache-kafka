package io.aiven.kafka.connect.common.source.impl;

import java.nio.ByteBuffer;

/**
 * A "native" object for testing.
 */
public class NativeObject {
    // instance vars are package private.
    final String key;
    final ByteBuffer data;

    /**
     * Constructor.
     * @param key the key for this object.
     * @param data the data for this object.
     */
    public NativeObject(String key, ByteBuffer data) {
        this.key = key;
        this.data = data;
    }
}
