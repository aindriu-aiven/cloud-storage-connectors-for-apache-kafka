package io.aiven.kafka.connect.common.source.impl;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A "native" client.   This client returns lists of native objects and ByteBuffers for specific native keys.
 */
public interface NativeClient {
    /**
     * Gets a list of native objects.
     * @return the list of native objects.
     */
    List<NativeObject> listObjects();

    /**
     * Gets the ByteBuffer for a key.
     * @param key the key to get the byte buffer for.
     * @return The ByteBuffer for a key, or {@code null} if not set.
     */
    ByteBuffer getObjectAsBytes(String key);
}
