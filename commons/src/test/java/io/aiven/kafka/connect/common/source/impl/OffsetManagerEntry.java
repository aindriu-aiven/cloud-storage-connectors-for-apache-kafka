package io.aiven.kafka.connect.common.source.impl;

import com.google.common.base.Objects;
import io.aiven.kafka.connect.common.source.OffsetManager;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of OffsetManagerEntry.
 * This entry has 3 values stored in the map.
 */
public class OffsetManagerEntry implements OffsetManager.OffsetManagerEntry<OffsetManagerEntry> {
    public Map<String, Object> data;

    private int recordCount;

    /**
     * Constructor.
     * @param one the first value.
     * @param two the second value.
     * @param three the third value.
     */
    public OffsetManagerEntry(final String one, final String two, final String three) {
        this();
        data.put("segment1", one);
        data.put("segment2", two);
        data.put("segment3", three);
    }

    /**
     * Constructor.
     */
    public OffsetManagerEntry() {
        data = new HashMap<>();
        data.put("segment1", "The First Segment");
        data.put("segment2", "The Second Segment");
        data.put("segment3", "The Third Segment");
    }

    /**
     * A constructor.
     * @param properties THe data map to use.
     */
    public OffsetManagerEntry(final Map<String, Object> properties) {
        this();
        data.putAll(properties);
    }

    @Override
    public OffsetManagerEntry fromProperties(final Map<String, Object> properties) {
        return new OffsetManagerEntry(properties);
    }

    @Override
    public Map<String, Object> getProperties() {
        return data;
    }

    @Override
    public Object getProperty(final String key) {
        return data.get(key);
    }

    @Override
    public void setProperty(final String key, final Object value) {
        data.put(key, value);
    }

    @Override
    public OffsetManager.OffsetManagerKey getManagerKey() {
        return () -> Map.of("segment1", data.get("segment1"), "segment2", data.get("segment2"), "segment3",
                data.get("segment3"));
    }

    @Override
    public void incrementRecordCount() {
        recordCount++;
    }

    @Override
    public long getRecordCount() {
        return recordCount;
    }

    @Override
    public boolean equals(final Object other) {
        if (other instanceof OffsetManagerEntry) {
            return this.compareTo((OffsetManagerEntry) other) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getProperty("segment1"), getProperty("segment2"), getProperty("segment3"));
    }

    @Override
    public int compareTo(final OffsetManagerEntry other) {
        if (other == this) { // NOPMD
            return 0;
        }
        int result = ((String) getProperty("segment1")).compareTo((String) other.getProperty("segment1"));
        if (result == 0) {
            result = ((String) getProperty("segment2")).compareTo((String) other.getProperty("segment2"));
            if (result == 0) {
                result = ((String) getProperty("segment3")).compareTo((String) other.getProperty("segment3"));
            }
        }
        return result;
    }
}
