package io.aiven.kafka.connect.common.source.input;

/**
 * A testing fixture to generate JSON data.
 */
final public class JsonTestDataFixture {

    /**
     * Creates the specified number of JSON records encoded into a string.
     * @param recordCount the number of records to generate.
     * @return The specified number of JSON records encoded into a string.
     */
    public static String getJsonRecs(final int recordCount) {
        final StringBuilder jsonRecords = new StringBuilder();
        for (int i = 0; i < recordCount; i++) {
            jsonRecords.append(String.format("{\"key\":\"value%d\"}", i));
            if (i < recordCount) {
                jsonRecords.append("\n"); // NOPMD AppendCharacterWithChar
            }
        }
        return jsonRecords.toString();
    }
}
