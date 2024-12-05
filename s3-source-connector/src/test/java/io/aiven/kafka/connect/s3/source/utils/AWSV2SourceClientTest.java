/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.s3.source.utils;

import static io.aiven.kafka.connect.config.s3.S3ConfigFragment.AWS_S3_BUCKET_NAME_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class AWSV2SourceClientTest {

    private AmazonS3 s3Client;

    private AWSV2SourceClient awsv2SourceClient;

    private static Map<String, String> getConfigMap(final int maxTasks, final int taskId) {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put("tasks.max", String.valueOf(maxTasks));
        configMap.put("task.id", String.valueOf(taskId));

        configMap.put(AWS_S3_BUCKET_NAME_CONFIG, "test-bucket");
        return configMap;
    }

    @ParameterizedTest
    @CsvSource({ "3, 1" })
    void testFetchObjectSummariesWithNoObjects(final int maxTasks, final int taskId) {
        initializeWithTaskConfigs(maxTasks, taskId);
        final ListObjectsV2Result listObjectsV2Result = createListObjectsV2Result(Collections.emptyList(), null);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result);

        final Iterator<String> summaries = awsv2SourceClient.getListOfObjectKeys(null);
        assertThat(summaries).isExhausted();
    }

    @ParameterizedTest
    @CsvSource({ "1, 0" })
    void testFetchObjectSummariesWithOneObjectWithBasicConfig(final int maxTasks, final int taskId) {
        final String objectKey = "any-key";

        initializeWithTaskConfigs(maxTasks, taskId);
        final Iterator<String> summaries = getS3ObjectKeysIterator(objectKey);
        assertThat(summaries).hasNext();
    }

    @ParameterizedTest
    @CsvSource({ "4, 2, key1", "4, 3, key2", "4, 0, key3", "4, 1, key4" })
    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdAssigned(final int maxTasks, final int taskId,
            final String objectKey) {
        initializeWithTaskConfigs(maxTasks, taskId);
        final Iterator<String> summaries = getS3ObjectKeysIterator(objectKey);
        assertThat(summaries).hasNext();
    }

    @ParameterizedTest
    @CsvSource({ "4, 1, key1", "4, 3, key1", "4, 0, key1", "4, 1, key2", "4, 2, key2", "4, 0, key2", "4, 1, key3",
            "4, 2, key3", "4, 3, key3", "4, 0, key4", "4, 2, key4", "4, 3, key4" })
    void testFetchObjectSummariesWithOneNonZeroByteObjectWithTaskIdUnassigned(final int maxTasks, final int taskId,
            final String objectKey) {
        initializeWithTaskConfigs(maxTasks, taskId);
        final Iterator<String> summaries = getS3ObjectKeysIterator(objectKey);

        assertThat(summaries).isExhausted();
    }

    @ParameterizedTest
    @CsvSource({ "4, 3", "4, 0" })
    void testFetchObjectSummariesWithZeroByteObject(final int maxTasks, final int taskId) {
        initializeWithTaskConfigs(maxTasks, taskId);
        final ListObjectsV2Result listObjectsV2Result = getListObjectsV2Result();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result);

        final Iterator<String> summaries = awsv2SourceClient.getListOfObjectKeys(null);

        // assigned 1 object to taskid
        assertThat(summaries).hasNext();
        assertThat(summaries.next()).isNotBlank();
        assertThat(summaries).isExhausted();
    }

    @Test
    void testFetchObjectSummariesWithPagination() throws IOException {
        initializeWithTaskConfigs(4, 3);
        final S3ObjectSummary object1 = createObjectSummary(3, "key1");
        final S3ObjectSummary object2 = createObjectSummary(2, "key2");
        final List<S3ObjectSummary> firstBatch = List.of(object1);
        final List<S3ObjectSummary> secondBatch = List.of(object2);

        final ListObjectsV2Result firstResult = createListObjectsV2Result(firstBatch, "nextToken");
        final ListObjectsV2Result secondResult = createListObjectsV2Result(secondBatch, null);

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(firstResult).thenReturn(secondResult);

        final Iterator<String> summaries = awsv2SourceClient.getListOfObjectKeys(null);
        verify(s3Client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
        assertThat(summaries.next()).isNotNull();
        assertThat(summaries.next()).isNotNull();
        assertThat(summaries.next()).isNotNull();
        // First page exhausted, calls continuation token.
        verify(s3Client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
        assertThat(summaries.next()).isNotNull();
        assertThat(summaries.next()).isNotNull();
        assertThat(summaries).isExhausted();
    }

    private ListObjectsV2Result createListObjectsV2Result(final List<S3ObjectSummary> summaries,
            final String nextToken) {
        final ListObjectsV2Result result = mock(ListObjectsV2Result.class);
        when(result.getObjectSummaries()).thenReturn(summaries);
        when(result.getNextContinuationToken()).thenReturn(nextToken);
        when(result.isTruncated()).thenReturn(nextToken != null);
        return result;
    }

    private S3ObjectSummary createObjectSummary(final long sizeOfObject, final String objectKey) {
        final S3ObjectSummary summary = mock(S3ObjectSummary.class);
        when(summary.getSize()).thenReturn(sizeOfObject);
        when(summary.getKey()).thenReturn(objectKey);
        return summary;
    }

    private Iterator<String> getS3ObjectKeysIterator(final String objectKey) {
        final S3ObjectSummary objectSummary = createObjectSummary(1, objectKey);
        final ListObjectsV2Result listObjectsV2Result = createListObjectsV2Result(
                Collections.singletonList(objectSummary), null);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Result);

        return awsv2SourceClient.getListOfObjectKeys(null);
    }

    public void initializeWithTaskConfigs(final int maxTasks, final int taskId) {
        final Map<String, String> configMap = getConfigMap(maxTasks, taskId);
        final S3SourceConfig s3SourceConfig = new S3SourceConfig(configMap);
        s3Client = mock(AmazonS3.class);
        awsv2SourceClient = new AWSV2SourceClient(s3Client, s3SourceConfig, Collections.emptySet());

    }

    private ListObjectsV2Result getListObjectsV2Result() {
        final S3ObjectSummary zeroByteObject = createObjectSummary(0, "key1");
        final S3ObjectSummary nonZeroByteObject1 = createObjectSummary(1, "key2");
        final S3ObjectSummary nonZeroByteObject2 = createObjectSummary(1, "key3");
        return createListObjectsV2Result(List.of(zeroByteObject, nonZeroByteObject1, nonZeroByteObject2), null);
    }
}