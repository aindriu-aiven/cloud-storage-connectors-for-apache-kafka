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

package io.aiven.kafka.connect.s3.source;

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPICS;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.TARGET_TOPIC_PARTITIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.ByteArrayWriter;
import io.aiven.kafka.connect.s3.source.output.OutputFormat;
import io.aiven.kafka.connect.s3.source.output.OutputWriter;
import io.aiven.kafka.connect.s3.source.testutils.BucketAccessor;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

final class S3SourceTaskTest {

    private static final Random RANDOM = new Random();
    private Map<String, String> properties;

    private static BucketAccessor testBucketAccessor;
    private static final String TEST_BUCKET = "test-bucket";

    private static S3Mock s3Api;
    private static AmazonS3 s3Client;

    private static Map<String, String> commonProperties;

    @Mock
    private SourceTaskContext mockedSourceTaskContext;

    @Mock
    private OffsetStorageReader mockedOffsetStorageReader;

    @BeforeAll
    public static void setUpClass() {
        final int s3Port = RANDOM.nextInt(10_000) + 10_000;

        s3Api = new S3Mock.Builder().withPort(s3Port).withInMemoryBackend().build();
        s3Api.start();

        commonProperties = Map.of(S3SourceConfig.AWS_ACCESS_KEY_ID_CONFIG, "test_key_id",
                S3SourceConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "test_secret_key",
                S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG, TEST_BUCKET, S3SourceConfig.AWS_S3_ENDPOINT_CONFIG,
                "http://localhost:" + s3Port, S3SourceConfig.AWS_S3_REGION_CONFIG, "us-west-2");

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        final BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                commonProperties.get(S3SourceConfig.AWS_ACCESS_KEY_ID_CONFIG),
                commonProperties.get(S3SourceConfig.AWS_SECRET_ACCESS_KEY_CONFIG));
        builder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
        builder.withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(commonProperties.get(S3SourceConfig.AWS_S3_ENDPOINT_CONFIG),
                        commonProperties.get(S3SourceConfig.AWS_S3_REGION_CONFIG)));
        builder.withPathStyleAccessEnabled(true);

        s3Client = builder.build();

        testBucketAccessor = new BucketAccessor(s3Client, TEST_BUCKET);
        testBucketAccessor.createBucket();
    }

    @AfterAll
    public static void tearDownClass() {
        s3Api.stop();
    }

    @BeforeEach
    public void setUp() {
        properties = new HashMap<>(commonProperties);
        s3Client.createBucket(TEST_BUCKET);
        mockedSourceTaskContext = mock(SourceTaskContext.class);
        mockedOffsetStorageReader = mock(OffsetStorageReader.class);
    }

    @AfterEach
    public void tearDown() {
        s3Client.deleteBucket(TEST_BUCKET);
    }

    @Test
    void testS3SourceTaskInitialization() {
        final S3SourceTask s3SourceTask = new S3SourceTask();
        startSourceTask(s3SourceTask);

        final Optional<Converter> keyConverter = s3SourceTask.getKeyConverter();
        assertThat(keyConverter).isPresent();
        assertThat(keyConverter.get()).isInstanceOf(ByteArrayConverter.class);

        final Converter valueConverter = s3SourceTask.getValueConverter();
        assertThat(valueConverter).isInstanceOf(ByteArrayConverter.class);

        final OutputWriter outputWriter = s3SourceTask.getOutputWriter();
        assertThat(outputWriter).isInstanceOf(ByteArrayWriter.class);

        final boolean taskInitialized = s3SourceTask.isTaskInitialized();
        assertThat(taskInitialized).isTrue();
    }

    private void startSourceTask(final S3SourceTask s3SourceTask) {
        s3SourceTask.initialize(mockedSourceTaskContext);
        when(mockedSourceTaskContext.offsetStorageReader()).thenReturn(mockedOffsetStorageReader);

        setBasicProperties();
        s3SourceTask.start(properties);
    }

    private void setBasicProperties() {
        properties.put(S3SourceConfig.OUTPUT_FORMAT_KEY, OutputFormat.BYTES.getValue());
        properties.put("name", "test_source_connector");
        properties.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        properties.put("tasks.max", "1");
        properties.put("connector.class", AivenKafkaConnectS3SourceConnector.class.getName());
        properties.put(TARGET_TOPIC_PARTITIONS, "0,1");
        properties.put(TARGET_TOPICS, "testtopic");
    }
}