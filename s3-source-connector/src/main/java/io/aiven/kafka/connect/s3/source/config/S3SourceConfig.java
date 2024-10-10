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

package io.aiven.kafka.connect.s3.source.config;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.s3.S3BaseConfig;

import io.aiven.kafka.connect.s3.source.output.TransformerFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;


import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.internal.BucketNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final public class S3SourceConfig extends S3BaseConfig {

    public static final Logger LOGGER = LoggerFactory.getLogger(S3SourceConfig.class);

    public static final String AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG = "aws.s3.backoff.delay.ms";

    public static final String AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG = "aws.s3.backoff.max.delay.ms";

    public static final String AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG = "aws.s3.backoff.max.retries";

    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";

    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.endpoint";

    private static final String GROUP_OTHER = "OTHER_CFG";
    private static final String GROUP_OFFSET_TOPIC = "OFFSET_TOPIC";

    // Default values from AWS SDK, since they are hidden
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    public static final String AWS_S3_BUCKET_NAME_CONFIG = "aws.s3.bucket.name";
    public static final String TARGET_TOPIC_PARTITIONS = "topic.partitions";
    public static final String TARGET_TOPICS = "topics";
    public static final String FETCH_PAGE_SIZE = "aws.s3.fetch.page.size";
    public static final String MAX_POLL_RECORDS = "max.poll.records";
    public static final String KEY_CONVERTER = "key.converter";
    public static final String VALUE_CONVERTER = "value.converter";
    public static final String OUTPUT_FORMAT_KEY = "output.format";


    public S3SourceConfig(final Map<String, String> properties) {
        super(configDef(), properties);
        validate(); // NOPMD ConstructorCallsOverridableMethod getStsRole is called
    }

    public static ConfigDef configDef() {
        final var configDef = new S3SourceConfigDef();
        addSchemaRegistryGroup(configDef);
        addOffsetStorageConfig(configDef);
        addAwsStsConfigGroup(configDef);
        addAwsConfigGroup(configDef);
        addDeprecatedConfiguration(configDef);
        addS3RetryPolicies(configDef);
        addOtherConfig(configDef);
        return configDef;
    }

    private static void addSchemaRegistryGroup(final ConfigDef configDef) {
        int srCounter = 0;
        configDef.define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "SCHEMA REGISTRY URL", GROUP_OTHER, srCounter++, ConfigDef.Width.NONE,
                SCHEMA_REGISTRY_URL);
        configDef.define(OUTPUT_FORMAT_KEY, ConfigDef.Type.STRING, TransformerFactory.DEFAULT_TRANSFORMER_NAME,
                new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM, "Output format avro/json/parquet/bytes or other registered transformer.",
                GROUP_OTHER, srCounter++, // NOPMD
                ConfigDef.Width.NONE, OUTPUT_FORMAT_KEY);

        configDef.define(VALUE_SERIALIZER, ConfigDef.Type.CLASS, "io.confluent.kafka.serializers.KafkaAvroSerializer",
                ConfigDef.Importance.MEDIUM, "Value serializer", GROUP_OTHER, srCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, VALUE_SERIALIZER);
    }

    private static void addOtherConfig(final S3SourceConfigDef configDef) {
        int awsOtherGroupCounter = 0;
        configDef.define(FETCH_PAGE_SIZE, ConfigDef.Type.INT, 10, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Fetch page size", GROUP_OTHER, awsOtherGroupCounter++, // NOPMD
                                                                                                     // UnusedAssignment
                ConfigDef.Width.NONE, FETCH_PAGE_SIZE);
        configDef.define(MAX_POLL_RECORDS, ConfigDef.Type.INT, 500, ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM, "Max poll records", GROUP_OTHER, awsOtherGroupCounter++, // NOPMD
                                                                                                      // UnusedAssignment
                ConfigDef.Width.NONE, MAX_POLL_RECORDS);
        configDef.define(KEY_CONVERTER, ConfigDef.Type.CLASS, "org.apache.kafka.connect.converters.ByteArrayConverter",
                ConfigDef.Importance.MEDIUM, "Key converter", GROUP_OTHER, awsOtherGroupCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, KEY_CONVERTER);
        configDef.define(VALUE_CONVERTER, ConfigDef.Type.CLASS,
                "org.apache.kafka.connect.converters.ByteArrayConverter", ConfigDef.Importance.MEDIUM,
                "Value converter", GROUP_OTHER, awsOtherGroupCounter++, // NOPMD
                // UnusedAssignment
                ConfigDef.Width.NONE, VALUE_CONVERTER);
    }

    private static void addOffsetStorageConfig(final ConfigDef configDef) {
        configDef.define(TARGET_TOPIC_PARTITIONS, ConfigDef.Type.STRING, "0", new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : 0,1", GROUP_OFFSET_TOPIC, 0, ConfigDef.Width.NONE,
                TARGET_TOPIC_PARTITIONS);
        configDef.define(TARGET_TOPICS, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM, "eg : connect-storage-offsets", GROUP_OFFSET_TOPIC, 0,
                ConfigDef.Width.NONE, TARGET_TOPICS);
    }

    protected static class AwsRegionValidator implements ConfigDef.Validator {
        private static final String SUPPORTED_AWS_REGIONS = Arrays.stream(Regions.values())
                .map(Regions::getName)
                .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                final Region region = RegionUtils.getRegion(valueStr);
                if (!RegionUtils.getRegions().contains(region)) {
                    throw new ConfigException(name, valueStr, "supported values are: " + SUPPORTED_AWS_REGIONS);
                }
            }
        }
    }

    private static class BucketNameValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            try {
                if (value != null) {
                    BucketNameUtils.validateBucketName((String) value);
                }
            } catch (final IllegalArgumentException e) {
                throw new ConfigException("Illegal bucket name: " + e.getMessage());
            }
        }
    }

    private void validate() {
        LOGGER.debug("Validating config.");
    }

    public long getS3RetryBackoffDelayMs() {
        return getLong(AWS_S3_RETRY_BACKOFF_DELAY_MS_CONFIG);
    }

    public long getS3RetryBackoffMaxDelayMs() {
        return getLong(AWS_S3_RETRY_BACKOFF_MAX_DELAY_MS_CONFIG);
    }

    public int getS3RetryBackoffMaxRetries() {
        return getInt(AWS_S3_RETRY_BACKOFF_MAX_RETRIES_CONFIG);
    }

    public String getAwsS3BucketName() {
        return getString(AWS_S3_BUCKET_NAME_CONFIG);
    }

    String getTargetTopics() {
        return getString(TARGET_TOPICS);
    }

    String getTargetTopicPartitions() {
        return getString(TARGET_TOPIC_PARTITIONS);
    }

    String getSchemaRegistryUrl() {
        return getString(SCHEMA_REGISTRY_URL);
    }
}
