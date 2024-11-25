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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.MAX_POLL_RECORDS;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.source.config.S3ClientFactory;
import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.input.Transformer;
import io.aiven.kafka.connect.s3.source.input.TransformerFactory;
import io.aiven.kafka.connect.s3.source.utils.FileReader;
import io.aiven.kafka.connect.s3.source.utils.OffsetManager;
import io.aiven.kafka.connect.s3.source.utils.RecordProcessor;
import io.aiven.kafka.connect.s3.source.utils.S3OffsetManagerEntry;
import io.aiven.kafka.connect.s3.source.utils.S3SourceRecord;
import io.aiven.kafka.connect.s3.source.utils.SourceRecordIterator;
import io.aiven.kafka.connect.s3.source.utils.Version;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3SourceTask is a Kafka Connect SourceTask implementation that reads from source-s3 buckets and generates Kafka
 * Connect records.
 */
@SuppressWarnings({ "PMD.TooManyMethods", "PMD.ExcessiveImports" })
public class S3SourceTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SourceTask.class);

    /**
     * @deprecated use {@link S3OffsetManagerEntry#BUCKET}.
     */
    @Deprecated
    public static final String BUCKET = S3OffsetManagerEntry.BUCKET;
    /**
     * @deprecated use {@link S3OffsetManagerEntry#TOPIC}.
     */
    @Deprecated
    public static final String TOPIC = S3OffsetManagerEntry.TOPIC;

    /**
     * @deprecated use {@link S3OffsetManagerEntry#OBJECT_KEY}.
     */
    @Deprecated
    public static final String OBJECT_KEY = S3OffsetManagerEntry.OBJECT_KEY;
    /**
     * @deprecated use {@link S3OffsetManagerEntry#PARTITION}.
     */
    @Deprecated
    public static final String PARTITION = S3OffsetManagerEntry.PARTITION;

    private static final long S_3_POLL_INTERVAL_MS = 10_000L;
    private static final long ERROR_BACKOFF = 1000L;

    private S3SourceConfig s3SourceConfig;
    private AmazonS3 s3Client;

    private Iterator<S3SourceRecord> sourceRecordIterator;
    private Optional<Converter> keyConverter;

    private Converter valueConverter;

    private Transformer transformer;

    private boolean taskInitialized;

    private final AtomicBoolean connectorStopped = new AtomicBoolean();
    private final S3ClientFactory s3ClientFactory = new S3ClientFactory();

    private final Object pollLock = new Object();
    private FileReader fileReader;
    private final Set<String> failedObjectKeys = new HashSet<>();

    private OffsetManager offsetManager;

    @SuppressWarnings("PMD.UnnecessaryConstructor")
    public S3SourceTask() {
        super();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("S3 Source task started.");
        s3SourceConfig = new S3SourceConfig(props);
        initializeConverters();
        initializeS3Client();
        transformer = TransformerFactory.getTransformer(s3SourceConfig);
        offsetManager = new OffsetManager(context, s3SourceConfig);
        final String s3Bucket = s3SourceConfig.getString(AWS_S3_BUCKET_NAME_CONFIG);
        fileReader = new FileReader(s3SourceConfig, s3Bucket, failedObjectKeys);
        prepareReaderFromOffsetStorageReader();
        this.taskInitialized = true;
    }

    private void initializeConverters() {
        try {
            keyConverter = Optional
                    .of((Converter) Class.forName((String) s3SourceConfig.originals().get("key.converter"))
                            .getDeclaredConstructor()
                            .newInstance());
            valueConverter = (Converter) Class.forName((String) s3SourceConfig.originals().get("value.converter"))
                    .getDeclaredConstructor()
                    .newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw new ConnectException("Connect converters could not be instantiated.", e);
        }
    }

    private void initializeS3Client() {
        this.s3Client = s3ClientFactory.createAmazonS3Client(s3SourceConfig);
        LOGGER.debug("S3 client initialized");
    }

    private void prepareReaderFromOffsetStorageReader() {
        sourceRecordIterator = new SourceRecordIterator(s3SourceConfig, s3Client, offsetManager, this.transformer,
                fileReader.fetchObjectSummaries(s3Client));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        LOGGER.info("Polling for new records...");
        synchronized (pollLock) {
            final List<SourceRecord> results = new ArrayList<>(s3SourceConfig.getInt(MAX_POLL_RECORDS));

            if (connectorStopped.get()) {
                LOGGER.info("Connector has been stopped. Returning empty result list.");
                return results;
            }

            while (!connectorStopped.get()) {
                try {
                    extractSourceRecords(results);
                    LOGGER.info("Number of records extracted and sent: {}", results.size());
                    return results;
                } catch (AmazonS3Exception exception) {
                    if (exception.isRetryable()) {
                        LOGGER.warn("Retryable error encountered during polling. Waiting before retrying...",
                                exception);
                        pollLock.wait(ERROR_BACKOFF);

                        prepareReaderFromOffsetStorageReader();
                    } else {
                        LOGGER.warn("Non-retryable AmazonS3Exception occurred. Stopping polling.", exception);
                        return null; // NOPMD
                    }
                } catch (DataException exception) {
                    LOGGER.warn("DataException occurred during polling. No retries will be attempted.", exception);
                } catch (final Throwable t) { // NOPMD
                    LOGGER.error("Unexpected error encountered. Closing resources and stopping task.", t);
                    closeResources();
                    throw t;
                }
            }
            return results;
        }
    }

    private List<SourceRecord> extractSourceRecords(final List<SourceRecord> results) throws InterruptedException {
        waitForObjects();
        if (connectorStopped.get()) {
            return results;
        }
        // TODO pull RecordProcessor.processRecords into this class.
        return RecordProcessor.processRecords(sourceRecordIterator, results, s3SourceConfig, keyConverter,
                valueConverter, connectorStopped);
    }

    private void waitForObjects() throws InterruptedException {
        while (!sourceRecordIterator.hasNext() && !connectorStopped.get()) {
            LOGGER.debug("Blocking until new S3 files are available.");
            Thread.sleep(S_3_POLL_INTERVAL_MS);
            prepareReaderFromOffsetStorageReader();
        }
    }

    @Override
    public void stop() {
        this.taskInitialized = false;
        this.connectorStopped.set(true);
        synchronized (pollLock) {
            closeResources();
        }
    }

    private void closeResources() {
        s3Client.shutdown();
    }

    // below for visibility in tests
    Optional<Converter> getKeyConverter() {
        return keyConverter;
    }

    Converter getValueConverter() {
        return valueConverter;
    }

    Transformer getTransformer() {
        return transformer;
    }

    boolean isTaskInitialized() {
        return taskInitialized;
    }

    AtomicBoolean getConnectorStopped() {
        return new AtomicBoolean(connectorStopped.get());
    }
}
