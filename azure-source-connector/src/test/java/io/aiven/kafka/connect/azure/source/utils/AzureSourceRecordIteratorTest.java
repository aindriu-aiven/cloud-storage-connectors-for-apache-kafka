package io.aiven.kafka.connect.azure.source.utils;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.common.config.SourceCommonConfig;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIterator;
import io.aiven.kafka.connect.common.source.AbstractSourceRecordIteratorTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AzureSourceRecordIteratorTest extends AbstractSourceRecordIteratorTest<BlobItem, String, AzureOffsetManagerEntry, AzureBlobSourceRecord> {

    private AzureBlobClient azureBlobClient;

    @Override
    protected String createKFrom(String key) {
        return key;
    }

    @Override
    protected AbstractSourceRecordIterator<BlobItem, String, AzureOffsetManagerEntry, AzureBlobSourceRecord> createSourceRecordIterator(SourceCommonConfig mockConfig, OffsetManager<AzureOffsetManagerEntry> mockOffsetManager, Transformer transformer) {
        return new SourceRecordIterator((AzureBlobSourceConfig) mockConfig, mockOffsetManager, transformer, azureBlobClient);
    }

    @Override
    protected ClientMutator<BlobItem, String, ?> createClientMutator() {
        return new Mutator();
    }

    @Override
    protected SourceCommonConfig createMockedConfig() {
        AzureBlobSourceConfig config = mock(AzureBlobSourceConfig.class);
        when(config.getAzureContainerName()).thenReturn("container1");
        return config;
    }

    private class Mutator extends AbstractSourceRecordIteratorTest.ClientMutator<BlobItem, String, Mutator> {

        @Override
        protected BlobItem createObject(String key, ByteBuffer data) {
            BlobItem blobItem = new BlobItem();
            blobItem.setName(key);
            BlobItemProperties blobItemProperties = new BlobItemProperties();
            blobItemProperties.setContentLength((long)data.capacity());
            blobItem.setProperties(blobItemProperties);
            return blobItem;
        }

        /**
         * Create a S3 ListObjectV2Respone object from a single block.
         * @return the new ListObjectV2Response
         */
        private Stream<BlobItem> dequeueData() {
            // Dequeue a block.  Sets the objects.
            dequeueBlock();
            return objects.stream();
        }

        private Flux<ByteBuffer> getStream(String key) {
            ByteBuffer buffer = getData(key);
            if (buffer != null) {
                return Flux.just(buffer);
            }
            return Flux.empty();
        }

        @Override
        public void build() {
            // if there are objects create the last block from them.
            if (!objects.isEmpty()) {
                endOfBlock();
            }

            azureBlobClient = mock(AzureBlobClient.class);
            when(azureBlobClient.getAzureBlobStream()).thenAnswer( env -> dequeueData());
            when(azureBlobClient.getBlob(anyString())).thenAnswer( env -> getStream(env.getArgument(0)));
        }
    }
}
