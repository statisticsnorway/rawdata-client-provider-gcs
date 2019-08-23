package no.ssb.rawdata.gcs;

import com.google.cloud.storage.BlobId;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

class GCSRawdataClient implements RawdataClient {

    final Path localLmdbTopicFolder;
    final String bucket;
    final String gcsFolder;
    final AtomicBoolean closed = new AtomicBoolean(false);

    final RawdataClient lmdbRawdataClient;
    final List<GCSRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<GCSRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    GCSRawdataClient(Path localLmdbTopicFolder, String bucket, String gcsFolder) {
        this.localLmdbTopicFolder = localLmdbTopicFolder;
        this.bucket = bucket;
        this.gcsFolder = gcsFolder;
        lmdbRawdataClient = ProviderConfigurator.configure(
                Map.of("lmdb.folder", localLmdbTopicFolder.toString()),
                "lmdb",
                RawdataClientInitializer.class
        );
    }

    private BlobId getBlobId(String topic) {
        return BlobId.of(bucket, gcsFolder + "/" + topic);
    }

    @Override
    public RawdataProducer producer(String topic) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        GCSRawdataProducer producer = new GCSRawdataProducer(bucket, gcsFolder, lmdbRawdataClient, localLmdbTopicFolder, topic);
        producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, String initialPosition) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        GCSRawdataConsumer consumer = new GCSRawdataConsumer(bucket, gcsFolder, lmdbRawdataClient, localLmdbTopicFolder, topic, initialPosition);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            for (GCSRawdataProducer producer : producers) {
                producer.close();
            }
            producers.clear();
            for (GCSRawdataConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();
            lmdbRawdataClient.close();
        }
    }
}
