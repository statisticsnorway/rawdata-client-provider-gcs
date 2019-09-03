package no.ssb.rawdata.gcs;

import com.google.cloud.storage.BlobId;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

class GCSRawdataClient implements RawdataClient {

    final Path localLmdbTopicFolder;
    final String bucket;
    final String gcsFolder;
    final AtomicBoolean closed = new AtomicBoolean(false);

    final RawdataClient stagingRawdataClient;
    final List<GCSRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<GCSRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    GCSRawdataClient(RawdataClient stagingRawdataClient, Path localLmdbTopicFolder, String bucket, String gcsFolder) {
        this.stagingRawdataClient = stagingRawdataClient;
        this.localLmdbTopicFolder = localLmdbTopicFolder;
        this.bucket = bucket;
        this.gcsFolder = gcsFolder;
    }

    BlobId getBlobId(String topic) {
        return BlobId.of(bucket, gcsFolder + "/" + topic);
    }

    @Override
    public RawdataProducer producer(String topic) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        GCSRawdataProducer producer = new GCSRawdataProducer(bucket, gcsFolder, stagingRawdataClient, localLmdbTopicFolder, topic);
        producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, ULID.Value initialUlid, boolean inclusive) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        GCSRawdataConsumer consumer = new GCSRawdataConsumer(bucket, gcsFolder, stagingRawdataClient, localLmdbTopicFolder, topic, initialUlid, inclusive);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public ULID.Value ulidOfPosition(String topic, String position) {
        return stagingRawdataClient.ulidOfPosition(topic, position);
    }

    @Override
    public RawdataMessage lastMessage(String topic) throws RawdataClosedException {
        return stagingRawdataClient.lastMessage(topic);
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
            stagingRawdataClient.close();
        }
    }
}
