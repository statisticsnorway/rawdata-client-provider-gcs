package no.ssb.rawdata.gcs;

import com.google.cloud.storage.BlobId;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNoSuchPositionException;
import no.ssb.rawdata.api.RawdataProducer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.ofNullable;

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
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        ULID.Value value = ofNullable((GCSCursor) cursor).map(c -> c.ulid).orElse(null);
        boolean inclusive = ofNullable((GCSCursor) cursor).map(c -> c.inclusive).orElse(true);
        GCSRawdataConsumer consumer = new GCSRawdataConsumer(bucket, gcsFolder, stagingRawdataClient, localLmdbTopicFolder, topic, value, inclusive);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive) {
        return new GCSCursor(ulid, inclusive);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) throws RawdataNoSuchPositionException {
        return cursorOf(topic, ulidOfPosition(position, approxTimestamp, tolerance), inclusive);
    }

    private ULID.Value ulidOfPosition(String position, long approxTimestamp, Duration tolerance) {
        throw new UnsupportedOperationException();
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
