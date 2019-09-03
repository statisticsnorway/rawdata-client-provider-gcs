package no.ssb.rawdata.gcs;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class GCSRawdataConsumer implements RawdataConsumer {

    final String bucket;
    final String gcsFolder;
    final RawdataClient stagingClient;
    final Path localLmdbTopicFolder;
    final String topic;
    final RawdataConsumer stagingConsumer;

    GCSRawdataConsumer(String bucket, String gcsFolder, RawdataClient stagingClient, Path localLmdbTopicFolder, String topic, ULID.Value initialUlid, boolean inclusive) {
        this.bucket = bucket;
        this.gcsFolder = gcsFolder;
        this.stagingClient = stagingClient;
        this.localLmdbTopicFolder = localLmdbTopicFolder;
        this.topic = topic;
        this.stagingConsumer = stagingClient.consumer(topic, initialUlid, inclusive);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
        return stagingConsumer.receive(timeout, unit);
    }

    @Override
    public CompletableFuture<? extends RawdataMessage> receiveAsync() {
        return stagingConsumer.receiveAsync();
    }

    @Override
    public void seek(long timestamp) {
        stagingConsumer.seek(timestamp);
    }

    @Override
    public boolean isClosed() {
        return stagingConsumer.isClosed();
    }

    @Override
    public void close() throws Exception {
        stagingConsumer.close();
    }
}
