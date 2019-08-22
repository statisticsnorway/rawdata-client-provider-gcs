package no.ssb.rawdata.gcs;

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
    final RawdataClient lmdbClient;
    final Path localLmdbTopicFolder;
    final String topic;
    final RawdataConsumer lmdbConsumer;

    GCSRawdataConsumer(String bucket, String gcsFolder, RawdataClient lmdbClient, Path localLmdbTopicFolder, String topic, String initialPosition) {
        this.bucket = bucket;
        this.gcsFolder = gcsFolder;
        this.lmdbClient = lmdbClient;
        this.localLmdbTopicFolder = localLmdbTopicFolder;
        this.topic = topic;
        this.lmdbConsumer = lmdbClient.consumer(topic, initialPosition);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
        return lmdbConsumer.receive(timeout, unit);
    }

    @Override
    public CompletableFuture<? extends RawdataMessage> receiveAsync() {
        return lmdbConsumer.receiveAsync();
    }

    @Override
    public boolean isClosed() {
        return lmdbConsumer.isClosed();
    }

    @Override
    public void close() throws Exception {
        lmdbConsumer.close();
    }
}
