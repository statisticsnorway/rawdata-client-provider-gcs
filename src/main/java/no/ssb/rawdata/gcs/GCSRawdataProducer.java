package no.ssb.rawdata.gcs;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

class GCSRawdataProducer implements RawdataProducer {

    final String bucket;
    final String gcsFolder;
    final RawdataClient stagingClient;
    final Path localLmdbTopicFolder;
    final String topic;
    final RawdataProducer stagingProducer;

    GCSRawdataProducer(String bucket, String gcsFolder, RawdataClient stagingClient, Path localLmdbTopicFolder, String topic) {
        this.bucket = bucket;
        this.gcsFolder = gcsFolder;
        this.stagingClient = stagingClient;
        this.localLmdbTopicFolder = localLmdbTopicFolder;
        this.topic = topic;
        stagingProducer = stagingClient.producer(topic);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        return stagingProducer.builder();
    }

    @Override
    public RawdataProducer buffer(RawdataMessage.Builder builder) throws RawdataClosedException {
        stagingProducer.buffer(builder);
        return this;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        stagingProducer.publish(positions);
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        return stagingProducer.publishAsync(positions);
    }

    @Override
    public boolean isClosed() {
        return stagingProducer.isClosed();
    }

    @Override
    public void close() throws Exception {
        stagingProducer.close();
    }
}
