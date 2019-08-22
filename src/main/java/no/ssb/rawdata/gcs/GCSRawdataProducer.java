package no.ssb.rawdata.gcs;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

class GCSRawdataProducer implements RawdataProducer {

    final String bucket;
    final String gcsFolder;
    final RawdataClient lmdbClient;
    final Path localLmdbTopicFolder;
    final String topic;
    final RawdataProducer lmdbProducer;

    GCSRawdataProducer(String bucket, String gcsFolder, RawdataClient lmdbClient, Path localLmdbTopicFolder, String topic) {
        this.bucket = bucket;
        this.gcsFolder = gcsFolder;
        this.lmdbClient = lmdbClient;
        this.localLmdbTopicFolder = localLmdbTopicFolder;
        this.topic = topic;
        lmdbProducer = lmdbClient.producer(topic);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String lastPosition() throws RawdataClosedException {
        return lmdbProducer.lastPosition();
    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        return lmdbProducer.builder();
    }

    @Override
    public RawdataMessage buffer(RawdataMessage.Builder builder) throws RawdataClosedException {
        return lmdbProducer.buffer(builder);
    }

    @Override
    public RawdataMessage buffer(RawdataMessage message) throws RawdataClosedException {
        return lmdbProducer.buffer(message);
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataContentNotBufferedException {
        lmdbProducer.publish(positions);
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        return lmdbProducer.publishAsync(positions);
    }

    @Override
    public boolean isClosed() {
        return lmdbProducer.isClosed();
    }

    @Override
    public void close() throws Exception {
        lmdbProducer.close();
    }
}
