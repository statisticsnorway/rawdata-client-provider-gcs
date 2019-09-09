package no.ssb.rawdata.gcs;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import no.ssb.service.provider.api.ProviderName;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

@ProviderName("gcs")
public class GCSRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public String providerId() {
        return "gcs";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "gcs.bucket.url",
                "gcs.bucket.folder",
                "staging.retention.max.days",
                "staging.retention.max.hours",
                "staging.retention.max.minutes",
                "staging.retention.buffer.days",
                "staging.retention.buffer.hours",
                "staging.retention.buffer.minutes",
                "lmdb.folder",
                "lmdb.map-size",
                "lmdb.message.file.max-size",
                "lmdb.topic.write-concurrency",
                "lmdb.topic.read-concurrency"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        Path localLmdbTopicFolder = Paths.get(configuration.get("lmdb.folder"));
        String bucket = configuration.get("gcs.bucket.url");
        String gcsFolder = configuration.get("gcs.bucket.folder");
        RawdataClient stagingClient = ProviderConfigurator.configure(configuration, "lmdb", RawdataClientInitializer.class);
        return new GCSRawdataClient(stagingClient, localLmdbTopicFolder, bucket, gcsFolder);
    }
}
