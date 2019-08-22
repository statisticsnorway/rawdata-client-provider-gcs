package no.ssb.rawdata.gcs;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
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
        return Set.of("lmdb.topic.folder", "gcs.bucket.url", "gcs.bucket.folder");
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        Path localLmdbTopicFolder = Paths.get(configuration.get("lmdb.topic.folder"));
        String bucket = configuration.get("gcs.bucket.url");
        String gcsFolder = configuration.get("gcs.bucket.folder");
        return new GCSRawdataClient(localLmdbTopicFolder, bucket, gcsFolder);
    }
}
