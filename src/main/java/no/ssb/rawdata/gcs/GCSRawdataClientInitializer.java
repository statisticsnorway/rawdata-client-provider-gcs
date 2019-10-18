package no.ssb.rawdata.gcs;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;

import java.nio.file.Path;
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
                "bucket",
                "local-temp-folder",
                "staging.retention.max.days",
                "staging.retention.max.hours",
                "staging.retention.max.minutes",
                "staging.retention.buffer.days",
                "staging.retention.buffer.hours",
                "staging.retention.buffer.minutes"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        String bucket = configuration.get("bucket");
        Path localTempFolder = Path.of(configuration.get("local-temp-folder"));
        return new GCSRawdataClient(bucket, localTempFolder);
    }
}
