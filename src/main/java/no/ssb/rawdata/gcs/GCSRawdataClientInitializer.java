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
                "local-temp-folder",
                "avro-file.max.seconds",
                "avro-file.max.bytes",
                "avro-file.sync.interval",
                "gcs.bucket-name",
                "gcs.listing.min-interval-seconds",
                "gcs.service-account.key-file"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        String bucket = configuration.get("gcs.bucket-name");
        Path localTempFolder = Path.of(configuration.get("local-temp-folder"));
        long avroMaxSeconds = Long.parseLong(configuration.get("avro-file.max.seconds"));
        long avroMaxBytes = Long.parseLong(configuration.get("avro-file.max.bytes"));
        int avroSyncInterval = Integer.parseInt(configuration.get("avro-file.sync.interval"));
        int gcsFileListingMaxIntervalSeconds = Integer.parseInt(configuration.get("gcs.listing.min-interval-seconds"));
        Path credPath = Path.of(configuration.get("gcs.service-account.key-file"));
        return new GCSRawdataClient(credPath, bucket, localTempFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, gcsFileListingMaxIntervalSeconds);
    }
}
