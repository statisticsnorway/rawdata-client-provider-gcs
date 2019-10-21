package no.ssb.rawdata.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
                "staging.max.seconds",
                "staging.max.bytes",
                "gcs.listing.max-interval-seconds"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        String bucket = configuration.get("bucket");
        Path localTempFolder = Path.of(configuration.get("local-temp-folder"));
        long stagingMaxSeconds = Long.parseLong(configuration.get("staging.max.seconds"));
        long stagingMaxBytes = Long.parseLong(configuration.get("staging.max.bytes"));
        int gcsFileListingMaxIntervalSeconds = Integer.parseInt(configuration.get("gcs.listing.max-interval-seconds"));

        Storage storage = StorageOptions.getDefaultInstance().getService(); // TODO, replace with credentials from configuration

        return new GCSRawdataClient(storage, bucket, localTempFolder, stagingMaxSeconds, stagingMaxBytes, gcsFileListingMaxIntervalSeconds);
    }
}
