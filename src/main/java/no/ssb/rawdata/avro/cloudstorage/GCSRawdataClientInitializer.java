package no.ssb.rawdata.avro.cloudstorage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.avro.AvroRawdataClient;
import no.ssb.rawdata.avro.AvroRawdataUtils;
import no.ssb.service.provider.api.ProviderName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
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
        Path serviceAccountKeyPath = Path.of(configuration.get("gcs.service-account.key-file"));
        AvroRawdataUtils readOnlyGcsRawdataUtils = new GCSRawdataUtils(getReadOnlyStorage(serviceAccountKeyPath), bucket);
        AvroRawdataUtils readWriteGcsRawdataUtils = new GCSRawdataUtils(getWritableStorage(serviceAccountKeyPath), bucket);
        return new AvroRawdataClient(localTempFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, gcsFileListingMaxIntervalSeconds, readOnlyGcsRawdataUtils, readWriteGcsRawdataUtils);
    }

    static Storage getWritableStorage(Path serviceAccountKeyPath) {
        ServiceAccountCredentials sourceCredentials;
        try {
            sourceCredentials = ServiceAccountCredentials.fromStream(Files.newInputStream(serviceAccountKeyPath, StandardOpenOption.READ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        GoogleCredentials scopedCredentials = sourceCredentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.read_write"));
        Storage storage = StorageOptions.newBuilder().setCredentials(scopedCredentials).build().getService();
        return storage;
    }

    static Storage getReadOnlyStorage(Path serviceAccountKeyPath) {
        ServiceAccountCredentials sourceCredentials;
        try {
            sourceCredentials = ServiceAccountCredentials.fromStream(Files.newInputStream(serviceAccountKeyPath, StandardOpenOption.READ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        GoogleCredentials scopedCredentials = sourceCredentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.read_only"));
        Storage storage = StorageOptions.newBuilder().setCredentials(scopedCredentials).build().getService();
        return storage;
    }
}
