package no.ssb.rawdata.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class GCSTest {

    static final Storage storage;

    static {
        storage = StorageOptions.getDefaultInstance().getService();
    }

    @Test
    public void create() {

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("-YYYY-MM-dd-HHmmssSSS");
        LocalDateTime dt = LocalDateTime.now(ZoneOffset.UTC);
        String dtString = dt.format(dtf);
        final String fileName = "test-1" + dtString + ".txt";

        System.out.printf("%s%n", fileName);

        byte[] content = "Hello GCS!".getBytes(StandardCharsets.UTF_8);
        BlobId blobId = BlobId.of("kim_gaarder_rawdata_experiment", fileName);
        Blob blob = storage.create(BlobInfo.newBuilder(blobId)
                .setContentType("text/plain")
                .build(), content);
    }
}
