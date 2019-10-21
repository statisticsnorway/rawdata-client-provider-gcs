package no.ssb.rawdata.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;

import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class GCSTopicAvroFileCache {

    final Storage storage;
    final String bucket;
    final String topic;
    final int maxGcsFileListingIntervalSeconds;

    final AtomicReference<NavigableMap<Long, Blob>> topicBlobsByFromTimestampRef = new AtomicReference<>();
    final AtomicLong timestampOfLastGCSListing = new AtomicLong(0);

    GCSTopicAvroFileCache(Storage storage, String bucket, String topic, int maxGcsFileListingIntervalSeconds) {
        this.storage = storage;
        this.bucket = bucket;
        this.topic = topic;
        this.maxGcsFileListingIntervalSeconds = maxGcsFileListingIntervalSeconds;
    }

    NavigableMap<Long, Blob> blobsByTimestamp() {
        if ((System.currentTimeMillis() - timestampOfLastGCSListing.get()) > TimeUnit.SECONDS.toMillis(maxGcsFileListingIntervalSeconds)) {
            // refresh entire cache by listing all files from GCS
            topicBlobsByFromTimestampRef.set(new GCSRawdataUtils(storage).getTopicBlobs(bucket, topic));
            timestampOfLastGCSListing.set(System.currentTimeMillis());
        }
        return topicBlobsByFromTimestampRef.get();
    }

    void clear() {
        topicBlobsByFromTimestampRef.set(null);
        timestampOfLastGCSListing.set(0);
    }
}