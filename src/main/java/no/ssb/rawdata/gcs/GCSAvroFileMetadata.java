package no.ssb.rawdata.gcs;

import com.google.cloud.storage.BlobId;
import de.huxhorn.sulky.ulid.ULID;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class GCSAvroFileMetadata {
    final AtomicReference<ULID.Value> idOfFirstRecord = new AtomicReference<>();
    final AtomicReference<String> positionOfFirstRecord = new AtomicReference<>();
    final AtomicLong count = new AtomicLong(0);

    void clear() {
        idOfFirstRecord.set(null);
        positionOfFirstRecord.set(null);
        count.set(0);
    }

    void incrementCounter(long increment) {
        count.addAndGet(increment);
    }

    long getCount() {
        return count.get();
    }

    void setPositionOfFirstRecord(String position) {
        positionOfFirstRecord.compareAndSet(null, position);
    }

    String getPositionOfFirstRecord() {
        return positionOfFirstRecord.get();
    }

    void setIdOfFirstRecord(ULID.Value id) {
        idOfFirstRecord.compareAndSet(null, id);
    }

    ULID.Value getIdOfFirstRecord() {
        return idOfFirstRecord.get();
    }

    String toFilename() {
        String fromTime = GCSRawdataUtils.formatTimestamp(getIdOfFirstRecord().timestamp());
        return fromTime + "_" + getCount() + "_" + getPositionOfFirstRecord() + ".avro";
    }

    BlobId toBlobId(String bucket, String topic) {
        return BlobId.of(bucket, topic + "/" + toFilename());
    }
}
