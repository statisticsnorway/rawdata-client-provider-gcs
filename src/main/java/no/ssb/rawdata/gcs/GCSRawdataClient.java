package no.ssb.rawdata.gcs;

import com.google.cloud.storage.Blob;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNoSuchPositionException;
import no.ssb.rawdata.api.RawdataProducer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class GCSRawdataClient implements RawdataClient {

    final AtomicBoolean closed = new AtomicBoolean(false);

    final String bucket;
    final Path tmpFileFolder;
    final long stagingMaxSeconds;
    final long stagingMaxBytes;
    final int gcsFileListingMaxIntervalSeconds;

    final List<GCSRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<GCSRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    GCSRawdataClient(String bucket, Path tmpFileFolder, long stagingMaxSeconds, long stagingMaxBytes, int gcsFileListingMaxIntervalSeconds) {
        this.bucket = bucket;
        this.tmpFileFolder = tmpFileFolder;
        this.stagingMaxSeconds = stagingMaxSeconds;
        this.stagingMaxBytes = stagingMaxBytes;
        this.gcsFileListingMaxIntervalSeconds = gcsFileListingMaxIntervalSeconds;
    }

    @Override
    public RawdataProducer producer(String topic) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        GCSRawdataProducer producer = new GCSRawdataProducer(bucket, tmpFileFolder, stagingMaxSeconds, stagingMaxBytes, topic);
        producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        GCSRawdataConsumer consumer = new GCSRawdataConsumer(bucket, topic, (GCSCursor) cursor, gcsFileListingMaxIntervalSeconds);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive) {
        return new GCSCursor(ulid, inclusive);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) throws RawdataNoSuchPositionException {
        return cursorOf(topic, ulidOfPosition(topic, position, approxTimestamp, tolerance), inclusive);
    }

    private ULID.Value ulidOfPosition(String topic, String position, long approxTimestamp, Duration tolerance) throws RawdataNoSuchPositionException {
        ULID.Value lowerBoundUlid = RawdataConsumer.beginningOf(approxTimestamp - tolerance.toMillis());
        ULID.Value upperBoundUlid = RawdataConsumer.beginningOf(approxTimestamp + tolerance.toMillis());
        try (GCSRawdataConsumer consumer = new GCSRawdataConsumer(bucket, topic, new GCSCursor(lowerBoundUlid, true), gcsFileListingMaxIntervalSeconds)) {
            RawdataMessage message;
            while ((message = consumer.receive(2, TimeUnit.SECONDS)) != null) {
                if (message.timestamp() > upperBoundUlid.timestamp()) {
                    throw new RawdataNoSuchPositionException(
                            String.format("Unable to find position, reached upper-bound. Time-range=[%s,%s), position=%s",
                                    formatTimestamp(lowerBoundUlid.timestamp()),
                                    formatTimestamp(upperBoundUlid.timestamp()),
                                    position));
                }
                if (position.equals(message.position())) {
                    return message.ulid(); // found matching position
                }
            }
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RawdataNoSuchPositionException(
                String.format("Unable to find position, reached end-of-stream. Time-range=[%s,%s), position=%s",
                        formatTimestamp(lowerBoundUlid.timestamp()),
                        formatTimestamp(upperBoundUlid.timestamp()),
                        position));
    }

    String formatTimestamp(long timestamp) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss.SSS");
        LocalDateTime dt = LocalDateTime.ofInstant(new Date(timestamp).toInstant(), ZoneOffset.UTC);
        return dt.format(dtf);
    }

    @Override
    public RawdataMessage lastMessage(String topic) throws RawdataClosedException {
        NavigableMap<Long, Blob> topicBlobs = GCSRawdataUtils.getTopicBlobs(bucket, topic);
        if (topicBlobs.isEmpty()) {
            return null;
        }
        Blob blob = topicBlobs.lastEntry().getValue();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(GCSRawdataProducer.schema);
        DataFileReader<GenericRecord> dataFileReader;
        try {
            dataFileReader = new DataFileReader<>(new GCSSeekableInput(blob.reader(), blob.getSize()), datumReader);
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
            }
            return record == null ? null : GCSRawdataConsumer.toRawdataMessage(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            for (GCSRawdataProducer producer : producers) {
                producer.close();
            }
            producers.clear();
            for (GCSRawdataConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();
        }
    }
}
