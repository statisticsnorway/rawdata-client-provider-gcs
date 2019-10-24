package no.ssb.rawdata.gcs;

import com.google.cloud.storage.Storage;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

class GCSRawdataProducer implements RawdataProducer {

    static final Schema schema = SchemaBuilder.record("RawdataMessage")
            .fields()
            .name("id").type().fixed("ulid").size(16).noDefault()
            .name("orderingGroup").type().nullable().stringType().noDefault()
            .name("sequenceNumber").type().longType().longDefault(0)
            .name("position").type().stringType().noDefault()
            .name("data").type().map().values().bytesType().noDefault()
            .endRecord();

    final AtomicBoolean closed = new AtomicBoolean(false);

    final ULID ulid = new ULID();
    final AtomicReference<ULID.Value> prevUlid = new AtomicReference<>(ulid.nextValue());

    final GCSRawdataUtils gcsRawdataUtils;
    final String bucket;
    final Path tmpFolder;
    final long stagingMaxSeconds;
    final long stagingMaxBytes;
    final String topic;

    final Map<String, GCSRawdataMessage.Builder> buffer = new ConcurrentHashMap<>();

    final AtomicReference<DataFileWriter<GenericRecord>> dataFileWriterRef = new AtomicReference<>();
    final AtomicReference<Path> pathRef = new AtomicReference<>();

    final AtomicLong timestampOfFirstMessageInWindow = new AtomicLong(-1);
    final GCSAvroFileMetadata activeAvrofileMetadata = new GCSAvroFileMetadata();

    final ReentrantLock lock = new ReentrantLock();

    GCSRawdataProducer(Storage storage, String bucket, Path tmpFolder, long stagingMaxSeconds, long stagingMaxBytes, String topic) {
        this.gcsRawdataUtils = new GCSRawdataUtils(storage);
        this.bucket = bucket;
        this.tmpFolder = tmpFolder;
        this.stagingMaxSeconds = stagingMaxSeconds;
        this.stagingMaxBytes = stagingMaxBytes;
        this.topic = topic;
        try {
            Path topicFolder = tmpFolder.resolve(topic);
            Files.createDirectories(topicFolder);
            Path path = Files.createTempFile(topicFolder, "", ".avro");
            pathRef.set(path);
            createOrOverwriteLocalAvroFile(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createOrOverwriteLocalAvroFile(Path path) {
        try {
            if (!lock.tryLock(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Unable to acquire lock within 5 minutes");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            activeAvrofileMetadata.clear();
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.setSyncInterval(2 * 1024 * 1024); // 2 MiB
            dataFileWriter.setFlushOnEveryBlock(true);
            dataFileWriterRef.set(dataFileWriter);
            try {
                dataFileWriter.create(schema, path.toFile());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } finally {
            lock.unlock();
        }
    }

    private void closeAvroFileAndUploadToGCS() {
        try {
            if (!lock.tryLock(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Unable to acquire lock within 5 minutes");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            DataFileWriter<GenericRecord> dataFileWriter = dataFileWriterRef.getAndSet(null);
            if (dataFileWriter != null) {
                dataFileWriter.flush();
                dataFileWriter.close();
            }
            Path path = pathRef.get();
            if (path != null) {
                if (activeAvrofileMetadata.getCount() > 0) {
                    gcsRawdataUtils.copyLocalFileToGCSBlob(path.toFile(), activeAvrofileMetadata.toBlobId(bucket, topic));
                } else {
                    // no records, no need to write file to GCS
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        return new GCSRawdataMessage.Builder();
    }

    @Override
    public RawdataProducer buffer(RawdataMessage.Builder _builder) throws RawdataClosedException {
        GCSRawdataMessage.Builder builder = (GCSRawdataMessage.Builder) _builder;
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        buffer.put(builder.position, builder);
        return this;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        try {
            if (!lock.tryLock(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Unable to acquire lock within 5 minutes");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            for (String position : positions) {
                long now = System.currentTimeMillis();
                timestampOfFirstMessageInWindow.compareAndSet(-1, now);

                boolean timeLimitExceeded = timestampOfFirstMessageInWindow.get() + 1000 * stagingMaxSeconds < now;
                if (timeLimitExceeded) {
                    closeAvroFileAndUploadToGCS();
                    createOrOverwriteLocalAvroFile(pathRef.get());
                    timestampOfFirstMessageInWindow.set(now);
                }

                GCSRawdataMessage.Builder builder = buffer.remove(position);
                if (builder == null) {
                    throw new RawdataNotBufferedException(String.format("position %s has not been buffered", position));
                }
                if (builder.ulid == null) {
                    ULID.Value value = RawdataProducer.nextMonotonicUlid(ulid, prevUlid.get());
                    builder.ulid(value);
                }
                GCSRawdataMessage message = builder.build();
                prevUlid.set(message.ulid());

                activeAvrofileMetadata.setIdOfFirstRecord(message.ulid());
                activeAvrofileMetadata.setPositionOfFirstRecord(position);

                try {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("id", new GenericData.Fixed(schema.getField("id").schema(), message.ulid().toBytes()));
                    record.put("orderingGroup", message.orderingGroup());
                    record.put("sequenceNumber", message.sequenceNumber());
                    record.put("position", message.position());
                    record.put("data", message.data().entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> ByteBuffer.wrap(e.getValue()))));

                    dataFileWriterRef.get().append(record);
                    activeAvrofileMetadata.incrementCounter(1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                boolean sizeLimitExceeded = pathRef.get().toFile().length() > stagingMaxBytes;
                if (sizeLimitExceeded) {
                    closeAvroFileAndUploadToGCS();
                    createOrOverwriteLocalAvroFile(pathRef.get());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return CompletableFuture.runAsync(() -> publish(positions));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeAvroFileAndUploadToGCS();
            buffer.clear();
        }
    }
}
