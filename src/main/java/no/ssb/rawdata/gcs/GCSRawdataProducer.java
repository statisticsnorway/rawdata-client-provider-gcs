package no.ssb.rawdata.gcs;

import com.google.cloud.storage.BlobId;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    final String bucket;
    final Path tmpFolder;
    final String topic;

    final Map<String, GCSRawdataMessage.Builder> buffer = new ConcurrentHashMap<>();

    final AtomicReference<DataFileWriter<GenericRecord>> dataFileWriterRef = new AtomicReference<>();
    final AtomicReference<Path> pathRef = new AtomicReference<>();

    GCSRawdataProducer(String bucket, Path tmpFolder, String topic) {
        this.bucket = bucket;
        this.tmpFolder = tmpFolder;
        this.topic = topic;
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
        File file = new File(tmpFolder.toFile(), topic + "/rawdata.avro");
        try {
            Files.createDirectories(file.toPath().getParent());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (pathRef.compareAndSet(null, file.toPath())) {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriterRef.set(dataFileWriter);
            try {
                dataFileWriter.create(schema, file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        for (String position : positions) {
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

            try {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", new GenericData.Fixed(schema.getField("id").schema(), message.ulid().toBytes()));
                record.put("orderingGroup", message.orderingGroup());
                record.put("sequenceNumber", message.sequenceNumber());
                record.put("position", message.position());
                record.put("data", message.data().entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> ByteBuffer.wrap(e.getValue()))));

                dataFileWriterRef.get().append(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            DataFileWriter<GenericRecord> dataFileWriter = dataFileWriterRef.getAndSet(null);
            if (dataFileWriter != null) {
                dataFileWriter.close();
            }
            Path path = pathRef.getAndSet(null);
            if (path != null) {
                String fileName = computeFilenameOfLocalAvroFile(path.toFile());
                GCSRawdataUtils.copyLocalFileToGCSBlob(path.toFile(), BlobId.of(bucket, topic + "/" + fileName));
            }
            buffer.clear();
        }
    }

    private String computeFilenameOfLocalAvroFile(File file) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            if (!dataFileReader.hasNext()) {
                return null;
            }
            GenericRecord firstRecord = dataFileReader.next();
            GenericRecord lastRecord = getLastAvroRecordInFile(dataFileReader);
            if (lastRecord == null) {
                lastRecord = firstRecord;
            }

            GenericData.Fixed firstId = (GenericData.Fixed) firstRecord.get("id");
            ULID.Value firstUlid = ULID.fromBytes(firstId.bytes());

            GenericData.Fixed lastId = (GenericData.Fixed) lastRecord.get("id");
            ULID.Value lastUlid = ULID.fromBytes(lastId.bytes());

            String fromTime = GCSRawdataUtils.formatTimestamp(firstUlid.timestamp());
            String toTime = GCSRawdataUtils.formatTimestamp(lastUlid.timestamp() + 1);

            return fromTime + "_" + toTime + "_.avro";
        }
    }

    private GenericRecord getLastAvroRecordInFile(DataFileReader<GenericRecord> dataFileReader) throws IOException {
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            dataFileReader.next(record);
        }
        return record;
    }

}
