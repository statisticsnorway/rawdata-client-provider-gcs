package no.ssb.rawdata.gcs;

import com.google.cloud.storage.Blob;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

class GCSRawdataConsumer implements RawdataConsumer {

    final String topic;
    final AtomicReference<NavigableMap<Long, Blob>> topicBlobsByFromTimestampRef = new AtomicReference<>();
    final AtomicReference<Long> activeBlobFromKeyRef = new AtomicReference<>(null);
    final AtomicReference<DataFileReader<GenericRecord>> activeBlobDataFileReaderRef = new AtomicReference<>(null);
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Deque<GCSRawdataMessage> preloadedMessages = new ConcurrentLinkedDeque<>();

    GCSRawdataConsumer(String bucket, String topic, GCSCursor cursor) {
        this.topic = topic;
        topicBlobsByFromTimestampRef.set(GCSRawdataUtils.getTopicBlobs(bucket, topic));
        if (cursor == null) {
            seek(0);
        } else {
            seek(cursor.ulid.timestamp());
            try {
                GCSRawdataMessage msg;
                while ((msg = (GCSRawdataMessage) receive(2, TimeUnit.SECONDS)) != null) {
                    if (msg.ulid().equals(cursor.ulid)) {
                        if (cursor.inclusive) {
                            preloadedMessages.addFirst(msg);
                        }
                        break; // found match
                    }
                    if (msg.timestamp() > cursor.ulid.timestamp()) {
                        // past possible point of match, use this message as starting point
                        preloadedMessages.addFirst(msg);
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
        GCSRawdataMessage preloadedMessage = preloadedMessages.poll();
        if (preloadedMessage != null) {
            return preloadedMessage;
        }
        DataFileReader<GenericRecord> dataFileReader = activeBlobDataFileReaderRef.get();
        if (dataFileReader == null) {
            return null;
        }
        if (!dataFileReader.hasNext()) {
            Long currentBlobKey = activeBlobFromKeyRef.get();
            NavigableMap<Long, Blob> blobByFrom = topicBlobsByFromTimestampRef.get(); // TODO consider re-loading list from gcs
            Map.Entry<Long, Blob> nextEntry = blobByFrom.higherEntry(currentBlobKey);
            if (nextEntry == null) {
                activeBlobDataFileReaderRef.set(null);
                activeBlobFromKeyRef.set(null);
                return null;
                // TODO Continue from Cloud Pub-Sub skipping up to and including the last message returned earlier
            }
            activeBlobFromKeyRef.set(nextEntry.getKey());
            Blob blob = nextEntry.getValue();
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(GCSRawdataProducer.schema);
            DataFileReader<GenericRecord> newDataFileReader;
            try {
                newDataFileReader = new DataFileReader<>(new GCSSeekableInput(blob.reader(), blob.getSize()), datumReader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            activeBlobDataFileReaderRef.set(newDataFileReader);
            return receive(timeout, unit);
        }
        GenericRecord record = dataFileReader.next();
        GCSRawdataMessage msg = toRawdataMessage(record);
        return msg;
    }

    static GCSRawdataMessage toRawdataMessage(GenericRecord record) {
        GenericData.Fixed id = (GenericData.Fixed) record.get("id");
        ULID.Value ulid = ULID.fromBytes(id.bytes());
        String orderingGroup = ofNullable(record.get("orderingGroup")).map(Object::toString).orElse(null);
        long sequenceNumber = (Long) record.get("sequenceNumber");
        String position = record.get("position").toString();
        Map<Utf8, ByteBuffer> data = (Map<Utf8, ByteBuffer>) record.get("data");
        Map<String, byte[]> map = data.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().array()));

        return new GCSRawdataMessage(ulid, orderingGroup, sequenceNumber, position, map);
    }

    @Override
    public CompletableFuture<? extends RawdataMessage> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void seek(long timestamp) {
        preloadedMessages.clear();
        DataFileReader<GenericRecord> previousDataFileReader = activeBlobDataFileReaderRef.getAndSet(null);
        if (previousDataFileReader != null) {
            try {
                previousDataFileReader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Map.Entry<Long, Blob> firstEntryHigherOrEqual = topicBlobsByFromTimestampRef.get().floorEntry(timestamp);
        if (firstEntryHigherOrEqual == null) {
            firstEntryHigherOrEqual = topicBlobsByFromTimestampRef.get().ceilingEntry(timestamp);
        }
        if (firstEntryHigherOrEqual == null) {
            activeBlobFromKeyRef.set(null);
            activeBlobDataFileReaderRef.set(null);
            return;
        }
        activeBlobFromKeyRef.set(firstEntryHigherOrEqual.getKey());
        Blob blob = firstEntryHigherOrEqual.getValue();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(GCSRawdataProducer.schema);
        DataFileReader<GenericRecord> dataFileReader;
        try {
            dataFileReader = new DataFileReader<>(new GCSSeekableInput(blob.reader(), blob.getSize()), datumReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        activeBlobDataFileReaderRef.set(dataFileReader);
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            try {
                record = dataFileReader.next(record);
                GCSRawdataMessage message = toRawdataMessage(record);
                long msgTimestamp = message.timestamp();
                if (msgTimestamp >= timestamp) {
                    preloadedMessages.add(message);
                    return; // first match
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            DataFileReader<GenericRecord> dataFileReader = activeBlobDataFileReaderRef.getAndSet(null);
            if (dataFileReader != null) {
                dataFileReader.close();
            }
            activeBlobFromKeyRef.set(null);
            topicBlobsByFromTimestampRef.set(null);
            preloadedMessages.clear();
        }
    }
}
