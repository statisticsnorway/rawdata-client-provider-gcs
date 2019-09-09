package no.ssb.rawdata.gcs;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNoSuchPositionException;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class GCSRawdataClientTck {

    static final int MiB = 1024 * 1024;

    RawdataClient client;

    @BeforeMethod
    public void createRawdataClient() throws IOException {
        Map<String, String> configuration = new LinkedHashMap<>();
        configuration.put("gcs.bucket.url", "gcs:bucket");
        configuration.put("gcs.bucket.folder", "tck");
        configuration.put("staging.retention.max.days", "1");
        configuration.put("staging.retention.max.hours", "0");
        configuration.put("staging.retention.max.minutes", "0");
        configuration.put("staging.retention.buffer.days", "0");
        configuration.put("staging.retention.buffer.hours", "0");
        configuration.put("staging.retention.buffer.minutes", "30");
        configuration.put("lmdb.folder", "target/tck/lmdb");
        configuration.put("lmdb.map-size", Long.toString(64 * MiB));
        configuration.put("lmdb.message.file.max-size", Integer.toString(1 * MiB));
        configuration.put("lmdb.topic.write-concurrency", "1");
        configuration.put("lmdb.topic.read-concurrency", "3");

        Path lmdbFolder = Paths.get(configuration.get("lmdb.folder"));
        if (Files.exists(lmdbFolder)) {
            Files.walk(lmdbFolder).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        client = ProviderConfigurator.configure(configuration, "gcs", RawdataClientInitializer.class);
    }

    @AfterMethod
    public void closeRawdataClient() throws Exception {
        client.close();
    }

    @Test
    public void thatLastPositionOfEmptyTopicCanBeRead() {
        assertNull(client.lastMessage("the-topic"));
    }

    @Test
    public void thatLastPositionOfProducerCanBeRead() {
        RawdataProducer producer = client.producer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.publish("a", "b");

        assertEquals(client.lastMessage("the-topic").position(), "b");

        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("c");

        assertEquals(client.lastMessage("the-topic").position(), "c");
    }

    @Test(expectedExceptions = RawdataNotBufferedException.class)
    public void thatPublishNonBufferedMessagesThrowsException() {
        RawdataProducer producer = client.producer("the-topic");
        producer.publish("unbuffered-1");
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        ULID.Value ulid = new ULID().nextValue();
        producer.buffer(producer.builder().ulid(ulid).orderingGroup("og1").sequenceNumber(1).position("a").put("payload1", new byte[3]).put("payload2", new byte[7]));
        producer.publish("a");

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.ulid(), ulid);
        assertEquals(message.orderingGroup(), "og1");
        assertEquals(message.sequenceNumber(), 1);
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
        assertEquals(message.get("payload1"), new byte[3]);
        assertEquals(message.get("payload2"), new byte[7]);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.publish("a");

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<? extends RawdataMessage> future = consumer.receiveAsync();

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.publish("a");

        RawdataMessage message = future.join();
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

        RawdataMessage message1 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message1.position(), "a");
        assertEquals(message2.position(), "b");
        assertEquals(message3.position(), "c");
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

        List<RawdataMessage> messages = future.join();

        assertEquals(messages.get(0).position(), "a");
        assertEquals(messages.get(1).position(), "b");
        assertEquals(messages.get(2).position(), "c");
    }

    private CompletableFuture<List<RawdataMessage>> receiveAsyncAddMessageAndRepeatRecursive(RawdataConsumer consumer, String endPosition, List<RawdataMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endPosition.equals(message.position())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endPosition, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer1 = client.consumer("the-topic");
        RawdataConsumer consumer2 = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
        CompletableFuture<List<RawdataMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

        List<RawdataMessage> messages1 = future1.join();
        assertEquals(messages1.get(0).position(), "a");
        assertEquals(messages1.get(1).position(), "b");
        assertEquals(messages1.get(2).position(), "c");

        List<RawdataMessage> messages2 = future2.join();
        assertEquals(messages2.get(0).position(), "a");
        assertEquals(messages2.get(1).position(), "b");
        assertEquals(messages2.get(2).position(), "c");
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "a", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "b", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "d", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }

    @Test
    public void thatSeekToWorks() throws Exception {
        long timestampBeforeA;
        long timestampBeforeB;
        long timestampBeforeC;
        long timestampBeforeD;
        long timestampAfterD;
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            timestampBeforeA = System.currentTimeMillis();
            producer.publish("a");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            timestampBeforeB = System.currentTimeMillis();
            producer.publish("b");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            timestampBeforeC = System.currentTimeMillis();
            producer.publish("c");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            timestampBeforeD = System.currentTimeMillis();
            producer.publish("d");
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "a");
        }
    }

    @Test
    public void thatPositionCursorOfValidPositionIsFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c");
        }
        assertNotNull(client.cursorOf("the-topic", "a", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "b", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = RawdataNoSuchPositionException.class)
    public void thatPositionCursorOfInvalidPositionIsNotFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c");
        }
        assertNull(client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = RawdataNoSuchPositionException.class)
    public void thatPositionCursorOfEmptyTopicIsNotFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
        }
        client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1));
    }
}