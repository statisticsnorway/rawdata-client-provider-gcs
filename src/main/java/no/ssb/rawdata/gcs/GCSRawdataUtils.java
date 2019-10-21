package no.ssb.rawdata.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class GCSRawdataUtils {

    final Storage storage;

    static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    GCSRawdataUtils(Storage storage) {
        this.storage = storage;
    }

    static String formatTimestamp(long timestamp) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(new Date(timestamp).toInstant(), ZoneOffset.UTC);
        return zonedDateTime.format(dateTimeFormatter);
    }

    static long parseTimestamp(String timestamp) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestamp, dateTimeFormatter);
        return zonedDateTime.toInstant().toEpochMilli();
    }

    static final Pattern topicAndFilenamePattern = Pattern.compile("(?<topic>[^/]+)/(?<filename>.+)");

    static String topic(BlobId blobId) {
        Matcher topicAndFilenameMatcher = topicAndFilenamePattern.matcher(blobId.getName());
        if (!topicAndFilenameMatcher.matches()) {
            throw new RuntimeException("GCS BlobId does not match topicAndFilenamePattern. blobId=" + blobId.getName());
        }
        String topic = topicAndFilenameMatcher.group("topic");
        return topic;
    }

    static String filename(BlobId blobId) {
        Matcher topicAndFilenameMatcher = topicAndFilenamePattern.matcher(blobId.getName());
        if (!topicAndFilenameMatcher.matches()) {
            throw new RuntimeException("GCS BlobId does not match topicAndFilenamePattern. blobId=" + blobId.getName());
        }
        String filename = topicAndFilenameMatcher.group("filename");
        return filename;
    }

    static final Pattern filenamePattern = Pattern.compile("(?<from>[^_]+)_(?<count>[0123456789]+)_(?<position>.+)\\.avro");

    /**
     * @return lower-bound (inclusive) timestamp of this file range
     */
    static long getFromTimestamp(BlobId blobId) {
        String filename = filename(blobId);
        Matcher filenameMatcher = filenamePattern.matcher(filename);
        if (!filenameMatcher.matches()) {
            throw new RuntimeException("GCS filename does not match filenamePattern. filename=" + filename);
        }
        String from = filenameMatcher.group("from");
        return parseTimestamp(from);
    }

    /**
     * @return lower-bound (inclusive) position of this file range
     */
    static String getFirstPosition(BlobId blobId) {
        String filename = filename(blobId);
        Matcher filenameMatcher = filenamePattern.matcher(filename);
        if (!filenameMatcher.matches()) {
            throw new RuntimeException("GCS filename does not match filenamePattern. filename=" + filename);
        }
        String position = filenameMatcher.group("position");
        return position;
    }

    /**
     * @return count of messages in the file
     */
    static long getMessageCount(BlobId blobId) {
        String filename = filename(blobId);
        Matcher filenameMatcher = filenamePattern.matcher(filename);
        if (!filenameMatcher.matches()) {
            throw new RuntimeException("GCS filename does not match filenamePattern. filename=" + filename);
        }
        long count = Long.parseLong(filenameMatcher.group("count"));
        return count;
    }

    Stream<Blob> listTopicFiles(String bucketName, String topic) {
        Page<Blob> page = storage.list(bucketName, Storage.BlobListOption.prefix(topic));
        Stream<Blob> stream = StreamSupport.stream(page.iterateAll().spliterator(), false);
        return stream.filter(blob -> !blob.isDirectory());
    }

    void copyLocalFileToGCSBlob(File file, BlobId blobId) throws IOException {
        try (WriteChannel writeChannel = storage.writer(BlobInfo.newBuilder(blobId)
                .setContentType("text/plain")
                .build())) {
            writeChannel.setChunkSize(8 * 1024 * 1024); // 8 MiB
            copyFromFileToChannel(file, writeChannel);
        }
    }

    static void copyFromFileToChannel(File file, WritableByteChannel target) {
        try (FileChannel source = new RandomAccessFile(file, "r").getChannel()) {
            long bytesTransferred = 0;
            while (bytesTransferred < source.size()) {
                bytesTransferred += source.transferTo(bytesTransferred, source.size(), target);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    NavigableMap<Long, Blob> getTopicBlobs(String bucket, String topic) {
        NavigableMap<Long, Blob> map = new TreeMap<>();
        listTopicFiles(bucket, topic).forEach(blob -> {
            long fromTimestamp = GCSRawdataUtils.getFromTimestamp(blob.getBlobId());
            map.put(fromTimestamp, blob);
        });
        return map;
    }
}
