package no.ssb.rawdata.avro.filesystem;

import no.ssb.rawdata.avro.AvroFileMetadata;
import no.ssb.rawdata.avro.RawdataAvroFile;

import java.nio.file.Path;

class FilesystemAvroFileMetadata extends AvroFileMetadata {

    final Path storageFolder;

    FilesystemAvroFileMetadata(Path storageFolder) {
        this.storageFolder = storageFolder;
    }

    @Override
    public RawdataAvroFile toRawdataAvroFile(String topic) {
        return new FilesystemRawdataAvroFile(storageFolder.resolve(topic).resolve(toFilename()));
    }
}
