# rawdata-client-provider-gcs
Rawdata provider for Google Cloud Storage.

Rawdata topics are organized such that each topic has a separate folder 
of Avro files in GCS. All files in the topic folder are part of the stream, 
and each file is named using the timestamp and position of the first message 
in the file, and the total message count. The files have path according to
the pattern: `/<topic-name>/<timestamp>_<count>_<position>.avro`

Producers will keep a local Avro file to buffer all published 
messages. When producer is closed, or a time or size limit is 
reached, the local Avro file is uploaded to GCS into the configured
bucket and appropriate topic folder. After uploading a file, producers
will truncate and re-use the local-file to continue buffering (unless
the producer was closed). Files on GCS are named so that the file name 
contains the timestamp of the first and last message in the file.

Consumers list all files relevant to a topic on-demand and will wait 
at least a configured amount of seconds between list operations. 
Consumers know which file(s) to read based on the file-names and the
requested read operations. Consumers are able to detect new files 
created on GCS while tailing the stream.

## Configuration Options
| Configuration Key | Example | Required | Description |
| ----------------- |:-------:|:--------:| ----------- |
| local-temp-folder |temp |  yes | Path to local folder where topic folders and buffer-files can be created |
| avro-file.max.seconds | 3600 | yes | Max number of seconds in a producer window |
| avro-file.max.bytes | 10485760 | yes | Max number of bytes in a producer window |
| avro-file.sync.interval | 524288 | yes | Will start a new Avro block after message that breaks this threshold is written |
| gcs.bucket-name | test-bucket | yes | Name of bucket |
| gcs.listing.min-interval-seconds | 60 | yes | Minimum number-of seconds between GCS list operations |
| gcs.service-account.key-file | secret/my_gcs_sa.json | yes | Path to json service-account key file |

## Example usage
```java
    Map<String, String> configuration = Map.of(
            "local-temp-folder", "temp",
            "avro-file.max.seconds", "3600",
            "avro-file.max.bytes", "10485760",
            "avro-file.sync.interval", "524288",
            "gcs.bucket-name", "my-awesome-test-bucket",
            "gcs.listing.min-interval-seconds", "3",
            "gcs.service-account.key-file", "secret/my_gcs_sa.json"
    );

    RawdataClient client = ProviderConfigurator.configure(configuration,
            "gcs", RawdataClientInitializer.class);
```

For the full example, see the ExampleApp.java file in the src/test/java
