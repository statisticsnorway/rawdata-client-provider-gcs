# rawdata-client-provider-gcs
Rawdata provider for Google Cloud Storage.

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
| gcs.bucket-name | test-bucket | yes | Name of bucket |
| gcs.listing.min-interval-seconds | 60 | yes | Minimum number-of seconds between GCS list operations |
| gcs.service-account.key-file | secret/my_gcs_sa.json | yes | Path to json service-account key file |

## Example usage
```java
```
