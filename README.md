# rawdata-client-provider-gcs
Rawdata provider for Google Cloud Storage.

The environment variable GOOGLE_APPLICATION_CREDENTIALS must be set 
to the path of a serivce-account json key file that has the needed
access to a GCS rawdata bucket.

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