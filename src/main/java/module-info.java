import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.gcs.GCSRawdataClientInitializer;

module no.ssb.rawdata.gcs {
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;
    requires org.slf4j;
    requires google.cloud.storage;
    requires gax;
    requires org.apache.avro;
    requires google.cloud.core;

    provides RawdataClientInitializer with GCSRawdataClientInitializer;
}
