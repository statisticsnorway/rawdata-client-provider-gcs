import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.gcs.GCSRawdataClientInitializer;

module no.ssb.rawdata.gcs {
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;
    requires org.slf4j;
    requires org.apache.avro;

    requires gax;
    requires google.cloud.storage;
    requires google.cloud.core;
    requires com.google.auth.oauth2;

    provides RawdataClientInitializer with GCSRawdataClientInitializer;
}
