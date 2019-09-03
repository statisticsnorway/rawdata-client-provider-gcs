import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.gcs.GCSRawdataClientInitializer;

module no.ssb.rawdata.gcs {
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;
    requires org.slf4j;
    requires google.cloud.storage;
    requires de.huxhorn.sulky.ulid;

    provides RawdataClientInitializer with GCSRawdataClientInitializer;
}
