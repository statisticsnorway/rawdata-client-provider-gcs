package no.ssb.rawdata.gcs;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataCursor;

class GCSCursor implements RawdataCursor {

    final ULID.Value ulid;
    final boolean inclusive;

    GCSCursor(ULID.Value ulid, boolean inclusive) {
        this.ulid = ulid;
        this.inclusive = inclusive;
    }
}
