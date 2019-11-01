package no.ssb.rawdata.avro;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

class AvroRawdataMessage implements RawdataMessage {

    private final ULID.Value ulid;
    private final String orderingGroup;
    private final long sequenceNumber;
    private final String position;
    private final Map<String, byte[]> data;

    AvroRawdataMessage(ULID.Value ulid, String orderingGroup, long sequenceNumber, String position, Map<String, byte[]> data) {
        this.ulid = ulid;
        this.orderingGroup = orderingGroup;
        this.sequenceNumber = sequenceNumber;
        this.position = position;
        this.data = data;
    }

    @Override
    public ULID.Value ulid() {
        return ulid;
    }

    @Override
    public String orderingGroup() {
        return orderingGroup;
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String position() {
        return position;
    }

    @Override
    public Set<String> keys() {
        return data.keySet();
    }

    @Override
    public byte[] get(String key) {
        return data.get(key);
    }


    static class Builder implements RawdataMessage.Builder {

        ULID.Value ulid;
        String orderingGroup;
        long sequenceNumber = 0;
        String position;
        Map<String, byte[]> data = new LinkedHashMap<>();

        @Override
        public Builder orderingGroup(String orderingGroup) {
            this.orderingGroup = orderingGroup;
            return this;
        }

        @Override
        public String orderingGroup() {
            return orderingGroup;
        }

        @Override
        public Builder sequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        @Override
        public long sequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public Builder ulid(ULID.Value ulid) {
            this.ulid = ulid;
            return this;
        }

        @Override
        public ULID.Value ulid() {
            return ulid;
        }

        @Override
        public Builder position(String position) {
            this.position = position;
            return this;
        }

        @Override
        public String position() {
            return position;
        }

        @Override
        public Builder put(String key, byte[] payload) {
            data.put(key, payload);
            return this;
        }

        @Override
        public Set<String> keys() {
            return data.keySet();
        }

        @Override
        public byte[] get(String key) {
            return data.get(key);
        }

        @Override
        public AvroRawdataMessage build() {
            return new AvroRawdataMessage(ulid, orderingGroup, sequenceNumber, position, data);
        }
    }
}
