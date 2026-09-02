package io.volant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** One fetched record (native Fetch response). */
public final class Record {
    public final long offset;
    public final long timestampMs;
    /** Null means the wire optional-bytes null ({@code u32::MAX}). */
    public final byte[] key;
    public final byte[] value;
    public final List<Header> headers;

    public Record(long offset, long timestampMs, byte[] key, byte[] value, List<Header> headers) {
        this.offset = offset;
        this.timestampMs = timestampMs;
        this.key = key;
        this.value = value == null ? new byte[0] : value;
        this.headers = headers == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(headers));
    }

    /** One produce/fetch record header. */
    public static final class Header {
        public final String name;
        public final byte[] value;

        public Header(String name, byte[] value) {
            this.name = name == null ? "" : name;
            this.value = value == null ? new byte[0] : value;
        }
    }
}
