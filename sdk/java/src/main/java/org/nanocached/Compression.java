package org.nanocached;

import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Value compression: transparent, opt-in value compression. Values are
 * prefixed with a one-byte marker once {@code compress} is enabled on a
 * client — {@code 0x00} for stored-as-is (below threshold, or
 * compression didn't shrink it), {@code 0x01} for raw-DEFLATE-compressed
 * (RFC 1951, no zlib/gzip wrapper — {@link Deflater#Deflater(int,
 * boolean)}'s {@code nowrap=true}). Never present when {@code compress}
 * is off.
 */
final class Compression {

    private static final byte MARKER_RAW = 0x00;
    private static final byte MARKER_DEFLATE = 0x01;

    // Bounds a DEFLATE value's expanded output. The wire cap
    // (MAX_VALUE_LENGTH) bounds only the compressed bytes received;
    // without this, a small, highly-repetitive value written by a
    // compromised node could expand to gigabytes and exhaust client
    // memory on a plain get (a decompression bomb). Far above any
    // realistic cache value. Same 64 MiB cap as the other five SDKs
    // (issue #41).
    private static final int MAX_DECOMPRESSED_LENGTH = 64 * 1024 * 1024;

    private Compression() {}

    /**
     * Below {@code threshold}, or when compressing doesn't actually
     * shrink the value (incompressible data), the marker byte alone is
     * added and the value is stored unchanged. Always returns a value
     * with the marker byte prefixed.
     *
     * <p>Every return path here goes through {@link #prefixed}, which
     * always allocates a brand-new array — so the result never aliases
     * {@code value} (the caller's own array), even on the
     * marker-byte-only paths above. {@link NanocachedClient}'s
     * fire-and-forget replica legs rely on this: a leg only needs its own
     * defensive copy when {@code compress} is off (issue #326), since a
     * compressed value here is already safe to hand to a background task
     * without copying it again.
     */
    static byte[] compressValue(byte[] value, int threshold) {
        if (value.length < threshold) {
            return prefixed(MARKER_RAW, value);
        }

        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        try {
            deflater.setInput(value);
            deflater.finish();

            byte[] buffer = new byte[Math.max(64, value.length)];
            int total = 0;
            while (!deflater.finished()) {
                if (total == buffer.length) {
                    buffer = Arrays.copyOf(buffer, buffer.length * 2);
                }
                total += deflater.deflate(buffer, total, buffer.length - total);
            }

            if (total < value.length) {
                return prefixed(MARKER_DEFLATE, Arrays.copyOf(buffer, total));
            }
            return prefixed(MARKER_RAW, value);
        } finally {
            deflater.end();
        }
    }

    /**
     * The get/getBytes counterpart to {@link #compressValue}. Only ever
     * called when {@code compress} is enabled on this client — see
     * Value compression.
     */
    static byte[] decompressValue(byte[] value) {
        if (value.length == 0) {
            throw new NanocachedException.DecompressionFailed(
                    "nanocached: compress is enabled but this value has no marker byte "
                            + "(it's empty) — was it written by a client with compress disabled?");
        }

        byte marker = value[0];
        byte[] body = Arrays.copyOfRange(value, 1, value.length);

        if (marker == MARKER_RAW) {
            return body;
        }

        if (marker == MARKER_DEFLATE) {
            Inflater inflater = new Inflater(true);
            try {
                inflater.setInput(body);

                byte[] buffer = new byte[Math.max(64, body.length * 3)];
                int total = 0;
                while (!inflater.finished()) {
                    if (total == buffer.length) {
                        // Grow toward the cap, never past it: a buffer
                        // already at MAX_DECOMPRESSED_LENGTH + 1 that
                        // still isn't finished means more output would
                        // follow — a bomb — so stop before allocating for
                        // it. The +1 lets an output of exactly the cap
                        // still decompress, mirroring the Go/Rust/Python
                        // implementations.
                        if (total > MAX_DECOMPRESSED_LENGTH) {
                            throw new NanocachedException.DecompressionFailed(
                                    "nanocached: decompressed value exceeds the maximum size — "
                                            + "possible decompression bomb");
                        }
                        buffer = Arrays.copyOf(buffer,
                                (int) Math.min(2L * buffer.length, MAX_DECOMPRESSED_LENGTH + 1L));
                    }
                    int produced = inflater.inflate(buffer, total, buffer.length - total);
                    if (produced == 0 && inflater.needsInput()) {
                        // The stream ended without ever reaching a valid
                        // DEFLATE end-of-stream marker — corrupt input.
                        throw new NanocachedException.DecompressionFailed(
                                "nanocached: failed to decompress a value marked as compressed — "
                                        + "did every client sharing this key enable compress "
                                        + "(value compression)? (truncated DEFLATE stream)");
                    }
                    total += produced;
                }
                if (total > MAX_DECOMPRESSED_LENGTH) {
                    throw new NanocachedException.DecompressionFailed(
                            "nanocached: decompressed value exceeds the maximum size — "
                                    + "possible decompression bomb");
                }
                return Arrays.copyOf(buffer, total);
            } catch (DataFormatException error) {
                throw new NanocachedException.DecompressionFailed(
                        "nanocached: failed to decompress a value marked as compressed — did every "
                                + "client sharing this key enable compress (value compression)? ("
                                + error.getMessage() + ")",
                        error);
            } finally {
                inflater.end();
            }
        }

        throw new NanocachedException.DecompressionFailed(
                "nanocached: unrecognized compression marker byte " + (marker & 0xFF)
                        + " — was this value written by a client with compress disabled "
                        + "(value compression)?");
    }

    private static byte[] prefixed(byte marker, byte[] body) {
        byte[] result = new byte[body.length + 1];
        result[0] = marker;
        System.arraycopy(body, 0, result, 1, body.length);
        return result;
    }
}
