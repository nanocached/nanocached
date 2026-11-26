package org.nanocached.jcache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class KeyCodecTest {

    /** The shape of Spring's {@code SimpleKey}: an {@code Object[]} compared element by element. */
    record MultiArgumentKey(Object[] params) implements Serializable {
        @Override
        public boolean equals(Object other) {
            return other instanceof MultiArgumentKey that && Arrays.deepEquals(params, that.params);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(params);
        }
    }

    @Test
    void aRepeatedStringArgumentEncodesTheSameAsTwoEqualCopies() {
        String from = "2026-11-26";
        MultiArgumentKey sameInstance = new MultiArgumentKey(new Object[] {from, from});
        MultiArgumentKey copies = new MultiArgumentKey(new Object[] {from, new String(from)});
        assertEquals(sameInstance, copies);

        assertArrayEquals(KeyCodec.toKeyBytes(sameInstance), KeyCodec.toKeyBytes(copies));
    }

    @Test
    void aRepeatedBoxedNumberEncodesTheSameAsTwoEqualCopies() {
        Long thousand = 1000L;
        List<Object> sameInstance = new ArrayList<>(List.of(thousand, thousand));
        List<Object> copies = new ArrayList<>(List.of(thousand, Long.valueOf("1000")));
        assertEquals(sameInstance, copies);

        assertArrayEquals(KeyCodec.toKeyBytes(sameInstance), KeyCodec.toKeyBytes(copies));
    }

    @Test
    void aRepeatedUuidEncodesTheSameAsTwoEqualCopies() {
        UUID id = UUID.randomUUID();
        List<Object> sameInstance = new ArrayList<>(List.of(id, id));
        List<Object> copies = new ArrayList<>(List.of(id, UUID.fromString(id.toString())));

        assertArrayEquals(KeyCodec.toKeyBytes(sameInstance), KeyCodec.toKeyBytes(copies));
    }

    @Test
    void aRepeatedBigDecimalEncodesTheSameAsTwoEqualCopies() {
        BigDecimal price = new BigDecimal("19.90");
        List<Object> sameInstance = new ArrayList<>(List.of(price, price));
        List<Object> copies = new ArrayList<>(List.of(price, new BigDecimal("19.90")));

        assertArrayEquals(KeyCodec.toKeyBytes(sameInstance), KeyCodec.toKeyBytes(copies));
    }

    @Test
    void equalValuesOfDifferentTypesStayDistinct() {
        List<Object> asLong = new ArrayList<>(List.of(42L, 42L));
        List<Object> mixed = new ArrayList<>(List.of(42L, 42));

        assertFalse(Arrays.equals(KeyCodec.toKeyBytes(asLong), KeyCodec.toKeyBytes(mixed)));
    }

    @Test
    void aKeyWithoutRepeatedValuesKeepsPlainJdkSerialization() throws IOException {
        List<Object> key = new ArrayList<>(List.of("a", 1L, UUID.randomUUID(), true));
        ByteArrayOutputStream plain = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(plain)) {
            out.writeObject(key);
        }

        assertArrayEquals(plain.toByteArray(), KeyCodec.toKeyBytes(key));
    }
}
