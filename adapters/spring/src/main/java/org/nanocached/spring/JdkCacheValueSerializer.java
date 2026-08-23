package org.nanocached.spring;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/** The default {@link CacheValueSerializer}: plain JDK serialization. */
public final class JdkCacheValueSerializer implements CacheValueSerializer {

    @Override
    public byte[] serialize(Object value) {
        if (!(value instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "nanocached-spring: value of type "
                            + value.getClass().getName()
                            + " is not Serializable; configure a custom CacheValueSerializer"
                            + " or make the cached type Serializable");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        } catch (IOException e) {
            throw new IllegalStateException("nanocached-spring: failed to serialize cache value", e);
        }
        return bytes.toByteArray();
    }

    @Override
    public Object deserialize(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(
                    "nanocached-spring: failed to deserialize cache value", e);
        }
    }
}
