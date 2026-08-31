package org.nanocached.spring;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * The default {@link CacheValueSerializer}: plain JDK serialization.
 *
 * <p><b>Trust boundary.</b> {@link #deserialize} runs {@link ObjectInputStream#readObject()}
 * over whatever bytes are read back from the namespace — see this module's README
 * "Trust boundary / deserialization" section for what that means for anyone who can write
 * to that namespace. To bound what this class is willing to reconstruct, every {@code
 * ObjectInputStream} it creates has an {@link ObjectInputFilter} applied: by default the
 * JVM-wide filter, if one is configured (via {@code -Djdk.serialFilter} or {@link
 * ObjectInputFilter.Config#setSerialFilter}) — set explicitly here because {@code
 * ObjectInputStream} does not always pick up the process-wide filter on its own — or, when a
 * more specific policy than the process default is wanted, the filter passed to {@link
 * #JdkCacheValueSerializer(ObjectInputFilter)}.
 *
 * <p>The no-arg constructor's JVM-wide filter is re-read from {@link
 * ObjectInputFilter.Config#getSerialFilter()} on every {@link #deserialize} call, not captured
 * once at construction — the same reasoning as {@code nanocached-jcache}'s {@code
 * ValueCodec#deserialize}: this serializer can be built (e.g. as a Spring bean) before some
 * other part of the application installs the process-wide filter via {@code setSerialFilter},
 * and capturing {@code null} at that moment would silently and permanently disable filtering
 * for this instance even after the real filter is installed. The explicit-filter constructor
 * below is unaffected — a caller-supplied filter is deliberately fixed, not the live global one.
 */
public final class JdkCacheValueSerializer implements CacheValueSerializer {

    private final ObjectInputFilter explicitFilter;
    private final boolean useGlobalFilter;

    /** Uses the JVM-wide serial filter ({@link ObjectInputFilter.Config#getSerialFilter()}),
     * re-read on every {@link #deserialize} call — see the class javadoc. */
    public JdkCacheValueSerializer() {
        this.explicitFilter = null;
        this.useGlobalFilter = true;
    }

    /**
     * Uses {@code filter} instead of (or in addition to configuring) the JVM-wide filter.
     * Pass {@code null} to explicitly deserialize without a filter — not recommended; see the
     * class javadoc and the README's trust boundary section. Unlike the no-arg constructor,
     * {@code filter} is fixed for this instance's lifetime, not re-read.
     */
    public JdkCacheValueSerializer(ObjectInputFilter filter) {
        this.explicitFilter = filter;
        this.useGlobalFilter = false;
    }

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
            // Re-read the JVM-wide filter here rather than at construction
            // time (issue #365): setSerialFilter may run after this
            // serializer was built but before it is ever used, and a value
            // captured once would silently miss that.
            ObjectInputFilter filter = useGlobalFilter ? ObjectInputFilter.Config.getSerialFilter() : explicitFilter;
            if (filter != null) {
                in.setObjectInputFilter(filter);
            }
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(
                    "nanocached-spring: failed to deserialize cache value", e);
        }
    }
}
