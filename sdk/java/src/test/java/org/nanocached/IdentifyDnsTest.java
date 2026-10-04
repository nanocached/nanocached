package org.nanocached;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.nanocached.MockServers.MockNode;

/** Host name resolution is part of a dial's deadline. */
@Timeout(30)
class IdentifyDnsTest {
    private final int originalTimeout = Identify.connectTimeoutMillis;
    private final Identify.HostResolver originalResolver = Identify.hostResolver;

    @AfterEach
    void restore() {
        Identify.connectTimeoutMillis = originalTimeout;
        Identify.hostResolver = originalResolver;
    }

    @Test
    void aResolverThatHangsFailsTheDialWithinTheDeadline() throws Exception {
        // `new InetSocketAddress(host, port)` used to resolve on the calling
        // thread before the socket's own connect timeout started, so a
        // stalled DNS server hung connect()/refresh for the resolver's own
        // timeout (or forever) instead of failing over like any unreachable
        // address.
        CountDownLatch never = new CountDownLatch(1);
        Identify.hostResolver = host -> {
            try {
                never.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            throw new java.net.UnknownHostException(host);
        };
        Identify.connectTimeoutMillis = 300;

        long start = System.nanoTime();
        IOException error = assertThrows(IOException.class,
                () -> Identify.connectAndIdentify("stalled-dns.test", 9, null, null));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertInstanceOf(SocketTimeoutException.class, error);
        assertTrue(error.getMessage().contains("stalled-dns.test"), error.getMessage());
        assertTrue(elapsedMillis < 2_000, "the dial should give up at the deadline, took " + elapsedMillis + "ms");
        never.countDown();
    }

    @Test
    void aHostNameIsResolvedThroughTheBoundedResolverAndDialed() throws Exception {
        AtomicInteger lookups = new AtomicInteger();
        Identify.hostResolver = host -> {
            lookups.incrementAndGet();
            return InetAddress.getByAddress(host, new byte[] {127, 0, 0, 1});
        };
        try (MockNode node = new MockNode()) {
            Identify.Result result = Identify.connectAndIdentify("my-node.test", node.port(), null, null);
            assertInstanceOf(Identify.NodeTarget.class, result);
            ((Identify.NodeTarget) result).socket().close();
            assertEquals(1, lookups.get());
        }
    }

    @Test
    void anIpLiteralNeverTouchesTheResolver() throws Exception {
        Identify.hostResolver = host -> {
            throw new AssertionError("an IP literal must not be resolved: " + host);
        };
        try (MockNode node = new MockNode()) {
            Identify.Result result = Identify.connectAndIdentify("127.0.0.1", node.port(), null, null);
            assertInstanceOf(Identify.NodeTarget.class, result);
            ((Identify.NodeTarget) result).socket().close();
        }
    }

    @Test
    void anUnresolvableHostStillFailsAsAnIoException() {
        Identify.hostResolver = host -> {
            throw new java.net.UnknownHostException(host);
        };
        assertThrows(java.net.UnknownHostException.class,
                () -> Identify.connectAndIdentify("nope.invalid", 9, null, null));
    }
}
