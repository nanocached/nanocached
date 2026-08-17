package org.nanocached;

/**
 * A node's hash-ring identity (a random per-process UUID) and its network
 * address ({@code host:port}) — two different things since
 * doc/adr/0009-*.md: {@code name} is what routing hashes; {@code address}
 * is only for opening a connection.
 */
public record DiscoveredNode(String name, String address) {}
