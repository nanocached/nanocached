# 12. Derive node addresses from the registration connection

Date: 2026-08-18

## Status

Accepted — implemented on `refactor/derive-node-address`.

Builds on [9. Decouple node identity from network address](0009-decouple-node-identity-from-network-address.md)
and [10. Discovery HA via soft-state replicas and member announce](0010-discovery-ha-via-soft-state-replicas-and-member-announce.md).

## Context

Until now a node declared its own reachable address to the discovery
server via `--advertise-addr` (defaulting to `--host:--port`). That
default is wrong in exactly the environments nanocached is most likely
to run in: a container binds `0.0.0.0`, which is not an address anyone
can connect to, so every containerized deployment had to solve the same
puzzle — discover the container's own IP (an ECS metadata scrape, a
Kubernetes downward-API field) and thread it into the CLI. The AWS live
tests (2026-08) needed an entrypoint shell for nothing else.

The option was also redundant in principle. A node's `J`/`P`/`H` arrives
over a TCP connection whose source IP the discovery server can read, and
ADR-0009 already assumes no NAT between cluster parties: the topology in
which "the IP the node connected from" differs from "the IP others reach
it on" was never supported. An explicit override with no supported use
is just configuration that can be wrong.

Broadcast/multicast self-discovery was considered as an alternative and
rejected: cloud VPCs (AWS, and cloud networks generally) support
neither, so it would only ever work on-prem.

## Decision

Remove `--advertise-addr`. The node declares only the port it serves on;
the discovery server composes the node's address from the registration
connection's source IP plus that port.

The `J` and `P` frames change from carrying a full address to carrying
the port in the header:

    J <name-length> <port>\n<name>
    P <name-length> <port>\n<name>

Port `0` is rejected (nothing can serve on it). `H` is unchanged (name
only). `L` is unchanged — it still returns full `name`/`address` pairs,
so **no SDK is affected**; this is purely a node↔discovery wire change,
and both binaries ship together.

## Consequences

- Containerized deployment needs no address configuration at all: bind
  `0.0.0.0`, point `--discovery` at the replicas, done.
- Asymmetric port mapping (e.g. Docker's `-p 9999:8356`) is not
  supported — the composed address would carry the wrong port. This is
  NAT, which ADR-0009 already excludes; it is now documented rather than
  silently workaroundable.
- Every discovery replica derives the same address independently from
  its own connection with the node, so replicas stay consistent with no
  coordination — same soft-state property as ADR-0010.
- A node behind several interfaces registers as whichever source IP its
  route to discovery uses. That is by construction an IP discovery can
  reach; under the no-NAT assumption clients and other nodes can too.
- IPv6 nodes would compose as `<ipv6>:<port>` without brackets, which
  the address-splitting on the client side does not handle — unchanged
  from before this decision (an advertised IPv6 address had the same
  problem); IPv6 support remains out of scope.
