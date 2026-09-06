package nanocached

import "testing"

// TestNodeReplicationRefusesAProxyRoster (issue #486): a `Q` roster carries
// no replication factor, and reading one off it is a caller bug — the
// accessor must say so rather than hand back a meaningless zero.
func TestNodeReplicationRefusesAProxyRoster(t *testing.T) {
	proxies := &identified{nodes: []discoveredNode{{Name: "p", Address: "127.0.0.1:1"}}, list: listProxies}
	if _, err := proxies.nodeReplication(); err == nil {
		t.Fatal("expected an error reading the replication factor off a Q roster")
	}
	nodes := &identified{nodes: []discoveredNode{{Name: "n", Address: "127.0.0.1:1"}}, replication: 2, list: listNodes}
	if r, err := nodes.nodeReplication(); err != nil || r != 2 {
		t.Fatalf("expected replication 2 from an L roster, got %d, %v", r, err)
	}
}
