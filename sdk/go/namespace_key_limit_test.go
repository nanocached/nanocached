package nanocached

import (
	"fmt"
	"strings"
	"testing"
)

// The node refuses an `o`/`y` frame whose namespace length times key count
// passes 64 MiB (`En`, then it closes the connection). 170,000 x 400 keys is
// 68,000,000, over 67,108,864, so the 400-key chunk has to be split.
func TestSetManyAndTouchManySplitChunksUnderTheNamespaceKeyLimit(t *testing.T) {
	const limit = 64 * 1024 * 1024
	namespace := strings.Repeat("n", 170000)
	keys := make([]string, 400)
	values := make(map[string]string, 400)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		values[keys[i]] = "v"
	}

	node := startMockNode(t, nil)
	client := connectTo(t, node, Config{})
	ns := client.Namespace(namespace)

	if _, err := ns.SetMany(values, 0); err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	total := 0
	for _, count := range node.oKeyCountsSeen() {
		total += count
		if count*len(namespace) > limit {
			t.Errorf("o frame of %d keys passes the 64 MiB namespace x keys limit", count)
		}
	}
	if total != 400 {
		t.Errorf("o frames carried %d keys, want 400", total)
	}

	if _, err := ns.TouchMany(keys, 60); err != nil {
		t.Fatalf("TouchMany: %v", err)
	}
	total = 0
	for _, frame := range node.yFramesSeen() {
		total += len(frame.keys)
		if len(frame.keys)*len(namespace) > limit {
			t.Errorf("y frame of %d keys passes the 64 MiB namespace x keys limit", len(frame.keys))
		}
	}
	if total != 400 {
		t.Errorf("y frames carried %d keys, want 400", total)
	}
}

func TestMaxKeysPerOYChunk(t *testing.T) {
	for _, tc := range []struct{ namespaceLen, want int }{
		{0, 400},
		{167772, 400},
		{167773, 399},
		{170000, 394},
		{1 << 20, 64},
	} {
		if got := maxKeysPerOYChunk(make([]byte, tc.namespaceLen)); got != tc.want {
			t.Errorf("maxKeysPerOYChunk(%d bytes) = %d, want %d", tc.namespaceLen, got, tc.want)
		}
	}
}
