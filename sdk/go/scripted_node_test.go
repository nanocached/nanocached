package nanocached

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"testing"
)

// scriptedNode is a bare TCP server whose per-connection behaviour a test
// writes by hand — for the wire answers mockNode does not model (a bare
// `E`, an `En` that must arrive while another request is pipelined behind
// it). It acknowledges the `A` handshake itself and hands the script each
// connection after that, with a reader for the data frames the client then
// sends (G and t only).
type scriptedNode struct {
	listener net.Listener
	mu       sync.Mutex
	conns    int
	frames   []string // every data frame header seen, across connections
}

// scriptedFrame is one request frame: its header line without the newline.
type scriptedFrame struct {
	header string
}

func startScriptedNode(t *testing.T, script func(connIndex int, conn net.Conn, next func() (scriptedFrame, bool))) *scriptedNode {
	t.Helper()
	node := &scriptedNode{listener: listenLoopback(t)}
	t.Cleanup(func() { _ = node.listener.Close() })
	go func() {
		for {
			conn, err := node.listener.Accept()
			if err != nil {
				return
			}
			node.mu.Lock()
			index := node.conns
			node.conns++
			node.mu.Unlock()
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				header, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				parts := strings.Split(strings.TrimSuffix(header, "\n"), " ")
				if parts[0] != "A" {
					return
				}
				mustRead(reader, atoiOrPanic(parts[1]))
				if _, err := conn.Write([]byte("On\n")); err != nil {
					return
				}
				next := func() (scriptedFrame, bool) {
					line, err := reader.ReadString('\n')
					if err != nil {
						return scriptedFrame{}, false
					}
					line = strings.TrimSuffix(line, "\n")
					fields := strings.Split(line, " ")
					switch fields[0] {
					case "G":
						mustRead(reader, atoiOrPanic(fields[1]))
					case "t":
						mustRead(reader, atoiOrPanic(fields[1])+atoiOrPanic(fields[2]))
					default:
						panic("scriptedNode: unsupported frame " + line)
					}
					node.mu.Lock()
					node.frames = append(node.frames, line)
					node.mu.Unlock()
					return scriptedFrame{header: line}, true
				}
				script(index, conn, next)
			}()
		}
	}()
	return node
}

func (n *scriptedNode) address() string { return n.listener.Addr().String() }

func (n *scriptedNode) connectionCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.conns
}

// framesStarting counts the data frames seen whose header begins with prefix.
func (n *scriptedNode) framesStarting(prefix string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := 0
	for _, f := range n.frames {
		if strings.HasPrefix(f, prefix) {
			count++
		}
	}
	return count
}
