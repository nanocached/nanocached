// Go-SDK smoke for the AWS live tests: ./nanotest <write|read> <label> <count>
// Addresses via NANOTEST_ADDRESSES ("host:port,host:port").
package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	nanocached "github.com/nanocached/nanocached/sdk/go"
)

func parseAddresses(raw string) []nanocached.Address {
	parts := strings.Split(raw, ",")
	addresses := make([]nanocached.Address, 0, len(parts))
	for _, part := range parts {
		host, portStr, err := net.SplitHostPort(part)
		if err != nil {
			fmt.Println("invalid address in NANOTEST_ADDRESSES:", part)
			os.Exit(1)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			fmt.Println("invalid port in NANOTEST_ADDRESSES:", part)
			os.Exit(1)
		}
		addresses = append(addresses, nanocached.Address{Host: host, Port: port})
	}
	return addresses
}

func main() {
	os.Exit(run())
}

// run performs the smoke test and returns the process exit code. It is
// separated from main so that the deferred client.Close() (which drains
// in-flight background replica writes before the sockets are torn down)
// always completes before the process exits — os.Exit does not run
// deferred functions, so exiting directly from inside this function
// would skip the drain.
func run() int {
	client, err := nanocached.Connect(nanocached.Config{
		Addresses: parseAddresses(os.Getenv("NANOTEST_ADDRESSES")),
	})
	if err != nil {
		fmt.Println("connect failed:", err)
		return 1
	}
	defer client.Close()

	cmd, label := os.Args[1], os.Args[2]
	count, _ := strconv.Atoi(os.Args[3])

	switch cmd {
	case "write":
		for i := 0; i < count; i++ {
			if err := client.Set(fmt.Sprintf("x:%s:%d", label, i), fmt.Sprintf("v-%s-%d", label, i), 0); err != nil {
				fmt.Println("set failed:", err)
				return 1
			}
		}
		fmt.Printf("wrote %d keys for label %s\n", count, label)
	case "read":
		var bad []int
		for i := 0; i < count; i++ {
			value, ok, err := client.Get(fmt.Sprintf("x:%s:%d", label, i))
			if err != nil || !ok || value != fmt.Sprintf("v-%s-%d", label, i) {
				bad = append(bad, i)
			}
		}
		if len(bad) > 0 {
			fmt.Printf("label %s: %d/%d BAD (sample %v)\n", label, len(bad), count, bad[:min(5, len(bad))])
			return 1
		}
		fmt.Printf("label %s: %d/%d OK\n", label, count, count)
	default:
		fmt.Println("unknown command", cmd)
		return 2
	}
	return 0
}
