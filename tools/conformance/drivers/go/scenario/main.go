// Scenario driver for the Go SDK (churn, connection checks, lifecycle).
// conf-go-scenario <scenario> [args]; same scenarios and JSON events as drivers/python/scenario.py.
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	nanocached "github.com/nanocached/nanocached/sdk/go"
)

const sdk = "go"

var t0 = time.Now()
var limit = func() time.Duration {
	s, _ := strconv.ParseFloat(os.Getenv("CONN_TIMEOUT_S"), 64)
	if s == 0 {
		s = 60
	}
	return time.Duration(s * float64(time.Second))
}()

func emit(o map[string]any) {
	o["sdk"] = sdk
	o["t_ms"] = time.Since(t0).Milliseconds()
	b, _ := json.Marshal(o)
	fmt.Println(string(b))
}

func cfg() nanocached.Config {
	var addrs []nanocached.Address
	for _, part := range strings.Split(os.Getenv("NANOTEST_ADDRESSES"), ",") {
		h, p, _ := net.SplitHostPort(part)
		port, _ := strconv.Atoi(p)
		addrs = append(addrs, nanocached.Address{Host: h, Port: port})
	}
	return nanocached.Config{Addresses: addrs, ViaProxy: os.Getenv("NANOTEST_VIA_PROXY") == "1", AuthSecret: os.Getenv("NANOTEST_SECRET")}
}

func errf(err error) map[string]any {
	m := err.Error()
	if len(m) > 300 {
		m = m[:300]
	}
	return map[string]any{"outcome": "error", "error_class": fmt.Sprintf("%T", err), "error_message": m}
}

// bounded runs f, giving up after d (the goroutine is abandoned, which is what a caller can do in Go).
func bounded[T any](d time.Duration, f func() (T, error)) (T, error, bool) {
	type res struct {
		v   T
		err error
	}
	ch := make(chan res, 1)
	go func() { v, err := f(); ch <- res{v, err} }()
	select {
	case r := <-ch:
		return r.v, r.err, false
	case <-time.After(d):
		var z T
		return z, nil, true
	}
}

func step[T any](name string, d time.Duration, f func() (T, error)) (T, bool) {
	t := time.Now()
	v, err, to := bounded(d, f)
	ms := time.Since(t).Milliseconds()
	switch {
	case to:
		emit(map[string]any{"event": name, "outcome": "timeout", "ms": ms})
		return v, false
	case err != nil:
		e := errf(err)
		e["event"], e["ms"] = name, ms
		emit(e)
		return v, false
	}
	emit(map[string]any{"event": name, "outcome": "ok", "ms": ms})
	return v, true
}

func procStats() map[string]any {
	ents, _ := os.ReadDir("/proc/self/fd")
	st, _ := os.ReadFile("/proc/self/status")
	var threads, rss int
	for _, l := range strings.Split(string(st), "\n") {
		if strings.HasPrefix(l, "Threads:") {
			threads, _ = strconv.Atoi(strings.Fields(l)[1])
		} else if strings.HasPrefix(l, "VmRSS:") {
			rss, _ = strconv.Atoi(strings.Fields(l)[1])
		}
	}
	return map[string]any{"fds": len(ents), "threads": threads, "rss_kb": rss}
}

func connect() (*nanocached.Client, error) { return nanocached.Connect(cfg()) }

func main() {
	name, args := os.Args[1], os.Args[2:]
	switch name {
	case "conn":
		c, ok := step("connect", limit, connect)
		if !ok {
			return
		}
		step("set", limit, func() (struct{}, error) { return struct{}{}, c.Set("conn:k", "v", 0) })
		v, _ := step("get", limit, func() (string, error) { s, _, err := c.Get("conn:k"); return s, err })
		emit(map[string]any{"event": "get_value", "value": v})
		emit(map[string]any{"event": "describe", "text": fmt.Sprintf("%+v | %v | %+v", cfg(), c, c)})
		step("close", limit, func() (struct{}, error) { c.Close(); return struct{}{}, nil })
	case "churn":
		secs, _ := strconv.ParseFloat(args[0], 64)
		c, _ := connect()
		acked := make([]int, 200)
		keys := make([]string, 200)
		for i := range keys {
			keys[i] = fmt.Sprintf("ch:%s:%s:%d", args[1], sdk, i)
		}
		ops, errs, stale, miss, corrupt := 0, 0, 0, 0, 0
		var first, last any
		classes := map[string]int{}
		end := time.Now().Add(time.Duration(secs * float64(time.Second)))
		for i := 1; time.Now().Before(end); i++ {
			n := (i - 1) % 200
			ops++
			var err error
			if i%2 == 1 {
				v := acked[n] + 1
				_, e, to := bounded(10*time.Second, func() (struct{}, error) { return struct{}{}, c.Set(keys[n], fmt.Sprintf("v%d", v), 0) })
				if to {
					err = fmt.Errorf("timeout")
				} else if err = e; err == nil {
					acked[n] = v
				}
			} else {
				type g struct {
					b  []byte
					ok bool
				}
				r, e, to := bounded(10*time.Second, func() (g, error) { b, ok, err := c.GetBytes(keys[n]); return g{b, ok}, err })
				if to {
					err = fmt.Errorf("timeout")
				} else if err = e; err == nil && acked[n] > 0 {
					if !r.ok {
						miss++
					} else if len(r.b) == 0 || r.b[0] != 'v' {
						corrupt++
					} else if got, _ := strconv.Atoi(string(r.b[1:])); got < acked[n] {
						stale++
					}
				}
			}
			if err != nil {
				errs++
				classes[fmt.Sprintf("%T", err)]++
				now := time.Since(t0).Milliseconds()
				if first == nil {
					first = now
				}
				last = now
			}
			time.Sleep(5 * time.Millisecond)
		}
		emit(map[string]any{"event": "summary", "ops": ops, "errors": errs, "stale": stale, "miss": miss, "corrupt": corrupt, "classes": classes, "first_err_ms": first, "last_err_ms": last})
		c.Close()
	case "life":
		life(args[0], args[1:])
	}
}

func life(test string, a []string) {
	switch test {
	case "close_inflight":
		c, _ := connect()
		c.Set("life:big", strings.Repeat("b", 900000), 0)
		var wg sync.WaitGroup
		res := make([]map[string]any, 4)
		for i := range res {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				b, _, err := c.GetBytes("life:big")
				if err != nil {
					res[i] = errf(err)
				} else {
					res[i] = map[string]any{"outcome": "ok", "len": len(b)}
				}
			}(i)
		}
		time.Sleep(200 * time.Millisecond)
		t := time.Now()
		step("close", limit, func() (struct{}, error) { c.Close(); return struct{}{}, nil })
		emit(map[string]any{"event": "close_ms", "ms": time.Since(t).Milliseconds()})
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
		}
		for i, r := range res {
			if r == nil {
				r = map[string]any{"outcome": "timeout"}
			}
			r["event"], r["i"] = "pending", i
			emit(r)
		}
	case "after_close":
		c, _ := connect()
		c.Set("life:k", "v", 0)
		c.Close()
		step("after_close.get", limit, func() (string, error) { s, _, err := c.Get("life:k"); return s, err })
		step("after_close.set", limit, func() (struct{}, error) { return struct{}{}, c.Set("life:k", "v", 0) })
		step("after_close.delete", limit, func() (bool, error) { return c.Delete("life:k") })
		step("after_close.incr", limit, func() (int64, error) { v, _, err := c.Incr("life:k", 1); return v, err })
		step("after_close.get_many", limit, func() (map[string]string, error) { return c.GetMany([]string{"life:k"}) })
	case "double_close", "concurrent_close":
		c, _ := connect()
		step("close1", limit, func() (struct{}, error) { c.Close(); return struct{}{}, nil })
		step("close2", limit, func() (struct{}, error) { c.Close(); return struct{}{}, nil })
		c, _ = connect()
		step("concurrent_close", limit, func() (struct{}, error) {
			var wg sync.WaitGroup
			for i := 0; i < 3; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); c.Close() }()
			}
			wg.Wait()
			return struct{}{}, nil
		})
	case "abandon":
		c, _ := connect()
		c.Set("life:a", "value-a", 0)
		c.Set("life:b", "value-b", 0)
		go func() { c.GetBytes("life:a") }() // Go has no cancellation: the caller simply stops waiting
		time.Sleep(300 * time.Millisecond)
		emit(map[string]any{"event": "cancelled"})
		r := map[string]string{}
		for _, kv := range [][2]string{{"b", "life:b"}, {"a", "life:a"}} {
			v, err, to := bounded(30*time.Second, func() (string, error) { s, _, err := c.Get(kv[1]); return s, err })
			switch {
			case to:
				r[kv[0]] = "timeout"
			case err != nil:
				r[kv[0]] = fmt.Sprintf("%T: %v", err, err)
			default:
				r[kv[0]] = v
			}
		}
		emit(map[string]any{"event": "after_cancel", "b": r["b"], "a": r["a"], "ok": r["b"] == "value-b" && r["a"] == "value-a"})
		c.Close()
	case "cycles":
		n, _ := strconv.Atoi(a[0])
		base := procStats()
		for i := 0; i < n; i++ {
			c, err := connect()
			if err != nil {
				continue
			}
			c.Set("life:c", "v", 0)
			c.Close()
		}
		time.Sleep(time.Second)
		emit(map[string]any{"event": "cycles", "n": n, "before": base, "after": procStats()})
	case "reconnect":
		secs, _ := strconv.ParseFloat(a[0], 64)
		c, _ := connect()
		end := time.Now().Add(time.Duration(secs * float64(time.Second)))
		ok, fail := 0, 0
		var ff, lf, fo any
		for time.Now().Before(end) {
			_, err, to := bounded(10*time.Second, func() (struct{}, error) { _, _, err := c.GetBytes("life:r"); return struct{}{}, err })
			if err != nil || to {
				fail++
				now := time.Since(t0).Milliseconds()
				if ff == nil {
					ff = now
				}
				lf = now
			} else {
				ok++
				if ff != nil && fo == nil {
					fo = time.Since(t0).Milliseconds()
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		emit(map[string]any{"event": "reconnect", "ok": ok, "fail": fail, "first_fail_ms": ff, "last_fail_ms": lf, "first_ok_after_ms": fo})
		c.Close()
	case "storm":
		secs, _ := strconv.ParseFloat(a[0], 64)
		c, _ := connect()
		end := time.Now().Add(time.Duration(secs * float64(time.Second)))
		attempts := 0
		for time.Now().Before(end) {
			attempts++
			bounded(5*time.Second, func() (struct{}, error) { _, _, err := c.GetBytes("life:s"); return struct{}{}, err })
			time.Sleep(50 * time.Millisecond)
		}
		emit(map[string]any{"event": "storm", "attempts": attempts})
		c.Close()
	case "exit_noclose":
		c, _ := connect()
		c.Set("life:e", "v", 0)
		emit(map[string]any{"event": "exiting_without_close"})
	}
}
