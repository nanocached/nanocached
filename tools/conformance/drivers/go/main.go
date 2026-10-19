// Conformance driver for the Go SDK: conf-go <cases.tsv> <phase> <path> <topology>
// One JSON line per step on stdout (case format: tools/conformance/make_cases.py).
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nanocached "github.com/nanocached/nanocached/sdk/go"
)

const sdkName = "go"
const stepTimeout = 10 * time.Second

var allSDKs = []string{"python", "typescript", "go", "java", "dotnet", "rust"}
var meta map[string]any

type step struct {
	c, op, key, vkind, varg, ttl string
	i, ms                        int
	x                            map[string]string
}

// kvAPI is what both *Client and *Namespace offer, so the same step runs on either.
type kvAPI interface {
	Get(key string) (string, bool, error)
	GetBytes(key string) ([]byte, bool, error)
	SetBytes(key string, value []byte, ttlSeconds int64) error
	Delete(key string) (bool, error)
	Incr(key string, delta int64) (int64, bool, error)
	Decr(key string, delta int64) (int64, bool, error)
	GetWithToken(key string) ([]byte, nanocached.CasToken, bool, error)
	Replace(key string, token nanocached.CasToken, newValue []byte, ttlSeconds int64) (bool, error)
	DeleteIfMatches(key string, token nanocached.CasToken) (bool, error)
	PutIfAbsent(key string, value []byte, ttlSeconds int64) (bool, error)
	ReplaceIfPresent(key string, value []byte, ttlSeconds int64) (bool, error)
	GetManyBytes(keys []string) (map[string][]byte, error)
	SetManyBytes(values map[string][]byte, ttlSeconds int64) (map[string]bool, error)
}

var regs = map[string]nanocached.CasToken{}

type item struct{ name, tmpl, vkind, varg, ttl string }

func emit(rec map[string]any) {
	for k, v := range meta {
		rec[k] = v
	}
	b, _ := json.Marshal(rec)
	fmt.Println(string(b))
}

func value(kind, arg string) []byte {
	if kind == "len" {
		n, _ := strconv.Atoi(arg)
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i % 251)
		}
		return b
	}
	b, _ := base64.StdEncoding.DecodeString(arg)
	return b
}

func keyOf(b64 string) string {
	if b64 == "=" {
		return ""
	}
	b, _ := base64.StdEncoding.DecodeString(b64)
	return string(b)
}

func hit(v []byte, ok bool) map[string]any {
	if !ok {
		return map[string]any{"outcome": "ok", "hit": false}
	}
	sum := sha256.Sum256(v)
	return map[string]any{"outcome": "ok", "hit": true, "len": len(v), "sha256": hex.EncodeToString(sum[:])}
}

func errRec(err error) map[string]any {
	msg := err.Error()
	if len(msg) > 300 {
		msg = msg[:300]
	}
	return map[string]any{"outcome": "error", "error_class": fmt.Sprintf("%T", err), "error_message": msg}
}

// ttlOf maps the case's TTL text onto Go's int64 parameter; a value int64
// cannot hold (fractional, or beyond 2^63-1) is reported as not expressible.
func ttlOf(t string) (int64, string) {
	if t == "-" {
		return 0, ""
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, "TTL " + t + " cannot be expressed as int64"
	}
	return n, ""
}

func genValue(i, n int) []byte {
	b := make([]byte, n)
	for j := range b {
		b[j] = byte((i + j) % 251)
	}
	return b
}

func b64d(s string) []byte {
	if s == "=" || s == "" {
		return nil
	}
	b, _ := base64.StdEncoding.DecodeString(s)
	return b
}

func manyFields(found map[string][]byte) (pairs [][2]string, checksum string) {
	keys := make([]string, 0, len(found))
	for k := range found {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		sum := sha256.Sum256(found[k])
		hx := hex.EncodeToString(sum[:])
		pairs = append(pairs, [2]string{base64.StdEncoding.EncodeToString([]byte(k)), hx})
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(hx))
	}
	return pairs, hex.EncodeToString(h.Sum(nil))
}

func tokenOf(s step) (nanocached.CasToken, error) {
	reg := s.x["reg"]
	if strings.HasPrefix(reg, "lit:") {
		raw, err := hex.DecodeString(reg[4:])
		if err != nil || len(raw) != 16 {
			return nanocached.CasToken{}, fmt.Errorf("bad literal token")
		}
		var d [16]byte
		copy(d[:], raw)
		return nanocached.TokenFromDigest(d), nil
	}
	return regs[reg], nil
}

func one(c *nanocached.Client, s step) map[string]any {
	switch s.op {
	case "sleep":
		time.Sleep(time.Duration(s.ms) * time.Millisecond)
		return map[string]any{"outcome": "ok"}
	case "probe":
		k := fmt.Sprintf("probe:%x", rand.Int63())
		if err := c.Set(k, "p", 0); err != nil {
			return errRec(err)
		}
		v, ok, err := c.Get(k)
		if err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok", "usable": ok && v == "p"}
	case "flush":
		if err := c.ClearAll(); err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok"}
	}
	var t kvAPI = c
	var ns *nanocached.Namespace
	if raw, has := s.x["ns"]; has {
		ns = c.Namespace(string(b64d(raw)))
		t = ns
	}
	if s.op == "clear" {
		if ns == nil {
			return map[string]any{"outcome": "error", "error_class": "driver", "error_message": "clear needs a namespace"}
		}
		if err := ns.Clear(); err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok"}
	}
	key := keyOf(s.key)
	ttl, na := ttlOf(s.ttl)
	if na != "" {
		return map[string]any{"outcome": "na", "error_message": na}
	}
	switch s.op {
	case "set":
		if err := t.SetBytes(key, value(s.vkind, s.varg), ttl); err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok"}
	case "get":
		v, ok, err := t.GetBytes(key)
		if err != nil {
			return errRec(err)
		}
		return hit(v, ok)
	case "get_text":
		v, ok, err := t.Get(key)
		if err != nil {
			return errRec(err)
		}
		return hit([]byte(v), ok)
	case "delete":
		existed, err := t.Delete(key)
		if err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok", "deleted": existed}
	case "incr", "decr":
		delta, _ := strconv.ParseInt(s.x["delta"], 10, 64)
		var v int64
		var ok bool
		var err error
		if s.op == "incr" {
			v, ok, err = t.Incr(key, delta)
		} else {
			v, ok, err = t.Decr(key, delta)
		}
		if err != nil {
			return errRec(err)
		}
		if !ok {
			return map[string]any{"outcome": "ok", "miss": true}
		}
		return map[string]any{"outcome": "ok", "n": v}
	case "incr_par":
		w, _ := strconv.Atoi(s.x["w"])
		m, _ := strconv.Atoi(s.x["m"])
		var wg sync.WaitGroup
		errs := make(chan error, w)
		for i := 0; i < w; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < m; j++ {
					if _, _, err := t.Incr(key, 1); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		if err, bad := <-errs; bad {
			return errRec(err)
		}
		v, _, err := t.Incr(key, 0)
		if err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok", "n": v}
	case "cas_get":
		v, tok, ok, err := t.GetWithToken(key)
		if err != nil {
			return errRec(err)
		}
		if !ok {
			return map[string]any{"outcome": "ok", "hit": false}
		}
		regs[s.x["reg"]] = tok
		r := hit(v, true)
		r["token"] = tok.Hex()
		return r
	case "cas_replace":
		tok, err := tokenOf(s)
		if err != nil {
			return errRec(err)
		}
		okk, err := t.Replace(key, tok, value(s.vkind, s.varg), ttl)
		if err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok", "result": okk}
	case "cas_delete":
		tok, err := tokenOf(s)
		if err != nil {
			return errRec(err)
		}
		okk, err := t.DeleteIfMatches(key, tok)
		if err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok", "result": okk}
	case "put_if_absent", "replace_if_present":
		var okk bool
		var err error
		if s.op == "put_if_absent" {
			okk, err = t.PutIfAbsent(key, value(s.vkind, s.varg), ttl)
		} else {
			okk, err = t.ReplaceIfPresent(key, value(s.vkind, s.varg), ttl)
		}
		if err != nil {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok", "result": okk}
	case "cas_race":
		_, tok, _, err := t.GetWithToken(key)
		if err != nil {
			return errRec(err)
		}
		w, _ := strconv.Atoi(s.x["w"])
		var wg sync.WaitGroup
		var won int64
		for i := 0; i < w; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if okk, err := t.Replace(key, tok, []byte(fmt.Sprintf("w%d", i)), 0); err == nil && okk {
					atomic.AddInt64(&won, 1)
				}
			}(i)
		}
		wg.Wait()
		return map[string]any{"outcome": "ok", "n": won}
	case "set_par", "setdel_par":
		w, _ := strconv.Atoi(s.x["w"])
		m, _ := strconv.Atoi(s.x["m"])
		var wg sync.WaitGroup
		errs := make(chan error, w)
		for i := 0; i < w; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < m; j++ {
					var err error
					if s.op == "setdel_par" && i%2 == 1 {
						_, err = t.Delete(key)
					} else {
						err = t.SetBytes(key, []byte(fmt.Sprintf("%s-%d-%d", sdkName, i, j)), ttl)
					}
					if err != nil {
						errs <- err
						return
					}
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		if err, bad := <-errs; bad {
			return errRec(err)
		}
		return map[string]any{"outcome": "ok"}
	case "many_set":
		rec := map[string][]byte{}
		if kv := s.x["kv"]; kv != "" {
			for _, item := range strings.Split(kv, ",") {
				p := strings.Split(item, ":")
				rec[string(b64d(p[0]))] = value(p[1], p[2])
			}
		}
		return manySet(t, rec, ttl)
	case "many_get":
		var keys []string
		if k := s.x["keys"]; k != "" {
			for _, item := range strings.Split(k, ",") {
				keys = append(keys, string(b64d(item)))
			}
		}
		found, err := t.GetManyBytes(keys)
		if err != nil {
			return errRec(err)
		}
		pairs, _ := manyFields(found)
		items := make([][]string, 0, len(pairs))
		for _, p := range pairs {
			items = append(items, []string{p[0], p[1]})
		}
		return map[string]any{"outcome": "ok", "items": items}
	case "many_set_gen":
		n, _ := strconv.Atoi(s.x["n"])
		vlen, _ := strconv.Atoi(s.x["vlen"])
		prefix := string(b64d(s.x["prefix"]))
		rec := make(map[string][]byte, n)
		for i := 0; i < n; i++ {
			rec[prefix+strconv.Itoa(i)] = genValue(i, vlen)
		}
		return manySet(t, rec, ttl)
	case "many_get_gen":
		n, _ := strconv.Atoi(s.x["n"])
		prefix := string(b64d(s.x["prefix"]))
		keys := make([]string, n)
		for i := range keys {
			keys[i] = prefix + strconv.Itoa(i)
		}
		found, err := t.GetManyBytes(keys)
		if err != nil {
			return errRec(err)
		}
		pairs, checksum := manyFields(found)
		return map[string]any{"outcome": "ok", "count": len(pairs), "checksum": checksum}
	}
	return map[string]any{"outcome": "error", "error_class": "driver", "error_message": "unknown op " + s.op}
}

func manySet(t kvAPI, rec map[string][]byte, ttl int64) map[string]any {
	res, err := t.SetManyBytes(rec, ttl)
	if err != nil {
		return errRec(err)
	}
	failed := 0
	for _, ok := range res {
		if !ok {
			failed++
		}
	}
	if failed > 0 {
		return map[string]any{"outcome": "error", "error_class": "partial", "error_message": fmt.Sprintf("%d of %d keys reported false", failed, len(res))}
	}
	return map[string]any{"outcome": "ok"}
}

func timed(c *nanocached.Client, s step) map[string]any {
	t0 := time.Now()
	var res map[string]any
	if s.op == "sleep" {
		res = one(c, s)
	} else {
		done := make(chan map[string]any, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					done <- map[string]any{"outcome": "error", "error_class": "panic", "error_message": fmt.Sprint(r)}
				}
			}()
			done <- one(c, s)
		}()
		select {
		case res = <-done:
		case <-time.After(stepTimeout):
			res = map[string]any{"outcome": "timeout"}
		}
	}
	res["ms"] = int(time.Since(t0).Milliseconds())
	return res
}

func main() { os.Exit(run()) }

func run() int {
	casesPath, phase, path, topology := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	meta = map[string]any{"sdk": sdkName, "path": path, "topology": topology}
	f, err := os.Open(casesPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var steps []step
	var items []item
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		p := strings.Split(sc.Text(), "\t")
		switch p[0] {
		case "S":
			i, _ := strconv.Atoi(p[2])
			ms, _ := strconv.Atoi(p[8])
			x := map[string]string{}
			if len(p) > 9 {
				for _, kv := range strings.Split(p[9], ";") {
					if i := strings.Index(kv, "="); i > 0 {
						x[kv[:i]] = kv[i+1:]
					}
				}
			}
			steps = append(steps, step{c: p[1], i: i, op: p[3], key: p[4], vkind: p[5], varg: p[6], ttl: p[7], ms: ms, x: x})
		case "X":
			t, _ := base64.StdEncoding.DecodeString(p[2])
			items = append(items, item{name: p[1], tmpl: string(t), vkind: p[3], varg: p[4], ttl: p[5]})
		}
	}
	var addrs []nanocached.Address
	for _, part := range strings.Split(os.Getenv("NANOTEST_ADDRESSES"), ",") {
		host, portStr, _ := net.SplitHostPort(part)
		port, _ := strconv.Atoi(portStr)
		addrs = append(addrs, nanocached.Address{Host: host, Port: port})
	}
	cfg := nanocached.Config{
		Addresses:  addrs,
		ViaProxy:   os.Getenv("NANOTEST_VIA_PROXY") == "1",
		AuthSecret: os.Getenv("NANOTEST_SECRET"),
	}
	// NANOTEST_OPTS: comma list of ff (fire-and-forget replica writes), rr (read repair), hedge=<ms>
	for _, o := range strings.Split(os.Getenv("NANOTEST_OPTS"), ",") {
		switch {
		case o == "ff":
			cfg.FireAndForgetReplicas = true
		case o == "rr":
			cfg.ReadRepair = true
		case strings.HasPrefix(o, "hedge="):
			ms, _ := strconv.Atoi(o[6:])
			cfg.ReadHedgeAfter = time.Duration(ms) * time.Millisecond
		}
	}
	client, err := nanocached.Connect(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect failed:", err)
		return 1
	}
	defer func() {
		statsLine := func(label string) {
			st := client.Stats()
			emit(map[string]any{"case": label, "step": 0, "op": "stats", "outcome": "ok", "replica_write_failures": st.ReplicaWriteFailures, "read_repair_failures": st.ReadRepairFailures, "refresh_failures": st.RefreshFailures, "transient_retries": st.TransientRetries})
		}
		if phase == "main" {
			statsLine("_stats_before_close")
		}
		done := make(chan struct{})
		go func() { client.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(stepTimeout):
		}
		if phase == "main" {
			statsLine("_stats")
		}
	}()
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	if phase == "main" {
		skip := ""
		for _, s := range steps {
			if skip == s.c {
				continue
			}
			r := timed(client, s)
			r["case"], r["step"], r["op"] = s.c, s.i, s.op
			emit(r)
			if r["outcome"] == "timeout" {
				skip = s.c
			}
		}
		for _, it := range items {
			s := step{x: map[string]string{}, op: "set", key: b64(strings.ReplaceAll(it.tmpl, "{sdk}", sdkName)), vkind: it.vkind, varg: it.varg, ttl: it.ttl}
			r := timed(client, s)
			r["case"], r["step"], r["op"] = "xsdk.write."+it.name, 0, "set"
			emit(r)
		}
	} else {
		for _, w := range allSDKs {
			for _, it := range items {
				r := timed(client, step{x: map[string]string{}, op: "get", key: b64(strings.ReplaceAll(it.tmpl, "{sdk}", w))})
				r["case"], r["step"], r["op"] = fmt.Sprintf("xsdk.read.%s.%s", w, it.name), 0, "get"
				emit(r)
			}
		}
	}
	return 0
}
