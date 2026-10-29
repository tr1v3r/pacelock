// Isolated experiment, not a production scalar-state implementation.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Result struct {
	Name    string    `json:"name"`
	Unit    string    `json:"unit"`
	Samples []float64 `json:"samples"`
	Median  float64   `json:"median"`
	Min     float64   `json:"min"`
	Max     float64   `json:"max"`
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func summarize(r *Result) {
	v := append([]float64(nil), r.Samples...)
	sort.Float64s(v)
	r.Min = v[0]
	r.Max = v[len(v)-1]
	r.Median = v[len(v)/2]
	if len(v)%2 == 0 {
		r.Median = (v[len(v)/2-1] + v[len(v)/2]) / 2
	}
}

const scalarReader = `local function quota_state(raw, now)
 local tat = tonumber(raw)
 if not tat or tat < 0 or tat ~= math.floor(tat) or tat > exact or tat > now + horizon then return nil end
 return {tat, nil, nil, tat <= now}
end
`

func main() {
	addr := flag.String("addr", "", "required: isolated, disposable Redis address")
	rounds := flag.Int("rounds", 15, "randomized rounds")
	iterations := flag.Int("iterations", 50000, "iterations per micro script")
	batch := flag.Int("batch", 1000, "full-script calls per sample")
	flag.Parse()
	if *addr == "" || *rounds < 1 || *iterations < 1 || *iterations > 1000000 || *batch < 1 || *batch > 10000 {
		fmt.Fprintln(os.Stderr, "require -addr, rounds >= 1, iterations in [1,1000000], batch in [1,10000]")
		os.Exit(2)
	}
	protocol, err := os.ReadFile("internal/engine/protocol.lua")
	must(err)
	gcra, err := os.ReadFile("internal/engine/gcra.lua")
	must(err)
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: *addr, MaxRetries: -1, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second})
	defer c.Close()
	must(c.Ping(ctx).Err())
	info, err := c.Info(ctx, "server").Result()
	must(err)
	start := strings.Index(string(protocol), "local function quota_state(")
	end := strings.Index(string(protocol), "local function policy_state(")
	if start < 0 || end < 0 {
		panic("reader boundaries missing")
	}
	scalarProtocol := string(protocol[:start]) + scalarReader + string(protocol[end:])
	rng := rand.New(rand.NewSource(20261028))

	type microCase struct {
		name, prelude, decode string
		tuple                 bool
		sha                   string
	}
	cases := []microCase{
		{name: "loop_baseline", decode: `return 1700000001000000 + index`},
		{name: "scalar_parse", decode: `return tonumber(raw)`},
		{name: "tuple_parse", tuple: true, decode: `local tat, step, burst = string.match(raw, '^(%d+):(%d+):(%d+)$')
tat, step, burst = tonumber(tat), tonumber(step), tonumber(burst)
return tat`},
		{name: "scalar_reader", prelude: scalarProtocol, decode: `local state=quota_state(raw,now)
if not state then error('invalid scalar') end
return math.max(state[1], now)`},
		{name: "tuple_reader_policy", prelude: string(protocol), tuple: true, decode: `local state=quota_state(raw,now)
if not state then error('invalid tuple') end
if not state[4] and (state[2] ~= 100000 or state[3] ~= 10) then error('policy') end
return math.max(state[1], now)`},
	}
	results := make([]*Result, len(cases))
	for i := range cases {
		cs := &cases[i]
		rawExpr := `string.format('%.0f', 1700000001000000+i)`
		if cs.tuple {
			rawExpr = `string.format('%.0f:100000:10', 1700000001000000+i)`
		}
		script := cs.prelude + "\nlocal function decode(raw, now, index)\n" + cs.decode + "\nend\n" + `
local values={}
for i=1,64 do values[i]=` + rawExpr + ` end
local n=tonumber(ARGV[1])
local sum=0
for i=1,n do
 local index=(i-1)%64+1
 local tat=decode(values[index],1700000000000000,index)
 sum=sum+tat-1700000000000000
end
return sum
`
		cs.sha, err = c.ScriptLoad(ctx, script).Result()
		must(err)
		results[i] = &Result{Name: cs.name, Unit: "ns/parse", Samples: []float64{}}
	}
	// Redis TIME can be frozen within a script. Use Redis commandstats timing,
	// not TIME or client wall time. Fixture setup is amortized across iterations.
	stats := func() (int64, int64) {
		text, err := c.Info(ctx, "commandstats").Result()
		must(err)
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, "cmdstat_evalsha:") {
				fields := strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "cmdstat_evalsha:")), ",")
				var calls, usec int64
				for _, f := range fields {
					p := strings.SplitN(f, "=", 2)
					if len(p) != 2 {
						continue
					}
					if p[0] == "calls" {
						calls, err = strconv.ParseInt(p[1], 10, 64)
						must(err)
					}
					if p[0] == "usec" {
						usec, err = strconv.ParseInt(p[1], 10, 64)
						must(err)
					}
				}
				return calls, usec
			}
		}
		return 0, 0 // No EVALSHA has run yet on a new server.
	}
	var checksum int64
	for i := 0; i < *iterations; i++ {
		checksum += 1000000 + int64(i%64+1)
	}
	// Three warmup rounds, excluded. Network and SCRIPT LOAD are not timed.
	for round := -3; round < *rounds; round++ {
		for _, i := range rng.Perm(len(cases)) {
			calls0, us0 := stats()
			value, err := c.EvalSha(ctx, cases[i].sha, nil, *iterations).Int64()
			must(err)
			calls1, us1 := stats()
			if calls1-calls0 != 1 || us1 <= us0 {
				panic("invalid micro timing")
			}
			if value != checksum {
				panic(fmt.Sprintf("checksum mismatch: %s %v", cases[i].name, value))
			}
			if round >= 0 {
				results[i].Samples = append(results[i].Samples, float64(us1-us0)*1000/float64(*iterations))
			}
		}
	}

	// Actual Allow script versus an experimental scalar-state version. All Lua
	// commands, response construction, and MSET remain; only state reading,
	// policy comparison and quota encoding differ. This is NOT contract-equivalent.
	policyBlock := `            if state[2] ~= step or state[3] ~= burst then
                return redis.error_reply('PACE_POLICY quota policy changed while state is active')
            end
`
	oldEncoding := `string.format('%.0f:%s:%s', nexttat, ARGV[offset + 1], ARGV[offset + 2])`
	if strings.Count(string(gcra), policyBlock) != 1 || strings.Count(string(gcra), oldEncoding) != 1 {
		panic("unexpected production script")
	}
	scalarGCRA := strings.Replace(string(gcra), policyBlock, "", 1)
	scalarGCRA = strings.Replace(scalarGCRA, oldEncoding, `string.format('%.0f', nexttat)`, 1)
	type fullCase struct {
		name, sha string
		keys      []string
		args      []interface{}
		tuple     bool
		result    *Result
	}
	full := []fullCase{}
	for _, n := range []int{1, 4, 32} {
		for _, tuple := range []bool{false, true} {
			mode, script := "scalar", scalarProtocol+"\nlocal managed = false\n"+scalarGCRA
			if tuple {
				mode, script = "tuple", string(protocol)+"\nlocal managed = false\n"+string(gcra)
			}
			sha, err := c.ScriptLoad(ctx, script).Result()
			must(err)
			f := fullCase{name: fmt.Sprintf("allow_%s_%d_quotas", mode, n), sha: sha, tuple: tuple, args: []interface{}{}}
			for j := 0; j < n; j++ {
				f.keys = append(f.keys, fmt.Sprintf("bench:{lua-parse}:%s:%d:%d", mode, n, j))
				f.args = append(f.args, 1, 1000000000, 1)
			}
			f.result = &Result{Name: f.name, Unit: "us/request (Redis commandstats)", Samples: []float64{}}
			full = append(full, f)
		}
	}
	for round := -3; round < *rounds; round++ {
		for _, i := range rng.Perm(len(full)) {
			f := &full[i]
			now, err := c.Time(ctx).Result()
			must(err)
			seed := fmt.Sprint(now.UnixMicro() + 60000000)
			if f.tuple {
				seed += ":1:1000000000"
			}
			pairs := []interface{}{}
			for _, k := range f.keys {
				pairs = append(pairs, k, seed)
			}
			must(c.MSet(ctx, pairs...).Err())
			calls0, us0 := stats()
			pipe := c.Pipeline()
			commands := make([]*redis.Cmd, *batch)
			for j := range commands {
				commands[j] = pipe.EvalSha(ctx, f.sha, f.keys, f.args...)
			}
			_, err = pipe.Exec(ctx)
			must(err)
			calls1, us1 := stats()
			if calls1-calls0 != int64(*batch) {
				panic("unexpected evalsha call count")
			}
			for _, cmd := range commands {
				v, err := cmd.Slice()
				must(err)
				if len(v) != len(f.keys)+2 || v[0].(int64) != 1 || v[1].(int64) != 0 {
					panic(fmt.Sprintf("not allowed: %v", v))
				}
			}
			if round >= 0 {
				f.result.Samples = append(f.result.Samples, float64(us1-us0)/float64(*batch))
			}
		}
	}
	for _, f := range full {
		results = append(results, f.result)
		must(c.Del(ctx, f.keys...).Err())
	}
	for _, r := range results {
		summarize(r)
	}
	out := struct {
		Server                    string `json:"server"`
		QuotaFormat               string `json:"quota_format"`
		LuaSHA256                 string `json:"lua_sha256"`
		Rounds, Iterations, Batch int
		Results                   []*Result `json:"results"`
	}{info, "tat_us:interval_us:burst", fmt.Sprintf("%x", sha256.Sum256([]byte(string(protocol)+"\nlocal managed = false\n"+string(gcra)))), *rounds, *iterations, *batch, results}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	must(enc.Encode(out))
}
