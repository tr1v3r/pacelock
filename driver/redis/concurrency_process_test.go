package redisdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Launch the same race-instrumented test executable as four independent SDK
// processes. A file barrier ensures all clients are connected before admission.
func testContentionProcesses(t *testing.T, f *contentionFixture) {
	for _, mode := range []string{"allow"} {
		t.Run(mode, func(t *testing.T) {
			l := f.limiter(t)
			publishRules(t, l, contentionRule("q", 127, 1))
			dir := t.TempDir()
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			type processResult struct {
				index  int
				output []byte
				err    error
			}
			done := make(chan processResult, 4)
			for i := 0; i < 4; i++ {
				cmd := exec.CommandContext(ctx, exe, "-test.run=^TestConcurrencyProcessWorker$", "-test.timeout=35s")
				cmd.Env = append(os.Environ(), "PACELOCK_CONTENTION_WORKER="+dir, "PACELOCK_CONTENTION_INDEX="+strconv.Itoa(i), "PACELOCK_CONTENTION_PREFIX="+l.prefix, "PACELOCK_CONTENTION_CLUSTER="+strconv.FormatBool(f.cluster), "PACELOCK_CONTENTION_MODE="+mode)
				go func(i int) { out, err := cmd.CombinedOutput(); done <- processResult{i, out, err} }(i)
			}
			// Always join children before fixture and temporary-file cleanup, even if
			// startup fails. Early exit is reported rather than silently timing out.
			finished := 0
			defer func() {
				cancel()
				for finished < 4 {
					<-done
					finished++
				}
			}()
			for {
				ready := 0
				for i := 0; i < 4; i++ {
					if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("ready%d", i))); err == nil {
						ready++
					}
				}
				if ready == 4 {
					break
				}
				select {
				case r := <-done:
					finished++
					t.Fatalf("worker %d exited before barrier: %v\n%s", r.index, r.err, r.output)
				case <-ctx.Done():
					t.Fatal("workers did not reach start barrier")
				case <-time.After(10 * time.Millisecond):
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "start"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			var total int64
			for finished < 4 {
				r := <-done
				finished++
				if r.err != nil {
					t.Errorf("worker %d: %v\n%s", r.index, r.err, r.output)
					continue
				}
				b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("result%d", r.index)))
				if err != nil {
					t.Fatal(err)
				}
				var count int64
				if err := json.Unmarshal(b, &count); err != nil {
					t.Fatal(err)
				}
				total += count
			}
			want := int64(127)
			if total != want {
				t.Fatalf("cross-process admissions=%d, want %d", total, want)
			}
			burst := int64(127)
			requireDenied(t, l, contentionRule("q", burst, 1))
		})
	}
}

func TestConcurrencyProcessWorker(t *testing.T) {
	dir := os.Getenv("PACELOCK_CONTENTION_WORKER")
	if dir == "" {
		t.Skip("subprocess helper for TestRedisConcurrency")
	}
	cluster, err := strconv.ParseBool(os.Getenv("PACELOCK_CONTENTION_CLUSTER"))
	if err != nil {
		t.Fatal(err)
	}
	c := contentionClient(cluster, 8)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	l := &limiter{client: c, prefix: os.Getenv("PACELOCK_CONTENTION_PREFIX")}
	index := os.Getenv("PACELOCK_CONTENTION_INDEX")
	if err := os.WriteFile(filepath.Join(dir, "ready"+index), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "start")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	var admitted atomic.Int64
	contentionWave(t, 64, func(ctx context.Context, i int) {
		d, err := l.admitRules(ctx, contentionRule("q", 127, 1))
		if err != nil {
			t.Error(err)
		} else if d.Allowed {
			admitted.Add(1)
		}
	})
	b, err := json.Marshal(admitted.Load())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result"+index), b, 0600); err != nil {
		t.Fatal(err)
	}
}
