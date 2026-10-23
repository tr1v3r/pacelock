// An application-owned journal; this is not part of the limiter API.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tr1v3r/pacelock"
)

const retention = 10 * time.Minute
const policyVersion = 1

type attempt struct {
	ID       string
	Tenant   string
	Tokens   int64
	Policy   int
	Created  time.Time
	State    string
	Decision pacelock.Decision
}

// Policy comes from trusted application code, never from request-supplied rules.
// Keep this version available for the lifetime of its retained attempts.
func rules(a attempt) ([]pacelock.Rule, error) {
	if a.Policy != policyVersion || a.Tenant == "" || len(a.Tenant) > 128 || a.Tokens <= 0 || a.Tokens > 1000 {
		return nil, errors.New("unsupported policy, tenant or known input cost")
	}
	r := []pacelock.Rule{
		{Name: "account:rpm", Rate: 60, Period: time.Minute, Burst: 10, Cost: 1},
		{Name: "model:input-tpm", Rate: 60000, Period: time.Minute, Burst: 12000, Cost: a.Tokens},
		{Name: "tenant:" + a.Tenant + ":input-tpm", Rate: 6000, Period: time.Minute, Burst: 1000, Cost: a.Tokens},
	}
	return r, pacelock.Validate(r...)
}

type admission interface {
	AllowOnce(context.Context, string, time.Duration, ...pacelock.Rule) (pacelock.Receipt, error)
	Lookup(context.Context, string) (pacelock.Decision, bool, error)
}

type journal struct{ dir string }

// All commands hold an exclusive application lock. A crashed owner's lock must
// be investigated by an operator, never automatically stolen on a timeout.
func openJournal(dir string) (journal, func(), error) {
	j := journal{dir: dir}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return j, nil, err
	}
	lock := filepath.Join(dir, "lock")
	if err := os.Mkdir(lock, 0700); err != nil {
		return j, nil, fmt.Errorf("exclusive journal lock: %w", err)
	}
	return j, func() { _ = os.Remove(lock) }, nil
}

func (j journal) save(a attempt) error {
	data, err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(j.dir, ".attempt-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(j.dir, "attempt.json")); err != nil {
		return err
	}
	d, err := os.Open(j.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (j journal) load() (attempt, error) {
	var a attempt
	b, err := os.ReadFile(filepath.Join(j.dir, "attempt.json"))
	if err != nil {
		return a, err
	}
	err = json.Unmarshal(b, &a)
	if err == nil && (a.ID == "" || a.Created.IsZero()) {
		err = errors.New("invalid journal identity")
	}
	return a, err
}

// recover only observes admission. The durable execution state, not an allowed
// receipt alone, determines whether a later explicit run may execute work.
func advance(ctx context.Context, j journal, l admission, action, fault string, execute func(string) error) error {
	a, err := j.load()
	if err != nil {
		return err
	}
	r, err := rules(a)
	if err != nil {
		return err
	}
	if a.State == "done" || a.State == "denied" || a.State == "not_charged" {
		return nil
	}
	if a.State == "executing" {
		return errors.New("upstream outcome unknown: reconcile manually; do not execute again")
	}
	if time.Now().Before(a.Created) || time.Since(a.Created) >= retention {
		return errors.New("recovery window ended or clock moved backward: preserve uncertainty; do not reuse ID")
	}
	if action == "recover" {
		if a.State != "admitting" {
			return nil
		}
		d, found, err := l.Lookup(ctx, a.ID)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("admission missing: may be in flight, expired or lost; preserve journal")
		}
		a.Decision = d
		a.State = "denied"
		if d.Allowed {
			a.State = "admitted"
		}
		return j.save(a)
	}
	if a.State == "admitting" {
		return errors.New("admission outcome unknown: use recover with a fresh deadline")
	}
	if a.State == "prepared" {
		a.State = "admitting"
		if err = j.save(a); err != nil {
			return err
		}
		receipt, callErr := l.AllowOnce(ctx, a.ID, retention, r...)
		if fault == "after-admission" {
			return errors.New("injected lost admission response; journal remains admitting")
		}
		if callErr != nil {
			if pacelock.OutcomeOf(callErr) == pacelock.NotCharged {
				a.State = "not_charged"
				if err = j.save(a); err != nil {
					return err
				}
			}
			return callErr
		}
		a.Decision = receipt.Decision
		a.State = "denied"
		if receipt.Allowed {
			a.State = "admitted"
		}
		if err = j.save(a); err != nil {
			return err
		}
	}
	if a.State == "denied" {
		return nil
	}
	if a.State != "admitted" {
		return fmt.Errorf("unrecognized journal state %q", a.State)
	}
	// This durable intent must precede every external side effect. A crash between
	// intent and completion deliberately sacrifices automatic retry for safety.
	a.State = "executing"
	if err = j.save(a); err != nil {
		return err
	}
	if err = execute(a.ID); err != nil {
		return err
	}
	if fault == "after-execution" {
		return errors.New("injected lost upstream completion; reconcile manually")
	}
	a.State = "done"
	return j.save(a)
}

func run() error {
	dir := flag.String("dir", "./attempt-demo", "one attempt journal directory")
	tenant := flag.String("tenant", "tenant-a", "trusted tenant identity for new attempt")
	tokens := flag.Int64("tokens", 100, "known input tokens for new attempt")
	fault := flag.String("fault", "", "after-admission or after-execution")
	flag.Parse()
	if flag.NArg() != 1 {
		return errors.New("usage: durable [flags] new|run|recover|status")
	}
	action := flag.Arg(0)
	if action != "new" && action != "run" && action != "recover" && action != "status" {
		return errors.New("unknown action")
	}
	if *fault != "" && *fault != "after-admission" && *fault != "after-execution" {
		return errors.New("unknown fault")
	}
	j, close, err := openJournal(*dir)
	if err != nil {
		return err
	}
	defer close()
	if action == "new" {
		if _, err := os.Stat(filepath.Join(*dir, "attempt.json")); !os.IsNotExist(err) {
			return errors.New("journal exists or cannot be checked; use a new directory for a genuinely new attempt")
		}
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return err
		}
		a := attempt{ID: hex.EncodeToString(id[:]), Tenant: *tenant, Tokens: *tokens, Policy: policyVersion, Created: time.Now(), State: "prepared"}
		if _, err := rules(a); err != nil {
			return err
		}
		return j.save(a)
	}
	if action == "status" {
		a, err := j.load()
		if err != nil {
			return err
		}
		b, _ := json.MarshalIndent(a, "", "  ")
		fmt.Println(string(b))
		return nil
	}
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:16379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer c.Close()
	l, err := pacelock.New(c, "durable-example", "provider-account")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return advance(ctx, j, l, action, *fault, func(id string) error {
		fmt.Println("MOCK upstream executed for", id)
		return nil
	})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
