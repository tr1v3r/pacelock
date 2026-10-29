package pacelock

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestGCRATimeline(t *testing.T) {
	r := Rule{Name: "q", Policy: Policy{Rate: 2, Period: time.Second, Burst: 5}}
	var state QuotaState
	for _, tc := range []struct {
		at      int64
		cost    int64
		allowed bool
		wait    time.Duration
		tat     int64
	}{
		{0, 3, true, 0, 1500000}, {0, 2, true, 0, 2500000},
		{250000, 1, false, 250 * time.Millisecond, 2500000},
		{500000, 1, true, 0, 3000000}, {1000000, 3, false, time.Second, 3000000},
		{2000000, 3, true, 0, 4500000},
	} {
		r.Cost = tc.cost
		d, next, err := Evaluate(tc.at, []Rule{r}, []QuotaState{state})
		if err != nil || d.Allowed != tc.allowed || d.RetryAfter != tc.wait {
			t.Fatalf("%+v: %+v %v", tc, d, err)
		}
		if d.Allowed {
			state = next[0]
		} else if next != nil {
			t.Fatal("denied transition")
		}
		if state.TAT != tc.tat {
			t.Fatal(state)
		}
	}
}

func TestGCRAEnvelopeAndJointDenial(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	r := Rule{Name: "q", Policy: Policy{Rate: 4, Period: time.Second, Burst: 20}, Cost: 1}
	var state QuotaState
	var now, accepted int64
	for range 10000 {
		now += int64(rng.Intn(5000))
		r.Cost = int64(rng.Intn(5) + 1)
		d, next, err := Evaluate(now, []Rule{r}, []QuotaState{state})
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			accepted += r.Cost
			state = next[0]
		}
		if accepted > 20+now/250000 {
			t.Fatalf("envelope exceeded: %d at %d", accepted, now)
		}
	}
	rules := []Rule{{Name: "free", Policy: Policy{Rate: 1, Period: time.Second, Burst: 1}, Cost: 1}, {Name: "busy", Policy: Policy{Rate: 1, Period: time.Second, Burst: 1}, Cost: 1}}
	states := []QuotaState{{}, {TAT: 1000000, Interval: 1000000, Burst: 1, ExpiresAt: 1000000}}
	before := append([]QuotaState(nil), states...)
	d, next, err := Evaluate(0, rules, states)
	if err != nil || d.Allowed || next != nil || !reflect.DeepEqual(states, before) || !reflect.DeepEqual(d.Limited, []string{"busy"}) {
		t.Fatal(d, next, err)
	}
}

func TestGCRAPolicyAndExpiry(t *testing.T) {
	r := sampleRule()
	_, next, _ := Evaluate(1000000, []Rule{r}, []QuotaState{{}})
	r.Policy.Burst++
	if _, _, err := Evaluate(1000001, []Rule{r}, next); !errors.Is(err, ErrPolicyMismatch) {
		t.Fatal(err)
	}
	if d, _, err := Evaluate(next[0].ExpiresAt, []Rule{r}, next); err != nil || !d.Allowed {
		t.Fatal(d, err)
	}
	if _, _, err := Evaluate(-1, []Rule{r}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := Evaluate(-1, []Rule{r}, []QuotaState{{}}); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
}

// The reference keeps fractional available tokens, independently of TAT storage.
func TestGCRAAgainstTokenBucketModel(t *testing.T) {
	rng := rand.New(rand.NewSource(491))
	for _, dimensions := range []int{1, 4, 32} {
		rules := make([]Rule, dimensions)
		states := make([]QuotaState, dimensions)
		credit := make([]float64, dimensions)
		steps := make([]float64, dimensions)
		for i := range rules {
			rules[i] = Rule{Name: fmt.Sprint(i), Policy: Policy{Rate: int64(1 + rng.Intn(9)), Period: time.Duration(1+rng.Intn(50)) * time.Millisecond, Burst: int64(1 + rng.Intn(30))}, Cost: 1}
			step, _ := rules[i].Interval()
			steps[i] = float64(step.Microseconds())
			credit[i] = float64(rules[i].Policy.Burst)
		}
		now := int64(1_000_000_000_000)
		for range 2000 {
			delta := int64(rng.Intn(50000))
			now += delta
			allowed := true
			var limited []string
			var wait float64
			for i := range rules {
				rules[i].Cost = int64(1 + rng.Intn(int(rules[i].Policy.Burst)))
				credit[i] = math.Min(float64(rules[i].Policy.Burst), credit[i]+float64(delta)/steps[i])
				missing := float64(rules[i].Cost) - credit[i]
				if missing > 1e-9 {
					allowed = false
					limited = append(limited, rules[i].Name)
					wait = math.Max(wait, missing*steps[i])
				}
			}
			d, next, err := Evaluate(now, rules, states)
			if err != nil || d.Allowed != allowed || !reflect.DeepEqual(d.Limited, limited) || math.Abs(float64(d.RetryAfter.Microseconds())-wait) > 1.001 {
				t.Fatalf("dimensions=%d decision=%+v model=%v/%v/%f error=%v", dimensions, d, allowed, limited, wait, err)
			}
			if allowed {
				states = next
				for i := range rules {
					credit[i] -= float64(rules[i].Cost)
				}
			}
		}
	}
}
