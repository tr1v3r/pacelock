package postgres

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tr1v3r/pacelock"
)

func TestDecodeDecision(t *testing.T) {
	for _, want := range []pacelock.Decision{
		{Allowed: true},
		{Allowed: true, Limited: []string{}},
		{RetryAfter: time.Second, Limited: []string{"requests", "tokens"}},
	} {
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeDecision(raw)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip %s: %+v %v", raw, got, err)
		}
	}
	for _, raw := range []string{
		`null`, `[]`, `{}`, `{"Allowed":true}`,
		`{"allowed":true,"RetryAfter":0,"Limited":null}`,
		`{"Allowed":true,"RetryAfter":0,"Limited":null,"extra":1}`,
		`{"Allowed":true,"Allowed":false,"RetryAfter":1,"Limited":["q"]}`,
		`{"Allowed":null,"RetryAfter":1,"Limited":["q"]}`,
		`{"Allowed":true,"RetryAfter":null,"Limited":null}`,
		`{"Allowed":false,"RetryAfter":1,"Limited":[null]}`,
		`{"Allowed":false,"RetryAfter":0,"Limited":["q"]}`,
		`{"Allowed":true,"RetryAfter":0,"Limited":["q"]}`,
		`{"Allowed":false,"RetryAfter":1,"Limited":["q","q"]}`,
		`{"Allowed":true,"RetryAfter":0,"Limited":null} {}`,
		`{"Allowed":true,"RetryAfter":0,"Limited":null`,
		"{\"Allowed\":false,\"RetryAfter\":1,\"Limited\":[\"\xff\"]}",
		strings.Repeat(" ", 65537),
	} {
		if got, err := decodeDecision([]byte(raw)); !errors.Is(err, pacelock.ErrState) || !reflect.DeepEqual(got, pacelock.Decision{}) {
			t.Fatalf("accepted malformed record %q: %+v %v", raw, got, err)
		}
	}
}
