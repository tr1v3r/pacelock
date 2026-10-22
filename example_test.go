package pacelock_test

import (
	"fmt"
	"time"

	"github.com/tr1v3r/pacelock"
)

func ExampleValidate() {
	err := pacelock.Validate(
		pacelock.Rule{Name: "requests", Rate: 60, Period: time.Minute, Burst: 10, Cost: 1},
		pacelock.Rule{Name: "input-tokens", Rate: 60000, Period: time.Minute, Burst: 10000, Cost: 1500},
	)
	fmt.Println(err)
	// Output: <nil>
}
