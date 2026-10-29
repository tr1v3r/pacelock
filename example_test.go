package pacelock_test

import (
	"fmt"
	"time"

	"github.com/tr1v3r/pacelock"
)

func ExampleAdmission_Validate() {
	request := pacelock.Admission{
		Scope:   pacelock.Scope{Namespace: "app", Domain: "account"},
		Charges: []pacelock.Charge{{Name: "requests", Cost: 1}},
	}
	// Dynamic admission carries no local policy: the backend resolves it.
	fmt.Println(request.Validate())
	// Output: <nil>
}

func ExampleValidate() {
	err := pacelock.Validate(
		pacelock.Rule{Name: "requests", Policy: pacelock.Policy{Rate: 60, Period: time.Minute, Burst: 10}, Cost: 1},
		pacelock.Rule{Name: "input-tokens", Policy: pacelock.Policy{Rate: 60000, Period: time.Minute, Burst: 10000}, Cost: 1500},
	)
	fmt.Println(err)
	// Output: <nil>
}
