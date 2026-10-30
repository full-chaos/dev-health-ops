package audit_test

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

const (
	drillAttemptRevokedName  = "drill-11232-wl-revoked"
	drillAttemptDisabledName = "drill-11232-wl-disabled"
)

type drillAttemptOutcome struct {
	Name     string `json:"name"`
	Accepted bool   `json:"accepted"`
	Reason   string `json:"refusal_reason,omitempty"`
}

// checkAttempts is the rule of the authentication-attempt drill (CHAOS-11972), pure so it is unit-tested without docker.
// tries are the revoked secret's outcome, then the disabled secret's.
//
// Where a refusal is required (the end-of-WAL copy, expect F, and every fenced copy) each secret must be refused, and for
// ITS reason: the revoked one "revoked", the disabled one "disabled". A store error or any other reason is not a pass: it
// would hide a verifier that failed instead of refusing. On the unfenced PITR copy both secrets must be accepted: that is
// the measured gap, and a drill that does not reproduce it fails rather than passing silently. Messages name the secret
// label and the reason, never a value.
func checkAttempts(expect string, fenced bool, tries []drillAttemptOutcome) error {
	if len(tries) != 2 || tries[0].Name != drillAttemptRevokedName || tries[1].Name != drillAttemptDisabledName {
		return fmt.Errorf("the attempts are not exactly [%s, %s]: %d recorded", drillAttemptRevokedName, drillAttemptDisabledName, len(tries))
	}
	want := map[string]string{
		drillAttemptRevokedName:  string(workload.ReasonRevoked),
		drillAttemptDisabledName: string(workload.ReasonDisabled),
	}
	if expect == "F" || fenced {
		for _, try := range tries {
			switch {
			case try.Accepted:
				return fmt.Errorf("revoked access is still usable: %s was accepted (expect=%s fenced=%v)", try.Name, expect, fenced)
			case try.Reason != want[try.Name]:
				return fmt.Errorf("%s was refused for the reason %q, want %q (expect=%s fenced=%v)", try.Name, try.Reason, want[try.Name], expect, fenced)
			}
		}
		return nil
	}
	for _, try := range tries {
		if !try.Accepted {
			return fmt.Errorf("the unfenced PITR copy did not reproduce the gap: %s was not accepted (reason %q)", try.Name, try.Reason)
		}
	}
	return nil
}
