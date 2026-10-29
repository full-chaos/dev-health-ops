package audit_test

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
// Stub: accepts everything.
func checkAttempts(expect string, fenced bool, tries []drillAttemptOutcome) error {
	_, _, _ = expect, fenced, tries
	return nil
}
