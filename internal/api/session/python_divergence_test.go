package session

import "testing"

// pythonDivergences are the places the Go forgot and resend routes are stricter
// than the Python ones (D6129), and the one place the validate budget is larger. Each names the Python behaviour, the Go answer and why; the Python side is
// never edited.
var pythonDivergences = []struct{ route, python, goBehaviour, reason string }{
	{"POST /api/v1/auth/forgot-password", "a failure of the token write, the audit row or the mail after the lookup answered 500, for a known address only",
		"one 200 for every address; the failure is logged by purpose and counted",
		"the 500 told a caller that the address has an account; the answer must not depend on it"},
	{"POST /api/v1/auth/resend-verification", "a failure of the token write or the mail after the lookup answered 500, for an unverified known address only",
		"one 200 for every address; the failure is logged by purpose and counted",
		"the 500 told a caller that the address has an unverified account"},
	{"POST /api/v1/auth/validate", "30 checks per token per 15 minutes, then 429",
		"httpapi.ValidateLimitCount (100) per token per 15 minutes, the same count on auth-service's compatibility route",
		"web checks each session once a minute per web process, and every process spends from the same per-token budget; at 30 two processes sit at the limit and a 429 is a transient that widens the freshness bound"},
}

func TestPythonDivergencesEachNameAReason(t *testing.T) {
	if len(pythonDivergences) != 3 {
		t.Fatalf("%d divergence rows, want the forgot, the resend and the validate row", len(pythonDivergences))
	}
	for _, row := range pythonDivergences {
		if row.route == "" || row.python == "" || row.goBehaviour == "" || row.reason == "" {
			t.Errorf("incomplete divergence row %+v", row)
		}
	}
}
