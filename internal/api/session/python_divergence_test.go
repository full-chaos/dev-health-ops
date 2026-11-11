package session

import "testing"

// pythonDivergences is the one place the Go validate budget is larger than
// Python's. Each row names the Python behaviour, the Go answer and why; the
// Python side is never edited.
var pythonDivergences = []struct{ route, python, goBehaviour, reason string }{
	{"POST /api/v1/auth/validate", "30 checks per token per 15 minutes, then 429",
		"httpapi.ValidateLimitCount (100) per token per 15 minutes, the same count on auth-service's compatibility route",
		"web checks each session once a minute per web process, and every process spends from the same per-token budget; at 30 two processes sit at the limit and a 429 is a transient that widens the freshness bound"},
}

func TestPythonDivergencesEachNameAReason(t *testing.T) {
	if len(pythonDivergences) != 1 {
		t.Fatalf("%d divergence rows, want the validate row", len(pythonDivergences))
	}
	for _, row := range pythonDivergences {
		if row.route == "" || row.python == "" || row.goBehaviour == "" || row.reason == "" {
			t.Errorf("incomplete divergence row %+v", row)
		}
	}
}
