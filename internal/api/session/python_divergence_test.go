package session

import "testing"

// pythonDivergences are the places the Go forgot and resend routes are stricter
// than the Python ones (D6129). Each names the Python behaviour, the Go answer and why; the Python side is
// never edited.
var pythonDivergences = []struct{ route, python, goBehaviour, reason string }{
	{"POST /api/v1/auth/forgot-password", "a failure of the token write, the audit row or the mail after the lookup answered 500, for a known address only",
		"one 200 for every address; the failure is logged by purpose and counted",
		"the 500 told a caller that the address has an account; the answer must not depend on it"},
	{"POST /api/v1/auth/resend-verification", "a failure of the token write or the mail after the lookup answered 500, for an unverified known address only",
		"one 200 for every address; the failure is logged by purpose and counted",
		"the 500 told a caller that the address has an unverified account"},
}

func TestPythonDivergencesEachNameAReason(t *testing.T) {
	if len(pythonDivergences) != 2 {
		t.Fatalf("%d divergence rows, want the forgot and the resend row", len(pythonDivergences))
	}
	for _, row := range pythonDivergences {
		if row.route == "" || row.python == "" || row.goBehaviour == "" || row.reason == "" {
			t.Errorf("incomplete divergence row %+v", row)
		}
	}
}
