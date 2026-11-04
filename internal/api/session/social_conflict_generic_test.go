package session

import "testing"

func TestSocialLoginEmailConflictAnswerIsGeneric(t *testing.T) {
	const want = "This sign-in could not be completed. Contact your administrator if this continues."
	if socialAccountExists != want {
		t.Fatalf("socialAccountExists = %q, want %q", socialAccountExists, want)
	}
}
