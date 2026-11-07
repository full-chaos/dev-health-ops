package sso

import (
	"encoding/json"
	"testing"
)

func TestIdentityConflictDetailIsGeneric(t *testing.T) {
	raw, err := json.Marshal(identityConflictDetail())
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"message":"This sign-in could not be completed. Contact your administrator if this continues."}`
	if string(raw) != want {
		t.Fatalf("detail %s, want %s", raw, want)
	}
}
