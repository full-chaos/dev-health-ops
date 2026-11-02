package valkeyreply

import "testing"

func TestErrorCarriesTheReplyText(t *testing.T) {
	err := Error(t, "WRONGTYPE Operation against a key holding the wrong kind of value")
	if got := err.Error(); got != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("reply text %q", got)
	}
}
