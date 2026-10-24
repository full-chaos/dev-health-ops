package logging

import (
	"strings"
	"testing"
)

func TestRedactKeyedTextHidesProtectedKeysAndScrubsTheRest(t *testing.T) {
	core := strings.Repeat("A1b2C3d4", 5)
	if got := RedactKeyedText("password", "anything"); got != redacted {
		t.Errorf("protected key: %q", got)
	}
	if got := RedactKeyedText("note", "see gh"+"p_"+core+" now"); strings.Contains(got, core) {
		t.Errorf("credential survived: %q", got)
	}
	if got := RedactKeyedText("note", "plain text"); got != "plain text" {
		t.Errorf("plain text changed: %q", got)
	}
	if got := RedactKeyedText("note", strings.Repeat("x", maxLoggedValueBytes+10)); len(got) > maxLoggedValueBytes+len(truncatedSuffix) {
		t.Errorf("value not bounded: %d bytes", len(got))
	}
	if !KeyProtected("api_key") || KeyProtected("outcome") {
		t.Error("KeyProtected disagrees with ProtectedKey")
	}
}
