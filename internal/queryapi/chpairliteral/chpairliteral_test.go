package chpairliteral

import (
	"encoding/hex"
	"testing"
)

func TestEncodeHexJoinsPairsWithColon(t *testing.T) {
	got := Encode([][2]string{{"velocity", "run-1"}})
	want := "['" + hex.EncodeToString([]byte("velocity")) + ":" + hex.EncodeToString([]byte("run-1")) + "']"
	if got != want {
		t.Fatalf("Encode() = %q, want %q", got, want)
	}
}

func TestEncodeMultiplePairsCommaJoined(t *testing.T) {
	got := Encode([][2]string{{"a", "1"}, {"b", "2"}})
	want := "['" + hex.EncodeToString([]byte("a")) + ":" + hex.EncodeToString([]byte("1")) + "'," +
		"'" + hex.EncodeToString([]byte("b")) + ":" + hex.EncodeToString([]byte("2")) + "']"
	if got != want {
		t.Fatalf("Encode() = %q, want %q", got, want)
	}
}

func TestEncodeEmpty(t *testing.T) {
	if got := Encode(nil); got != "[]" {
		t.Fatalf("Encode(nil) = %q, want %q", got, "[]")
	}
}

// TestEncodeHostileContentProducesOnlyHexDigitsAndSafePunctuation regresses
// CHAOS-4745: quotes, backslashes, colons and unicode in either field must
// never require escaping, because the output alphabet excludes them
// entirely.
func TestEncodeHostileContentProducesOnlyHexDigitsAndSafePunctuation(t *testing.T) {
	hostile := [2]string{`it's a "quote" \ and : a colon`, "unicode: é中文"}
	got := Encode([][2]string{hostile})
	for _, r := range got {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r == '\'' || r == ':' || r == ',' || r == '[' || r == ']':
		default:
			t.Fatalf("Encode() produced a byte outside the safe alphabet: %q in %q", r, got)
		}
	}
}
