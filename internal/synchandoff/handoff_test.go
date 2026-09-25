package synchandoff

import (
	"testing"
	"time"
)

func TestOccurrenceIdentityIsThePythonIdentity(t *testing.T) {
	at := time.Date(2026, 3, 10, 12, 0, 0, 123456000, time.UTC)
	got := OccurrenceIdentity("000000c0-0000-4000-8000-000000000001", at)
	if want := "sha256:1031419303d071c2e5dc8c9f9d5884a6ecc741a3609ecbc4859087ba11b3e1e2"; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestTruthyIsPythonTruthiness(t *testing.T) {
	for value, want := range map[string]bool{"": false, "x": true} {
		if got := truthy(value); got != want {
			t.Errorf("truthy(%q) = %v, want %v", value, got, want)
		}
	}
	if truthy(nil) || truthy(false) || truthy(float64(0)) || truthy([]any{}) || truthy(map[string]any{}) || !truthy(float64(1)) || !truthy([]any{1}) {
		t.Error("truthy disagrees with Python")
	}
}
