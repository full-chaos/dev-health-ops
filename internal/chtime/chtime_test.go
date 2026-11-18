package chtime

import (
	"testing"
	"time"
)

func TestMicrosKeepsTheSubSecondPartAndTheInstant(t *testing.T) {
	zone := time.FixedZone("x", 5*3600)
	at := time.Date(2026, 11, 10, 23, 59, 59, 500_123_000, zone)
	if got, want := Micros(at), at.UTC().UnixMicro(); got != want {
		t.Fatalf("Micros = %d, want %d", got, want)
	}
	if got := Micros(at) % 1_000_000; got != 500_123 {
		t.Fatalf("sub-second micros = %d, want 500123", got)
	}
}
