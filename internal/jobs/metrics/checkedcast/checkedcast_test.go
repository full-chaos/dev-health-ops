package checkedcast

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestUint8(t *testing.T) {
	cases := []struct {
		name    string
		value   int
		want    uint8
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{"ordinary", 12, 12, false},
		{"exact boundary", math.MaxUint8, math.MaxUint8, false},
		{"one past boundary", math.MaxUint8 + 1, 0, true},
		{"negative", -1, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Uint8(tc.value, "t_family", "t_field")
			assertChecked(t, tc.value, got, err, tc.want, tc.wantErr)
		})
	}
}

func TestUint16(t *testing.T) {
	cases := []struct {
		name    string
		value   int
		want    uint16
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{"ordinary", 4000, 4000, false},
		{"exact boundary", math.MaxUint16, math.MaxUint16, false},
		{"one past boundary", math.MaxUint16 + 1, 0, true},
		{"negative", -1, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Uint16(tc.value, "t_family", "t_field")
			assertChecked(t, tc.value, got, err, tc.want, tc.wantErr)
		})
	}
}

// TestUint32RejectsOutOfRangeAndNegative is the red-first proof for the
// UInt32 half of this package: a bare uint32(x) conversion would wrap both
// "one past boundary" and "negative" into plausible-looking small numbers
// instead of erroring.
func TestUint32(t *testing.T) {
	cases := []struct {
		name    string
		value   int
		want    uint32
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{"ordinary", 42, 42, false},
		{"exact boundary", math.MaxUint32, math.MaxUint32, false},
		{"one past boundary", math.MaxUint32 + 1, 0, true},
		{"negative", -1, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Uint32(tc.value, "t_family", "t_field")
			assertChecked(t, tc.value, got, err, tc.want, tc.wantErr)
		})
	}
}

func TestUint64(t *testing.T) {
	cases := []struct {
		name    string
		value   int
		want    uint64
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{"ordinary", 42, 42, false},
		{"largest int", math.MaxInt64, math.MaxInt64, false},
		{"negative", -1, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Uint64(tc.value, "t_family", "t_field")
			assertChecked(t, tc.value, got, err, tc.want, tc.wantErr)
		})
	}
}

// TestErrorNamesFamilyAndField proves the failure the way a caller (and the
// audit trail) actually depends on: the error text and the error chain both
// identify exactly which family/field refused, not just that something
// failed.
func TestErrorNamesFamilyAndField(t *testing.T) {
	_, err := Uint32(-1, "repo_metrics_daily", "commits_count")
	if err == nil {
		t.Fatal("expected an error for a negative value, got nil")
	}
	if !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("error = %v, want it to wrap ErrOutOfRange", err)
	}
	if !strings.Contains(err.Error(), "repo_metrics_daily") || !strings.Contains(err.Error(), "commits_count") {
		t.Fatalf("error = %q, want it to name the family and field", err.Error())
	}
}

func assertChecked[T comparable](t *testing.T, value int, got T, err error, want T, wantErr bool) {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Fatalf("value=%d: expected an error, got %v", value, got)
		}
		if !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("value=%d: error = %v, want it to wrap ErrOutOfRange", value, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("value=%d: unexpected error: %v", value, err)
	}
	if got != want {
		t.Fatalf("value=%d: got %v, want %v", value, got, want)
	}
}
