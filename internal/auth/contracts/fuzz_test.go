package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// FuzzParseErrorEnvelope drives the error.v1 client parser (schema validation,
// duplicate-member refusal, status agreement, UTC range and skew checks) with
// arbitrary bytes and an arbitrary HTTP status. CHAOS-4887: a hand-written
// parser on the auth wire is where a divergence ships silently. Properties: it
// never panics, and an accepted envelope agrees with the status line it arrived
// on and is not from the future (the two checks the schema cannot perform).
func FuzzParseErrorEnvelope(f *testing.F) {
	wd, err := os.Getwd()
	if err != nil {
		f.Fatal(err)
	}
	root, err := RepoRoot(wd)
	if err != nil {
		f.Fatal(err)
	}
	for _, name := range []string{
		"valid-403-grant-absent.json",
		"client-rejected-duplicate-reason-code.json",
		"client-rejected-status-disagrees-with-http.json",
		"client-rejected-occurred-at-far-future.json",
		"client-rejected-utc-range-overflow.json",
	} {
		raw, err := os.ReadFile(filepath.Join(root, "contracts/auth/v1/examples/error", name))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw, 403)
	}
	f.Add([]byte(`{}`), 0)
	f.Add([]byte(`[`), 500)
	f.Fuzz(func(t *testing.T, raw []byte, status int) {
		envelope, err := ParseErrorEnvelope(root, raw, status, errorNow)
		if err != nil {
			return
		}
		if envelope.Status != status {
			t.Fatalf("accepted an envelope with status %d on a %d response", envelope.Status, status)
		}
		if envelope.OccurredAt.After(errorNow.Add(MaxClockSkew)) {
			t.Fatalf("accepted occurred_at %s beyond the skew bound", envelope.OccurredAt.Format(time.RFC3339Nano))
		}
	})
}

// FuzzRevisionUnmarshal: a revision is a cache-invalidation key, so two
// different wire values must never collapse to one in-memory value. Accepted
// input must round-trip to itself, and no fractional text may be accepted.
func FuzzRevisionUnmarshal(f *testing.F) {
	for _, seed := range []string{"1", "1.0", "1.5", "9223372036854775807", "9223372036854775808", "-1", "1e3", "\"1\"", "null", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		var revision Revision
		if err := revision.UnmarshalJSON([]byte(text)); err != nil {
			return
		}
		if strings.Contains(text, ".") && !strings.ContainsAny(text, "eE") {
			trimmed := strings.TrimRight(strings.SplitN(strings.TrimSpace(text), ".", 2)[1], "0")
			if trimmed != "" {
				t.Fatalf("accepted fractional revision %q as %d", text, revision)
			}
		}
		encoded, err := json.Marshal(revision)
		if err != nil {
			t.Fatalf("marshal accepted revision %d: %v", revision, err)
		}
		var again Revision
		if err := again.UnmarshalJSON(encoded); err != nil || again != revision {
			t.Fatalf("revision %d does not round-trip through %s: %v", revision, encoded, err)
		}
	})
}

// FuzzTimestampUnmarshal: an accepted timestamp round-trips through its own
// marshalled form at microsecond precision and never moves FORWARD (the
// one-directional guarantee MaxDelegationDuration depends on).
func FuzzTimestampUnmarshal(f *testing.F) {
	for _, seed := range []string{
		`"2026-09-03T06:00:00Z"`, `"2026-09-03T06:00:00.123456789Z"`, `"2026-09-03T06:00:00+14:00"`,
		`"0001-01-01T00:00:00Z"`, `"9999-12-31T23:59:59Z"`, `"2026-02-30T00:00:00Z"`, `null`, `1`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		var stamp Timestamp
		if err := stamp.UnmarshalJSON([]byte(text)); err != nil {
			return
		}
		encoded, err := stamp.MarshalJSON()
		if err != nil {
			t.Fatalf("marshal accepted timestamp: %v", err)
		}
		var again Timestamp
		if err := again.UnmarshalJSON(encoded); err != nil {
			t.Fatalf("re-parse %s: %v", encoded, err)
		}
		if again.After(stamp.Time) {
			t.Fatalf("round trip moved %s forward to %s", stamp.Time, again.Time)
		}
	})
}
