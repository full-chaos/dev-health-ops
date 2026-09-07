package externalrecompute

// utc_boundary_test.go pins the r2 P1-b fix: the bridge payload's UTC contract
// is enforced where it is READ, not merely where it happens to be written.
//
// The planner takes UTC calendar dates; the Python planner takes the calendar
// date in the producer's offset. For 2026-06-26T00:30+05:00 those are different
// days. r1 guarded that with a test on the sole writer, which constrains that
// writer and nothing else -- a hand-written row, a restored backup or a second
// producer can carry an offset, and time.Parse accepts it silently. These tests
// cover the reader.

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func bridgePayloadJSON(t *testing.T, started, ended string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"bridgeVersion":   1,
		"bridgeKind":      CompatibilityBridgeKind,
		"bridgeId":        "11111111-2222-4333-8444-555555555555",
		"repoIds":         []string{"repo-a"},
		"recordKinds":     []string{"commit.v1"},
		"windowStartedAt": started,
		"windowEndedAt":   ended,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestDecodeBridgeScopeRefusesANonZuluOffset is the finding itself: before the
// fix this payload decoded happily and produced UTC day 2026-06-25 where the
// Python planner produces 2026-06-26, with nothing anywhere to notice.
func TestDecodeBridgeScopeRefusesANonZuluOffset(t *testing.T) {
	raw := bridgePayloadJSON(t, "2026-06-25T23:30:00+05:00", "2026-06-26T00:30:00+05:00")
	_, _, err := decodeBridgeScope(raw, "org-1", "11111111-2222-4333-8444-555555555555")
	if err == nil {
		t.Fatal("an offset-bearing window was accepted; it selects a different recompute day")
	}
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("error %v is not permanent; a bad payload would be retried forever", err)
	}
	if !strings.Contains(err.Error(), "windowStartedAt") {
		t.Fatalf("error %q does not name the offending field", err)
	}
	// The message must name the field and the rule, never the payload value.
	if strings.Contains(err.Error(), "2026-06-25T23:30:00+05:00") {
		t.Fatalf("error %q echoed the payload value", err)
	}
}

func TestDecodeBridgeScopeAcceptsZuluAndNegativeZeroOffsets(t *testing.T) {
	for name, testCase := range map[string]struct{ started, ended string }{
		"zulu":              {"2026-06-25T00:00:00Z", "2026-06-26T00:00:00Z"},
		"explicit +00:00":   {"2026-06-25T00:00:00+00:00", "2026-06-26T00:00:00+00:00"},
		"zulu with nanos":   {"2026-06-25T00:00:00.123456789Z", "2026-06-26T00:00:00Z"},
		"absent boundaries": {"", ""},
	} {
		raw := bridgePayloadJSON(t, testCase.started, testCase.ended)
		scope, matched, err := decodeBridgeScope(raw, "org-1", "11111111-2222-4333-8444-555555555555")
		if err != nil || !matched {
			t.Fatalf("%s: matched=%v err=%v", name, matched, err)
		}
		for field, value := range map[string]*time.Time{
			"start": scope.WindowStart, "end": scope.WindowEnd,
		} {
			if value == nil {
				continue
			}
			if _, offset := value.Zone(); offset != 0 {
				t.Fatalf("%s: %s kept a non-zero offset", name, field)
			}
		}
	}
}

// TestDrainRetiresAnOffsetBearingPayload proves the refusal is terminal rather
// than a hot loop: the row is marked failed with its cause, so an operator sees
// it instead of it being reclaimed by the lease every five minutes forever.
func TestDecodeBridgeScopeOffsetRefusalIsPermanentNotTransient(t *testing.T) {
	raw := bridgePayloadJSON(t, "2026-06-25T23:30:00-08:00", "2026-06-26T00:30:00-08:00")
	_, _, err := decodeBridgeScope(raw, "org-1", "11111111-2222-4333-8444-555555555555")
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("error %v must be permanent so the row terminalizes", err)
	}
}
