package fixturesgen

import (
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The generation ceiling is derived from the table's TTL, not transcribed: the migration
// that creates product_telemetry_events says 180 days, the shelf-life margin is 30 and the
// strict headroom 1 (ttl_horizon.py). A migration that changes the TTL fails this until the
// ceiling follows.
func TestCeilingFollowsTheTableTTL(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "src", "dev_health_ops", "migrations", "clickhouse", "041_product_telemetry_events.sql"))
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`(?i)TTL\s+.*?\+\s*INTERVAL\s+(\d+)\s+DAY\s+DELETE`).FindStringSubmatch(string(raw))
	if match == nil {
		t.Fatal("product_telemetry_events lost its TTL clause: the generation ceiling has no horizon to follow")
	}
	days, _ := strconv.Atoi(match[1])
	if got := days - 30 - 1; got != ProductTelemetryCeilingDays {
		t.Fatalf("TTL is %d days, so the ceiling is %d, but ProductTelemetryCeilingDays = %d", days, got, ProductTelemetryCeilingDays)
	}
}

func generated(t *testing.T, spec ProductTelemetrySpec) []ProductTelemetryRow {
	t.Helper()
	rows, err := GenerateProductTelemetry(spec)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestGenerationIsDeterministicAndOrgScoped(t *testing.T) {
	seed := int64(7)
	end := time.Date(2026, 9, 26, 7, 31, 22, 0, time.UTC)
	spec := ProductTelemetrySpec{OrgID: "org-a", Days: 3, SessionsPerDay: 4, Seed: &seed, EndTime: end}
	first, second := generated(t, spec), generated(t, spec)
	if len(first) == 0 || len(first) != len(second) {
		t.Fatalf("generated %d then %d rows", len(first), len(second))
	}
	for i := range first {
		if first[i].EventID != second[i].EventID || first[i].PayloadJSON != second[i].PayloadJSON || !first[i].OccurredAt.Equal(second[i].OccurredAt) {
			t.Fatalf("row %d differs between two runs of the same spec", i)
		}
	}
	other := spec
	other.OrgID = "org-b"
	if generated(t, other)[0].EventID == first[0].EventID {
		t.Fatal("two orgs with one seed drew the same stream")
	}
	if first[0].OrgIDHash != ProductTelemetryOrgHash("org-a") || len(first[0].OrgIDHash) != 64 {
		t.Fatalf("org hash %q", first[0].OrgIDHash)
	}
	// End time moves timestamps, never the draws.
	moved := spec
	moved.EndTime = end.Add(48 * time.Hour)
	shifted := generated(t, moved)
	for i := range first {
		if shifted[i].EventID != first[i].EventID || shifted[i].PayloadJSON != first[i].PayloadJSON {
			t.Fatalf("row %d: the end time changed a draw", i)
		}
	}
}

func TestDaysAreClampedToTheCeilingAndNothingGeneratesForEmptyInput(t *testing.T) {
	seed := int64(1)
	end := time.Date(2026, 9, 26, 7, 0, 0, 0, time.UTC)
	spec := ProductTelemetrySpec{OrgID: "o", SessionsPerDay: 1, Seed: &seed, EndTime: end}
	spec.Days = ProductTelemetryCeilingDays
	atCeiling := generated(t, spec)
	spec.Days = ProductTelemetryCeilingDays + 50
	beyond := generated(t, spec)
	if len(atCeiling) != len(beyond) {
		t.Fatalf("days above the ceiling generated %d rows, the ceiling itself %d", len(beyond), len(atCeiling))
	}
	oldest := end.Add(-time.Duration(ProductTelemetryCeilingDays) * 24 * time.Hour)
	for _, row := range beyond {
		if row.OccurredAt.Before(time.Date(oldest.Year(), oldest.Month(), oldest.Day(), 9, 0, 0, 0, time.UTC)) {
			t.Fatalf("row at %s is older than the ceiling allows", row.OccurredAt)
		}
	}
	for _, empty := range []ProductTelemetrySpec{{OrgID: "o", Days: 0, SessionsPerDay: 5, EndTime: end}, {OrgID: "o", Days: -2, SessionsPerDay: 5, EndTime: end}, {OrgID: "o", Days: 3, SessionsPerDay: 0, EndTime: end}} {
		if rows := generated(t, empty); len(rows) != 0 {
			t.Fatalf("%+v generated %d rows", empty, len(rows))
		}
	}
}

func TestPayloadsNeverCarryABlockedKeyAndAreSortedCompactJSON(t *testing.T) {
	rows := generated(t, ProductTelemetrySpec{OrgID: "o", Days: 5, SessionsPerDay: 20, EndTime: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)})
	names := map[string]bool{}
	for _, row := range rows {
		names[row.Name] = true
		for key := range blockedTelemetryFields {
			if strings.Contains(row.PayloadJSON, `"`+key+`":`) {
				t.Fatalf("%s payload carries blocked key %s: %s", row.Name, key, row.PayloadJSON)
			}
		}
		if strings.Contains(row.PayloadJSON, ": ") || strings.Contains(row.PayloadJSON, ", ") {
			t.Fatalf("payload is not compact: %s", row.PayloadJSON)
		}
	}
	for _, name := range []string{"session_started", "page_viewed", "feature_viewed", "filter_changed", "chart_interacted", "navigation_interacted", "guide_opened", "session_ended"} {
		if !names[name] {
			t.Fatalf("a hundred sessions never produced a %s event: %v", name, names)
		}
	}
}

func TestBlockedPayloadKeysAreRefused(t *testing.T) {
	g := &telemetryGenerator{rng: NewRand(big.NewInt(1))}
	g.event("x", time.Now(), "s", "a", nil, map[string]any{"email": "e"})
	if g.err == nil {
		t.Fatal("a payload with a blocked key was accepted")
	}
}

// A non-UTC end time anchors each day at 09:00 in ITS zone, and every row still carries a UTC
// instant (persist converts to UTC after the arithmetic). 09:00 at -05:00 is 14:00Z, so the first
// session of the first day starts no earlier than 14:00Z.
func TestNonUTCEndTimeAnchorsInItsOwnZoneAndRowsAreUTC(t *testing.T) {
	seed := int64(17)
	end := time.Date(2026, 3, 8, 12, 0, 0, 0, time.FixedZone("", -5*3600))
	rows := generated(t, ProductTelemetrySpec{OrgID: "o", Days: 1, SessionsPerDay: 1, Seed: &seed, EndTime: end})
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	floor := time.Date(2026, 3, 7, 14, 0, 0, 0, time.UTC)
	if rows[0].OccurredAt.Before(floor) {
		t.Fatalf("first occurred_at %s is before 09:00 at -05:00 (%s): the anchor is not in the end time's zone", rows[0].OccurredAt, floor)
	}
	for _, row := range rows {
		if _, offset := row.OccurredAt.Zone(); offset != 0 {
			t.Fatalf("row at %s carries offset %d, not UTC", row.OccurredAt, offset)
		}
	}
}
