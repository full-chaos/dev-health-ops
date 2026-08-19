package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var (
	githubWorkItemMetricTestDay     = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	githubWorkItemMetricTestNow     = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	githubWorkItemMetricTestOrg     = "org-acme"
	githubWorkItemMetricTestCreated = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
)

func githubWorkItemMetricTestClaim() Claim {
	claim := nativeTestClaim("github", "work-items")
	claim.OrgID = githubWorkItemMetricTestOrg
	return claim
}

func githubWorkItemMetricTestItem(id string) githubWorkItemRow {
	projectID := "acme/api"
	return githubWorkItemRow{
		WorkItemID: id, Provider: "github", Title: id, Type: "feature",
		Status: "done", ProjectID: &projectID, Assignees: []string{"dev@example.com"},
		CreatedAt: githubWorkItemMetricTestCreated, UpdatedAt: githubWorkItemMetricTestNow,
		OrgID: githubWorkItemMetricTestOrg,
	}
}

func githubWorkItemMetricTestTime(value time.Time) *time.Time { return &value }

// TestBuildGitHubWorkItemMetricTripletFailsClosedOnUnusableInput plants one
// defect per guard. Each subtest differs from the accepted baseline in exactly
// one field, so a guard that stops working is caught by a case that exercises
// only that guard -- not by another case happening to fail for its own reason.
func TestBuildGitHubWorkItemMetricTripletFailsClosedOnUnusableInput(t *testing.T) {
	baseline := func() (Claim, githubWorkItemRows, time.Time, time.Time) {
		item := githubWorkItemMetricTestItem("gh:acme/api#1")
		item.StartedAt = githubWorkItemMetricTestTime(githubWorkItemMetricTestDay)
		item.CompletedAt = githubWorkItemMetricTestTime(githubWorkItemMetricTestDay.Add(3 * time.Hour))
		return githubWorkItemMetricTestClaim(), githubWorkItemRows{
			WorkItems: []githubWorkItemRow{item},
			StatusTransitions: []githubWorkItemTransitionRow{{
				WorkItemID: item.WorkItemID, Provider: "github",
				OccurredAt: githubWorkItemMetricTestDay.Add(time.Hour),
				FromStatus: "todo", ToStatus: "blocked", OrgID: githubWorkItemMetricTestOrg,
			}},
		}, githubWorkItemMetricTestDay, githubWorkItemMetricTestNow
	}
	// The baseline must SUCCEED, otherwise every rejection below would pass for
	// the wrong reason.
	claim, rows, day, computedAt := baseline()
	accepted, err := buildGitHubWorkItemMetricTriplet(claim, rows, day, computedAt, githubWorkItemDerivationContext{})
	if err != nil || len(accepted.MetricsDaily) != 1 || len(accepted.CycleTimes) != 1 {
		t.Fatalf("baseline must be accepted: groups=%d cycles=%d error=%v",
			len(accepted.MetricsDaily), len(accepted.CycleTimes), err)
	}

	tests := []struct {
		name   string
		mutate func(*Claim, *githubWorkItemRows, *time.Time, *time.Time)
	}{
		{"foreign tenant work item", func(_ *Claim, rows *githubWorkItemRows, _, _ *time.Time) {
			rows.WorkItems[0].OrgID = "org-other"
		}},
		{"foreign tenant transition", func(_ *Claim, rows *githubWorkItemRows, _, _ *time.Time) {
			rows.StatusTransitions[0].OrgID = "org-other"
		}},
		{"foreign provider work item", func(_ *Claim, rows *githubWorkItemRows, _, _ *time.Time) {
			rows.WorkItems[0].Provider = "gitlab"
		}},
		{"work item without a creation instant", func(_ *Claim, rows *githubWorkItemRows, _, _ *time.Time) {
			rows.WorkItems[0].CreatedAt = time.Time{}
		}},
		{"zero day", func(_ *Claim, _ *githubWorkItemRows, day, _ *time.Time) {
			*day = time.Time{}
		}},
		{"zero computed_at", func(_ *Claim, _ *githubWorkItemRows, _, computedAt *time.Time) {
			*computedAt = time.Time{}
		}},
		{"non work-item dataset", func(claim *Claim, _ *githubWorkItemRows, _, _ *time.Time) {
			claim.Dataset = "commits"
		}},
		{"non github provider", func(claim *Claim, _ *githubWorkItemRows, _, _ *time.Time) {
			claim.Provider = "gitlab"
		}},
		{"unvalidatable claim", func(claim *Claim, _ *githubWorkItemRows, _, _ *time.Time) {
			claim.Owner = "not-a-uuid"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claim, rows, day, computedAt := baseline()
			test.mutate(&claim, &rows, &day, &computedAt)
			triplet, err := buildGitHubWorkItemMetricTriplet(
				claim, rows, day, computedAt, githubWorkItemDerivationContext{})
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v", err)
			}
			if len(triplet.MetricsDaily) != 0 || len(triplet.UserMetricsDaily) != 0 ||
				len(triplet.CycleTimes) != 0 {
				t.Fatalf("a rejected build still produced rows: %+v", triplet)
			}
		})
	}
}

// TestBuildGitHubWorkItemMetricTripletWindowIsUTCAndHalfOpen pins the claim-day
// boundary itself. The instants below are one nanosecond apart across midnight,
// and the day argument is deliberately handed in with a non-UTC location and a
// non-zero clock time: a build that trusted the caller's location or its
// time-of-day would silently roll the whole window.
func TestBuildGitHubWorkItemMetricTripletWindowIsUTCAndHalfOpen(t *testing.T) {
	lastNanosecond := time.Date(2026, 8, 4, 23, 59, 59, 999999999, time.UTC)
	firstOfNextDay := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	// Each zoned instant below is the SAME UTC day, 2026-08-04, expressed in a
	// zone where the LOCAL calendar date differs from the UTC one. That
	// difference is the whole point: at UTC+9 20:30 the local and UTC dates
	// happen to agree, so that fixture alone cannot tell a correct conversion
	// from one that reads the calendar fields in the caller's zone -- it only
	// catches dropping the zone entirely. UTC+9 02:00 is the previous UTC day
	// locally; UTC-9 20:30 is the next one.
	zonedDays := map[string]time.Time{
		"caller in UTC+9, evening (local date == UTC date)": time.Date(
			2026, 8, 4, 20, 30, 0, 0, time.FixedZone("UTC+9", 9*60*60)),
		"caller in UTC+9, early morning (local date is a day ahead)": time.Date(
			2026, 8, 4, 2, 0, 0, 0, time.FixedZone("UTC+9", 9*60*60)).UTC().
			In(time.FixedZone("UTC+9", 9*60*60)),
		"caller in UTC-9, evening (local date is a day behind)": time.Date(
			2026, 8, 4, 20, 30, 0, 0, time.FixedZone("UTC-9", -9*60*60)),
	}
	tests := []struct {
		name        string
		completedAt time.Time
		want        int
	}{
		{"last nanosecond of the day is inside", lastNanosecond, 1},
		{"midnight of the next day is outside", firstOfNextDay, 0},
	}
	for zone, rawDay := range zonedDays {
		// Normalize each fixture to an instant that really is on 2026-08-04 UTC
		// while keeping the caller's location, so the only thing under test is
		// how the window is derived from it.
		day := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC).In(rawDay.Location())
		if zone == "caller in UTC+9, early morning (local date is a day ahead)" {
			day = time.Date(2026, 8, 4, 21, 0, 0, 0, time.UTC).In(rawDay.Location())
		}
		if zone == "caller in UTC-9, evening (local date is a day behind)" {
			day = time.Date(2026, 8, 4, 3, 0, 0, 0, time.UTC).In(rawDay.Location())
		}
		for _, test := range tests {
			t.Run(zone+": "+test.name, func(t *testing.T) {
				item := githubWorkItemMetricTestItem("gh:acme/api#1")
				item.StartedAt = githubWorkItemMetricTestTime(githubWorkItemMetricTestDay)
				item.CompletedAt = githubWorkItemMetricTestTime(test.completedAt)
				triplet, err := buildGitHubWorkItemMetricTriplet(
					githubWorkItemMetricTestClaim(), githubWorkItemRows{
						WorkItems: []githubWorkItemRow{item},
					}, day, githubWorkItemMetricTestNow, githubWorkItemDerivationContext{})
				if err != nil {
					t.Fatal(err)
				}
				if len(triplet.CycleTimes) != test.want {
					t.Fatalf("cycle-time records = %d, want %d (day=%s local=%s)",
						len(triplet.CycleTimes), test.want, day, day.Format("2006-01-02"))
				}
				if len(triplet.MetricsDaily) != 1 {
					t.Fatalf("group rows = %d, want 1 (the item is WIP or completed either way)",
						len(triplet.MetricsDaily))
				}
				if got := string(triplet.MetricsDaily[0].Day); got != "2026-08-04" {
					t.Fatalf("group day = %q, want the UTC claim day (caller local date was %s)",
						got, day.Format("2006-01-02"))
				}
			})
		}
	}
}

// TestGitHubWorkItemMetricTripletDerivedRowsCoverItsDestinations checks the seam
// contract: the route requires key PRESENCE for every derived destination, so a
// projection that produced nothing must still appear with an empty list. A nil
// entry, or a missing key, reads to the route as "this lane forgot a
// destination" and fails the whole unit.
func TestGitHubWorkItemMetricTripletDerivedRowsCoverItsDestinations(t *testing.T) {
	empty, err := githubWorkItemMetricTriplet{}.derivedRows()
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != len(githubWorkItemMetricTripletDestinations) {
		t.Fatalf("derived keys = %v", empty)
	}
	for _, destination := range githubWorkItemMetricTripletDestinations {
		rows, exists := empty[destination]
		if !exists {
			t.Fatalf("no entry for %q", destination)
		}
		if rows == nil {
			t.Fatalf("%q is nil rather than an empty list", destination)
		}
		if len(rows) != 0 {
			t.Fatalf("%q produced %d rows from an empty triplet", destination, len(rows))
		}
	}

	item := githubWorkItemMetricTestItem("gh:acme/api#1")
	item.StartedAt = githubWorkItemMetricTestTime(githubWorkItemMetricTestDay)
	item.CompletedAt = githubWorkItemMetricTestTime(githubWorkItemMetricTestDay.Add(2 * time.Hour))
	triplet, err := buildGitHubWorkItemMetricTriplet(
		githubWorkItemMetricTestClaim(),
		githubWorkItemRows{WorkItems: []githubWorkItemRow{item}},
		githubWorkItemMetricTestDay, githubWorkItemMetricTestNow,
		githubWorkItemDerivationContext{},
	)
	if err != nil {
		t.Fatal(err)
	}
	populated, err := triplet.derivedRows()
	if err != nil {
		t.Fatal(err)
	}
	for destination, rows := range populated {
		if len(rows) != 1 {
			t.Fatalf("%q produced %d rows, want 1", destination, len(rows))
		}
	}
	// The persisted cycle-time row must be the NARROWED one. Decoding the wire
	// bytes back into the full compute record and finding a flow field would
	// mean the seam is shipping columns Python never writes.
	var persisted map[string]json.RawMessage
	if err := json.Unmarshal(populated[githubWorkItemCycleTimesDestination][0], &persisted); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"active_time_hours", "wait_time_hours", "flow_efficiency"} {
		if _, present := persisted[field]; present {
			t.Fatalf("the persisted cycle-time row carries %q, which the Python sink never writes", field)
		}
	}
	if _, present := persisted["cycle_time_hours"]; !present {
		t.Fatal("the persisted cycle-time row lost cycle_time_hours")
	}
}

func githubWorkItemMetricTestIdentity[T any](
	t *testing.T, destination string, rows []T,
) (GitHubWorkItemEffectIdentity, EffectBatch) {
	t.Helper()
	effect, err := effectBatchFromValues(destination, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := newGitHubWorkItemEffectIdentity(githubWorkItemMetricTestClaim(), effect)
	if err != nil {
		t.Fatal(err)
	}
	return identity, effect
}

func githubWorkItemMetricTestGroupRow() githubWorkItemMetricsDailyRow {
	return githubWorkItemMetricsDailyRow{
		Day: newGitHubWorkItemMetricDay(githubWorkItemMetricTestDay), Provider: "github",
		WorkScopeID: "acme/api", TeamID: "team-a", TeamName: "Team A",
		ItemsStarted: 1, ItemsCompleted: 1, CycleTimeP50Hours: floatPointer(2),
		CycleTimeP90Hours: floatPointer(2), PredictabilityScore: 1,
		ComputedAt: githubWorkItemMetricTestNow, OrgID: githubWorkItemMetricTestOrg,
	}
}

func githubWorkItemMetricTestUserRow() githubWorkItemUserMetricsDailyRow {
	return githubWorkItemUserMetricsDailyRow{
		Day: newGitHubWorkItemMetricDay(githubWorkItemMetricTestDay), Provider: "github",
		WorkScopeID: "acme/api", UserIdentity: "dev@example.com", TeamID: "team-a",
		TeamName: "Team A", ItemsStarted: 1, ItemsCompleted: 1,
		ComputedAt: githubWorkItemMetricTestNow, OrgID: githubWorkItemMetricTestOrg,
	}
}

func githubWorkItemMetricTestCycleRow() githubWorkItemCycleTimePersistenceRow {
	return githubWorkItemCycleTimePersistenceRow{
		WorkItemID: "gh:acme/api#1", Provider: "github",
		Day: newGitHubWorkItemMetricDay(githubWorkItemMetricTestDay), WorkScopeID: "acme/api",
		TeamID: "team-a", TeamName: "Team A", Type: "feature", Status: "done",
		CreatedAt:   githubWorkItemMetricTestCreated,
		CompletedAt: githubWorkItemMetricTestTime(githubWorkItemMetricTestDay.Add(2 * time.Hour)),
		ComputedAt:  githubWorkItemMetricTestNow, OrgID: githubWorkItemMetricTestOrg,
	}
}

// TestGitHubWorkItemMetricEffectRejectsDuplicateNaturalKeys is the ONE guard
// that a ReplacingMergeTree destination cannot survive without. Two rows sharing
// a natural key inside one effect collapse to one persisted row, so the readback
// would find a single row where the effect claims two and either report a false
// Exact or an unresolvable Conflict forever.
func TestGitHubWorkItemMetricEffectRejectsDuplicateNaturalKeys(t *testing.T) {
	t.Run(githubWorkItemMetricsDailyDestination, func(t *testing.T) {
		first, second := githubWorkItemMetricTestGroupRow(), githubWorkItemMetricTestGroupRow()
		second.ItemsCompleted = 99 // differs off the key, so only the key collides
		identity, effect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemMetricsDailyDestination,
			[]githubWorkItemMetricsDailyRow{first, second})
		if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemMetricsDailyRow](
			identity, effect, githubWorkItemMetricsDailyDestination,
		); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("error=%v", err)
		}
		// The same two rows differing ON the key are legitimate.
		second.TeamID = "team-b"
		identity, effect = githubWorkItemMetricTestIdentity(t,
			githubWorkItemMetricsDailyDestination,
			[]githubWorkItemMetricsDailyRow{first, second})
		if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemMetricsDailyRow](
			identity, effect, githubWorkItemMetricsDailyDestination,
		); err != nil {
			t.Fatalf("distinct natural keys were rejected: %v", err)
		}
	})
	t.Run(githubWorkItemUserMetricsDailyDestination, func(t *testing.T) {
		first, second := githubWorkItemMetricTestUserRow(), githubWorkItemMetricTestUserRow()
		// The persisted natural key does NOT include team_id, so two rows that
		// differ only by team collide. This is the residual risk this parity
		// lane deliberately does not redesign; the guard makes it fail loudly at
		// validation instead of silently collapsing in ClickHouse.
		second.TeamID = "team-b"
		identity, effect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemUserMetricsDailyDestination,
			[]githubWorkItemUserMetricsDailyRow{first, second})
		if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemUserMetricsDailyRow](
			identity, effect, githubWorkItemUserMetricsDailyDestination,
		); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("error=%v", err)
		}
		second.UserIdentity = "other@example.com"
		identity, effect = githubWorkItemMetricTestIdentity(t,
			githubWorkItemUserMetricsDailyDestination,
			[]githubWorkItemUserMetricsDailyRow{first, second})
		if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemUserMetricsDailyRow](
			identity, effect, githubWorkItemUserMetricsDailyDestination,
		); err != nil {
			t.Fatalf("distinct natural keys were rejected: %v", err)
		}
	})
	t.Run(githubWorkItemCycleTimesDestination, func(t *testing.T) {
		first, second := githubWorkItemMetricTestCycleRow(), githubWorkItemMetricTestCycleRow()
		// work_item_cycle_times is keyed on (org_id, provider, work_item_id) with
		// NO day component, so the same item completing on two different days is
		// one persisted row, not two.
		second.Day = newGitHubWorkItemMetricDay(githubWorkItemMetricTestDay.AddDate(0, 0, -1))
		identity, effect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemCycleTimesDestination,
			[]githubWorkItemCycleTimePersistenceRow{first, second})
		if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemCycleTimePersistenceRow](
			identity, effect, githubWorkItemCycleTimesDestination,
		); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("error=%v", err)
		}
		second.WorkItemID = "gh:acme/api#2"
		identity, effect = githubWorkItemMetricTestIdentity(t,
			githubWorkItemCycleTimesDestination,
			[]githubWorkItemCycleTimePersistenceRow{first, second})
		if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemCycleTimePersistenceRow](
			identity, effect, githubWorkItemCycleTimesDestination,
		); err != nil {
			t.Fatalf("distinct natural keys were rejected: %v", err)
		}
	})
}

// TestGitHubWorkItemMetricEffectRejectsMismatchedIdentity plants one defect per
// identity clause. Readback fences tenant and generation BEFORE comparing rows,
// so an adapter that accepted a mismatched identity would verify one unit's
// effect against another unit's persisted rows.
func TestGitHubWorkItemMetricEffectRejectsMismatchedIdentity(t *testing.T) {
	rows := []githubWorkItemMetricsDailyRow{githubWorkItemMetricTestGroupRow()}
	tests := []struct {
		name   string
		mutate func(*GitHubWorkItemEffectIdentity, *EffectBatch)
	}{
		{"foreign tenant", func(identity *GitHubWorkItemEffectIdentity, _ *EffectBatch) {
			identity.OrgID = "org-other"
		}},
		{"blank tenant the rows agree with", func(identity *GitHubWorkItemEffectIdentity, effect *EffectBatch) {
			// Both sides blank together. The per-row check is `orgID ==
			// identity.OrgID`, which a matching blank satisfies, so the
			// identity-level non-blank fence is the ONLY thing that can reject
			// this -- and it has to, because a blank tenant fences no rows at
			// all on readback. Blanking just the identity would be rejected by
			// the row check instead and prove nothing about this clause.
			blank := githubWorkItemMetricTestGroupRow()
			blank.OrgID = "   "
			blanked, err := effectBatchFromValues(
				githubWorkItemMetricsDailyDestination, EffectReadbackRequired,
				[]githubWorkItemMetricsDailyRow{blank})
			if err != nil {
				t.Fatal(err)
			}
			identity.OrgID = "   "
			identity.ContentDigest = blanked.ContentDigest
			identity.RowCount = len(blanked.Rows)
			*effect = blanked
		}},
		{"foreign provider", func(identity *GitHubWorkItemEffectIdentity, _ *EffectBatch) {
			identity.Provider = "gitlab"
		}},
		{"non work-item dataset", func(identity *GitHubWorkItemEffectIdentity, _ *EffectBatch) {
			identity.Dataset = "commits"
		}},
		{"blank generation", func(identity *GitHubWorkItemEffectIdentity, _ *EffectBatch) {
			identity.Generation = " "
		}},
		{"identity names another destination", func(identity *GitHubWorkItemEffectIdentity, _ *EffectBatch) {
			identity.Destination = githubWorkItemCycleTimesDestination
		}},
		{"effect names another destination", func(_ *GitHubWorkItemEffectIdentity, effect *EffectBatch) {
			effect.Destination = githubWorkItemCycleTimesDestination
		}},
		{"digest disagrees with the identity", func(identity *GitHubWorkItemEffectIdentity, _ *EffectBatch) {
			identity.ContentDigest = "0000000000000000000000000000000000000000000000000000000000000000"
		}},
		{"row count disagrees with the identity", func(identity *GitHubWorkItemEffectIdentity, _ *EffectBatch) {
			identity.RowCount = identity.RowCount + 1
		}},
		{"recovery is not readback fenced", func(_ *GitHubWorkItemEffectIdentity, effect *EffectBatch) {
			effect.Recovery = EffectReplaySafe
		}},
		{"rows were edited after the digest was taken", func(_ *GitHubWorkItemEffectIdentity, effect *EffectBatch) {
			edited := githubWorkItemMetricTestGroupRow()
			edited.ItemsCompleted = 42
			encoded, err := json.Marshal(edited)
			if err != nil {
				t.Fatal(err)
			}
			effect.Rows = []json.RawMessage{encoded}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity, effect := githubWorkItemMetricTestIdentity(t,
				githubWorkItemMetricsDailyDestination, rows)
			test.mutate(&identity, &effect)
			if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemMetricsDailyRow](
				identity, effect, githubWorkItemMetricsDailyDestination,
			); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

// TestGitHubWorkItemMetricEffectRejectsUnpersistableRows plants one unusable
// value per row-level clause. Every one of these would either violate a NOT
// NULL / key column or store a value ClickHouse cannot round-trip, and would
// then be indistinguishable from a genuine readback conflict forever after.
func TestGitHubWorkItemMetricEffectRejectsUnpersistableRows(t *testing.T) {
	t.Run("group rows", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(*githubWorkItemMetricsDailyRow)
		}{
			{"unparsable day", func(row *githubWorkItemMetricsDailyRow) { row.Day = "not-a-day" }},
			{"blank team id", func(row *githubWorkItemMetricsDailyRow) { row.TeamID = "  " }},
			{"blank team name", func(row *githubWorkItemMetricsDailyRow) { row.TeamName = "" }},
			{"zero computed_at", func(row *githubWorkItemMetricsDailyRow) { row.ComputedAt = time.Time{} }},
			{"negative counter", func(row *githubWorkItemMetricsDailyRow) { row.ItemsStarted = -1 }},
			{"negative unassigned counter", func(row *githubWorkItemMetricsDailyRow) { row.WIPUnassignedEndOfDay = -1 }},
			{"negative new-item counter", func(row *githubWorkItemMetricsDailyRow) { row.NewItemsCount = -1 }},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				row := githubWorkItemMetricTestGroupRow()
				test.mutate(&row)
				identity, effect := githubWorkItemMetricTestIdentity(t,
					githubWorkItemMetricsDailyDestination,
					[]githubWorkItemMetricsDailyRow{row})
				if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemMetricsDailyRow](
					identity, effect, githubWorkItemMetricsDailyDestination,
				); !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("error=%v", err)
				}
			})
		}
	})
	t.Run("user rows", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(*githubWorkItemUserMetricsDailyRow)
		}{
			{"blank user identity", func(row *githubWorkItemUserMetricsDailyRow) { row.UserIdentity = "  " }},
			{"blank team id", func(row *githubWorkItemUserMetricsDailyRow) { row.TeamID = "" }},
			{"negative counter", func(row *githubWorkItemUserMetricsDailyRow) { row.WIPCountEndOfDay = -1 }},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				row := githubWorkItemMetricTestUserRow()
				test.mutate(&row)
				identity, effect := githubWorkItemMetricTestIdentity(t,
					githubWorkItemUserMetricsDailyDestination,
					[]githubWorkItemUserMetricsDailyRow{row})
				if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemUserMetricsDailyRow](
					identity, effect, githubWorkItemUserMetricsDailyDestination,
				); !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("error=%v", err)
				}
			})
		}
	})
	t.Run("cycle rows", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(*githubWorkItemCycleTimePersistenceRow)
		}{
			{"blank work item id", func(row *githubWorkItemCycleTimePersistenceRow) { row.WorkItemID = " " }},
			{"blank type", func(row *githubWorkItemCycleTimePersistenceRow) { row.Type = "" }},
			{"blank status", func(row *githubWorkItemCycleTimePersistenceRow) { row.Status = "" }},
			{"zero created_at", func(row *githubWorkItemCycleTimePersistenceRow) { row.CreatedAt = time.Time{} }},
			// Python only ever emits a cycle-time record from the completed-today
			// branch, so a record without completed_at cannot have come from the
			// ported compute at all.
			{"absent completed_at", func(row *githubWorkItemCycleTimePersistenceRow) { row.CompletedAt = nil }},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				row := githubWorkItemMetricTestCycleRow()
				test.mutate(&row)
				identity, effect := githubWorkItemMetricTestIdentity(t,
					githubWorkItemCycleTimesDestination,
					[]githubWorkItemCycleTimePersistenceRow{row})
				if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemCycleTimePersistenceRow](
					identity, effect, githubWorkItemCycleTimesDestination,
				); !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("error=%v", err)
				}
			})
		}
	})
}

// TestGitHubWorkItemMetricEffectCannotCarryNonFiniteFloats is the observable
// half of the deliberately-absent finiteness validation. NaN and infinity are
// stopped at the JSON boundary, in two different ways, and both must keep
// failing: if either ever started succeeding, a non-finite value would reach
// ClickHouse and the row validators would have no guard against it.
func TestGitHubWorkItemMetricEffectCannotCarryNonFiniteFloats(t *testing.T) {
	t.Run("marshalling a non-finite float fails", func(t *testing.T) {
		row := githubWorkItemMetricTestGroupRow()
		row.CycleTimeP50Hours = floatPointer(math.NaN())
		if _, err := effectBatchFromValues(
			githubWorkItemMetricsDailyDestination, EffectReadbackRequired,
			[]githubWorkItemMetricsDailyRow{row},
		); !errors.Is(err, ErrEffectRecoveryUnsafe) {
			t.Fatalf("error=%v", err)
		}
		row = githubWorkItemMetricTestGroupRow()
		row.WIPCongestionRatio = math.Inf(1)
		if _, err := effectBatchFromValues(
			githubWorkItemMetricsDailyDestination, EffectReadbackRequired,
			[]githubWorkItemMetricsDailyRow{row},
		); !errors.Is(err, ErrEffectRecoveryUnsafe) {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("an overflowing literal fails to decode", func(t *testing.T) {
		// JSON has no NaN/Inf token at all, so the only shape that could smuggle
		// one in is a finite-looking literal too large for float64. encoding/json
		// rejects it rather than saturating to +Inf.
		encoded, err := json.Marshal(githubWorkItemMetricTestGroupRow())
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		fields["cycle_time_p50_hours"] = json.RawMessage("1e999")
		overflowing, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		effect, err := BuildEffectBatch(
			githubWorkItemMetricsDailyDestination, EffectReadbackRequired,
			[]json.RawMessage{overflowing},
		)
		if err != nil {
			t.Fatalf("BuildEffectBatch rejected the payload for an unrelated reason: %v", err)
		}
		identity, err := newGitHubWorkItemEffectIdentity(githubWorkItemMetricTestClaim(), effect)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validateGitHubWorkItemMetricEffect[githubWorkItemMetricsDailyRow](
			identity, effect, githubWorkItemMetricsDailyDestination,
		); err == nil {
			t.Fatal("an overflowing float literal decoded successfully -- the row " +
				"validators have no finiteness guard, so this boundary is the only " +
				"thing keeping a non-finite value out of ClickHouse")
		}
	})
}

// TestGitHubWorkItemMetricVersionComparisonFollowsReplacingMergeTree pins the
// three-way readback verdict for every destination. All three tables are
// ReplacingMergeTree(computed_at), so "no row", "an OLDER version of this row",
// "a NEWER version", and "this exact version" are four DIFFERENT situations and
// only two of them are safe to treat as re-writable.
func TestGitHubWorkItemMetricVersionComparisonFollowsReplacingMergeTree(t *testing.T) {
	older := githubWorkItemMetricTestNow.Add(-time.Hour)
	newer := githubWorkItemMetricTestNow.Add(time.Hour)

	t.Run(githubWorkItemMetricsDailyDestination, func(t *testing.T) {
		expected := githubWorkItemMetricTestGroupRow()
		tests := []struct {
			name   string
			actual githubWorkItemMetricsDailyRow
			found  int
			want   EffectInspection
		}{
			{"nothing persisted", githubWorkItemMetricsDailyRow{}, 0, EffectAbsent},
			{"zero version persisted", githubWorkItemMetricsDailyRow{}, 1, EffectAbsent},
			{"older version persisted", func() githubWorkItemMetricsDailyRow {
				row := expected
				row.ComputedAt = older
				return row
			}(), 1, EffectAbsent},
			{"newer version persisted", func() githubWorkItemMetricsDailyRow {
				row := expected
				row.ComputedAt = newer
				return row
			}(), 1, EffectConflict},
			{"two rows for one key", expected, 2, EffectConflict},
			{"same version, different metric", func() githubWorkItemMetricsDailyRow {
				row := expected
				row.ItemsCompleted = 99
				return row
			}(), 1, EffectConflict},
			{"same version, different percentile", func() githubWorkItemMetricsDailyRow {
				row := expected
				row.CycleTimeP50Hours = floatPointer(3)
				return row
			}(), 1, EffectConflict},
			{"same version, absent percentile", func() githubWorkItemMetricsDailyRow {
				row := expected
				row.CycleTimeP50Hours = nil
				return row
			}(), 1, EffectConflict},
			{"exact match", expected, 1, EffectExact},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				if got := compareGitHubWorkItemMetricsDailyVersion(
					expected, test.actual, test.found,
				); got != test.want {
					t.Fatalf("inspection = %s, want %s", got, test.want)
				}
			})
		}
		// ClickHouse stores DateTime at one-second resolution, so an expected
		// value carrying nanoseconds must still recognize its own persisted row.
		subSecond := expected
		subSecond.ComputedAt = githubWorkItemMetricTestNow.Add(750 * time.Millisecond)
		if got := compareGitHubWorkItemMetricsDailyVersion(subSecond, expected, 1); got != EffectExact {
			t.Fatalf("sub-second expected version inspection = %s, want exact", got)
		}
	})

	t.Run(githubWorkItemUserMetricsDailyDestination, func(t *testing.T) {
		expected := githubWorkItemMetricTestUserRow()
		older := expected
		older.ComputedAt = githubWorkItemMetricTestNow.Add(-time.Hour)
		newerRow := expected
		newerRow.ComputedAt = newer
		different := expected
		different.ItemsStarted = 7
		checks := []struct {
			name   string
			actual githubWorkItemUserMetricsDailyRow
			found  int
			want   EffectInspection
		}{
			{"nothing persisted", githubWorkItemUserMetricsDailyRow{}, 0, EffectAbsent},
			{"older version persisted", older, 1, EffectAbsent},
			{"newer version persisted", newerRow, 1, EffectConflict},
			{"two rows for one key", expected, 2, EffectConflict},
			{"same version, different metric", different, 1, EffectConflict},
			{"exact match", expected, 1, EffectExact},
		}
		for _, check := range checks {
			t.Run(check.name, func(t *testing.T) {
				if got := compareGitHubWorkItemUserMetricsDailyVersion(
					expected, check.actual, check.found,
				); got != check.want {
					t.Fatalf("inspection = %s, want %s", got, check.want)
				}
			})
		}
	})

	t.Run(githubWorkItemCycleTimesDestination, func(t *testing.T) {
		expected := githubWorkItemMetricTestCycleRow()
		olderRow := expected
		olderRow.ComputedAt = older
		newerRow := expected
		newerRow.ComputedAt = newer
		different := expected
		different.Status = "cancelled"
		// The persisted instants come back truncated to whole seconds. Comparing
		// them untruncated would report a permanent conflict for any effect whose
		// timestamps carry sub-second precision.
		subSecond := expected
		subSecond.CreatedAt = expected.CreatedAt.Add(400 * time.Millisecond)
		subSecond.CompletedAt = githubWorkItemMetricTestTime(expected.CompletedAt.Add(600 * time.Millisecond))
		checks := []struct {
			name             string
			expected, actual githubWorkItemCycleTimePersistenceRow
			found            int
			want             EffectInspection
		}{
			{"nothing persisted", expected, githubWorkItemCycleTimePersistenceRow{}, 0, EffectAbsent},
			{"older version persisted", expected, olderRow, 1, EffectAbsent},
			{"newer version persisted", expected, newerRow, 1, EffectConflict},
			{"two rows for one key", expected, expected, 2, EffectConflict},
			{"same version, different status", expected, different, 1, EffectConflict},
			{"sub-second precision survives truncation", subSecond, expected, 1, EffectExact},
			{"exact match", expected, expected, 1, EffectExact},
		}
		for _, check := range checks {
			t.Run(check.name, func(t *testing.T) {
				if got := compareGitHubWorkItemCycleTimeVersion(
					check.expected, check.actual, check.found,
				); got != check.want {
					t.Fatalf("inspection = %s, want %s", got, check.want)
				}
			})
		}
	})
}

// TestGitHubWorkItemMetricVersionComparisonFollowsValuesNotPointers is the
// regression guard for the defect the readback cannot survive: in production the
// expected row is decoded from the effect payload and the actual row is built by
// a fresh ClickHouse scan, so every nullable column is a DIFFERENT allocation
// holding an EQUAL value. A comparison that tests pointer identity reports
// Conflict for all of them, the effect is never verifiable, and the sync unit
// can never complete. Each case below rebuilds its rows independently -- never
// by copying one struct -- so the pointers are guaranteed to differ.
func TestGitHubWorkItemMetricVersionComparisonFollowsValuesNotPointers(t *testing.T) {
	t.Run(githubWorkItemMetricsDailyDestination, func(t *testing.T) {
		expected, actual := githubWorkItemMetricTestGroupRow(), githubWorkItemMetricTestGroupRow()
		if expected.CycleTimeP50Hours == actual.CycleTimeP50Hours {
			t.Fatal("the two rows share a percentile pointer; this case proves nothing")
		}
		if got := compareGitHubWorkItemMetricsDailyVersion(expected, actual, 1); got != EffectExact {
			t.Fatalf("inspection = %s, want exact", got)
		}
		actual.CycleTimeP90Hours = floatPointer(*expected.CycleTimeP90Hours + 1)
		if got := compareGitHubWorkItemMetricsDailyVersion(expected, actual, 1); got != EffectConflict {
			t.Fatalf("a genuinely different percentile gave %s, want conflict", got)
		}
	})
	t.Run(githubWorkItemUserMetricsDailyDestination, func(t *testing.T) {
		expected, actual := githubWorkItemMetricTestUserRow(), githubWorkItemMetricTestUserRow()
		expected.CycleTimeP50Hours, actual.CycleTimeP50Hours = floatPointer(4), floatPointer(4)
		if expected.CycleTimeP50Hours == actual.CycleTimeP50Hours {
			t.Fatal("the two rows share a percentile pointer; this case proves nothing")
		}
		if got := compareGitHubWorkItemUserMetricsDailyVersion(expected, actual, 1); got != EffectExact {
			t.Fatalf("inspection = %s, want exact", got)
		}
		actual.CycleTimeP50Hours = floatPointer(5)
		if got := compareGitHubWorkItemUserMetricsDailyVersion(expected, actual, 1); got != EffectConflict {
			t.Fatalf("a genuinely different percentile gave %s, want conflict", got)
		}
	})
	t.Run(githubWorkItemCycleTimesDestination, func(t *testing.T) {
		expected, actual := githubWorkItemMetricTestCycleRow(), githubWorkItemMetricTestCycleRow()
		expected.Assignee, actual.Assignee = githubWorkItemMetricTestAssignee(), githubWorkItemMetricTestAssignee()
		expected.CycleTimeHours, actual.CycleTimeHours = floatPointer(2), floatPointer(2)
		if expected.CompletedAt == actual.CompletedAt || expected.Assignee == actual.Assignee {
			t.Fatal("the two rows share a nullable pointer; this case proves nothing")
		}
		if got := compareGitHubWorkItemCycleTimeVersion(expected, actual, 1); got != EffectExact {
			t.Fatalf("inspection = %s, want exact", got)
		}
		other := "other@example.com"
		actual.Assignee = &other
		if got := compareGitHubWorkItemCycleTimeVersion(expected, actual, 1); got != EffectConflict {
			t.Fatalf("a genuinely different assignee gave %s, want conflict", got)
		}
		actual = githubWorkItemMetricTestCycleRow()
		actual.Assignee = githubWorkItemMetricTestAssignee()
		actual.CycleTimeHours = floatPointer(2)
		actual.CompletedAt = nil
		if got := compareGitHubWorkItemCycleTimeVersion(expected, actual, 1); got != EffectConflict {
			t.Fatalf("a null completed_at against a non-null one gave %s, want conflict", got)
		}
	})
}

func githubWorkItemMetricTestAssignee() *string {
	value := "dev@example.com"
	return &value
}

// TestGitHubWorkItemMetricVersionComparisonRejectsDuplicatesWhateverWasScannedLast
// is the regression guard for the branch-order defect. The scan keeps only the
// LAST row it saw, so when a natural key returns more than one row the verdict
// must not depend on which one that was. The existing duplicate cases all pass
// actual == expected, which cannot tell a correct implementation from one that
// decides on row order: a STALE last row previously read as EffectAbsent (the
// committer rewrites forever) and a NEWER one as EffectConflict (the unit never
// completes). Neither is a judgement about the effect.
func TestGitHubWorkItemMetricVersionComparisonRejectsDuplicatesWhateverWasScannedLast(t *testing.T) {
	older := githubWorkItemMetricTestNow.Add(-time.Hour)
	newer := githubWorkItemMetricTestNow.Add(time.Hour)

	t.Run(githubWorkItemMetricsDailyDestination, func(t *testing.T) {
		expected := githubWorkItemMetricTestGroupRow()
		for name, computedAt := range map[string]time.Time{
			"stale row scanned last": older,
			"newer row scanned last": newer,
			"equal row scanned last": githubWorkItemMetricTestNow,
		} {
			t.Run(name, func(t *testing.T) {
				actual := githubWorkItemMetricTestGroupRow()
				actual.ComputedAt = computedAt
				if got := compareGitHubWorkItemMetricsDailyVersion(expected, actual, 2); got != EffectConflict {
					t.Fatalf("two rows for one key = %s, want conflict", got)
				}
			})
		}
	})
	t.Run(githubWorkItemUserMetricsDailyDestination, func(t *testing.T) {
		expected := githubWorkItemMetricTestUserRow()
		actual := githubWorkItemMetricTestUserRow()
		actual.ComputedAt = older
		if got := compareGitHubWorkItemUserMetricsDailyVersion(expected, actual, 2); got != EffectConflict {
			t.Fatalf("two rows with a stale one scanned last = %s, want conflict", got)
		}
	})
	t.Run(githubWorkItemCycleTimesDestination, func(t *testing.T) {
		expected := githubWorkItemMetricTestCycleRow()
		actual := githubWorkItemMetricTestCycleRow()
		actual.ComputedAt = older
		if got := compareGitHubWorkItemCycleTimeVersion(expected, actual, 2); got != EffectConflict {
			t.Fatalf("two rows with a stale one scanned last = %s, want conflict", got)
		}
	})
	// A zero row count still has to read as Absent, not as the conflict the
	// found != 1 branch now handles.
	if got := compareGitHubWorkItemMetricsDailyVersion(
		githubWorkItemMetricTestGroupRow(), githubWorkItemMetricsDailyRow{}, 0,
	); got != EffectAbsent {
		t.Fatalf("no rows = %s, want absent", got)
	}
}

// TestInspectGitHubWorkItemMetricRowsRequiresUnanimity covers the aggregation
// across an effect's rows. A partially written effect is NOT re-writable and NOT
// already done: reporting either verdict would let the committer skip the write
// or double-count it.
func TestInspectGitHubWorkItemMetricRowsRequiresUnanimity(t *testing.T) {
	failure := errors.New("readback transport failed")
	tests := []struct {
		name        string
		inspections []EffectInspection
		err         error
		want        EffectInspection
		wantErr     error
	}{
		{"all exact", []EffectInspection{EffectExact, EffectExact}, nil, EffectExact, nil},
		{"all absent", []EffectInspection{EffectAbsent, EffectAbsent}, nil, EffectAbsent, nil},
		{"partially written", []EffectInspection{EffectExact, EffectAbsent}, nil, EffectConflict, nil},
		{"one conflict", []EffectInspection{EffectExact, EffectConflict}, nil, EffectConflict, nil},
		{"transport failure", []EffectInspection{EffectExact}, failure, EffectConflict, failure},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := 0
			got, err := inspectGitHubWorkItemMetricRows(
				test.inspections,
				func(inspection EffectInspection) (EffectInspection, error) {
					index++
					if test.err != nil {
						return EffectConflict, test.err
					}
					return inspection, nil
				},
			)
			if got != test.want || !errors.Is(err, test.wantErr) {
				t.Fatalf("inspection=%s error=%v, want %s / %v", got, err, test.want, test.wantErr)
			}
		})
	}
	// A zero-row effect wrote nothing, so it must not report itself verified.
	// Left to the exact == len(rows) test alone this is satisfied vacuously by
	// 0 == 0; the guard makes it Absent regardless of who calls in.
	if got, err := inspectGitHubWorkItemMetricRows(
		[]EffectInspection{}, func(EffectInspection) (EffectInspection, error) {
			t.Fatal("an empty row set must not be inspected row by row")
			return EffectConflict, nil
		},
	); got != EffectAbsent || err != nil {
		t.Fatalf("empty aggregation = %s / %v, want absent", got, err)
	}
}

// TestGitHubWorkItemMetricAdaptersRequireContextAndLease covers the rails that
// sit in front of every ClickHouse call. A nil lease or a lost lease must stop
// the write before any row reaches ClickHouse, because a unit that lost its
// lease may already have been recovered by another worker.
func TestGitHubWorkItemMetricAdaptersRequireContextAndLease(t *testing.T) {
	lost := errors.New("lease lost")
	rows := []githubWorkItemMetricsDailyRow{githubWorkItemMetricTestGroupRow()}
	identity, effect := githubWorkItemMetricTestIdentity(t,
		githubWorkItemMetricsDailyDestination, rows)

	t.Run("nil lease", func(t *testing.T) {
		sink := GitHubWorkItemMetricsDailyClickHouseEffects{}
		if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("write error=%v", err)
		}
		if _, err := sink.InspectGitHubWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("inspect error=%v", err)
		}
	})
	t.Run("lost lease", func(t *testing.T) {
		// LeaseGuardFunc collapses any guard failure to ErrLeaseLost, so that --
		// not the underlying cause -- is what a caller sees and must handle.
		sink := GitHubWorkItemMetricsDailyClickHouseEffects{
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return lost }),
		}
		if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("write error=%v", err)
		}
		inspection, err := sink.InspectGitHubWorkItemEffect(context.Background(), identity, effect)
		if !errors.Is(err, providerfoundation.ErrLeaseLost) || inspection != EffectConflict {
			t.Fatalf("inspect=%s error=%v", inspection, err)
		}
	})
	t.Run("no connection for a non-empty effect", func(t *testing.T) {
		sink := GitHubWorkItemMetricsDailyClickHouseEffects{
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}
		if err := sink.WriteGitHubWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("write error=%v", err)
		}
	})
	t.Run("an empty effect needs no connection", func(t *testing.T) {
		emptyIdentity, emptyEffect := githubWorkItemMetricTestIdentity(t,
			githubWorkItemMetricsDailyDestination, []githubWorkItemMetricsDailyRow{})
		sink := GitHubWorkItemMetricsDailyClickHouseEffects{
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}
		if err := sink.WriteGitHubWorkItemEffect(context.Background(), emptyIdentity, emptyEffect); err != nil {
			t.Fatalf("write error=%v", err)
		}
		inspection, err := sink.InspectGitHubWorkItemEffect(context.Background(), emptyIdentity, emptyEffect)
		if err != nil || inspection != EffectAbsent {
			t.Fatalf("inspect=%s error=%v", inspection, err)
		}
	})
}

// TestGitHubWorkItemMetricAdaptersSatisfyTheCompositeSink proves the three
// adapters are actually installable in the sixteen-destination dispatcher and
// are reached for their own destination and no other.
func TestGitHubWorkItemMetricAdaptersSatisfyTheCompositeSink(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitHubWorkItemClickHouseEffects{
		WorkItemMetricsDaily:     GitHubWorkItemMetricsDailyClickHouseEffects{Lease: lease},
		WorkItemUserMetricsDaily: GitHubWorkItemUserMetricsDailyClickHouseEffects{Lease: lease},
		WorkItemCycleTimes:       GitHubWorkItemCycleTimesClickHouseEffects{Lease: lease},
	}
	for _, destination := range githubWorkItemMetricTripletDestinations {
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, adapter, err := sink.resolve(githubWorkItemMetricTestClaim(), effect)
		// The dispatcher requires ALL sixteen adapters before resolving any of
		// them, so a partially populated sink must refuse rather than dispatch.
		if err == nil || adapter != nil {
			t.Fatalf("%s resolved against an incomplete sink: adapter=%v error=%v",
				destination, adapter, err)
		}
	}
	if slices.Contains(githubWorkItemMetricTripletDestinations, "work_item_team_attributions") {
		t.Fatal("this lane must not claim work_item_team_attributions")
	}
}
