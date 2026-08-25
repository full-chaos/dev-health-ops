package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
)

func TestNormalizeLinearProjectMembershipsBuildsRowsFromHistoryProjectFields(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	fromID, toID := "project-old", "project-new"
	history := []linearHistoryEntry{
		{
			ID:            "hist-1",
			CreatedAt:     "2026-07-30T11:00:00.000Z",
			FromProjectID: &fromID,
			ToProjectID:   &toID,
			Actor:         &linearIdentityPayload{Email: "alice@example.com"},
		},
		// A pure status entry: FromProjectID/ToProjectID both nil. Must not
		// mint a membership row -- sharing normalizeLinearTransitions' own
		// history slice is exactly why this is a separate loop.
		{
			ID:        "hist-2",
			CreatedAt: "2026-07-30T12:00:00.000Z",
			FromState: &linearStatePayload{Name: "Todo", Type: "unstarted"},
			ToState:   &linearStatePayload{Name: "In Progress", Type: "started"},
		},
	}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	memberships, catalog := normalizeLinearProjectMemberships(
		claim, "linear:ENG-1", toID, "Platform", history, normalizedAt,
	)
	if len(memberships) != 1 {
		t.Fatalf("memberships=%+v, want exactly one (the status-only entry must be skipped)", memberships)
	}
	row := memberships[0]
	if row.FromProjectID != fromID || row.ToProjectID != toID {
		t.Fatalf("ids: from=%q to=%q, want from=%q to=%q", row.FromProjectID, row.ToProjectID, fromID, toID)
	}
	if row.FromProjectKey != "" || row.ToProjectKey != "" {
		t.Fatalf("keys: from=%q to=%q, want both empty -- ruled 2026-08-24, Linear projects have no key concept", row.FromProjectKey, row.ToProjectKey)
	}
	if row.EventID != "linear:hist-1" {
		t.Fatalf("event_id=%q, want linear:hist-1 (native history id, prefixed)", row.EventID)
	}
	if row.Actor != "alice@example.com" {
		t.Fatalf("actor=%q, want alice@example.com", row.Actor)
	}
	if row.SubjectKind != "work_item" || row.SubjectID != "linear:ENG-1" || row.Provider != "linear" {
		t.Fatalf("subject=%+v", row)
	}
	// Two catalog rows: one per touched project id. The TO side gets the
	// current-project name for free (its id matches the work item's own
	// current project); the FROM side gets "" -- ruled: no live lookup.
	if len(catalog) != 2 {
		t.Fatalf("catalog=%+v, want 2 rows (one per touched project id)", catalog)
	}
	byID := map[string]string{}
	for _, entry := range catalog {
		byID[entry.ID] = entry.Name
		if entry.Provider != "linear" || entry.ProjectKey != "" {
			t.Fatalf("catalog entry=%+v, want provider=linear key=\"\"", entry)
		}
	}
	if byID[toID] != "Platform" {
		t.Fatalf("to-side catalog name=%q, want Platform (free, from the work item's own current project)", byID[toID])
	}
	if byID[fromID] != "" {
		t.Fatalf("from-side catalog name=%q, want \"\" -- no live lookup, ruled 2026-08-24", byID[fromID])
	}
}

// The catalog ensure-row's updated_at is pinned to the Unix epoch, never a
// live sync timestamp -- `projects` is ReplacingMergeTree(updated_at) and
// Linear has its OWN richer reference-catalog writer (lifecycle/team/lead/
// url columns); a base-columns-only row stamped with the current sync time
// would out-version and blank those columns the moment it ran after a real
// catalog sync (codex review finding, CHAOS-4193). Epoch-anchoring means
// this row can only ever win when no row exists yet, never displace a real
// one.
func TestNormalizeLinearProjectMembershipsCatalogRowsNeverOutversionARealSync(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	toID := "project-new"
	history := []linearHistoryEntry{
		{ID: "hist-1", CreatedAt: "2026-07-30T11:00:00.000Z", ToProjectID: &toID},
	}
	normalizedAt := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	_, catalog := normalizeLinearProjectMemberships(
		claim, "linear:ENG-1", "", "", history, normalizedAt,
	)
	if len(catalog) != 1 {
		t.Fatalf("catalog=%+v, want one entry", catalog)
	}
	if !catalog[0].UpdatedAt.Equal(linearProjectsEpoch) {
		t.Fatalf("updated_at=%v, want the epoch %v (never the live sync time %v)", catalog[0].UpdatedAt, linearProjectsEpoch, normalizedAt)
	}
	if !catalog[0].LastSynced.Equal(normalizedAt) {
		t.Fatalf("last_synced=%v, want the real sync time %v (observability is unaffected by the epoch anchor)", catalog[0].LastSynced, normalizedAt)
	}
}

// A FromProjectID-only entry (ToProjectID nil/"") is a REMOVAL, not
// malformed: migration 077's own doc comment defines a removal as (P, "").
// It must reach the sink so project_membership_presence can retire the
// membership, matching CHAOS-4193's codex-review correction (a prior draft
// of this producer dropped removals entirely).
func TestNormalizeLinearProjectMembershipsEmitsARemovalWithNoDestinationProject(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	fromID := "project-old"
	history := []linearHistoryEntry{
		{ID: "hist-1", CreatedAt: "2026-07-30T11:00:00.000Z", FromProjectID: &fromID},
	}
	memberships, catalog := normalizeLinearProjectMemberships(
		claim, "linear:ENG-1", "", "", history, time.Now().UTC(),
	)
	if len(memberships) != 1 {
		t.Fatalf("memberships=%+v, want exactly one removal row", memberships)
	}
	row := memberships[0]
	if row.FromProjectID != fromID || row.ToProjectID != "" {
		t.Fatalf("row=%+v, want from=%q to=\"\" (a removal)", row, fromID)
	}
	if len(catalog) != 1 || catalog[0].ID != fromID {
		t.Fatalf("catalog=%+v, want one entry for the FROM side only", catalog)
	}
}

// The one real contradiction migration 077 refuses: a touch naming NEITHER
// side. Distinct from the removal case above, which names the FROM side.
func TestNormalizeLinearProjectMembershipsSkipsATouchNamingNeitherSide(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	empty := ""
	history := []linearHistoryEntry{
		{ID: "hist-1", CreatedAt: "2026-07-30T11:00:00.000Z", FromProjectID: &empty, ToProjectID: &empty},
	}
	memberships, catalog := normalizeLinearProjectMemberships(
		claim, "linear:ENG-1", "", "", history, time.Now().UTC(),
	)
	if len(memberships) != 0 || len(catalog) != 0 {
		t.Fatalf("memberships=%+v catalog=%+v, want none -- (\"\", \"\") names no membership at all", memberships, catalog)
	}
}

// A history entry whose native createdAt does not parse is dropped, not
// defaulted to the sync clock: occurred_at is a sorting-key member, and
// substituting normalizedAt would mint a new key on every later sync instead
// of collapsing to one row.
func TestNormalizeLinearProjectMembershipsDropsAnEntryWithNoParseableTimestamp(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	toID := "project-new"
	history := []linearHistoryEntry{
		{ID: "hist-1", CreatedAt: "not-a-timestamp", ToProjectID: &toID},
	}
	memberships, _ := normalizeLinearProjectMemberships(
		claim, "linear:ENG-1", "", "", history, time.Now().UTC(),
	)
	if len(memberships) != 0 {
		t.Fatalf("memberships=%+v, want none -- a malformed timestamp must not fall back to the sync clock", memberships)
	}
}

// Reproduces the exact cross-issue ordering codex flagged: two work items in
// the same Collect call both touch project id X, but only ONE currently
// belongs to it (a known name, free); the other only touches it historically
// (no lookup, "" name, ruled). Appended in the order that puts the blank-name
// row LAST -- the order a naive sink dedup would resolve wrong -- the merge
// must still keep the known name (codex review finding, CHAOS-4193).
func TestMergeLinearProjectCatalogNamesKeepsAKnownNameOverABlankOne(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	projectX := "project-x"

	// Issue A: project X is only historical for it (FROM side, no lookup).
	historyA := []linearHistoryEntry{{ID: "hist-a", CreatedAt: "2026-07-30T11:00:00.000Z", FromProjectID: &projectX}}
	_, catalogA := normalizeLinearProjectMemberships(claim, "linear:ENG-1", "", "", historyA, time.Now().UTC())

	// Issue B: project X IS its own current project (known name, free).
	historyB := []linearHistoryEntry{{ID: "hist-b", CreatedAt: "2026-07-30T12:00:00.000Z", FromProjectID: &projectX}}
	_, catalogB := normalizeLinearProjectMemberships(claim, "linear:ENG-2", projectX, "Platform", historyB, time.Now().UTC())

	combined := append(append([]projectmembership.CatalogRow{}, catalogA...), catalogB...)
	merged := mergeLinearProjectCatalogNames(combined)
	if len(merged) != 1 {
		t.Fatalf("merged=%+v, want exactly one row for the shared key", merged)
	}
	if merged[0].Name != "Platform" {
		t.Fatalf("name=%q, want Platform -- the known name from issue B must survive, not be blanked by issue A's row", merged[0].Name)
	}
}
