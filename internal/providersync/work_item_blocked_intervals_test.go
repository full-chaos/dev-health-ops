package providersync

// CHAOS-8493 at the sync-time deriver: a work item is "blocked" while a
// blocking relation names an open blocker, for every provider that carries
// such a relation.
//
// Each provider case goes through that provider's REAL relation normalizer,
// fed the payload shape the provider returns, so a normalizer that stopped
// emitting the canonical "blocks" row (or turned it around) fails here by the
// provider's name. The rule itself is pinned clause by clause in
// internal/jobs/metrics/workitemmetrics; these tests pin that each provider's
// rows REACH it, and reach it with the direction the rule reads.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// blockedFactsSource is a derivation source whose store holds the given
// blocking facts. It records what the loader asked it for.
type blockedFactsSource struct {
	relations []workitemmetrics.BlockingRelation
	ends      []workitemmetrics.RelationEnd
	err       error

	calls   int
	itemIDs []string
	fresh   []workitemmetrics.BlockingRelation
}

func (source *blockedFactsSource) Load(
	context.Context, Claim, teamattribution.GithubWorkItemDerivationLoadRequest,
) (teamattribution.GithubWorkItemDerivationFacts, error) {
	return teamattribution.GithubWorkItemDerivationFacts{}, nil
}

func (source *blockedFactsSource) LoadStoredInheritableEdges(
	context.Context, Claim, []string,
) ([]githubWorkItemDependencyRow, error) {
	return nil, nil
}

func (source *blockedFactsSource) LoadStoredBlockingFacts(
	_ context.Context, _ Claim, itemIDs []string, fresh []workitemmetrics.BlockingRelation,
) ([]workitemmetrics.BlockingRelation, []workitemmetrics.RelationEnd, error) {
	source.calls++
	source.itemIDs = append([]string(nil), itemIDs...)
	source.fresh = append([]workitemmetrics.BlockingRelation(nil), fresh...)
	if source.err != nil {
		return nil, nil, source.err
	}
	return source.relations, source.ends, nil
}

var (
	blockedTestDay        = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	blockedTestSyncedAt   = time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	blockedTestComputedAt = time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
)

func blockedTestClaim(t *testing.T, provider string) Claim {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	claim.Provider = provider
	capability, ok := Capability(provider, "work-items")
	if !ok {
		t.Fatalf("no work-items capability for %s", provider)
	}
	claim.CostClass = capability.CostClass
	if err := claim.Validate(); err != nil {
		t.Fatalf("%s claim: %v", provider, err)
	}
	return claim
}

// blockedTestRows is one work item of the unit: created at 00:00 of the day,
// todo -> in_progress at 06:00, still open. With no blocker it contributes
// todo 6h + in_progress 18h to the day.
func blockedTestRows(claim Claim, provider, workItemID string, dependencies []githubWorkItemDependencyRow) githubWorkItemRows {
	return githubWorkItemRows{
		WorkItems: []githubWorkItemRow{{
			WorkItemID: workItemID, Provider: provider, Title: "the blocked item", Type: "issue",
			Status: "in_progress", ProjectID: stringPointer("acme/api"),
			CreatedAt: blockedTestDay, UpdatedAt: blockedTestDay.Add(6 * time.Hour),
			LastSynced: blockedTestSyncedAt, OrgID: claim.OrgID,
		}},
		StatusTransitions: []githubWorkItemTransitionRow{{
			WorkItemID: workItemID, Provider: provider,
			OccurredAt: blockedTestDay.Add(6 * time.Hour),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
		}},
		Dependencies: dependencies,
	}
}

func blockedTestBlocker(provider, workItemID string, completedAt *time.Time) workitemmetrics.RelationEnd {
	status := "in_progress"
	if completedAt != nil {
		status = "done"
	}
	return workitemmetrics.RelationEnd{
		WorkItemID: workItemID, Provider: provider, Status: status,
		CreatedAt: blockedTestDay.AddDate(0, 0, -10), CompletedAt: completedAt, LastSynced: blockedTestSyncedAt,
	}
}

// blockedTestStored is the STORED copy of a relation the unit emits again: the
// same key, written by an earlier sync and first seen three days before the
// day under test.
func blockedTestStored(fresh githubWorkItemDependencyRow) workitemmetrics.BlockingRelation {
	firstSeen := blockedTestDay.AddDate(0, 0, -3)
	return workitemmetrics.BlockingRelation{
		SourceID: fresh.SourceWorkItemID, TargetID: fresh.TargetWorkItemID,
		RelationshipType: fresh.RelationshipType, SemanticsVersion: fresh.RelationshipSemanticsVersion,
		LastSynced: blockedTestDay.AddDate(0, 0, -1), FirstSeenAt: &firstSeen,
	}
}

func blockedTestHours(t *testing.T, provider string, claim Claim, rows githubWorkItemRows, source *blockedFactsSource) map[string]float64 {
	t.Helper()
	blocked, err := loadWorkItemBlockedIntervalsForProvider(context.Background(), provider, claim, rows, source)
	if err != nil {
		t.Fatalf("load blocked intervals: %v", err)
	}
	surfaces, err := buildWorkItemDerivedSurfacesForProvider(
		provider, claim, rows, blockedTestDay, blockedTestComputedAt,
		teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{}),
		blocked,
	)
	if err != nil {
		t.Fatalf("build surfaces: %v", err)
	}
	hours := map[string]float64{}
	for _, row := range surfaces.StateDurations {
		if row.Provider != provider || row.OrgID != claim.OrgID {
			t.Fatalf("state duration row = %+v, want provider %s in org %s", row, provider, claim.OrgID)
		}
		hours[row.Status] += row.DurationHours
	}
	return hours
}

func TestEveryProviderDerivesBlockedFromItsOwnBlockingRelation(t *testing.T) {
	type providerCase struct {
		blockedID, blockerID string
		// naming is what the store is asked for: the unit's item id and, for
		// a jira or linear item, the external-key form of its issue key (a
		// relation read from text in another item names it by that key).
		naming []string
		// dependencies runs the provider's real relation normalizer over the
		// payload the provider returns for the BLOCKED item.
		dependencies func(t *testing.T, claim Claim) []githubWorkItemDependencyRow
	}
	cases := map[string]providerCase{
		// Jira: the issue's `issuelinks`, here the inward side of a "Blocks"
		// link ("OPS-2 is blocked by OPS-1").
		"jira": {"jira:OPS-2", "jira:OPS-1", []string{"extkey:OPS-2", "jira:OPS-2"}, func(t *testing.T, claim Claim) []githubWorkItemDependencyRow {
			var issue map[string]any
			if err := json.Unmarshal([]byte(`{"key":"OPS-2","fields":{"issuelinks":[
				{"type":{"name":"Blocks","inward":"is blocked by","outward":"blocks"},"inwardIssue":{"key":"OPS-1"}}
			]}}`), &issue); err != nil {
				t.Fatal(err)
			}
			return normalizeJiraDependencies(claim, "jira:OPS-2", issue, blockedTestSyncedAt)
		}},
		// GitLab: the issue links API, link_type from this issue's side.
		"gitlab": {"gitlab:acme/api#7", "gitlab:acme/api#5", []string{"gitlab:acme/api#7"}, func(t *testing.T, claim Claim) []githubWorkItemDependencyRow {
			var links []gitlabIssueLinkPayload
			if err := json.Unmarshal([]byte(`[{"link_type":"is_blocked_by","iid":5,"references":{"full":"acme/api#5"}}]`), &links); err != nil {
				t.Fatal(err)
			}
			rows, _ := normalizeGitLabDependencies(claim, "gitlab:acme/api#7", "acme/api", "", links, blockedTestSyncedAt)
			return rows
		}},
		// Linear: the issue's inverse relations ("OPS-1 blocks OPS-2").
		"linear": {"linear:OPS-2", "linear:OPS-1", []string{"extkey:OPS-2", "linear:OPS-2"}, func(t *testing.T, claim Claim) []githubWorkItemDependencyRow {
			var payload linearWorkItemPayload
			if err := json.Unmarshal([]byte(`{"identifier":"OPS-2","inverseRelations":{"nodes":[
				{"type":"blocks","issue":{"identifier":"OPS-1"},"relatedIssue":{"identifier":"OPS-2"}}
			]}}`), &payload); err != nil {
				t.Fatal(err)
			}
			return normalizeLinearDependencies(claim, payload, "linear:OPS-2", blockedTestSyncedAt)
		}},
		// GitHub: no native relation is synced. The relation is TEXT in the
		// issue body.
		"github": {"gh:acme/api#7", "gh:acme/api#12", []string{"gh:acme/api#7"}, func(t *testing.T, claim Claim) []githubWorkItemDependencyRow {
			rows, err := extractGitHubWorkItemDependencies(claim, "gh:acme/api#7", "acme/api", "Blocked by #12", "", nil, blockedTestSyncedAt)
			if err != nil {
				t.Fatal(err)
			}
			return rows
		}},
	}
	if len(cases) != 4 {
		t.Fatalf("%d provider cases, want jira, gitlab, linear and github", len(cases))
	}

	for provider, tc := range cases {
		t.Run(provider, func(t *testing.T) {
			claim := blockedTestClaim(t, provider)
			dependencies := tc.dependencies(t, claim)
			// The normalizer's own output is the fact under test: one
			// canonical row, the blocker as its source.
			if len(dependencies) != 1 || dependencies[0].SourceWorkItemID != tc.blockerID ||
				dependencies[0].TargetWorkItemID != tc.blockedID || dependencies[0].RelationshipType != "blocks" ||
				dependencies[0].RelationshipSemanticsVersion != workitemmetrics.CanonicalBlocksSemantics {
				t.Fatalf("%s normalizer emitted %+v, want one canonical `blocks` row from %s to %s", provider, dependencies, tc.blockerID, tc.blockedID)
			}
			rows := blockedTestRows(claim, provider, tc.blockedID, dependencies)

			// The store already holds the relation (first seen before the day),
			// and the blocker: a stored item, not part of this unit, and open.
			stored := []workitemmetrics.BlockingRelation{blockedTestStored(dependencies[0])}
			open := &blockedFactsSource{relations: stored, ends: []workitemmetrics.RelationEnd{blockedTestBlocker(provider, tc.blockerID, nil)}}
			if got, want := blockedTestHours(t, provider, claim, rows, open), (map[string]float64{"blocked": 24}); !reflect.DeepEqual(got, want) {
				t.Fatalf("open blocker: hours by status = %v, want %v", got, want)
			}
			if open.calls != 1 || !reflect.DeepEqual(open.itemIDs, tc.naming) || len(open.fresh) != 1 {
				t.Fatalf("the store was asked %d time(s) for %v with %d fresh relation(s), want once for %v with the unit's relation",
					open.calls, open.itemIDs, len(open.fresh), tc.naming)
			}
			if open.fresh[0].Raw != dependencies[0].RelationshipTypeRaw || open.fresh[0].Raw == "" {
				t.Fatalf("the unit's relation reached the rule with the raw type %q, want the normalizer's %q", open.fresh[0].Raw, dependencies[0].RelationshipTypeRaw)
			}

			// The blocker was completed at 12:00: blocked until then.
			completed := blockedTestDay.Add(12 * time.Hour)
			closed := &blockedFactsSource{relations: stored, ends: []workitemmetrics.RelationEnd{blockedTestBlocker(provider, tc.blockerID, &completed)}}
			if got, want := blockedTestHours(t, provider, claim, rows, closed), (map[string]float64{"blocked": 12, "in_progress": 12}); !reflect.DeepEqual(got, want) {
				t.Fatalf("blocker completed at 12:00: hours by status = %v, want %v", got, want)
			}

			// THIS sync is the first to see the relation (the store does not
			// hold it): it starts now, so no hour of an earlier day is blocked.
			// The item's creation time is never the start.
			firstSeenNow := &blockedFactsSource{ends: []workitemmetrics.RelationEnd{blockedTestBlocker(provider, tc.blockerID, nil)}}
			if got, want := blockedTestHours(t, provider, claim, rows, firstSeenNow), (map[string]float64{"todo": 6, "in_progress": 18}); !reflect.DeepEqual(got, want) {
				t.Fatalf("relation first seen by this sync: hours by status = %v, want %v", got, want)
			}

			// The blocker is not a stored work item: nothing is derived.
			unknown := &blockedFactsSource{relations: stored}
			if got, want := blockedTestHours(t, provider, claim, rows, unknown), (map[string]float64{"todo": 6, "in_progress": 18}); !reflect.DeepEqual(got, want) {
				t.Fatalf("unknown blocker: hours by status = %v, want %v", got, want)
			}

			// No relation: the day is as it was before this rule.
			none := blockedTestRows(claim, provider, tc.blockedID, nil)
			if got, want := blockedTestHours(t, provider, claim, none, open), (map[string]float64{"todo": 6, "in_progress": 18}); !reflect.DeepEqual(got, want) {
				t.Fatalf("no relation: hours by status = %v, want %v", got, want)
			}
		})
	}
}

// At sync time the unit's own rows are newer than the store and are not in it
// yet. A link the provider no longer reports is a STORED relation that the
// re-synced item did not emit again: it blocked the item until the last time
// a sync saw it, and not after. A link it still reports is emitted again and
// is the newer row; it keeps the stored first-seen time.
func TestSyncTimeBlockedUsesTheUnitsFreshRowsOverTheStore(t *testing.T) {
	claim := blockedTestClaim(t, "jira")
	firstSeen := blockedTestDay.AddDate(0, 0, -3)
	lastSeen := blockedTestDay.Add(12 * time.Hour)
	stored := workitemmetrics.BlockingRelation{
		SourceID: "jira:OPS-1", TargetID: "jira:OPS-2", RelationshipType: "blocks",
		SemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: lastSeen, FirstSeenAt: &firstSeen,
	}
	// Both ends were synced after the stored relation row: the stored copy of
	// the blocked item is OLD (it is the unit's fresh row that is new), the
	// blocker was synced again later.
	storedEnds := []workitemmetrics.RelationEnd{
		{WorkItemID: "jira:OPS-2", Provider: "jira", Status: "todo", CreatedAt: blockedTestDay, LastSynced: lastSeen},
		blockedTestBlocker("jira", "jira:OPS-1", nil),
	}

	// The unit re-synced OPS-2 and did NOT emit the relation again: blocked
	// until 12:00, the last time a sync saw the relation.
	removed := blockedTestRows(claim, "jira", "jira:OPS-2", nil)
	source := &blockedFactsSource{relations: []workitemmetrics.BlockingRelation{stored}, ends: storedEnds}
	if got, want := blockedTestHours(t, "jira", claim, removed, source), (map[string]float64{"blocked": 12, "in_progress": 12}); !reflect.DeepEqual(got, want) {
		t.Fatalf("link removed at the provider: hours by status = %v, want %v", got, want)
	}

	// The unit re-synced OPS-2 and emitted the relation again: still blocked,
	// from the STORED first-seen time (before the day), not from this sync.
	fresh := githubWorkItemDependencyRow{
		SourceWorkItemID: "jira:OPS-1", TargetWorkItemID: "jira:OPS-2", RelationshipType: "blocks",
		RelationshipTypeRaw: "is blocked by", RelationshipSemanticsVersion: workitemmetrics.CanonicalBlocksSemantics,
		LastSynced: blockedTestSyncedAt, OrgID: claim.OrgID,
	}
	kept := blockedTestRows(claim, "jira", "jira:OPS-2", []githubWorkItemDependencyRow{fresh})
	if got, want := blockedTestHours(t, "jira", claim, kept, source), (map[string]float64{"blocked": 24}); !reflect.DeepEqual(got, want) {
		t.Fatalf("link still reported: hours by status = %v, want %v", got, want)
	}
}

// A unit that cannot read the stored blocking facts fails. Computing without
// them would write full-length rows for the statuses the blocked hours belong
// to, with nothing saying the read had failed.
func TestSyncTimeBlockedFailsClosedAndStaysInItsTenant(t *testing.T) {
	claim := blockedTestClaim(t, "linear")
	rows := blockedTestRows(claim, "linear", "linear:OPS-2", nil)
	ctx := context.Background()

	boom := errors.New("clickhouse is away")
	if _, err := loadWorkItemBlockedIntervalsForProvider(ctx, "linear", claim, rows, &blockedFactsSource{err: boom}); !errors.Is(err, boom) {
		t.Fatalf("failed read: err = %v, want the read's error", err)
	}

	// The claim is for another provider, or no source: refused before any read.
	source := &blockedFactsSource{}
	if _, err := loadWorkItemBlockedIntervalsForProvider(ctx, "jira", claim, rows, source); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("claim of another provider: err = %v", err)
	}
	if _, err := loadWorkItemBlockedIntervalsForProvider(ctx, "linear", claim, rows, nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("no source: err = %v", err)
	}

	// A row of another organization in the unit is a scope error, never read.
	foreignItem := blockedTestRows(claim, "linear", "linear:OPS-2", nil)
	foreignItem.WorkItems[0].OrgID = "another-org"
	if _, err := loadWorkItemBlockedIntervalsForProvider(ctx, "linear", claim, foreignItem, source); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("item of another organization: err = %v", err)
	}
	foreignRelation := blockedTestRows(claim, "linear", "linear:OPS-2", []githubWorkItemDependencyRow{{
		SourceWorkItemID: "linear:OPS-1", TargetWorkItemID: "linear:OPS-2", RelationshipType: "blocks",
		RelationshipSemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: blockedTestSyncedAt, OrgID: "another-org",
	}})
	if _, err := loadWorkItemBlockedIntervalsForProvider(ctx, "linear", claim, foreignRelation, source); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("relation of another organization: err = %v", err)
	}
	if source.calls != 0 {
		t.Fatalf("the store was read %d time(s) for a refused unit", source.calls)
	}

	// A unit with no work item reads nothing.
	if blocked, err := loadWorkItemBlockedIntervalsForProvider(ctx, "linear", claim, githubWorkItemRows{}, source); err != nil || blocked != nil || source.calls != 0 {
		t.Fatalf("empty unit: %v, %v, %d read(s)", blocked, err, source.calls)
	}
}

// Of a stored and a fresh row for one relation the one synced last is the
// relation, the result does not depend on the order of the rows, and the two
// start times are carried: the stored first-seen time survives, a relation
// the store does not hold was first seen by this sync, and a provider time is
// not lost when the newer row has none.
func TestMergeBlockingRelationsKeepsTheRowSyncedLastAndTheStartTimes(t *testing.T) {
	at := func(hour int) time.Time { return blockedTestDay.Add(time.Duration(hour) * time.Hour) }
	ptr := func(hour int) *time.Time { value := at(hour); return &value }
	row := func(source, target string, synced int, firstSeen, started *time.Time) workitemmetrics.BlockingRelation {
		return workitemmetrics.BlockingRelation{
			SourceID: source, TargetID: target, RelationshipType: "blocks",
			SemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: at(synced),
			FirstSeenAt: firstSeen, StartedAt: started,
		}
	}
	stored := []workitemmetrics.BlockingRelation{
		row("b", "c", 1, ptr(1), nil),    // stored only
		row("a", "c", 5, ptr(2), ptr(0)), // re-emitted by the unit, which carries no provider time
		row("a", "b", 9, ptr(3), nil),    // the stored row is NEWER than the unit's
		row("e", "f", 4, nil, nil),       // stored, with no stored first-seen time
		row("g", "h", 2, ptr(2), nil),    // two stored copies of one relation:
		row("g", "h", 6, ptr(2), nil),    // the one synced last is the relation
	}
	fresh := []workitemmetrics.BlockingRelation{
		row("a", "c", 7, nil, nil),
		row("a", "b", 3, nil, ptr(1)),
		row("d", "e", 7, nil, nil), // not stored: first seen by this sync
		row("e", "f", 8, nil, nil),
	}
	want := []workitemmetrics.BlockingRelation{
		row("a", "b", 9, ptr(3), ptr(1)),
		row("a", "c", 7, ptr(2), ptr(0)),
		row("b", "c", 1, ptr(1), nil),
		row("d", "e", 7, ptr(7), nil),
		row("e", "f", 8, nil, nil),
		row("g", "h", 6, ptr(2), nil),
	}
	if got := mergeBlockingRelations(stored, fresh); !reflect.DeepEqual(got, want) {
		t.Fatalf("merge =\n  %+v\nwant\n  %+v", got, want)
	}
	reversedStored := []workitemmetrics.BlockingRelation{stored[5], stored[4], stored[3], stored[2], stored[1], stored[0]}
	reversedFresh := []workitemmetrics.BlockingRelation{fresh[3], fresh[2], fresh[1], fresh[0]}
	if got := mergeBlockingRelations(reversedStored, reversedFresh); !reflect.DeepEqual(got, want) {
		t.Fatalf("merge of the same rows in another order =\n  %+v\nwant\n  %+v", got, want)
	}
	// A different relationship type between the same items is another relation.
	other := row("a", "b", 1, nil, nil)
	other.RelationshipType = "blocked_by"
	if got := mergeBlockingRelations([]workitemmetrics.BlockingRelation{row("a", "b", 9, ptr(3), nil)}, []workitemmetrics.BlockingRelation{other}); len(got) != 2 {
		t.Fatalf("two relation types between the same items merged into %d row(s)", len(got))
	}
}

// PINS A KNOWN UNDERSTATEMENT (CHAOS-8493, named in
// docs/reference/metrics/definitions.md): a github issue on a Projects v2
// board.
//
// The pass that reads an issue's text is incremental: it reads an issue again
// only after the issue is updated. The board pass reads every board item on
// every run, writes the issue's work_items row again with a new last_synced,
// and reads no issue text (its query has no body), so it writes no relation.
// The rule cannot tell that sync from one that read the text and found the
// relation gone: the item that writes the relation has a later sync that did
// not write it. So the relation ends at its last write, though the text is
// still there.
//
// This test shows that behaviour with the real normalizers, and that the run
// counts it as a board candidate. It is here so that the day the stored row
// can tell the two syncs apart, this test is the one that changes.
func TestAGitHubBoardPassEndsTheTextRelationOfAnIssueNobodyUpdates(t *testing.T) {
	claim := blockedTestClaim(t, "github")
	// The text was read two times: first seen two days before the last read.
	textRead := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	firstSeen := textRead.AddDate(0, 0, -2)

	// The pass that reads the text: "Blocked by #12" in issue #7.
	dependencies, err := extractGitHubWorkItemDependencies(claim, "gh:acme/api#7", "acme/api", "Blocked by #12", "", nil, textRead)
	if err != nil {
		t.Fatal(err)
	}
	if len(dependencies) != 1 || dependencies[0].RelationshipTypeRaw != "blocked by #12" {
		t.Fatalf("the text normalizer emitted %+v, want one relation with the phrase as its raw type", dependencies)
	}
	stored := workitemmetrics.BlockingRelation{
		SourceID: dependencies[0].SourceWorkItemID, TargetID: dependencies[0].TargetWorkItemID,
		RelationshipType: dependencies[0].RelationshipType, Raw: dependencies[0].RelationshipTypeRaw,
		SemanticsVersion: dependencies[0].RelationshipSemanticsVersion,
		LastSynced:       dependencies[0].LastSynced, FirstSeenAt: &firstSeen,
	}
	issue := workitemmetrics.RelationEnd{
		WorkItemID: "gh:acme/api#7", Provider: "github", ProjectID: "acme/api", Status: "in_progress",
		CreatedAt: textRead.AddDate(0, 0, -5), LastSynced: textRead,
	}
	blocker := workitemmetrics.RelationEnd{
		WorkItemID: "gh:acme/api#12", Provider: "github", ProjectID: "acme/api", Status: "in_progress",
		CreatedAt: textRead.AddDate(0, 0, -9), LastSynced: textRead,
	}

	// Before any board pass the relation is open.
	intervals, stats := workitemmetrics.BlockedIntervalsWithStats([]workitemmetrics.BlockingRelation{stored}, []workitemmetrics.RelationEnd{issue, blocker})
	if want := (map[string][]workitemmetrics.BlockedInterval{"gh:acme/api#7": {{Start: firstSeen}}}); !reflect.DeepEqual(intervals, want) {
		t.Fatalf("before the board pass: intervals = %+v, want %+v", intervals, want)
	}
	if len(stats.Ended) != 0 || stats.GitHubBoardCandidates != 0 {
		t.Fatalf("before the board pass: stats = %+v, want nothing ended", stats)
	}

	// The board pass, one day later, through its real normalizer: the SAME
	// work item id, a new last_synced, a board project id, and no relation.
	boardRow, _, emitted := buildGitHubProjectV2OracleRows(t, gitHubProjectV2OracleInput())
	if !emitted || boardRow.WorkItemID != issue.WorkItemID {
		t.Fatalf("the board pass emitted %t a row for %q, want a row for %q", emitted, boardRow.WorkItemID, issue.WorkItemID)
	}
	if !boardRow.LastSynced.After(textRead) || boardRow.ProjectID == nil ||
		!strings.HasPrefix(*boardRow.ProjectID, workitemmetrics.GitHubBoardProjectPrefix) {
		t.Fatalf("the board row = last_synced %s, project %v; want a later sync and a %q project id",
			boardRow.LastSynced, boardRow.ProjectID, workitemmetrics.GitHubBoardProjectPrefix)
	}
	if boardRow.Description != nil && *boardRow.Description != "" {
		t.Fatalf("the board row carries issue text %q: the board pass can read relations now, and this exception is gone", *boardRow.Description)
	}
	rows := githubWorkItemRows{
		WorkItems: []githubWorkItemRow{boardRow},
		StatusTransitions: []githubWorkItemTransitionRow{{
			WorkItemID: boardRow.WorkItemID, Provider: "github", OccurredAt: textRead.AddDate(0, 0, -4),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
		}},
	}
	source := &blockedFactsSource{relations: []workitemmetrics.BlockingRelation{stored}, ends: []workitemmetrics.RelationEnd{issue, blocker}}

	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(previous)
	blocked, err := loadWorkItemBlockedIntervalsForProvider(context.Background(), "github", claim, rows, source)
	slog.SetDefault(previous)
	if err != nil {
		t.Fatal(err)
	}

	// TODAY'S BEHAVIOUR: the relation ends at its last write, the text pass.
	want := map[string][]workitemmetrics.BlockedInterval{"gh:acme/api#7": {{Start: firstSeen, End: &textRead}}}
	if !reflect.DeepEqual(blocked, want) {
		t.Fatalf("after the board pass: intervals = %+v, want %+v (ended at the last sync that read the text)", blocked, want)
	}
	// ONE log line for the run, with the count and no id.
	var lines []string
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, workitemmetrics.EndedRelationsLogMessage) {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("%d log lines of the end rule for one run, want 1:\n%s", len(lines), logged.String())
	}
	for _, part := range []string{
		`msg="` + workitemmetrics.EndedRelationsLogMessage + `"`, "writer=sync_time_deriver",
		"relations=1", "ended=1", "ended_github=1", "ended_gitlab=0", "ended_jira=0", "ended_linear=0", "ended_other=0",
		"github_board_candidates=1",
	} {
		if !strings.Contains(lines[0], part) {
			t.Fatalf("the log line has no %q:\n%s", part, lines[0])
		}
	}
	for _, id := range []string{"gh:acme/api#7", "gh:acme/api#12", claim.OrgID, claim.ID} {
		if strings.Contains(lines[0], id) {
			t.Fatalf("the log line holds the id %q:\n%s", id, lines[0])
		}
	}
}

// PINS A KNOWN UNDERSTATEMENT (CHAOS-8493, named in
// docs/reference/metrics/definitions.md): the gitlab description keyword
// "blocks".
//
// "blocks #7" in the description of #5 is stored with the raw value "blocks",
// which is also the raw value of a native issue link seen from the blocker.
// A native link is written by both issues; the keyword only by #5. The stored
// row cannot say which it is, so both issues are taken as writers: when the
// BLOCKED issue #7 is synced later, the relation ends at its last write,
// though the text in #5 is still there. With the keyword "blocking", whose
// raw value is its own, the same sync ends nothing.
func TestAGitLabBlocksKeywordEndsWhenTheBlockedIssueIsSyncedLater(t *testing.T) {
	claim := blockedTestClaim(t, "gitlab")
	written := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	firstSeen := written.AddDate(0, 0, -2)
	later := written.Add(24 * time.Hour)
	ends := []workitemmetrics.RelationEnd{
		{WorkItemID: "gitlab:acme/api#5", Provider: "gitlab", Status: "in_progress", CreatedAt: written.AddDate(0, 0, -9), LastSynced: written},
		// The blocked issue was synced again; it holds no text about #5.
		{WorkItemID: "gitlab:acme/api#7", Provider: "gitlab", Status: "in_progress", CreatedAt: written.AddDate(0, 0, -5), LastSynced: later},
	}
	for description, tc := range map[string]struct {
		raw  string
		want workitemmetrics.BlockedInterval
	}{
		"this blocks #7":      {"blocks", workitemmetrics.BlockedInterval{Start: firstSeen, End: &written}},
		"this is blocking #7": {"blocking", workitemmetrics.BlockedInterval{Start: firstSeen}},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			rows, _ := normalizeGitLabDependencies(claim, "gitlab:acme/api#5", "acme/api", description, nil, written)
			if len(rows) != 1 || rows[0].SourceWorkItemID != "gitlab:acme/api#5" || rows[0].TargetWorkItemID != "gitlab:acme/api#7" ||
				rows[0].RelationshipType != "blocks" || rows[0].RelationshipTypeRaw != tc.raw {
				t.Fatalf("the description %q gave %+v, want one `blocks` row from #5 to #7 with the raw value %q", description, rows, tc.raw)
			}
			relation := workitemmetrics.BlockingRelation{
				SourceID: rows[0].SourceWorkItemID, TargetID: rows[0].TargetWorkItemID, RelationshipType: rows[0].RelationshipType,
				Raw: rows[0].RelationshipTypeRaw, SemanticsVersion: rows[0].RelationshipSemanticsVersion,
				LastSynced: rows[0].LastSynced, FirstSeenAt: &firstSeen,
			}
			got := workitemmetrics.BlockedIntervalsByItem([]workitemmetrics.BlockingRelation{relation}, ends)
			if want := (map[string][]workitemmetrics.BlockedInterval{"gitlab:acme/api#7": {tc.want}}); !reflect.DeepEqual(got, want) {
				t.Fatalf("intervals = %+v, want %+v", got, want)
			}
		})
	}
	// The native link from the blocker's side stores the same raw value.
	var links []gitlabIssueLinkPayload
	if err := json.Unmarshal([]byte(`[{"link_type":"blocks","iid":7,"references":{"full":"acme/api#7"}}]`), &links); err != nil {
		t.Fatal(err)
	}
	native, _ := normalizeGitLabDependencies(claim, "gitlab:acme/api#5", "acme/api", "", links, written)
	if len(native) != 1 || native[0].RelationshipTypeRaw != "blocks" || native[0].SourceWorkItemID != "gitlab:acme/api#5" {
		t.Fatalf("the native link gave %+v, want one row with the raw value `blocks`: the two forms are no longer the same stored row, and this exception is gone", native)
	}
}
