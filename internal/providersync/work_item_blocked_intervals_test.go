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
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
		// dependencies runs the provider's real relation normalizer over the
		// payload the provider returns for the BLOCKED item.
		dependencies func(t *testing.T, claim Claim) []githubWorkItemDependencyRow
	}
	cases := map[string]providerCase{
		// Jira: the issue's `issuelinks`, here the inward side of a "Blocks"
		// link ("OPS-2 is blocked by OPS-1").
		"jira": {"jira:OPS-2", "jira:OPS-1", func(t *testing.T, claim Claim) []githubWorkItemDependencyRow {
			var issue map[string]any
			if err := json.Unmarshal([]byte(`{"key":"OPS-2","fields":{"issuelinks":[
				{"type":{"name":"Blocks","inward":"is blocked by","outward":"blocks"},"inwardIssue":{"key":"OPS-1"}}
			]}}`), &issue); err != nil {
				t.Fatal(err)
			}
			return normalizeJiraDependencies(claim, "jira:OPS-2", issue, blockedTestSyncedAt)
		}},
		// GitLab: the issue links API, link_type from this issue's side.
		"gitlab": {"gitlab:acme/api#7", "gitlab:acme/api#5", func(t *testing.T, claim Claim) []githubWorkItemDependencyRow {
			var links []gitlabIssueLinkPayload
			if err := json.Unmarshal([]byte(`[{"link_type":"is_blocked_by","iid":5,"references":{"full":"acme/api#5"}}]`), &links); err != nil {
				t.Fatal(err)
			}
			return normalizeGitLabDependencies(claim, "gitlab:acme/api#7", "acme/api", "", links, blockedTestSyncedAt)
		}},
		// Linear: the issue's inverse relations ("OPS-1 blocks OPS-2").
		"linear": {"linear:OPS-2", "linear:OPS-1", func(t *testing.T, claim Claim) []githubWorkItemDependencyRow {
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
		"github": {"gh:acme/api#7", "gh:acme/api#12", func(t *testing.T, claim Claim) []githubWorkItemDependencyRow {
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

			// The blocker is a STORED item, not part of this unit, and open.
			open := &blockedFactsSource{ends: []workitemmetrics.RelationEnd{blockedTestBlocker(provider, tc.blockerID, nil)}}
			if got, want := blockedTestHours(t, provider, claim, rows, open), (map[string]float64{"blocked": 24}); !reflect.DeepEqual(got, want) {
				t.Fatalf("open blocker: hours by status = %v, want %v", got, want)
			}
			if open.calls != 1 || !reflect.DeepEqual(open.itemIDs, []string{tc.blockedID}) || len(open.fresh) != 1 {
				t.Fatalf("the store was asked %d time(s) for items %v with %d fresh relation(s), want once for [%s] with the unit's relation",
					open.calls, open.itemIDs, len(open.fresh), tc.blockedID)
			}

			// The blocker was completed at 12:00: blocked until then.
			completed := blockedTestDay.Add(12 * time.Hour)
			closed := &blockedFactsSource{ends: []workitemmetrics.RelationEnd{blockedTestBlocker(provider, tc.blockerID, &completed)}}
			if got, want := blockedTestHours(t, provider, claim, rows, closed), (map[string]float64{"blocked": 12, "in_progress": 12}); !reflect.DeepEqual(got, want) {
				t.Fatalf("blocker completed at 12:00: hours by status = %v, want %v", got, want)
			}

			// The blocker is not a stored work item: nothing is derived.
			unknown := &blockedFactsSource{}
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
// re-synced item did not emit again; a link it still reports is emitted again
// and is the newer row.
func TestSyncTimeBlockedUsesTheUnitsFreshRowsOverTheStore(t *testing.T) {
	claim := blockedTestClaim(t, "jira")
	earlier := blockedTestSyncedAt.Add(-48 * time.Hour)
	stored := workitemmetrics.BlockingRelation{
		SourceID: "jira:OPS-1", TargetID: "jira:OPS-2", RelationshipType: "blocks",
		SemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: earlier,
	}
	// Both ends were synced after the stored relation row: the stored copy of
	// the blocked item is OLD (it is the fresh row that is new), the blocker
	// was synced again later.
	storedEnds := []workitemmetrics.RelationEnd{
		{WorkItemID: "jira:OPS-2", Provider: "jira", Status: "todo", CreatedAt: blockedTestDay, LastSynced: earlier},
		blockedTestBlocker("jira", "jira:OPS-1", nil),
	}

	// The unit re-synced OPS-2 and did NOT emit the relation again.
	removed := blockedTestRows(claim, "jira", "jira:OPS-2", nil)
	source := &blockedFactsSource{relations: []workitemmetrics.BlockingRelation{stored}, ends: storedEnds}
	if got, want := blockedTestHours(t, "jira", claim, removed, source), (map[string]float64{"todo": 6, "in_progress": 18}); !reflect.DeepEqual(got, want) {
		t.Fatalf("link removed at the provider: hours by status = %v, want %v", got, want)
	}

	// The unit re-synced OPS-2 and emitted the relation again.
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
// relation, and the result does not depend on the order of the inputs.
func TestMergeBlockingRelationsKeepsTheRowSyncedLast(t *testing.T) {
	row := func(source, target string, hour int) workitemmetrics.BlockingRelation {
		return workitemmetrics.BlockingRelation{
			SourceID: source, TargetID: target, RelationshipType: "blocks",
			SemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: blockedTestDay.Add(time.Duration(hour) * time.Hour),
		}
	}
	stored := []workitemmetrics.BlockingRelation{row("b", "c", 1), row("a", "c", 5), row("a", "b", 9)}
	fresh := []workitemmetrics.BlockingRelation{row("a", "c", 7), row("a", "b", 3)}
	want := []workitemmetrics.BlockingRelation{row("a", "b", 9), row("a", "c", 7), row("b", "c", 1)}
	if got := mergeBlockingRelations(stored, fresh); !reflect.DeepEqual(got, want) {
		t.Fatalf("merge(stored, fresh) = %+v, want %+v", got, want)
	}
	if got := mergeBlockingRelations(fresh, stored); !reflect.DeepEqual(got, want) {
		t.Fatalf("merge(fresh, stored) = %+v, want %+v", got, want)
	}
	// A different relationship type between the same items is another relation.
	other := row("a", "b", 1)
	other.RelationshipType = "blocked_by"
	if got := mergeBlockingRelations([]workitemmetrics.BlockingRelation{row("a", "b", 9)}, []workitemmetrics.BlockingRelation{other}); len(got) != 2 {
		t.Fatalf("two relation types between the same items merged into %d row(s)", len(got))
	}
}
