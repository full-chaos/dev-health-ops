package providersync

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-3978: a cross-provider donor edge is minted by ONE provider's sync and
// needed by ANOTHER provider's writer.
//
// `ghpr:...#1794 --relates_to--> linear:CHAOS-3914` (relationship_type_raw
// `linear_attachment`) is written exclusively by the Linear sync, from a Linear
// attachment. The GitHub work-items route never mints a `linear:` target at
// all, so under a fresh-edges-only donor load the edge is structurally
// invisible to the side that would inherit from it, and the PR is re-stamped
// `unassigned` on every run despite a valid, teamed donor. Prod: 85 items on
// 2026-08-23, grown from 82 on 2026-08-20.
//
// These fixtures are the prod shape, not a synthetic analogue: the subject is a
// GitHub PR, the donor is a Linear issue attributed `native_team`, the edge
// carries the real provenance string, and the subject's OWN cascade resolves to
// nothing (no repo, project, assignee or native key) so `linked_issue` is the
// only source that can produce a team.
const (
	crossProviderPR       = "ghpr:full-chaos/dev-health-ops#1794"
	crossProviderDonor    = "linear:CHAOS-3914"
	crossProviderRawKind  = "linear_attachment"
	crossProviderTeamID   = "fullchaos"
	crossProviderTeamName = "Fullchaos"
)

func crossProviderClaim() Claim {
	return githubWorkItemOracleClaim()
}

func crossProviderRows(claim Claim, dependencies ...githubWorkItemDependencyRow) githubWorkItemRows {
	return githubWorkItemRows{
		WorkItems:    []githubWorkItemRow{crossProviderWorkItem(claim, crossProviderPR)},
		Dependencies: dependencies,
	}
}

// crossProviderWorkItem carries the scalar fields the derived surfaces need
// and NOTHING that could resolve a team on its own -- no repo, project,
// assignee or native key -- so `linked_issue` is the only source that can hand
// this item a team.
func crossProviderWorkItem(claim Claim, workItemID string) githubWorkItemRow {
	return githubWorkItemRow{
		WorkItemID: workItemID, Provider: "github", Title: "t", Type: "pull_request",
		Status:    "done",
		CreatedAt: time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC),
		OrgID:     claim.OrgID,
	}
}

// crossProviderDonorFacts is the donor as ClickHouse holds it: a Linear issue
// whose native team key resolves through the teams fact, i.e. a rank-0
// `native_team` donor -- an allowed donor source.
func crossProviderDonorFacts(claim Claim) githubWorkItemDerivationFacts {
	nativeKey := crossProviderTeamID
	return githubWorkItemDerivationFacts{
		Teams: []githubWorkItemDerivationTeamFact{{
			Provider: "linear", TeamID: crossProviderTeamID,
			TeamName: crossProviderTeamName, ProjectKeys: []string{crossProviderTeamID},
		}},
		DonorItems: []githubWorkItemDerivationSubject{{
			WorkItemID: crossProviderDonor, Provider: "linear",
			NativeTeamKey: &nativeKey, OrgID: claim.OrgID,
		}},
	}
}

func crossProviderStoredEdge(claim Claim, syncedAt time.Time) githubWorkItemDependencyRow {
	return githubWorkItemDependencyRow{
		SourceWorkItemID: crossProviderPR, TargetWorkItemID: crossProviderDonor,
		RelationshipType: "relates_to", RelationshipTypeRaw: crossProviderRawKind,
		LastSynced: syncedAt, OrgID: claim.OrgID,
	}
}

func crossProviderSubject(claim Claim) githubWorkItemDerivationSubject {
	return githubWorkItemDerivationSubjectFromRow(crossProviderWorkItem(claim, crossProviderPR))
}

// TestCrossProviderStoredDonorEdgeIsVisibleToTheInheritingWriter is the fix,
// stated as the prod shape: the edge exists ONLY in the store, and the PR
// inherits its donor's team anyway.
func TestCrossProviderStoredDonorEdgeIsVisibleToTheInheritingWriter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim := crossProviderClaim()
	source := &fakeGitHubWorkItemDerivationContextSource{
		facts:       crossProviderDonorFacts(claim),
		storedEdges: []githubWorkItemDependencyRow{crossProviderStoredEdge(claim, now.AddDate(0, 0, -40))},
	}

	// RED CONTROL, in the same test so it cannot rot apart from the fix: the
	// IDENTICAL inputs minus the stored edge must resolve to no team. If this
	// ever stops holding, the assertion below is vacuous -- it would be
	// measuring some other path that hands this PR a team.
	control := &fakeGitHubWorkItemDerivationContextSource{facts: crossProviderDonorFacts(claim)}
	controlContext, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim, crossProviderRows(claim), control, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if teamID, _, candidates := controlContext.resolve(crossProviderSubject(claim)); teamID != nil {
		t.Fatalf("fixture no longer reproduces the defect: control resolved team=%v candidates=%+v",
			*teamID, candidates)
	}

	derivationContext, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim, crossProviderRows(claim), source, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	teamID, teamName, candidates := derivationContext.resolve(crossProviderSubject(claim))
	if teamID == nil || *teamID != crossProviderTeamID ||
		teamName == nil || *teamName != crossProviderTeamName {
		t.Fatalf("cross-provider donor did not resolve: team=%v name=%v candidates=%+v",
			teamID, teamName, candidates)
	}
	primary := ""
	for _, candidate := range candidates {
		if candidate.IsPrimary == 1 {
			primary = candidate.Source
			break
		}
	}
	if primary != "linked_issue" {
		t.Fatalf("primary source=%q want linked_issue (the rescue must be provenanced as inheritance)", primary)
	}

	// ANTI-VACUITY: the fixture must genuinely cross providers, and the donor
	// must be reachable ONLY through the stored edge. A same-provider donor
	// would pass every assertion above while proving nothing about CHAOS-3978.
	donor := source.facts.DonorItems[0]
	if donor.Provider == "github" || donor.Provider != "linear" {
		t.Fatalf("donor provider=%q; the fixture does not cross providers", donor.Provider)
	}
	if !strings.HasPrefix(crossProviderPR, "ghpr:") || !strings.HasPrefix(crossProviderDonor, "linear:") {
		t.Fatal("fixture ids no longer express a GitHub subject with a Linear donor")
	}
	if source.storedEdges[0].RelationshipTypeRaw != crossProviderRawKind {
		t.Fatalf("stored edge provenance=%q; the prod population is %q",
			source.storedEdges[0].RelationshipTypeRaw, crossProviderRawKind)
	}
}

// TestCrossProviderStoredEdgeReachesTheDonorLoad pins the ORDER the fix
// depends on: donor targets are computed AFTER the stored edges are merged.
// Merging afterwards would leave the donor item unloaded, and the rescue would
// silently do nothing -- the exact failure this ticket is about, one layer in.
func TestCrossProviderStoredEdgeReachesTheDonorLoad(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim := crossProviderClaim()
	source := &fakeGitHubWorkItemDerivationContextSource{
		facts:       crossProviderDonorFacts(claim),
		storedEdges: []githubWorkItemDependencyRow{crossProviderStoredEdge(claim, now)},
	}
	if _, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim, crossProviderRows(claim), source, now,
	); err != nil {
		t.Fatal(err)
	}
	if want := []string{crossProviderDonor}; !reflect.DeepEqual(source.request.DonorWorkItemIDs, want) {
		t.Fatalf("donor ids = %#v, want %#v (stored edge did not reach the donor load)",
			source.request.DonorWorkItemIDs, want)
	}
	if source.storedEdgeCalls != 1 {
		t.Fatalf("stored-edge loads=%d want 1", source.storedEdgeCalls)
	}
	if want := []string{crossProviderPR}; !reflect.DeepEqual(source.storedEdgeSubjectIDs, want) {
		t.Fatalf("stored-edge lookup ids=%#v want %#v (the read must be keyed on THIS run's items)",
			source.storedEdgeSubjectIDs, want)
	}
	if source.storedEdgeClaim.OrgID != claim.OrgID {
		t.Fatalf("stored-edge claim org=%q want %q", source.storedEdgeClaim.OrgID, claim.OrgID)
	}
}

// TestStoredEdgePruningKeyMatchesPython pins the removal proof to
// (source_work_item_id, relationship_type_raw) -- byte for byte the key
// _merge_stored_inheritable_edges uses in metrics/job_work_items.py.
//
// Both runtimes write work_item_team_attributions for the same items, so a
// DIVERGENCE in this key would undo CHAOS-4112 from whichever side drifted:
// a coarser key (per item) deletes stored edges whose extractor never ran, a
// finer one (per full edge identity) never prunes at all. Each arm below fails
// under exactly one of those drifts.
func TestStoredEdgePruningKeyMatchesPython(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim := crossProviderClaim()
	stored := crossProviderStoredEdge(claim, now.AddDate(0, 0, -40))

	t.Run("same provenance re-emitted this run prunes the stored edge", func(t *testing.T) {
		// The SAME extractor ran for this item and did not re-emit this link:
		// positive evidence the provider removed it.
		fresh := []githubWorkItemDependencyRow{{
			SourceWorkItemID: crossProviderPR, TargetWorkItemID: "linear:CHAOS-9999",
			RelationshipType: "relates_to", RelationshipTypeRaw: crossProviderRawKind,
			LastSynced: now, OrgID: claim.OrgID,
		}}
		merged, storedOnly, added := mergeStoredInheritableWorkItemEdges(
			fresh, []githubWorkItemDependencyRow{stored},
		)
		if added != 0 || len(storedOnly) != 0 || len(merged) != len(fresh) {
			t.Fatalf("stored edge survived its own extractor's re-run: added=%d merged=%d", added, len(merged))
		}
	})

	t.Run("different provenance for the same item keeps the stored edge", func(t *testing.T) {
		// A DIFFERENT extractor ran (PR-body parsing), which says nothing about
		// whether the Linear-attachment producer did. Pruning here would decay
		// exactly the population this ticket protects, which is why the proof
		// cannot be per item.
		fresh := []githubWorkItemDependencyRow{{
			SourceWorkItemID: crossProviderPR, TargetWorkItemID: "extkey:CHAOS-1",
			RelationshipType: "external_issue_key", RelationshipTypeRaw: "external_issue_key",
			LastSynced: now, OrgID: claim.OrgID,
		}}
		_, storedOnly, added := mergeStoredInheritableWorkItemEdges(
			fresh, []githubWorkItemDependencyRow{stored},
		)
		if added != 1 || len(storedOnly) != 1 {
			t.Fatalf("stored edge was pruned by an unrelated extractor: added=%d", added)
		}
	})

	t.Run("same provenance on a DIFFERENT item does not prune", func(t *testing.T) {
		// The proof is per (item, provenance): another PR's linear_attachment
		// edge is no evidence about this PR's links.
		fresh := []githubWorkItemDependencyRow{{
			SourceWorkItemID: "ghpr:full-chaos/dev-health-ops#1795",
			TargetWorkItemID: "linear:CHAOS-1", RelationshipType: "relates_to",
			RelationshipTypeRaw: crossProviderRawKind, LastSynced: now, OrgID: claim.OrgID,
		}}
		_, _, added := mergeStoredInheritableWorkItemEdges(
			fresh, []githubWorkItemDependencyRow{stored},
		)
		if added != 1 {
			t.Fatalf("another item's provenance pruned this item's stored edge: added=%d", added)
		}
	})

	t.Run("fresh edge is authoritative for its own identity", func(t *testing.T) {
		fresh := []githubWorkItemDependencyRow{{
			SourceWorkItemID: crossProviderPR, TargetWorkItemID: crossProviderDonor,
			RelationshipType: "relates_to", RelationshipTypeRaw: "github_comment_linear_url",
			LastSynced: now, OrgID: claim.OrgID,
		}}
		merged, storedOnly, added := mergeStoredInheritableWorkItemEdges(
			fresh, []githubWorkItemDependencyRow{stored},
		)
		if added != 0 || len(merged) != 1 || len(storedOnly) != 0 {
			t.Fatalf("stored row duplicated an identical fresh edge: merged=%d added=%d", len(merged), added)
		}
		if !merged[0].LastSynced.Equal(now) {
			t.Fatalf("merged edge last_synced=%s want the FRESH row's %s", merged[0].LastSynced, now)
		}
	})

	t.Run("non-inheritable stored relationship never enters the merge", func(t *testing.T) {
		blocking := stored
		blocking.RelationshipType = "blocked_by"
		_, _, added := mergeStoredInheritableWorkItemEdges(nil, []githubWorkItemDependencyRow{blocking})
		if added != 0 {
			t.Fatalf("a blocking stored edge was merged: added=%d", added)
		}
	})
}

// TestStoredEdgeRetypeIsStillSettledByRecency proves the union did not break
// the case that motivated the fresh-only rule: a relationship retyped
// `relates_to` -> `blocked_by` arrives fresh with a newer timestamp, and the
// latest-edge collapse must let the NEW type win, killing the inheritance.
func TestStoredEdgeRetypeIsStillSettledByRecency(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim := crossProviderClaim()
	retyped := githubWorkItemDependencyRow{
		SourceWorkItemID: crossProviderPR, TargetWorkItemID: crossProviderDonor,
		RelationshipType: "blocked_by", RelationshipTypeRaw: "linear_relation:blocks",
		LastSynced: now, OrgID: claim.OrgID,
	}
	source := &fakeGitHubWorkItemDerivationContextSource{
		facts:       crossProviderDonorFacts(claim),
		storedEdges: []githubWorkItemDependencyRow{crossProviderStoredEdge(claim, now.AddDate(0, 0, -40))},
	}
	derivationContext, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim, crossProviderRows(claim, retyped), source, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if teamID, _, _ := derivationContext.resolve(crossProviderSubject(claim)); teamID != nil {
		t.Fatalf("a retyped blocking relationship still donated a team: %v", *teamID)
	}
	if derivationContext.storedEdgeMerge.DonorRescues != 0 {
		t.Fatalf("rescue counted for an edge that did not donate: %+v", derivationContext.storedEdgeMerge)
	}
}

// TestStoredEdgeLoadFailureFailsClosed pins D17 on the new read.
//
// Degrading here would re-stamp `unassigned` over correct attributions with
// nothing in the row saying the run was blind -- the silent-degradation shape
// catalogued on the Python side as CHAOS-4150. A failed unit is loud,
// retryable and self-healing instead.
func TestStoredEdgeLoadFailureFailsClosed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim := crossProviderClaim()
	wantErr := errors.New("clickhouse unavailable")
	source := &fakeGitHubWorkItemDerivationContextSource{
		facts: crossProviderDonorFacts(claim), storedEdgeErr: wantErr,
	}
	_, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim, crossProviderRows(claim), source, now,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if source.called {
		t.Fatal("the run continued into the fact load after a failed stored-edge read")
	}
}

// TestStoredEdgeForeignTenantIsRejected keeps the new read on the same tenant
// rail as every other input to the derivation.
func TestStoredEdgeForeignTenantIsRejected(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim := crossProviderClaim()
	foreign := crossProviderStoredEdge(claim, now)
	foreign.OrgID = "org-other"
	source := &fakeGitHubWorkItemDerivationContextSource{
		facts: crossProviderDonorFacts(claim), storedEdges: []githubWorkItemDependencyRow{foreign},
	}
	_, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim, crossProviderRows(claim), source, now,
	)
	if !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("error = %v, want ErrInvalidScope", err)
	}
}

// TestCrossProviderRescueIsObservable pins the telemetry the deploy is judged
// on: the counts must describe the SAME attributions the resolver produced,
// and must separate "same-provider stale edge" from the cross-provider
// population this ticket exists for.
func TestCrossProviderRescueIsObservable(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim := crossProviderClaim()

	sameProviderDonor := "ghpr:full-chaos/dev-health-ops#1700"
	sameProviderSubject := "ghpr:full-chaos/dev-health-ops#1801"
	repoID := "33333333-3333-4333-8333-333333333333"
	facts := crossProviderDonorFacts(claim)
	facts.Repos = []githubWorkItemDerivationRepoFact{{
		Provider: "github", TeamID: "gh-team", TeamName: "GitHub Team",
		RepoID: &repoID, RepoFullName: "full-chaos/dev-health-ops", IsPrimary: 1,
	}}
	facts.DonorItems = append(facts.DonorItems, githubWorkItemDerivationSubject{
		WorkItemID: sameProviderDonor, Provider: "github", RepoID: &repoID, OrgID: claim.OrgID,
	})

	source := &fakeGitHubWorkItemDerivationContextSource{
		facts: facts,
		storedEdges: []githubWorkItemDependencyRow{
			crossProviderStoredEdge(claim, now.AddDate(0, 0, -40)),
			{
				SourceWorkItemID: sameProviderSubject, TargetWorkItemID: sameProviderDonor,
				RelationshipType: "relates_to", RelationshipTypeRaw: "github_pr_body",
				LastSynced: now.AddDate(0, 0, -40), OrgID: claim.OrgID,
			},
		},
	}
	rows := crossProviderRows(claim)
	rows.WorkItems = append(rows.WorkItems, crossProviderWorkItem(claim, sameProviderSubject))

	derivationContext, err := loadGitHubWorkItemDerivationContext(
		context.Background(), claim, rows, source, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := githubWorkItemStoredEdgeMergeObservation{
		StoredEdgesMerged: 2, DonorRescues: 2, CrossProviderRescues: 1,
	}
	if derivationContext.storedEdgeMerge != want {
		t.Fatalf("observation = %+v, want %+v", derivationContext.storedEdgeMerge, want)
	}
	// The cross-provider count is the CHAOS-3978 population specifically: it
	// must not simply mirror the total, or a same-provider decay fix would be
	// misreported as cross-provider recovery.
	if want.CrossProviderRescues == want.DonorRescues {
		t.Fatal("fixture cannot distinguish cross-provider rescues from same-provider ones")
	}
}

// TestDeriverReportsStoredEdgeObservationToTheRoute closes the path from the
// derivation to the unit payload: providersync has no logger and no metrics
// registry, so this observation IS the operator-visible surface.
func TestDeriverReportsStoredEdgeObservationToTheRoute(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim := crossProviderClaim()
	since := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	source := &fakeGitHubWorkItemDerivationContextSource{
		facts:       crossProviderDonorFacts(claim),
		storedEdges: []githubWorkItemDependencyRow{crossProviderStoredEdge(claim, now.AddDate(0, 0, -40))},
	}
	deriver := &GitHubWorkItemDeriver{
		Source: source, engine: githubWorkItemStubEngine{},
		observations: newWorkItemDerivationObservations(),
	}
	if _, err := deriver.Derive(
		context.Background(), claim, crossProviderRows(claim), now,
	); err != nil {
		t.Fatal(err)
	}
	observed := deriver.StoredEdgeMergeObservation()
	if observed.CrossProviderRescues != 1 || observed.StoredEdgesMerged != 1 {
		t.Fatalf("deriver observation = %+v, want one cross-provider rescue", observed)
	}

	result := attachWorkItemTeamInheritanceObservation(map[string]any{
		"observations": map[string]any{"provider_usage": "kept"},
	}, deriver)
	observations, ok := result["observations"].(map[string]any)
	if !ok {
		t.Fatalf("result observations = %#v", result["observations"])
	}
	if observations["provider_usage"] != "kept" {
		t.Fatal("attaching the inheritance observation dropped an existing observation")
	}
	if observations[workItemTeamInheritanceResultKey] != observed {
		t.Fatalf("result observation = %#v, want %+v",
			observations[workItemTeamInheritanceResultKey], observed)
	}
}

// TestStoredInheritableEdgeQueryIsBoundedAndRetried covers the ClickHouse
// implementation itself: the read must stay a keyed lookup (never a history
// scan), and one transient blip must not be allowed to become a silent
// downgrade of the whole unit's attribution.
func TestStoredInheritableEdgeQueryIsBoundedAndRetried(t *testing.T) {
	t.Parallel()
	claim := crossProviderClaim()

	t.Run("query is keyed and bounded", func(t *testing.T) {
		conn := &recordingGitHubWorkItemDerivationConn{}
		source := githubWorkItemClickHouseDerivationContextSource{
			Conn: conn, Lease: stubDerivationLease{},
		}
		if _, err := source.LoadStoredInheritableEdges(
			context.Background(), claim, []string{crossProviderPR},
		); err != nil {
			t.Fatal(err)
		}
		if len(conn.queries) != 1 {
			t.Fatalf("queries=%d want 1", len(conn.queries))
		}
		query := conn.queries[0]
		for _, fragment := range []string{
			"FROM work_item_dependencies FINAL",
			"org_id = ?",
			"has(?, source_work_item_id)",
			"has(?, relationship_type)",
			"LIMIT ?",
		} {
			if !strings.Contains(query, fragment) {
				t.Fatalf("query missing %q:\n%s", fragment, query)
			}
		}
		args := conn.args[0]
		if len(args) != 4 || args[0] != claim.OrgID {
			t.Fatalf("args=%#v want org, subjects, relationship types, limit", args)
		}
		subjects, ok := args[1].([]string)
		if !ok || !reflect.DeepEqual(subjects, []string{crossProviderPR}) {
			t.Fatalf("subject arg=%#v want %#v", args[1], []string{crossProviderPR})
		}
		relationshipTypes, ok := args[2].([]string)
		if !ok || !reflect.DeepEqual(
			relationshipTypes,
			[]string{"duplicates", "external_issue_key", "relates", "relates_to"},
		) {
			t.Fatalf("relationship types=%#v want the inheritable set, sorted", args[2])
		}
	})

	t.Run("no subjects means no query at all", func(t *testing.T) {
		conn := &recordingGitHubWorkItemDerivationConn{}
		source := githubWorkItemClickHouseDerivationContextSource{
			Conn: conn, Lease: stubDerivationLease{},
		}
		edges, err := source.LoadStoredInheritableEdges(context.Background(), claim, nil)
		if err != nil || len(edges) != 0 {
			t.Fatalf("edges=%#v err=%v", edges, err)
		}
		if len(conn.queries) != 0 {
			t.Fatalf("an empty run still queried: %#v", conn.queries)
		}
	})

	t.Run("one transient failure is retried, two fail the unit", func(t *testing.T) {
		blip := errors.New("clickhouse blip")
		once := &flakyGitHubWorkItemDerivationConn{failures: 1, err: blip}
		source := githubWorkItemClickHouseDerivationContextSource{
			Conn: once, Lease: stubDerivationLease{},
		}
		if _, err := source.LoadStoredInheritableEdges(
			context.Background(), claim, []string{crossProviderPR},
		); err != nil {
			t.Fatalf("a single blip was not retried: %v", err)
		}
		if once.calls != 2 {
			t.Fatalf("calls=%d want 2 (one failure, one retry)", once.calls)
		}

		always := &flakyGitHubWorkItemDerivationConn{failures: 5, err: blip}
		source.Conn = always
		if _, err := source.LoadStoredInheritableEdges(
			context.Background(), claim, []string{crossProviderPR},
		); !errors.Is(err, blip) {
			t.Fatalf("error=%v want the underlying failure (fail closed, D17)", err)
		}
		if always.calls != 2 {
			t.Fatalf("calls=%d want exactly 2 attempts", always.calls)
		}
	})
}

type flakyGitHubWorkItemDerivationConn struct {
	driver.Conn
	failures int
	err      error
	calls    int
}

func (conn *flakyGitHubWorkItemDerivationConn) Query(
	_ context.Context, _ string, _ ...any,
) (driver.Rows, error) {
	conn.calls++
	if conn.failures > 0 {
		conn.failures--
		return nil, conn.err
	}
	return emptyGitHubWorkItemDerivationRows{}, nil
}

type stubDerivationLease struct{}

func (stubDerivationLease) Assert(context.Context) error { return nil }
