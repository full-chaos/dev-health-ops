package providersync

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// snapshotAbandonedCounts reads the shared abandoned-close counter, keyed
// "provider/kind/reason".
func snapshotAbandonedCounts(t *testing.T) map[string]int64 {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := meterReader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != snapshotCloseAbandonedName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				provider, _ := point.Attributes.Value("provider")
				kind, _ := point.Attributes.Value("kind")
				reason, _ := point.Attributes.Value("reason")
				out[provider.AsString()+"/"+kind.AsString()+"/"+reason.AsString()] += point.Value
			}
		}
	}
	return out
}

// snapshotAbandonedMoved is what the shared counter gained since before.
func snapshotAbandonedMoved(t *testing.T, before map[string]int64) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for key, n := range snapshotAbandonedCounts(t) {
		if d := n - before[key]; d != 0 {
			out[key] = d
		}
	}
	return out
}

// kindCase is one fact kind of one provider with rows to drive the rule: own
// is a row of the kind, foreign is a row the same writer's table can hold that
// is NOT of the kind (another kind of the writer, another source, a team the
// kind leaves out).
type kindCase[R any] struct {
	provider string
	kind     SnapshotKind[R]
	empty    EmptyAnswer
	own      func(name string, validFrom time.Time) R
	foreign  func(name string, validFrom time.Time) R
	key      func(R) string
	stamp    func(R) time.Time
}

// runKindCase drives the one rule for one kind, one clause at a time. Each
// refusal has the control next to it, so a refusal is the rule's and not a
// harness that cannot close.
func runKindCase[R any](t *testing.T, c kindCase[R]) {
	t.Helper()
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	before := at.Add(-24 * time.Hour)
	lost, held := c.own("lost", before), c.own("held", before)
	foreignOpen := c.foreign("foreign-open", before)
	open := []R{lost, held, foreignOpen}
	freshHeld, freshForeign := c.own("held", at), c.foreign("foreign-fresh", at)
	closed := func(plan SnapshotPlan) []string {
		out := []string{}
		for _, retraction := range plan.Retract {
			out = append(out, c.key(open[retraction.Open]))
		}
		return out
	}
	planScoped := func(fresh []R, scope ScopeProof, proof SnapshotProof) SnapshotPlan {
		return PlanSnapshot(fresh, open, c.key, c.stamp, at, c.kind.Snapshot(scope, proof))
	}
	plan := func(fresh []R, proof SnapshotProof) SnapshotPlan { return planScoped(fresh, testSoleScope(), proof) }
	name := c.provider + "/" + c.kind.Name()

	t.Run(name+"/control: a proven answer that holds a row of the kind closes the row it lost", func(t *testing.T) {
		got := plan([]R{freshHeld, freshForeign}, testProof(true))
		if !reflect.DeepEqual(closed(got), []string{c.key(lost)}) {
			t.Fatalf("closed %v, want only the lost row of the kind: a row of no kind never closes", closed(got))
		}
		if got.ValidFrom[0] != before {
			t.Fatalf("the held fact moved to %v, want its first-seen %v", got.ValidFrom[0], before)
		}
		if got.OpenOfNoKind != 1 || len(got.Abandoned()) != 0 {
			t.Fatalf("open of no kind = %d, abandoned = %+v", got.OpenOfNoKind, got.Abandoned())
		}
	})
	t.Run(name+"/a walk whose end is not proven closes nothing", func(t *testing.T) {
		got := plan([]R{freshHeld, freshForeign}, testProof(false))
		if len(got.Retract) != 0 || !reflect.DeepEqual(got.SnapshotReasons(), []string{"test_walk_not_read_to_the_end"}) {
			t.Fatalf("closed %v reasons %v", closed(got), got.SnapshotReasons())
		}
	})
	t.Run(name+"/a proof nobody stated closes nothing", func(t *testing.T) {
		got := plan([]R{freshHeld}, SnapshotProof{})
		if len(got.Retract) != 0 || !reflect.DeepEqual(got.SnapshotReasons(), []string{snapshotProofNotStated}) {
			t.Fatalf("closed %v reasons %v", closed(got), got.SnapshotReasons())
		}
	})
	t.Run(name+"/another active integration of the provider in the organization closes nothing", func(t *testing.T) {
		counted := snapshotAbandonedCounts(t)
		got := planScoped([]R{freshHeld, freshForeign}, testSharedScope(), testProof(true))
		if len(got.Retract) != 0 || !reflect.DeepEqual(got.SnapshotReasons(), []string{OwnershipCloseSkippedScopeShared}) {
			t.Fatalf("a run that is not the only owner of the rows closed %v (reasons %v)", closed(got), got.SnapshotReasons())
		}
		if got.ValidFrom[0] != before {
			t.Fatalf("the held fact moved to %v, want its first-seen %v: a shared scope still writes what the run found", got.ValidFrom[0], before)
		}
		if !ReportSnapshotPlan(context.Background(), c.provider, "org-1", got) {
			t.Fatal("a shared scope was not reported")
		}
		want := map[string]int64{c.provider + "/" + c.kind.Name() + "/" + OwnershipCloseSkippedScopeShared: 1}
		if moved := snapshotAbandonedMoved(t, counted); !reflect.DeepEqual(moved, want) {
			t.Fatalf("%s moved %v, want %v", snapshotCloseAbandonedName, moved, want)
		}
	})
	t.Run(name+"/a scope nobody proved closes nothing", func(t *testing.T) {
		got := planScoped([]R{freshHeld, freshForeign}, ScopeProof{}, testProof(true))
		if len(got.Retract) != 0 || !reflect.DeepEqual(got.SnapshotReasons(), []string{ScopeNotProven}) {
			t.Fatalf("closed %v reasons %v", closed(got), got.SnapshotReasons())
		}
	})
	t.Run(name+"/an answer with no row of the kind, next to a row of another kind", func(t *testing.T) {
		got := plan([]R{freshForeign}, testProof(true))
		if c.empty == EmptyIsAnAnswer {
			if !reflect.DeepEqual(closed(got), []string{c.key(lost), c.key(held)}) || len(got.Abandoned()) != 0 {
				t.Fatalf("this kind's empty answer is an answer: closed %v, want both rows of the kind and no other", closed(got))
			}
			return
		}
		if len(got.Retract) != 0 {
			t.Fatalf("a row of another kind made the kind not empty: closed %v", closed(got))
		}
		if !reflect.DeepEqual(got.SnapshotReasons(), []string{SnapshotEmptyAnswer}) || got.Kinds[0].Fresh != 0 || got.Kinds[0].Open != 2 {
			t.Fatalf("outcome = %+v, want %s with 0 fresh and 2 open rows kept", got.Kinds[0], SnapshotEmptyAnswer)
		}
	})
	t.Run(name+"/the abandoned close is counted with the kind and the reason", func(t *testing.T) {
		counted := snapshotAbandonedCounts(t)
		if !ReportSnapshotPlan(context.Background(), c.provider, "org-1", plan([]R{freshHeld}, testProof(false))) {
			t.Fatal("an unproven kind was not reported")
		}
		want := map[string]int64{c.provider + "/" + c.kind.Name() + "/test_walk_not_read_to_the_end": 1}
		if got := snapshotAbandonedMoved(t, counted); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s moved %v, want %v", snapshotCloseAbandonedName, got, want)
		}
		counted = snapshotAbandonedCounts(t)
		if ReportSnapshotPlan(context.Background(), c.provider, "org-1", plan([]R{freshHeld}, testProof(true))) {
			t.Fatal("a kind that closed was reported as abandoned")
		}
		if got := snapshotAbandonedMoved(t, counted); len(got) != 0 {
			t.Fatalf("a close that ran moved %s: %v", snapshotCloseAbandonedName, got)
		}
	})
}

// TestEveryFactKindClosesOnlyOnItsOwnProofAndItsOwnRows is the provider x fact
// kind matrix of the snapshot rule: every kind of snapshotKindCensus, through
// the one rule, with the same clauses. Not parallel: it reads a process-wide
// counter.
func TestEveryFactKindClosesOnlyOnItsOwnProofAndItsOwnRows(t *testing.T) {
	const org = "org-1"
	ownership := func(team, source string, id func(name string) ProjectID) func(string, time.Time) OwnershipSnapshotRow {
		return func(name string, validFrom time.Time) OwnershipSnapshotRow {
			return OwnershipSnapshotRow{TeamID: team, Source: source, ProjectID: id(name), ValidFrom: validFrom}
		}
	}
	bare := func(name string) ProjectID { return testPID(name) }
	teamKey := func(name string) ProjectID { return mustProjectID(t)(LinearTeamKeyProjectID(org, name)) }
	stamp := func(row OwnershipSnapshotRow) time.Time { return row.ValidFrom }
	covered := map[string]bool{}
	for _, c := range []kindCase[OwnershipSnapshotRow]{
		{provider: "linear", kind: LinearProjectOwnershipKind(org), empty: EmptyClosesNothing,
			own: ownership("linear:QA", "native", bare), foreign: ownership("linear:QA", "native", teamKey)},
		{provider: "linear", kind: LinearTeamKeyOwnershipKind(org), empty: EmptyClosesNothing,
			own: ownership("linear:QA", "native", teamKey), foreign: ownership("linear:QA", "native", bare)},
		{provider: "jira", kind: JiraLegacyOwnershipKind(), empty: EmptyClosesNothing,
			own: ownership("jira:ops", jiraTeamCatalogLegacySource, bare), foreign: ownership("jira:ops", "native", bare)},
		{provider: "gitlab", kind: GitLabGroupProjectGrantKind([]string{"gitlab:a"}), empty: EmptyIsAnAnswer,
			own: ownership("gitlab:a", gitlabTeamCatalogSource, bare), foreign: ownership("gitlab:b", gitlabTeamCatalogSource, bare)},
		{provider: "github", kind: GitHubTeamRepoGrantKind([]string{"github:a"}), empty: EmptyIsAnAnswer,
			own: ownership("github:a", githubTeamCatalogSource, bare), foreign: ownership("github:b", githubTeamCatalogSource, bare)},
		{provider: "jira", kind: AtlassianTeamLinkKind("native", []string{"jira:unreadable"}), empty: EmptyIsAnAnswer,
			own: ownership("jira:a", "native", bare), foreign: ownership("jira:unreadable", "native", bare)},
	} {
		c.key, c.stamp = ownershipSnapshotKey, stamp
		covered[c.kind.Name()] = true
		runKindCase(t, c)
	}
	// The two kinds below have no row of another kind in their writer's read:
	// foreign is nil for them, and the kind holds every row it is given.
	memberships := kindCase[MembershipSnapshotRow]{provider: "jira", kind: AtlassianTeamMembershipKind(), empty: EmptyIsAnAnswer,
		own: func(name string, validFrom time.Time) MembershipSnapshotRow {
			return MembershipSnapshotRow{TeamID: "jira:a", MemberID: "jira:" + name, ValidFrom: validFrom}
		},
		key: MembershipSnapshotKey, stamp: func(row MembershipSnapshotRow) time.Time { return row.ValidFrom }}
	covered[memberships.kind.Name()] = true
	runSingleKindCase(t, memberships)
	// The three catalog membership kinds hold the memberships of the teams whose
	// own member read proved its end; a membership of another team is of no kind.
	membershipOf := func(team string) func(name string, validFrom time.Time) MembershipSnapshotRow {
		return func(name string, validFrom time.Time) MembershipSnapshotRow {
			return MembershipSnapshotRow{TeamID: team, MemberID: name, ValidFrom: validFrom}
		}
	}
	for _, c := range []kindCase[MembershipSnapshotRow]{
		{provider: "linear", kind: LinearTeamMembershipKind([]string{"linear:a"}), empty: EmptyIsAnAnswer,
			own: membershipOf("linear:a"), foreign: membershipOf("linear:b")},
		{provider: "github", kind: GitHubTeamMembershipKind([]string{"github:a"}), empty: EmptyIsAnAnswer,
			own: membershipOf("github:a"), foreign: membershipOf("github:b")},
		{provider: "gitlab", kind: GitLabTeamMembershipKind([]string{"gitlab:a"}), empty: EmptyIsAnAnswer,
			own: membershipOf("gitlab:a"), foreign: membershipOf("gitlab:b")},
	} {
		c.key, c.stamp = MembershipSnapshotKey, func(row MembershipSnapshotRow) time.Time { return row.ValidFrom }
		covered[c.kind.Name()] = true
		runKindCase(t, c)
	}
	teams := kindCase[TeamSnapshotRow]{provider: "jira", kind: AtlassianTeamCatalogKind(), empty: EmptyClosesNothing,
		own: func(name string, _ time.Time) TeamSnapshotRow { return TeamSnapshotRow{TeamID: "jira:" + name} },
		key: TeamSnapshotKey, stamp: func(TeamSnapshotRow) time.Time { return time.Time{} }}
	covered[teams.kind.Name()] = true
	runSingleKindCase(t, teams)

	for name := range snapshotKindCensus {
		if !covered[name] {
			t.Errorf("the fact kind %q is in the census and not in this matrix: every kind is driven through the rule", name)
		}
	}
	if len(covered) != len(snapshotKindCensus) {
		t.Errorf("the matrix covers %d kinds and the census names %d", len(covered), len(snapshotKindCensus))
	}
}

// runSingleKindCase is runKindCase for a kind that holds every row of its
// writer's read: there is no row of another kind to put next to it.
func runSingleKindCase[R any](t *testing.T, c kindCase[R]) {
	t.Helper()
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	before := at.Add(-24 * time.Hour)
	lost, held := c.own("lost", before), c.own("held", before)
	open := []R{lost, held}
	closed := func(plan SnapshotPlan) []string {
		out := []string{}
		for _, retraction := range plan.Retract {
			out = append(out, c.key(open[retraction.Open]))
		}
		return out
	}
	planScoped := func(fresh []R, scope ScopeProof, proof SnapshotProof) SnapshotPlan {
		return PlanSnapshot(fresh, open, c.key, c.stamp, at, c.kind.Snapshot(scope, proof))
	}
	plan := func(fresh []R, proof SnapshotProof) SnapshotPlan { return planScoped(fresh, testSoleScope(), proof) }
	name := c.provider + "/" + c.kind.Name()
	t.Run(name+"/control: a proven answer that holds a row of the kind closes the row it lost", func(t *testing.T) {
		if got := plan([]R{c.own("held", at)}, testProof(true)); !reflect.DeepEqual(closed(got), []string{c.key(lost)}) {
			t.Fatalf("closed %v, want only the lost row", closed(got))
		}
	})
	t.Run(name+"/a walk whose end is not proven closes nothing", func(t *testing.T) {
		got := plan([]R{c.own("held", at)}, testProof(false))
		if len(got.Retract) != 0 || !reflect.DeepEqual(got.SnapshotReasons(), []string{"test_walk_not_read_to_the_end"}) {
			t.Fatalf("closed %v reasons %v", closed(got), got.SnapshotReasons())
		}
	})
	t.Run(name+"/a proof nobody stated closes nothing", func(t *testing.T) {
		if got := plan([]R{c.own("held", at)}, SnapshotProof{}); len(got.Retract) != 0 {
			t.Fatalf("closed %v", closed(got))
		}
	})
	t.Run(name+"/another active integration of the provider in the organization closes nothing", func(t *testing.T) {
		counted := snapshotAbandonedCounts(t)
		got := planScoped([]R{c.own("held", at)}, testSharedScope(), testProof(true))
		if len(got.Retract) != 0 || !reflect.DeepEqual(got.SnapshotReasons(), []string{OwnershipCloseSkippedScopeShared}) {
			t.Fatalf("a run that is not the only owner of the rows closed %v (reasons %v)", closed(got), got.SnapshotReasons())
		}
		ReportSnapshotPlan(context.Background(), c.provider, "org-1", got)
		want := map[string]int64{c.provider + "/" + c.kind.Name() + "/" + OwnershipCloseSkippedScopeShared: 1}
		if moved := snapshotAbandonedMoved(t, counted); !reflect.DeepEqual(moved, want) {
			t.Fatalf("%s moved %v, want %v", snapshotCloseAbandonedName, moved, want)
		}
	})
	t.Run(name+"/a scope nobody proved closes nothing", func(t *testing.T) {
		got := planScoped([]R{c.own("held", at)}, ScopeProof{}, testProof(true))
		if len(got.Retract) != 0 || !reflect.DeepEqual(got.SnapshotReasons(), []string{ScopeNotProven}) {
			t.Fatalf("closed %v reasons %v", closed(got), got.SnapshotReasons())
		}
	})
	t.Run(name+"/an answer with no row at all", func(t *testing.T) {
		got := plan(nil, testProof(true))
		if c.empty == EmptyIsAnAnswer {
			if len(got.Retract) != 2 {
				t.Fatalf("this kind's empty answer is an answer: closed %v, want both rows", closed(got))
			}
			return
		}
		if len(got.Retract) != 0 || !reflect.DeepEqual(got.SnapshotReasons(), []string{SnapshotEmptyAnswer}) {
			t.Fatalf("an empty answer closed %v (reasons %v)", closed(got), got.SnapshotReasons())
		}
	})
	t.Run(name+"/the abandoned close is counted with the kind and the reason", func(t *testing.T) {
		counted := snapshotAbandonedCounts(t)
		ReportSnapshotPlan(context.Background(), c.provider, "org-1", plan([]R{c.own("held", at)}, testProof(false)))
		want := map[string]int64{c.provider + "/" + c.kind.Name() + "/test_walk_not_read_to_the_end": 1}
		if got := snapshotAbandonedMoved(t, counted); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s moved %v, want %v", snapshotCloseAbandonedName, got, want)
		}
	})
}

// An open row that two kinds hold is never closed: which walk proves it is
// not decided, so neither does.
func TestPlanSnapshotNeverClosesARowTwoKindsHold(t *testing.T) {
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	open := []OwnershipSnapshotRow{{TeamID: "T", ProjectID: testPID("p"), Source: "native", ValidFrom: at.Add(-time.Hour)}}
	fresh := []OwnershipSnapshotRow{{TeamID: "T", ProjectID: testPID("q"), Source: "native", ValidFrom: at}}
	one := testEveryRowKind(EmptyIsAnAnswer).Snapshot(testSoleScope(), testProof(true))
	if plan := PlanOwnershipSnapshot(fresh, open, at, one); len(plan.Retract) != 1 {
		t.Fatalf("control: one kind holds the row and closes it; got %+v", plan.Retract)
	}
	if plan := PlanOwnershipSnapshot(fresh, open, at, one, one); len(plan.Retract) != 0 || plan.OpenOfNoKind != 1 {
		t.Fatalf("two kinds hold the row: retract=%+v openOfNoKind=%d, want it kept", plan.Retract, plan.OpenOfNoKind)
	}
}

// An empty answer over a table that holds no row of the kind kept nothing: it
// is not reported. The same answer over an open row is.
func TestReportSnapshotPlanIsQuietWhenAnEmptyAnswerKeptNothing(t *testing.T) {
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	kind := JiraLegacyOwnershipKind().Snapshot(testSoleScope(), testProof(true))
	open := []OwnershipSnapshotRow{{TeamID: "T", ProjectID: testPID("p"), Source: jiraTeamCatalogLegacySource, ValidFrom: at.Add(-time.Hour)}}
	counted := snapshotAbandonedCounts(t)
	if ReportSnapshotPlan(context.Background(), "jira", "org-1", PlanOwnershipSnapshot(nil, nil, at, kind)) {
		t.Fatal("an empty answer over an empty table was reported")
	}
	if got := snapshotAbandonedMoved(t, counted); len(got) != 0 {
		t.Fatalf("counter moved %v", got)
	}
	if !ReportSnapshotPlan(context.Background(), "jira", "org-1", PlanOwnershipSnapshot(nil, open, at, kind)) {
		t.Fatal("an empty answer that kept an open row was not reported")
	}
	want := map[string]int64{"jira/jira_legacy_ownership/" + SnapshotEmptyAnswer: 1}
	if got := snapshotAbandonedMoved(t, counted); !reflect.DeepEqual(got, want) {
		t.Fatalf("counter moved %v, want %v", got, want)
	}
}

// The abandoned close is one WARN line that names the provider, the fact
// kind, the reasons and what was kept. Not parallel: it swaps the default
// logger.
func TestReportSnapshotPlanWritesOneWarnLinePerAbandonedKind(t *testing.T) {
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	const org = "org-1"
	open := []OwnershipSnapshotRow{
		{TeamID: "linear:QA", ProjectID: testPID("keep"), Source: "native", ValidFrom: at.Add(-time.Hour)},
		{TeamID: "linear:QA", ProjectID: testPID("other-source"), Source: "manual", ValidFrom: at.Add(-time.Hour)},
	}
	fresh := []OwnershipSnapshotRow{{TeamID: "linear:QA", ProjectID: mustProjectID(t)(LinearTeamKeyProjectID(org, "QA")), Source: "native", ValidFrom: at}}
	plan := PlanOwnershipSnapshot(fresh, open, at, linearOwnershipKindSnapshots(org, testSoleScope(), LinearReferenceCatalogEvidence{TeamsComplete: true, ProjectsComplete: true}, LinearReferenceCatalogResult{})...)
	if !ReportSnapshotPlan(context.Background(), "linear", org, plan) {
		t.Fatal("an empty project answer over an open project row was not reported")
	}
	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("log lines = %q, want one line for the one abandoned kind", lines)
	}
	for _, want := range []string{
		"level=WARN", "msg=" + SnapshotCloseAbandonedLog, "org_id=" + org, "provider=linear", "kind=linear_project_ownership",
		"reasons=" + SnapshotEmptyAnswer, "fresh_rows=0", "open_rows_kept=1", "open_rows_of_no_kind=1",
	} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("the line %q does not hold %q", lines[0], want)
		}
	}
}
