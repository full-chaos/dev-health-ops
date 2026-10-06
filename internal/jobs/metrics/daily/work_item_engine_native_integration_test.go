//go:build integration

package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
)

// The two engine families on a real ClickHouse at the migration head, for the
// one thing the differential tests in internal/providersync cannot show: what
// a RECOMPUTE leaves behind in an append-only table.
//
// issue_type_metrics_daily and investment_metrics_daily are plain MergeTree
// and their readers take the newest computed_at per key. A recompute that no
// longer produces a key would leave that key's older row the newest one for
// ever. The families write a row of zeros for it. The case here is the one
// that happens in production: an item is first computed with no team, then
// its repository gets an owner, and the next run puts the item under the
// team's key.

// workItemEngineTestNormalizer and workItemEngineTestClassifier are fixed
// engines: this file is about keys and versions, not about the two config
// files (the providersync matrix runs the real ones).
type workItemEngineTestNormalizer struct{}

func (workItemEngineTestNormalizer) NormalizeType(_, typeRaw string, labels []string) string {
	if len(labels) > 0 {
		return labels[0]
	}
	return typeRaw
}

type workItemEngineTestClassifier struct{}

func (workItemEngineTestClassifier) Classify(
	artifact workitemengine.InvestmentArtifact,
) (workitemengine.InvestmentClassification, error) {
	area, stream, rule := "product", "general", "test_rule"
	if len(artifact.Labels) > 0 {
		area = artifact.Labels[0]
	}
	return workitemengine.InvestmentClassification{
		InvestmentArea: &area, ProjectStream: &stream, Confidence: 1, RuleID: &rule,
	}, nil
}

func seedWorkItemEngineItem(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repoID uuid.UUID,
	workItemID, label string, createdAt time.Time, startedAt, completedAt *time.Time, storyPoints *float64,
) {
	t.Helper()
	status := "in_progress"
	if completedAt != nil {
		status = "done"
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, title, type, status, created_at, started_at, completed_at,
		labels, story_points, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(
		repoID, workItemID, "github", "Item "+workItemID, "issue", status, createdAt, startedAt, completedAt,
		[]string{label}, storyPoints, orgID, createdAt,
	); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
}

type workItemEngineVersions struct {
	rows      uint64
	completed uint64
	units     uint64
}

// readInvestmentMetricsKey returns, for one key of the day, the number of
// stored versions and the values of the NEWEST one.
func readInvestmentMetricsKey(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, teamID, area string,
) workItemEngineVersions {
	t.Helper()
	var result workItemEngineVersions
	if err := conn.QueryRow(ctx, `
SELECT count(), toUInt64(argMax(work_items_completed, computed_at)), toUInt64(argMax(delivery_units, computed_at))
FROM investment_metrics_daily
WHERE org_id = ? AND day = ? AND ifNull(team_id, '') = ? AND investment_area = ?`,
		orgID, day, teamID, area).Scan(&result.rows, &result.completed, &result.units); err != nil {
		t.Fatalf("read investment_metrics_daily key (%q, %q): %v", teamID, area, err)
	}
	return result
}

func readIssueTypeKey(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, teamID, issueType string,
) (versions, completed, active uint64) {
	t.Helper()
	if err := conn.QueryRow(ctx, `
SELECT count(), toUInt64(argMax(completed_count, computed_at)), toUInt64(argMax(active_count, computed_at))
FROM issue_type_metrics_daily
WHERE org_id = ? AND day = ? AND team_id = ? AND issue_type_norm = ?`,
		orgID, day, teamID, issueType).Scan(&versions, &completed, &active); err != nil {
		t.Fatalf("read issue_type_metrics_daily key (%q, %q): %v", teamID, issueType, err)
	}
	return versions, completed, active
}

func TestWorkItemEngineFamiliesWriteZeroForAKeyTheRecomputeNoLongerHolds(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-engine-" + uuid.NewString()
	repoID := uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	timePtr := func(value time.Time) *time.Time { return &value }
	points := 3.0
	// One item completed on the day (3 points), one open item.
	seedWorkItemEngineItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "quality",
		day.AddDate(0, 0, -2), timePtr(day.AddDate(0, 0, -1)), timePtr(day.Add(10*time.Hour)), &points)
	seedWorkItemEngineItem(t, ctx, conn, orgID, repoID, "gh:acme/api#2", "security",
		day.Add(8*time.Hour), nil, nil, nil)

	attribution, err := NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	issueType, err := NewWorkItemIssueTypeExecutor(conn, workItemEngineTestNormalizer{})
	if err != nil {
		t.Fatal(err)
	}
	investment, err := NewWorkItemInvestmentExecutor(conn, workItemEngineTestClassifier{})
	if err != nil {
		t.Fatal(err)
	}
	partition := Partition{ID: "p1", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(repoID.String())}}
	runAt := func(at time.Time) map[string]int {
		t.Helper()
		attribution.nowUTC = func() time.Time { return at }
		issueType.nowUTC = func() time.Time { return at }
		investment.nowUTC = func() time.Time { return at }
		run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
		written := map[string]int{}
		for _, family := range []struct {
			name     string
			executor NativeFamilyExecutor
		}{
			{"work_item_attribution", attribution},
			{WorkItemIssueTypeFamilyName, issueType},
			{WorkItemInvestmentFamilyName, investment},
		} {
			count, err := family.executor.ComputeFamily(ctx, run, partition)
			if err != nil {
				t.Fatalf("%s at %s: %v", family.name, at, err)
			}
			written[family.name] = count
		}
		return written
	}

	// --- run 1: the repository has no owner; the items have no team -------
	first := day.AddDate(0, 0, 1).Add(1 * time.Hour)
	runAt(first)
	if got := readInvestmentMetricsKey(t, ctx, conn, orgID, day, "", "quality"); got != (workItemEngineVersions{1, 1, 3}) {
		t.Fatalf("run 1, investment key (no team, quality) = %+v, want one version with 1 completed and 3 units", got)
	}
	if versions, completed, active := readIssueTypeKey(t, ctx, conn, orgID, day, "unassigned", "quality"); versions != 1 || completed != 1 || active != 1 {
		t.Fatalf("run 1, issue type key (unassigned, quality): versions=%d completed=%d active=%d, want 1, 1, 1", versions, completed, active)
	}

	// --- the repository gets an owner; run 2 ------------------------------
	seedWorkItemAttributionLinkedRepoOwnership(t, ctx, conn, orgID, repoID, "team-a", day.AddDate(0, 0, -30))
	second := first.Add(time.Hour)
	runAt(second)
	if got := readInvestmentMetricsKey(t, ctx, conn, orgID, day, "team-a", "quality"); got != (workItemEngineVersions{1, 1, 3}) {
		t.Fatalf("run 2, investment key (team-a, quality) = %+v, want one version with 1 completed and 3 units: the family reads the team from work_item_team_attributions", got)
	}
	// The state the zero row exists to reach: the completion is under ONE key.
	if got := readInvestmentMetricsKey(t, ctx, conn, orgID, day, "", "quality"); got != (workItemEngineVersions{2, 0, 0}) {
		t.Fatalf("run 2, investment key (no team, quality) = %+v, want two versions whose newest is 0 completed and 0 units: without the zero row the completion is counted under both teams", got)
	}
	var completedAcrossKeys uint64
	if err := conn.QueryRow(ctx, `
SELECT sum(completed) FROM (
  SELECT argMax(work_items_completed, computed_at) AS completed
  FROM investment_metrics_daily WHERE org_id = ? AND day = ?
  GROUP BY day, repo_id, team_id, investment_area, project_stream)`, orgID, day).Scan(&completedAcrossKeys); err != nil {
		t.Fatal(err)
	}
	if completedAcrossKeys != 1 {
		t.Fatalf("completed items of the day, newest per key = %d, want 1", completedAcrossKeys)
	}
	for _, issue := range []string{"quality", "security"} {
		if versions, completed, active := readIssueTypeKey(t, ctx, conn, orgID, day, "unassigned", issue); versions != 2 || completed != 0 || active != 0 {
			t.Fatalf("run 2, issue type key (unassigned, %s): versions=%d completed=%d active=%d, want 2 versions whose newest is all zeros", issue, versions, completed, active)
		}
		if versions, _, active := readIssueTypeKey(t, ctx, conn, orgID, day, "team-a", issue); versions != 1 || active != 1 {
			t.Fatalf("run 2, issue type key (team-a, %s): versions=%d active=%d, want one version with 1 active item", issue, versions, active)
		}
	}

	// --- run 3: nothing changed. A key that is already zero gets no more rows.
	runAt(second.Add(time.Hour))
	if got := readInvestmentMetricsKey(t, ctx, conn, orgID, day, "", "quality"); got != (workItemEngineVersions{2, 0, 0}) {
		t.Fatalf("run 3, investment key (no team, quality) = %+v, want still two versions: a key whose newest row is zero is left alone", got)
	}
	if versions, _, _ := readIssueTypeKey(t, ctx, conn, orgID, day, "unassigned", "quality"); versions != 2 {
		t.Fatalf("run 3, issue type key (unassigned, quality) has %d versions, want still 2", versions)
	}
	if got := readInvestmentMetricsKey(t, ctx, conn, orgID, day, "team-a", "quality"); got != (workItemEngineVersions{2, 1, 3}) {
		t.Fatalf("run 3, investment key (team-a, quality) = %+v, want two versions, newest 1 completed and 3 units", got)
	}

	// --- never fill a day that has no data ---------------------------------
	// A day before the first item exists: the investment family has no active
	// item and no earlier row, so it writes nothing.
	emptyDay := day.AddDate(0, 0, -40)
	written, err := investment.ComputeFamily(ctx,
		Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: emptyDay}, partition)
	if err != nil {
		t.Fatal(err)
	}
	var investmentRows, classificationRows uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM investment_metrics_daily WHERE org_id = ? AND day = ?`,
		orgID, emptyDay).Scan(&investmentRows); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM investment_classifications_daily WHERE org_id = ? AND day = ?`,
		orgID, emptyDay).Scan(&classificationRows); err != nil {
		t.Fatal(err)
	}
	if written != 0 || investmentRows != 0 || classificationRows != 0 {
		t.Fatalf("a day with no active item: the family reported %d rows, investment_metrics_daily holds %d, investment_classifications_daily holds %d, want 0, 0, 0",
			written, investmentRows, classificationRows)
	}
	// A repository with no stored item gets no row in either table.
	otherRepo := uuid.New()
	other := Partition{ID: "p2", RunID: "r1", RepoIDs: []RepositoryID{RepositoryID(otherRepo.String())}}
	for name, executor := range map[string]NativeFamilyExecutor{
		WorkItemIssueTypeFamilyName: issueType, WorkItemInvestmentFamilyName: investment,
	} {
		count, err := executor.ComputeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}, other)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s wrote %d rows for a repository with no stored item", name, count)
		}
	}
	var otherRows uint64
	if err := conn.QueryRow(ctx, `
SELECT (SELECT count() FROM issue_type_metrics_daily WHERE org_id = ? AND repo_id = ?)
     + (SELECT count() FROM investment_metrics_daily WHERE org_id = ? AND repo_id = ?)`,
		orgID, otherRepo, orgID, otherRepo).Scan(&otherRows); err != nil {
		t.Fatal(err)
	}
	if otherRows != 0 {
		t.Fatalf("a repository with no stored item holds %d rows", otherRows)
	}
}

// TestLoadWorkItemStateTransitionsReadsByWorkItemIDNotRepositoryOrProvider is
// the loader's own contract on the real schema: the transitions of a
// repository's items are found whatever the transition rows hold in repo_id
// and provider (the sync sink fills neither), the transitions of another
// repository's items are not, and a copy of one event that differs only in
// those two columns is one event.
func TestLoadWorkItemStateTransitionsReadsByWorkItemIDNotRepositoryOrProvider(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	orgID := "org-transitions-" + uuid.NewString()
	repoID, otherRepo := uuid.New(), uuid.New()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	seedWorkItemEngineItem(t, ctx, conn, orgID, repoID, "gh:acme/api#1", "quality", day.AddDate(0, 0, -2), nil, nil, nil)
	seedWorkItemEngineItem(t, ctx, conn, orgID, otherRepo, "gh:acme/web#9", "quality", day.AddDate(0, 0, -2), nil, nil, nil)
	// Another tenant holds an item with the same id in the same repository.
	seedWorkItemEngineItem(t, ctx, conn, "org-other", repoID, "gh:acme/api#1", "quality", day.AddDate(0, 0, -2), nil, nil, nil)

	insert := func(org string, repo uuid.UUID, workItemID, provider string, occurredAt time.Time, from, to string, synced time.Time) {
		t.Helper()
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_transitions (
			repo_id, work_item_id, occurred_at, provider, from_status, to_status,
			from_status_raw, to_status_raw, actor, org_id, last_synced)`)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(repo, workItemID, occurredAt, provider, from, to, from, to, "actor", org, synced); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}
	started := day.Add(9 * time.Hour)
	// As the sync writes it: no repository id, no provider.
	insert(orgID, uuid.Nil, "gh:acme/api#1", "", started, "todo", "in_progress", day.Add(10*time.Hour))
	// A copy of the same event from an older writer: with the repository id
	// and the provider.
	insert(orgID, repoID, "gh:acme/api#1", "github", started, "todo", "in_progress", day.Add(11*time.Hour))
	// A second, different event at the same instant must stay.
	insert(orgID, uuid.Nil, "gh:acme/api#1", "", started, "in_progress", "blocked", day.Add(10*time.Hour))
	// After the window end: not read.
	insert(orgID, uuid.Nil, "gh:acme/api#1", "", day.AddDate(0, 0, 2), "blocked", "done", day.AddDate(0, 0, 2))
	// Another repository's item, and another tenant's item of the same id.
	insert(orgID, uuid.Nil, "gh:acme/web#9", "", started, "todo", "in_progress", day.Add(10*time.Hour))
	insert("org-other", uuid.Nil, "gh:acme/api#1", "", started, "todo", "done", day.Add(10*time.Hour))

	transitions, err := LoadWorkItemStateTransitions(ctx, conn, orgID, repoID, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, transition := range transitions {
		got = append(got, transition.WorkItemID+" "+transition.FromStatus+">"+transition.ToStatus)
	}
	want := map[string]bool{
		"gh:acme/api#1 todo>in_progress":    true,
		"gh:acme/api#1 in_progress>blocked": true,
	}
	if len(got) != len(want) {
		t.Fatalf("transitions of the repository = %v, want exactly %d: %v", got, len(want), strings.Join(transitionKeys(want), ", "))
	}
	for _, transition := range got {
		if !want[transition] {
			t.Fatalf("transitions of the repository = %v, want exactly %v", got, transitionKeys(want))
		}
	}
}

func transitionKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	return keys
}
