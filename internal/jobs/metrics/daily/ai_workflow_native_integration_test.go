//go:build integration

package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiworkflow"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	awfOrg  = "00000000-0000-4000-8000-0000000000d0"
	awfRepo = "00000000-0000-4000-8000-0000000000d1"
)

func awfClickHouse(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestAIWorkflowIssuePRLinksFINALConvergesDuplicateVersions proves astra
// finding 3's "duplicate-multiplicity convergence" claim against a real
// server rather than by inspection: work_graph_issue_pr's sorting key is
// (org_id, repo_id, work_item_id, pr_number) -- unchanged by migration 084 --
// so a re-synced link (same key, different provenance/last_synced) is a
// DUPLICATE of the same link, not a different issue, and LoadAIWorkflowIssuePRLinks'
// FINAL must collapse it to one row. A genuinely different work_item_id for
// the same PR is a DIFFERENT key and must survive untouched.
func TestAIWorkflowIssuePRLinksFINALConvergesDuplicateVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := awfClickHouse(ctx, t)

	// version_rank reproduces migration 084's real expression
	// (rank(provenance)*MULTIPLIER + ms(last_synced)) so this fixture
	// exercises the SAME version column production actually merges on, not
	// a simplified stand-in.
	if err := conn.Exec(ctx, `CREATE TABLE work_graph_issue_pr (
    repo_id UUID, work_item_id String, pr_number UInt32, confidence Float32,
    provenance String, evidence String, last_synced DateTime64(3,'UTC'), org_id String,
    version_rank UInt64 MATERIALIZED
        multiIf(provenance = 'native', 3, provenance = 'explicit_text', 2,
                provenance = 'heuristic', 1, 0) * 1099511627776
        + toUnixTimestamp64Milli(last_synced)
) ENGINE = ReplacingMergeTree(version_rank)
ORDER BY (org_id, repo_id, work_item_id, pr_number)`); err != nil {
		t.Fatal(err)
	}

	insert := func(workItemID, provenance string, lastSynced time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `
INSERT INTO work_graph_issue_pr
  (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
SELECT toUUID(?), ?, toUInt32(101), toFloat32(0.8), ?, 'fixture',
       toDateTime64(?, 3, 'UTC'), ?`,
			awfRepo, workItemID, provenance, lastSynced, awfOrg,
		); err != nil {
			t.Fatalf("insert %s/%s: %v", workItemID, provenance, err)
		}
	}

	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	// Same key (jira:ABC-1, pr 101), re-synced 3 times by different
	// producers -- version_rank must pick the 'native' row (rank 3) as the
	// FINAL survivor regardless of insertion order or last_synced recency,
	// and FINAL must return exactly ONE row for this key either way.
	insert("jira:ABC-1", "heuristic", t0)
	insert("jira:ABC-1", "explicit_text", t0.Add(time.Hour))
	insert("jira:ABC-1", "native", t0.Add(-24*time.Hour)) // earliest wall-clock, highest rank

	// A genuinely different work item linked to the SAME PR: different key,
	// must survive as its own row.
	insert("jira:ABC-2", "native", t0)

	// POSITIVE CONTROL: prove the fixture really did write 4 physical rows
	// for this repo/pr before FINAL collapses anything -- otherwise this
	// test could pass by accident (nothing to converge).
	var rawCount uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM work_graph_issue_pr WHERE org_id = ? AND pr_number = 101`, awfOrg,
	).Scan(&rawCount); err != nil {
		t.Fatal(err)
	}
	if rawCount != 4 {
		t.Fatalf("fixture wrote %d raw rows, want 4 -- this test cannot exercise convergence", rawCount)
	}

	links, err := LoadAIWorkflowIssuePRLinks(ctx, conn, awfOrg, []uuid.UUID{uuid.MustParse(awfRepo)}, []int64{101})
	if err != nil {
		t.Fatalf("load links: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("got %d links, want 2 (the 3-way duplicate converged to 1, plus the distinct link) -- got %+v",
			len(links), links)
	}
	byWorkItem := map[string]int{}
	for _, l := range links {
		byWorkItem[l.WorkItemID]++
	}
	if byWorkItem["jira:ABC-1"] != 1 {
		t.Errorf("jira:ABC-1 appeared %d times, want exactly 1 (FINAL must dedupe the re-synced link)", byWorkItem["jira:ABC-1"])
	}
	if byWorkItem["jira:ABC-2"] != 1 {
		t.Errorf("jira:ABC-2 appeared %d times, want exactly 1 (a distinct link must not be affected)", byWorkItem["jira:ABC-2"])
	}
}

// TestAIWorkflowPullRequestsIgnoresImpoverishedColumns proves astra finding
// 2 against a real server: the physical git_pull_requests table (as
// production actually defines it) carries labels/author_login/
// author_user_type, but LoadAIWorkflowPullRequests' SELECT list omits them
// on purpose (to match production's live query byte-for-byte) -- so the
// returned PullRequestRow must come back with those fields at their zero
// value even though the database genuinely holds non-empty data for them.
// A future "helpful" widening of the SELECT would break Scan's positional
// column count immediately, but this test additionally pins the CURRENT,
// intentional omission as a fact about the query's OUTPUT, not just its
// text.
func TestAIWorkflowPullRequestsIgnoresImpoverishedColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := awfClickHouse(ctx, t)

	if err := conn.Exec(ctx, `CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, title Nullable(String), body Nullable(String),
    head_branch Nullable(String), author_name Nullable(String), author_email Nullable(String),
    author_login Nullable(String), author_user_type Nullable(String), labels Array(String),
    created_at DateTime64(3,'UTC'), merged_at Nullable(DateTime64(3,'UTC')),
    closed_at Nullable(DateTime64(3,'UTC')), last_synced DateTime64(3,'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number)`); err != nil {
		t.Fatal(err)
	}

	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests
  (repo_id, number, title, body, head_branch, author_name, author_email,
   author_login, author_user_type, labels, created_at, merged_at, closed_at, last_synced, org_id)
SELECT toUUID(?), toUInt32(101), 'Add caching', 'plain body, no AI mention',
       'feature/cache', 'dev-a', 'dev-a@example.com', 'some-bot[bot]', 'Bot',
       ['ai-assisted'], toDateTime64(?, 3, 'UTC'), toDateTime64(?, 3, 'UTC'),
       NULL, toDateTime64(?, 3, 'UTC'), ?`,
		awfRepo, start, start.Add(time.Hour), start.Add(2*time.Hour), awfOrg,
	); err != nil {
		t.Fatal(err)
	}

	rows, err := LoadAIWorkflowPullRequests(ctx, conn, awfOrg, []uuid.UUID{uuid.MustParse(awfRepo)},
		start, start.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("load pull requests: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 pull request row, got %d", len(rows))
	}
	row := rows[0]

	// The columns the loader DOES read must come through correctly --
	// otherwise a passing "omission" assertion below would be vacuous.
	if row.AuthorName != "dev-a" || row.HeadBranch != "feature/cache" || row.Title != "Add caching" {
		t.Fatalf("selected columns did not populate correctly: %+v", row)
	}

	// The columns the loader deliberately does NOT read, despite the
	// database genuinely holding non-empty values for them.
	if len(row.Labels) != 0 {
		t.Errorf("Labels = %v, want empty -- production's SELECT has no labels column; a non-empty "+
			"result here means the loader started reading it, silently widening detection vs. Python", row.Labels)
	}
	if row.AuthorLogin != "" {
		t.Errorf("AuthorLogin = %q, want empty -- production's SELECT has no author_login column", row.AuthorLogin)
	}
	if row.AuthorUserType != "" {
		t.Errorf("AuthorUserType = %q, want empty -- production's SELECT has no author_user_type column", row.AuthorUserType)
	}
}

// TestAIWorkflowPartialWriteAgainstRealClickHouse mirrors
// TestWorkGraphEdgesPartialWriteAgainstRealClickHouse's structural-failure
// shape for ai_workflow's own 3-table write order (runs, then artifact
// edges, then issue edges): only the runs table exists, so the SECOND write
// fails against a real driver after the first has genuinely landed.
func TestAIWorkflowPartialWriteAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	conn := awfClickHouse(ctx, t)

	// ONLY ai_workflow_runs. ai_workflow_artifact_edges is deliberately
	// absent so the SECOND write (step 2 of aiWorkflowWriteOrder) fails
	// after the first has landed.
	if err := conn.Exec(ctx, `CREATE TABLE ai_workflow_runs (
    run_id String, org_id UUID, provider LowCardinality(String), run_kind LowCardinality(String),
    status Nullable(String), tool Nullable(String), model Nullable(String), actor Nullable(String),
    repo_id Nullable(UUID), prompts_redacted Bool, prompt_hash Nullable(String),
    prompt_length Nullable(UInt32), started_at Nullable(DateTime64(3,'UTC')),
    completed_at Nullable(DateTime64(3,'UTC')), observed_at DateTime64(3,'UTC'),
    metadata String, computed_at DateTime64(3,'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at) ORDER BY (org_id, provider, run_id)`); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	orgUUID := uuid.MustParse(awfOrg)
	repoUUID := uuid.MustParse(awfRepo)
	status := aiworkflow.RunStatusCompleted

	runsWritten, err := WriteAIWorkflowRuns(ctx, conn, []aiworkflow.Run{{
		RunID: "run-1", OrgID: orgUUID, Provider: "github",
		RunKind: aiworkflow.RunKindChatAssisted, Status: &status, RepoID: &repoUUID,
		PromptsRedacted: true, ObservedAt: now,
		Metadata: map[string]any{"subject_type": "pull_request", "subject_id": awfRepo + ":101"},
	}}, now)
	if err != nil {
		t.Fatalf("the FIRST write must succeed for this test to mean anything: %v", err)
	}
	if runsWritten == 0 {
		t.Fatal("first write reported 0 rows, so the partial case cannot arise")
	}

	_, artifactErr := WriteAIWorkflowArtifactEdges(ctx, conn, []aiworkflow.ArtifactEdge{{
		EdgeID: "edge-1", OrgID: orgUUID, RunID: "run-1",
		ArtifactType: aiworkflow.ArtifactTypePullRequest, ArtifactID: awfRepo + ":101",
		Provider: "github", RepoID: &repoUUID, Confidence: 0.25, Source: "pr_body",
		Evidence: `{}`, ObservedAt: now,
	}}, now)
	if artifactErr == nil {
		t.Fatal("the artifact-edge write must FAIL (its table does not exist); fixture is wrong")
	}

	rowsReported, wrapped := wrapAIWorkflowPartialWrite(runsWritten, 2, artifactErr)
	if !errors.Is(wrapped, ErrPartialWrite) {
		t.Errorf("a real mid-sequence failure must wrap ErrPartialWrite so the bridge is "+
			"skipped rather than rewriting these tables; got %v", wrapped)
	}
	if !errors.Is(wrapped, artifactErr) {
		t.Errorf("the driver's own error must survive wrapping; got %v", wrapped)
	}
	if rowsReported != runsWritten {
		t.Errorf("rows-written: got %d, want %d (the count that actually landed)", rowsReported, runsWritten)
	}

	var persisted uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM ai_workflow_runs WHERE org_id = ?`, awfOrg,
	).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != uint64(runsWritten) {
		t.Errorf("ClickHouse holds %d run rows but the executor reported %d written; "+
			"the skip decision depends on that count being true", persisted, runsWritten)
	}
}
