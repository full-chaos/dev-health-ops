//go:build integration

package workgraph

import (
	"context"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestResolveLinkedIssues_ParityAcrossFlagStates is CHAOS-4980's
// resolver-facing parity proof: on ONE seeded fixture, ResolveLinkedIssues
// (the function the query-api Pr resolver calls) must return identical
// []model.PullRequestIssueLink whether investmentMaterializeNativeEnabled
// is on or off. issuepr_integration_test.go already proves the two raw
// readers agree at the issuePRLinkRow level (CHAOS-4924); this test proves
// the agreement survives one layer up, through the actual GraphQL-model
// mapping the resolver serves -- a mapping bug (e.g. a field transposed
// while building model.PullRequestIssueLink) would not be caught by the
// reader-level test alone.
func TestResolveLinkedIssues_ParityAcrossFlagStates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const (
		orgID    = "org-4980"
		repoID   = "00000000-4980-0000-0000-000000000001"
		prNumber = 7
	)

	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_graph_issue_pr (
            org_id, repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced
        )
    `)
	if err != nil {
		t.Fatalf("prepare work_graph_issue_pr batch: %v", err)
	}
	if err := batch.Append(
		orgID, repoID, "issue:OPS-42", uint32(prNumber), float32(0.82), "native", "resolver-parity-token",
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("append work_graph_issue_pr row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_graph_issue_pr batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	t.Setenv(investmentMaterializeNativeEnabledEnv, "")
	oracle, err := ResolveLinkedIssues(ctx, client, orgID, repoID, prNumber)
	if err != nil {
		t.Fatalf("ResolveLinkedIssues (oracle path): %v", err)
	}

	t.Setenv(investmentMaterializeNativeEnabledEnv, "1")
	native, err := ResolveLinkedIssues(ctx, client, orgID, repoID, prNumber)
	if err != nil {
		t.Fatalf("ResolveLinkedIssues (native path): %v", err)
	}

	if len(oracle) != 1 || len(native) != 1 {
		t.Fatalf("got %d oracle row(s), %d native row(s), want exactly 1 each: oracle=%+v native=%+v",
			len(oracle), len(native), oracle, native)
	}
	if !reflect.DeepEqual(oracle, native) {
		t.Fatalf("native path diverged from the oracle path: oracle=%+v native=%+v", oracle, native)
	}
	if got := oracle[0]; got.WorkItemID != "issue:OPS-42" || got.Provenance != "native" || got.Evidence != "resolver-parity-token" {
		t.Fatalf("got %+v, want the seeded row", got)
	}
}

// TestPRCoreRowExists_RealClickHouse is CHAOS-4980's nil-for-unknown proof
// against a REAL ClickHouse engine (the fake-client unit test in
// pr_test.go covers the query-shape/dispatch side; this proves the actual
// git_pull_requests table -- schema owned by chschema.Apply, not this
// test -- answers existence correctly for both a seeded PR and one that
// was never synced).
func TestPRCoreRowExists_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const (
		orgID           = "org-4980-exists"
		repoID          = "00000000-4980-0000-0000-000000000002"
		seededPRNumber  = 55
		unknownPRNumber = 999
	)

	// org_id is a REQUIRED column here, not backfilled after the fact:
	// migration 027_add_org_id_to_sorting_keys.py rebuilds git_pull_requests
	// with org_id prepended into its ORDER BY key ("(org_id, repo_id,
	// number)"), and ClickHouse refuses an ALTER ... UPDATE that targets a
	// sorting-key column -- chschema.Apply runs the full real migration
	// chain (.sql AND .py), so the table this test's INSERT targets is
	// already in that post-027 shape.
	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO git_pull_requests (
            org_id, repo_id, number, title, body, state, author_name, author_email,
            created_at, merged_at, closed_at, head_branch, base_branch,
            additions, deletions, changed_files, first_review_at,
            first_comment_at, changes_requested_count, reviews_count,
            comments_count, last_synced
        )
    `)
	if err != nil {
		t.Fatalf("prepare git_pull_requests batch: %v", err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := batch.Append(
		orgID, repoID, uint32(seededPRNumber), "a title", nil, "open", nil, nil,
		now, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, uint32(0), uint32(0),
		uint32(0), now,
	); err != nil {
		t.Fatalf("append git_pull_requests row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send git_pull_requests batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	exists, err := PRCoreRowExists(ctx, client, orgID, repoID, seededPRNumber)
	if err != nil {
		t.Fatalf("PRCoreRowExists (seeded): %v", err)
	}
	if !exists {
		t.Fatal("expected the seeded PR to exist")
	}

	exists, err = PRCoreRowExists(ctx, client, orgID, repoID, unknownPRNumber)
	if err != nil {
		t.Fatalf("PRCoreRowExists (unknown): %v", err)
	}
	if exists {
		t.Fatal("expected an unseeded PR number to not exist")
	}
}
