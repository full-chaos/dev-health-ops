//go:build integration

package issuepredges_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestFallbackInvariantOnRealClickHouse plants lower-tier links beside native
// ones, and one with a pull request as the issue side, in one org (a writer
// regression) and none in a control org, and runs
// the check against the real schema (FINAL over the version_rank table). The
// breached org is reported with its counts and a WARN; the check repairs
// nothing; after the retraction step the same org reports 0.
func TestFallbackInvariantOnRealClickHouse(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	breached, control := uuid.NewString(), uuid.NewString()
	repo := uuid.New()
	stamp := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	link := func(org string, pr uint32, workItemID, provenance string) issueprlinks.Link {
		return issueprlinks.Link{OrgID: org, RepoID: repo, PRNumber: pr, WorkItemID: workItemID,
			Provenance: provenance, Confidence: 0.9, Evidence: "fixture", LastSynced: stamp}
	}
	writer, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(ctx, []issueprlinks.Link{
		link(breached, 704, "linear:CHAOS-1", "native"),
		link(breached, 704, "linear:CHAOS-1", "explicit_text"),        // same key: FINAL keeps the native row
		link(breached, 704, "jira:API-1", "explicit_text"),            // beside the native pull request
		link(breached, 705, "linear:CHAOS-1", "heuristic"),            // beside the native issue
		link(breached, 706, "jira:BILL-1", "explicit_text"),           // no native on either side
		link(breached, 707, "ghpr:full-chaos/acr#5", "explicit_text"), // a pull request as the issue side
		link(control, 704, "linear:CHAOS-1", "native"),
		link(control, 705, "jira:API-1", "explicit_text"),
		link(control, 706, "jira:BILL-1", "heuristic"),
	}); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	loader, err := issuepredges.NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	service, err := issuepredges.NewService(loader, conn, writer, logger)
	if err != nil {
		t.Fatal(err)
	}

	got, err := service.CheckFallbackInvariant(ctx, breached)
	if err != nil {
		t.Fatalf("CheckFallbackInvariant(breached): %v", err)
	}
	want := issueprlinks.FallbacksBesideNative{ByPullRequest: 1, ByIssue: 1, PullRequestIssueSide: 1, Total: 3}
	if got.Beside != want || got.LinksRead != 5 {
		t.Fatalf("breached org = %+v, want %+v over 5 stored links", got, want)
	}
	if !strings.Contains(logs.String(), `"level":"WARN","msg":"`+issuepredges.FallbackInvariantBreachedLog+`"`) {
		t.Fatalf("no WARN line for the breached org:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "API-1") || strings.Contains(logs.String(), repo.String()) {
		t.Fatalf("log carries row contents:\n%s", logs.String())
	}

	clean, err := service.CheckFallbackInvariant(ctx, control)
	if err != nil {
		t.Fatalf("CheckFallbackInvariant(control): %v", err)
	}
	if clean.Beside != (issueprlinks.FallbacksBesideNative{}) || clean.LinksRead != 3 {
		t.Fatalf("control org = %+v, want 0 over 3 stored links", clean)
	}

	if again, err := service.CheckFallbackInvariant(ctx, breached); err != nil || again.Beside != want {
		t.Fatalf("second check = %+v, %v; the check must not repair (want %+v)", again, err, want)
	}

	if _, err := service.RetractOutrankedFallbacks(ctx, breached); err != nil {
		t.Fatalf("RetractOutrankedFallbacks: %v", err)
	}
	after, err := service.CheckFallbackInvariant(ctx, breached)
	if err != nil {
		t.Fatalf("CheckFallbackInvariant(after retraction): %v", err)
	}
	if after.Beside != (issueprlinks.FallbacksBesideNative{}) || after.LinksRead != 2 {
		t.Fatalf("after retraction = %+v, want 0 over 2 stored links", after)
	}
}
