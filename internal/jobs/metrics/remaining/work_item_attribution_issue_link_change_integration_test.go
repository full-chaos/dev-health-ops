//go:build integration

package remaining_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A work_graph_issue_pr change re-derives the linked issue at the next
// backstop run, for an issue of every provider. Yesterday's rows: R-1, M-1 and
// K-1 take team-a from explicit_text links to pull requests of repository A;
// N-1 has no link. Today two native links land and the real fallback
// retraction runs: PR A#7 is natively linked to N-1 (R-1's explicit_text row to
// A#7 is retracted), and M-1 is natively linked to PR B#9 of repository B (its
// explicit_text row to A#8 is retracted). No ownership changed. A second run
// writes nothing.
func TestALinkChangeRederivesTheLinkedIssue(t *testing.T) {
	ctx := context.Background()
	conn := linkChangeClickHouse(t, ctx)
	for _, kind := range []struct{ provider, itemType, prefix, suffix string }{
		{"jira", "story", "jira:L", "-1"},
		{"github", "issue", "gh:acme/tracker#", "1"},
		{"gitlab", "issue", "gitlab:acme/tracker#", "1"},
		{"linear", "issue", "linear:L", "-1"},
	} {
		t.Run(kind.provider, func(t *testing.T) {
			id := func(letter string) string {
				if kind.provider == "github" || kind.provider == "gitlab" {
					return kind.prefix + map[string]string{"R": "1", "M": "2", "K": "3", "N": "4"}[letter] + kind.suffix
				}
				return kind.prefix + letter + kind.suffix
			}
			linkChangeCell(t, ctx, conn, kind.provider, kind.itemType, id("R"), id("M"), id("K"), id("N"))
		})
	}
}

func linkChangeCell(t *testing.T, ctx context.Context, conn driver.Conn, provider, itemType, removed, moved, kept, added string) {
	writer, err := remaining.NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	backstop, err := remaining.NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	orgID := "org-link-change-" + uuid.NewString()
	repoA, repoB, tracker := uuid.New(), uuid.New(), uuid.New()
	now := time.Now().UTC()
	yesterday := now.Add(-24 * time.Hour)
	setup := now.Add(-72 * time.Hour)

	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	for _, pr := range []struct {
		id   string
		repo uuid.UUID
	}{{"ghpr:acme/a#7", repoA}, {"ghpr:acme/a#8", repoA}, {"ghpr:acme/b#9", repoB}} {
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, status, created_at, org_id, last_synced)
			VALUES (?, ?, 'github', 'pr', '', 'in_progress', ?, ?, ?)`, pr.repo, pr.id, setup, orgID, setup)
	}
	issues := []string{removed, moved, kept, added}
	for index, id := range issues {
		project, repo := "", tracker
		if provider == "jira" || provider == "linear" {
			project, repo = fmt.Sprintf("P%d", index), uuid.Nil
		}
		exec(`INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, project_key, status, created_at, org_id, last_synced)
			VALUES (?, ?, ?, ?, ?, ?, 'in_progress', ?, ?, ?)`, repo, id, provider, itemType, project, project, setup, orgID, setup)
	}
	for _, owner := range []struct {
		repo uuid.UUID
		team string
	}{{repoA, "team-a"}, {repoB, "team-b"}} {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source,
			is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
			orgID, owner.team, owner.repo, "acme/"+owner.team, setup, now.Add(-48*time.Hour))
	}
	link := func(repo uuid.UUID, issue string, pr uint32, tier string, at time.Time) {
		t.Helper()
		exec(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
			VALUES (?, ?, ?, 1.0, ?, 'cell', ?, ?)`, repo, issue, pr, tier, at, orgID)
	}
	link(repoA, removed, 7, "explicit_text", setup)
	link(repoA, moved, 8, "explicit_text", setup)
	link(repoA, kept, 8, "explicit_text", setup)

	// Yesterday's state: the rows the cascade gives the four issues on
	// yesterday's facts, and an org-wide backstop marker after the ownership.
	subjects, err := remaining.LoadWorkItemDonorSubjects(ctx, conn, orgID, issues, nil)
	if err != nil || len(subjects) != len(issues) {
		t.Fatalf("load subjects: %d %v", len(subjects), err)
	}
	facts, err := remaining.LoadWorkItemDerivationFacts(ctx, conn, orgID, yesterday)
	if err != nil {
		t.Fatalf("load facts: %v", err)
	}
	derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)
	if derived.LinkedPullRequests, err = remaining.LoadIssueLinkedPullRequests(ctx, conn, orgID, subjects); err != nil {
		t.Fatalf("load links: %v", err)
	}
	written := map[string]struct{}{}
	for _, id := range issues {
		written[id] = struct{}{}
	}
	rows := remaining.BuildWorkItemAttributionRows(orgID, yesterday, written, subjects, derived)
	if _, err := writer.WriteAttributions(ctx, remaining.WorkItemAttributionProducer{
		Writer: remaining.WorkItemAttributionWriterBackstop, RunID: uuid.NewString()}, rows); err != nil {
		t.Fatalf("write yesterday's rows: %v", err)
	}
	exec(`INSERT INTO work_item_attribution_backstop_runs (org_id, run_id, completed_at) VALUES (?, ?, ?)`,
		orgID, uuid.NewString(), yesterday)

	before := latestPrimary(t, ctx, conn, orgID)
	for id, want := range map[string]string{
		removed: "repo_ownership|team-a", moved: "repo_ownership|team-a",
		kept: "repo_ownership|team-a", added: "unassigned|",
	} {
		if before[id] != want {
			t.Fatalf("setup: %s yesterday = %q, want %q (all %v)", id, before[id], want, before)
		}
	}

	// Today: two native links land, then the real fallback retraction step.
	link(repoA, added, 7, "native", now)
	link(repoB, moved, 9, "native", now)
	loader, err := issuepredges.NewLoader(conn)
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}
	linksWriter, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatalf("new links writer: %v", err)
	}
	service, err := issuepredges.NewService(loader, conn, linksWriter, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	retraction, err := service.RetractOutrankedFallbacks(ctx, orgID)
	if err != nil {
		t.Fatalf("retract: %v", err)
	}
	stored, err := issueprlinks.ReadStoredLinks(ctx, conn, orgID)
	if err != nil {
		t.Fatalf("read links: %v", err)
	}
	var after []string
	for _, l := range stored {
		after = append(after, fmt.Sprintf("%s|%d|%s", l.WorkItemID, l.PRNumber, l.Provenance))
	}
	t.Logf("RED14149 retraction %+v links now %v", retraction, after)
	if retraction.LinksRetracted != 2 || len(stored) != 3 {
		t.Fatalf("setup: retraction %+v, links %v, want 2 retracted and 3 left", retraction, after)
	}

	outcome, err := backstop.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("ComputeOrg: %v", err)
	}
	got := latestPrimary(t, ctx, conn, orgID)
	t.Logf("RED14149 outcome %+v primary %v", outcome, got)
	for _, want := range []struct{ id, row, why string }{
		{removed, "unassigned|", "link removed: no team-a"},
		{moved, "repo_ownership|team-b", "link moved to repository B"},
		{added, "repo_ownership|team-a", "new native link to repository A"},
		{kept, "repo_ownership|team-a", "control: link kept"},
	} {
		if got[want.id] != want.row {
			t.Errorf("RED14149 FAIL %s (%s): primary %q, want %q", want.id, want.why, got[want.id], want.row)
		}
	}
	if outcome.RowsWritten == 0 {
		return
	}
	again, err := backstop.ComputeOrg(ctx, orgID)
	if err != nil {
		t.Fatalf("second ComputeOrg: %v", err)
	}
	if !again.SkippedNoop || again.RowsWritten != 0 {
		t.Errorf("RED14149 FAIL second run %+v, want a no-op that writes nothing", again)
	}
}

// latestPrimary is source|team of each item's primary row in its newest
// computed_at set, the fence every reader of the table applies.
func latestPrimary(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT work_item_id, toString(source), ifNull(team_id, '')
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND is_primary = 1
  AND (work_item_id, computed_at) IN (
      SELECT work_item_id, max(computed_at) FROM work_item_team_attributions
      WHERE org_id = ? GROUP BY work_item_id)`, orgID, orgID)
	if err != nil {
		t.Fatalf("query primary: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var id, source, team string
		if err := rows.Scan(&id, &source, &team); err != nil {
			t.Fatal(err)
		}
		out[id] = source + "|" + team
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func linkChangeClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := instance.Close(ctx); closeErr != nil {
			t.Logf("close clickhouse container: %v", closeErr)
		}
	})
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	password, _ := parsed.User.Password()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
