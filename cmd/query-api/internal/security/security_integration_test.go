//go:build integration

package security

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	reposDDL = `CREATE TABLE repos (
    id UUID, repo String, ref Nullable(String), created_at DateTime64(3, 'UTC'),
    settings Nullable(String), tags Nullable(String), last_synced DateTime64(3, 'UTC'),
    org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (id)`
	alertsDDL = `CREATE TABLE security_alerts (
    repo_id UUID, alert_id String, source String, severity Nullable(String), state Nullable(String),
    package_name Nullable(String), cve_id Nullable(String), url Nullable(String), title Nullable(String),
    description Nullable(String), created_at DateTime64(3, 'UTC'), fixed_at Nullable(DateTime64(3, 'UTC')),
    dismissed_at Nullable(DateTime64(3, 'UTC')), last_synced DateTime64(3, 'UTC'),
    org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, alert_id)`
)

const (
	repoA = "11111111-1111-1111-1111-111111111111"
	repoB = "22222222-2222-2222-2222-222222222222"
	repoX = "33333333-3333-3333-3333-333333333333"
)

func startSecurityStore(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, ddl := range []string{reposDDL, alertsDDL} {
		if err := conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return ctx, conn, client
}

// alert seeds one alert; created/fixed/dismissed are day offsets before now
// (nil = NULL).
func seedAlert(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, repo, id, source, sev, state, title string, created int, fixed, dismissed *int) {
	t.Helper()
	off := func(v *int) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("now64(3) - INTERVAL %d DAY", *v)
	}
	q := fmt.Sprintf(`INSERT INTO security_alerts (repo_id, alert_id, source, severity, state, package_name, cve_id, url, title, description, created_at, fixed_at, dismissed_at, last_synced, org_id)
SELECT '%s', '%s', '%s', %s, %s, 'pkg-%s', 'CVE-%s', '', '%s', NULL, now64(3) - INTERVAL %d DAY, %s, %s, now64(3), '%s'`,
		repo, id, source, quote(sev), quote(state), id, id, title, created, off(fixed), off(dismissed), org)
	if err := conn.Exec(ctx, q); err != nil {
		t.Fatalf("seed alert %s: %v", id, err)
	}
}

func quote(s string) string {
	if s == "" {
		return "NULL"
	}
	return "'" + s + "'"
}

func ip(v int) *int { return &v }

func seedRepos(t *testing.T, ctx context.Context, conn stdclickhouse.Conn) {
	t.Helper()
	for _, r := range [][3]string{{repoA, "acme/web", "org-1"}, {repoB, "acme/api", "org-1"}, {repoX, "other/secret", "org-2"}} {
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO repos (id, repo, created_at, last_synced, org_id) SELECT '%s', '%s', now64(3), now64(3), '%s'`, r[0], r[1], r[2])); err != nil {
			t.Fatal(err)
		}
	}
}

func seedAll(t *testing.T, ctx context.Context, conn stdclickhouse.Conn) {
	seedRepos(t, ctx, conn)
	// org-1
	seedAlert(t, ctx, conn, "org-1", repoA, "A1", "dependabot", "critical", "open", "lodash prototype", 3, nil, nil)
	seedAlert(t, ctx, conn, "org-1", repoA, "A2", "dependabot", "high", "detected", "axios ssrf", 40, nil, nil)
	seedAlert(t, ctx, conn, "org-1", repoA, "A3", "code_scanning", "high", "fixed", "sql injection", 20, ip(5), nil)
	seedAlert(t, ctx, conn, "org-1", repoB, "B1", "advisory", "low", "confirmed", "weak hash", 60, nil, nil)
	seedAlert(t, ctx, conn, "org-1", repoB, "B2", "gitlab_dependency", "", "", "no severity or state", 10, nil, nil)
	seedAlert(t, ctx, conn, "org-1", repoB, "B3", "dependabot", "medium", "dismissed", "dismissed one", 50, nil, ip(2))
	// org-2 (must never appear for org-1)
	seedAlert(t, ctx, conn, "org-2", repoX, "X1", "dependabot", "critical", "open", "foreign", 1, nil, nil)
}

func TestRealClickHouse_Alerts(t *testing.T) {
	ctx, conn, client := startSecurityStore(t)
	seedAll(t, ctx, conn)

	ids := func(c *model.SecurityAlertConnection) []string {
		var out []string
		for _, e := range c.Edges {
			out = append(out, e.Node.AlertID)
		}
		return out
	}
	eq := func(name string, got, want []string) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: got %v want %v", name, got, want)
		}
	}
	all, err := ResolveAlerts(ctx, client, "org-1", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// severity rank desc, then created_at desc; B2 (unknown->rank 0) last group.
	eq("all", ids(all), []string{"A1", "A3", "A2", "B3", "B1", "B2"})
	if all.Edges[5].Node.Severity != "unknown" || all.Edges[5].Node.State != "open" || all.Edges[0].Node.RepoName != "acme/web" || all.Edges[0].Node.RepoURL != nil {
		t.Errorf("coalesce/repo mapping: %#v", all.Edges[5].Node)
	}
	// no leakage
	foreign, _ := ResolveAlerts(ctx, client, "org-2", nil, nil)
	eq("org-2 sees only its own", ids(foreign), []string{"X1"})
	none, _ := ResolveAlerts(ctx, client, "org-3", nil, nil)
	eq("unknown org", ids(none), nil)

	cases := []struct {
		name string
		f    *model.SecurityAlertFilterInput
		want []string
	}{
		{"openOnly", &model.SecurityAlertFilterInput{OpenOnly: true}, []string{"A1", "A2", "B1"}}, // B2 state NULL -> coalesce in select only, filter sees NULL
		{"openOnly beats states", &model.SecurityAlertFilterInput{OpenOnly: true, States: []model.SecurityStateInput{model.SecurityStateInputFixed}}, []string{"A1", "A2", "B1"}},
		{"states", &model.SecurityAlertFilterInput{States: []model.SecurityStateInput{model.SecurityStateInputFixed, model.SecurityStateInputDismissed}}, []string{"A3", "B3"}},
		{"severities", &model.SecurityAlertFilterInput{Severities: []model.SecuritySeverityInput{model.SecuritySeverityInputHigh}}, []string{"A3", "A2"}},
		{"sources", &model.SecurityAlertFilterInput{Sources: []model.SecuritySourceInput{model.SecuritySourceInputAdvisory, model.SecuritySourceInputGitlabDependency}}, []string{"B1", "B2"}},
		{"repoIds", &model.SecurityAlertFilterInput{RepoIds: []string{repoB}}, []string{"B3", "B1", "B2"}},
		{"repoIds foreign repo", &model.SecurityAlertFilterInput{RepoIds: []string{repoX}}, nil},
		{"search title", &model.SecurityAlertFilterInput{Search: strp("SQL INJ")}, []string{"A3"}},
		{"search package", &model.SecurityAlertFilterInput{Search: strp("pkg-B1")}, []string{"B1"}},
		{"search cve", &model.SecurityAlertFilterInput{Search: strp("cve-a2")}, []string{"A2"}},
		{"search wildcard is not escaped", &model.SecurityAlertFilterInput{Search: strp("axios%")}, []string{"A2"}},
		{"since", &model.SecurityAlertFilterInput{Since: dayAgo(30)}, []string{"A1", "A3", "B2"}},
		{"until", &model.SecurityAlertFilterInput{Until: dayAgo(45)}, []string{"B3", "B1"}},
		{"since+until window", &model.SecurityAlertFilterInput{Since: dayAgo(45), Until: dayAgo(30)}, []string{"A2"}},
	}
	for _, c := range cases {
		got, err := ResolveAlerts(ctx, client, "org-1", c.f, nil)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		eq(c.name, ids(got), c.want)
	}

	p1, _ := ResolveAlerts(ctx, client, "org-1", nil, &model.SecurityPaginationInput{First: 2})
	eq("page1", ids(p1), []string{"A1", "A3"})
	if !p1.PageInfo.HasNextPage || p1.PageInfo.HasPreviousPage || p1.TotalCount != 2 {
		t.Errorf("page1 info %#v %d", p1.PageInfo, p1.TotalCount)
	}
	p2, _ := ResolveAlerts(ctx, client, "org-1", nil, &model.SecurityPaginationInput{First: 2, After: p1.PageInfo.EndCursor})
	eq("page2", ids(p2), []string{"A2", "B3"})
	p3, _ := ResolveAlerts(ctx, client, "org-1", nil, &model.SecurityPaginationInput{First: 5, After: strp("4")})
	eq("page3", ids(p3), []string{"B1", "B2"})
	if p3.PageInfo.HasNextPage || !p3.PageInfo.HasPreviousPage || p3.TotalCount != 6 {
		t.Errorf("page3 info %#v %d", p3.PageInfo, p3.TotalCount)
	}
}

func dayAgo(n int) *graphqldate.Date {
	d := time.Now().UTC().AddDate(0, 0, -n)
	v := graphqldate.New(d)
	return &v
}

func TestRealClickHouse_Overview(t *testing.T) {
	ctx, conn, client := startSecurityStore(t)
	seedAll(t, ctx, conn)

	got, err := ResolveOverview(ctx, client, "org-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	// open states: A1(critical) A2(high) B1(low) -> 3 open. B2 has NULL state -> not open.
	k := got.Kpis
	if k.OpenTotal != 3 || k.Critical != 1 || k.High != 1 {
		t.Errorf("kpis %#v", k)
	}
	// A3 fixed 5 days ago, created 20 -> 15 days to fix.
	if k.MeanDaysToFix30d == nil || *k.MeanDaysToFix30d != 15 {
		t.Errorf("meanDaysToFix30d = %v", k.MeanDaysToFix30d)
	}
	// created<=30d & open: A1 -> 1; created>30d & open & not fixed/dismissed in window: A2, B1 -> 2; delta = -1
	if k.OpenDelta30d != -1 {
		t.Errorf("openDelta30d = %d, want -1", k.OpenDelta30d)
	}
	if len(got.SeverityBreakdown) != 3 {
		t.Errorf("breakdown %#v", got.SeverityBreakdown)
	}
	if len(got.TopRepos) != 2 || got.TopRepos[0].RepoName != "acme/web" || got.TopRepos[0].Count != 2 || got.TopRepos[1].Count != 1 {
		t.Errorf("top repos %#v", got.TopRepos)
	}
	// trend: opened in last 30d: A1(3d) A3(20d) B2(10d); fixed: A3(5d).
	var opened, fixed int
	for _, p := range got.Trend {
		opened += p.Opened
		fixed += p.Fixed
	}
	if opened != 3 || fixed != 1 || len(got.Trend) != 4 {
		t.Errorf("trend %#v", got.Trend)
	}

	// No fixed alerts -> mean is absent (NaN from avgIf), never a number.
	filtered, err := ResolveOverview(ctx, client, "org-1", &model.SecurityAlertFilterInput{RepoIds: []string{repoB}})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Kpis.MeanDaysToFix30d != nil || filtered.Kpis.OpenTotal != 1 {
		t.Errorf("filtered kpis %#v", filtered.Kpis)
	}
	// Unknown org: one all-zero KPI row, empty lists.
	empty, err := ResolveOverview(ctx, client, "org-3", nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty.Kpis.OpenTotal != 0 || empty.Kpis.MeanDaysToFix30d != nil || len(empty.TopRepos) != 0 || len(empty.Trend) != 0 || len(empty.SeverityBreakdown) != 0 {
		t.Errorf("empty %#v", empty)
	}
}

// An alert stamped with one org that references another org's repo id is
// invisible to the repo's org, in the list and in every overview block.
func TestRealClickHouse_AlertOrgMustMatchRepoOrg(t *testing.T) {
	ctx, conn, client := startSecurityStore(t)
	seedRepos(t, ctx, conn)
	seedAlert(t, ctx, conn, "org-1", repoA, "A1", "dependabot", "critical", "open", "own", 3, nil, nil)
	seedAlert(t, ctx, conn, "org-2", repoA, "F1", "dependabot", "critical", "open", "foreign row on org-1 repo", 2, ip(1), nil)

	alerts, err := ResolveAlerts(ctx, client, "org-1", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts.Edges) != 1 || alerts.Edges[0].Node.AlertID != "A1" {
		t.Fatalf("alerts leaked a row stamped with another org: %#v", alerts.Edges)
	}
	ov, err := ResolveOverview(ctx, client, "org-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ov.Kpis.OpenTotal != 1 || ov.Kpis.Critical != 1 || ov.Kpis.MeanDaysToFix30d != nil {
		t.Errorf("kpis counted the foreign row: %#v", ov.Kpis)
	}
	if len(ov.SeverityBreakdown) != 1 || ov.SeverityBreakdown[0].Count != 1 {
		t.Errorf("breakdown counted the foreign row: %#v", ov.SeverityBreakdown)
	}
	if len(ov.TopRepos) != 1 || ov.TopRepos[0].Count != 1 {
		t.Errorf("top repos counted the foreign row: %#v", ov.TopRepos)
	}
	if len(ov.Trend) != 1 || ov.Trend[0].Opened != 1 || ov.Trend[0].Fixed != 0 {
		t.Errorf("trend counted the foreign row: %#v", ov.Trend)
	}
}
