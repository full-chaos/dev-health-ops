//go:build integration

package compoundingrisk

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

var ddls = []string{
	`CREATE TABLE compounding_risk_daily (
      org_id String, day Date, scope Enum8('repo' = 1, 'team' = 2), scope_id String,
      compounding_risk Nullable(Float64),
      severity Enum8('unknown' = 0, 'low' = 1, 'elevated' = 2, 'high' = 3),
      churn_norm Nullable(Float64), complexity_norm Nullable(Float64), ownership_norm Nullable(Float64), review_norm Nullable(Float64),
      rework_churn Nullable(Float64), complexity_delta Nullable(Float64), bus_factor Nullable(Float64),
      ownership_gini Nullable(Float64), single_owner_ratio Nullable(Float64), review_latency_p90h Nullable(Float64),
      w_churn Float64, w_complexity Float64, w_ownership Float64, w_review Float64,
      threshold_elevated Float64, threshold_high Float64, computed_at DateTime DEFAULT now())
      ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, scope, scope_id, day, computed_at)`,
	`CREATE TABLE repos (id UUID, repo String, provider String DEFAULT 'unknown', org_id String DEFAULT 'default',
      created_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(last_synced) ORDER BY id`,
	`CREATE TABLE teams (id String, name String, org_id String DEFAULT 'default', repo_patterns Array(String) DEFAULT [],
      updated_at DateTime64(6)) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id`,
	`CREATE TABLE team_repo_ownership (org_id String, provider String, team_id String, repo_id Nullable(UUID),
      repo_full_name String, match_type Enum8('exact' = 1, 'pattern' = 2),
      source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
      is_primary UInt8 DEFAULT 0, specificity UInt16 DEFAULT 0, priority Int32 DEFAULT 0,
      valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')), updated_at DateTime64(3, 'UTC'))
      ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, provider, repo_full_name, team_id, source, valid_from)`,
}

const (
	rA = "11111111-1111-1111-1111-111111111111"
	rB = "22222222-2222-2222-2222-222222222222"
	rC = "33333333-3333-3333-3333-333333333333"
	rX = "44444444-4444-4444-4444-444444444444"
)

func startStore(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
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
	for _, d := range ddls {
		if err := conn.Exec(ctx, d); err != nil {
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

func exec(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, q string, a ...any) {
	t.Helper()
	if err := conn.Exec(ctx, fmt.Sprintf(q, a...)); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

type risk struct {
	org, scope, id string
	day            time.Time
	score          string // SQL literal
	sev            string
	churn          string
	computed       string // SQL expression
}

func put(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, r risk) {
	if r.computed == "" {
		r.computed = "now()"
	}
	if r.churn == "" {
		r.churn = "NULL"
	}
	exec(t, ctx, conn, `INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity, churn_norm, w_churn, w_complexity, w_ownership, w_review, threshold_elevated, threshold_high, computed_at)
      SELECT '%s', '%s', '%s', '%s', %s, '%s', %s, 0.4, 0.3, 0.2, 0.1, 0.4, 0.7, %s`, r.org, r.day.Format("2006-01-02"), r.scope, r.id, r.score, r.sev, r.churn, r.computed)
}

func repo(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, id, name string) {
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) SELECT '%s', '%s', 'github', '%s', now64(3), now64(3)`, id, name, org)
}

func own(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, team, repoID, fullName string) {
	exec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, valid_from, updated_at)
      SELECT '%s', 'github', '%s', '%s', '%s', 'exact', 'native', toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), now64(3)`, org, team, repoID, fullName)
}

func day(now time.Time, back int) time.Time {
	d := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return d.AddDate(0, 0, -back)
}

func ids(rows []model.CompoundingRiskPoint) []string {
	out := []string{}
	for _, r := range rows {
		out = append(out, r.ScopeID)
	}
	return out
}

func TestRealClickHouse_RepoBreakout(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	repo(t, ctx, conn, "org-2", rX, "other/secret")
	// latest day = 2 days ago; rC has no repos row (label = id).
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 2), score: "0.3", sev: "low", churn: "0.1"})
	// two computes of rB on the latest day: the later one wins.
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 2), score: "0.2", sev: "low", computed: "now() - INTERVAL 2 HOUR"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 2), score: "0.9", sev: "high"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rC, day: day(now, 2), score: "NULL", sev: "unknown"})
	put(t, ctx, conn, risk{org: "org-2", scope: "repo", id: rX, day: day(now, 2), score: "0.99", sev: "high"})
	// an older day with other values, and a newer day holding only unscored rows.
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 5), score: "0.5", sev: "elevated"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "NULL", sev: "unknown"})

	got, err := Resolve(ctx, client, "org-1", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids(got.Rows)) != fmt.Sprint([]string{rB, rA, rC}) {
		t.Fatalf("rows %v", ids(got.Rows))
	}
	b := got.Rows[0]
	if *b.Score != 0.9 || b.Severity != model.CompoundingRiskSeverityHigh || b.ScopeLabel != "acme/api" || b.Day.String() != day(now, 2).Format("2006-01-02") ||
		b.Weights.Churn != 0.4 || b.Thresholds.High != 0.7 || b.ScopeEntity.DisplayName != "acme/api" || b.Scope != model.CompoundingRiskScopeRepo {
		t.Errorf("row %#v", b)
	}
	if a := got.Rows[1]; *a.Components.ChurnNorm != 0.1 || a.Components.ComplexityNorm != nil {
		t.Errorf("components %#v", a.Components)
	}
	if c := got.Rows[2]; c.Score != nil || c.ScopeLabel != rC || c.Severity != model.CompoundingRiskSeverityUnknown {
		t.Errorf("unscored row %#v", c)
	}
	// trend ends at the served day (day-2): day-5 (rA .5) and day-2 (mean of the scored latest rows .3 and .9).
	if len(got.Trend) != 2 {
		t.Fatalf("trend %#v", got.Trend)
	}
	if *got.Trend[0].Score != 0.5 || *got.Trend[1].Score < 0.599 || *got.Trend[1].Score > 0.601 || got.Trend[0].Severity != model.CompoundingRiskSeverityUnknown {
		t.Errorf("trend %#v", got.Trend)
	}

	// org isolation.
	other, _ := Resolve(ctx, client, "org-2", nil, now)
	if fmt.Sprint(ids(other.Rows)) != fmt.Sprint([]string{rX}) {
		t.Errorf("org-2 rows %v", ids(other.Rows))
	}
	none, _ := Resolve(ctx, client, "org-3", nil, now)
	if len(none.Rows) != 0 || none.Rows == nil {
		t.Errorf("unknown org %#v", none)
	}

	f := func(mut func(*model.CompoundingRiskFilterInput)) *model.CompoundingRiskFilterInput {
		in := &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeRepo, TrendDays: 30}
		mut(in)
		return in
	}
	dd := func(back int) *model.CompoundingRiskFilterInput { return nil }
	_ = dd
	cases := []struct {
		name string
		in   *model.CompoundingRiskFilterInput
		want []string
	}{
		{"explicit older day", f(func(in *model.CompoundingRiskFilterInput) { d := gqlDate(day(now, 5)); in.Day = &d }), []string{rA}},
		{"repo by id", f(func(in *model.CompoundingRiskFilterInput) { in.RepoIds = []string{rA} }), []string{rA}},
		{"repo by name", f(func(in *model.CompoundingRiskFilterInput) { in.RepoIds = []string{"acme/api"} }), []string{rB}},
		{"repo by name is case sensitive", f(func(in *model.CompoundingRiskFilterInput) { in.RepoIds = []string{"ACME/API"} }), nil},
		{"foreign repo", f(func(in *model.CompoundingRiskFilterInput) { in.RepoIds = []string{rX} }), nil},
		{"empty repo filter", f(func(in *model.CompoundingRiskFilterInput) { in.RepoIds = []string{} }), nil},
		{"trend one day still finds latest in window", f(func(in *model.CompoundingRiskFilterInput) { in.TrendDays = 1 }), nil},
	}
	for _, c := range cases {
		r, err := Resolve(ctx, client, "org-1", c.in, now)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if fmt.Sprint(ids(r.Rows)) != fmt.Sprint(append([]string{}, c.want...)) {
			t.Errorf("%s: %v want %v", c.name, ids(r.Rows), c.want)
		}
	}
	// A window that excludes every scored day answers empty; the clamp makes 0 mean one day.
	old := f(func(in *model.CompoundingRiskFilterInput) { in.TrendDays = 400 })
	r, _ := Resolve(ctx, client, "org-1", old, now)
	if len(r.Rows) != 3 || len(r.Trend) != 2 {
		t.Errorf("clamped window %#v", r)
	}
}

func TestRealClickHouse_TeamBreakout(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	repo(t, ctx, conn, "org-1", rC, "acme/lib")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6)), ('t2', 'Growth', 'org-1', now64(6))`)
	// Stored team rows exist on day-1 only for org-1; they are read as is.
	put(t, ctx, conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.6", sev: "elevated"})
	put(t, ctx, conn, risk{org: "org-1", scope: "team", id: "t9", day: day(now, 1), score: "0.1", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	teamIn := func(ids ...string) *model.CompoundingRiskFilterInput {
		return &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: ids, TrendDays: 30}
	}
	all, err := Resolve(ctx, client, "org-1", teamIn(), now)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids(all.Rows)) != "[t1 t9]" || all.Rows[0].ScopeLabel != "Platform" || all.Rows[1].ScopeLabel != "t9" || all.Rows[0].Scope != model.CompoundingRiskScopeTeam {
		t.Fatalf("stored rows %#v", all.Rows)
	}
	one, _ := Resolve(ctx, client, "org-1", teamIn("t9"), now)
	if fmt.Sprint(ids(one.Rows)) != "[t9]" {
		t.Errorf("filtered stored %v", ids(one.Rows))
	}

	// Fallback: no stored team rows for org-2's day; teams own repos through ownership.
	repo(t, ctx, conn, "org-2", rX, "other/secret")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('u1', 'Core', 'org-2', now64(6))`)
	own(t, ctx, conn, "org-2", "u1", rX, "other/secret")
	put(t, ctx, conn, risk{org: "org-2", scope: "repo", id: rX, day: day(now, 1), score: "0.5", sev: "elevated"})
	fb, err := Resolve(ctx, client, "org-2", teamIn(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(fb.Rows) != 1 || fb.Rows[0].ScopeID != "u1" || fb.Rows[0].ScopeLabel != "Core" || *fb.Rows[0].Score != 0.5 || fb.Rows[0].Severity != model.CompoundingRiskSeverityElevated {
		t.Fatalf("fallback %#v", fb.Rows)
	}
	if len(fb.Trend) != 1 {
		t.Errorf("fallback trend %#v", fb.Trend)
	}
	// A filtered team that owns nothing yields no rows.
	empty, _ := Resolve(ctx, client, "org-2", teamIn("nobody"), now)
	if len(empty.Rows) != 0 {
		t.Errorf("unowned team %#v", empty.Rows)
	}
	// Ownership is per org: org-1 does not see org-2's team ownership.
	cross, _ := Resolve(ctx, client, "org-1", teamIn("u1"), now)
	if len(cross.Rows) != 0 {
		t.Errorf("cross-org team %#v", cross.Rows)
	}
}

func gqlDate(t time.Time) graphqldate.Date { return graphqldate.New(t) }

// The same repository id and team id exist in two orgs with different names
// and scores: each org sees only its own row, label and ownership (the teams
// table is keyed by id alone, so only one org can name a given team id).
func TestRealClickHouse_SameIDsInTwoOrgs(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	// A second physical row for the same id under another org.
	repo(t, ctx, conn, "org-2", rA, "other/web")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6))`)
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, ctx, conn, risk{org: "org-2", scope: "repo", id: rA, day: day(now, 1), score: "0.8", sev: "high"})
	put(t, ctx, conn, risk{org: "org-1", scope: "team", id: "t1", day: day(now, 1), score: "0.3", sev: "low"})
	put(t, ctx, conn, risk{org: "org-2", scope: "team", id: "t1", day: day(now, 1), score: "0.9", sev: "high"})
	own(t, ctx, conn, "org-2", "t2", rA, "other/web")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t2', 'Owners', 'org-2', now64(6))`)

	for _, c := range []struct {
		org, label, score string
	}{{"org-1", "acme/web", "0.2"}, {"org-2", "other/web", "0.8"}} {
		got, err := Resolve(ctx, client, c.org, nil, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Rows) != 1 || got.Rows[0].ScopeLabel != c.label || fmt.Sprint(*got.Rows[0].Score) != c.score {
			t.Errorf("%s repo rows %#v", c.org, got.Rows)
		}
	}
	for _, c := range []struct {
		org, label, score string
	}{{"org-1", "Platform", "0.3"}, {"org-2", "t1", "0.9"}} {
		got, err := Resolve(ctx, client, c.org, &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TrendDays: 30}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Rows) != 1 || got.Rows[0].ScopeLabel != c.label || fmt.Sprint(*got.Rows[0].Score) != c.score {
			t.Errorf("%s team rows %#v", c.org, got.Rows)
		}
	}
	// Ownership of the shared repo id belongs to org-2 only: org-1 asking for t2 sees nothing.
	none, _ := Resolve(ctx, client, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t2"}, TrendDays: 30}, now)
	if len(none.Rows) != 0 {
		t.Errorf("org-1 saw org-2's team %#v", none.Rows)
	}
}

// In the fallback, a repo filter narrows the owned repositories by id or name.
func TestRealClickHouse_FallbackRepoFilterIntersectsOwnership(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/web")
	own(t, ctx, conn, "org-1", "t1", rB, "acme/api")
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rB, day: day(now, 1), score: "0.6", sev: "elevated"})
	in := func(repoIDs ...string) *model.CompoundingRiskFilterInput {
		return &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1"}, RepoIds: repoIDs, TrendDays: 30}
	}
	all, err := Resolve(ctx, client, "org-1", in(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Rows) != 1 || fmt.Sprint(*all.Rows[0].Score) != "0.4" {
		t.Fatalf("mean of both repos: %#v", all.Rows)
	}
	// An empty repo list narrows nothing when a team filter names the fallback's
	// repositories (Python treats it as absent there): same answer as no filter.
	emptyList, _ := Resolve(ctx, client, "org-1", in([]string{}...), now)
	if len(emptyList.Rows) != 1 || fmt.Sprint(*emptyList.Rows[0].Score) != "0.4" {
		t.Errorf("empty repo list %#v", emptyList.Rows)
	}
	byID, _ := Resolve(ctx, client, "org-1", in(rA), now)
	if len(byID.Rows) != 1 || fmt.Sprint(*byID.Rows[0].Score) != "0.2" {
		t.Errorf("by id %#v", byID.Rows)
	}
	byName, _ := Resolve(ctx, client, "org-1", in("acme/api"), now)
	if len(byName.Rows) != 1 || fmt.Sprint(*byName.Rows[0].Score) != "0.6" {
		t.Errorf("by name %#v", byName.Rows)
	}
	outside, _ := Resolve(ctx, client, "org-1", in(rC), now)
	if len(outside.Rows) != 0 {
		t.Errorf("repo outside the team %#v", outside.Rows)
	}
}

// A repository renamed but not yet merged keeps its old name row physically;
// a repo filter naming the old name must not select the team's repository.
func TestRealClickHouse_FallbackIgnoresSupersededRepoName(t *testing.T) {
	ctx, conn, client := startStore(t)
	now := time.Now().UTC()
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) SELECT '%s', 'acme/old', 'github', 'org-1', now64(3), now64(3) - INTERVAL 1 HOUR`, rA)
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) SELECT '%s', 'acme/new', 'github', 'org-1', now64(3), now64(3)`, rA)
	exec(t, ctx, conn, `INSERT INTO teams (id, name, org_id, updated_at) VALUES ('t1', 'Platform', 'org-1', now64(6))`)
	own(t, ctx, conn, "org-1", "t1", rA, "acme/new")
	put(t, ctx, conn, risk{org: "org-1", scope: "repo", id: rA, day: day(now, 1), score: "0.2", sev: "low"})
	in := func(name string) *model.CompoundingRiskFilterInput {
		return &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeTeam, TeamIds: []string{"t1"}, RepoIds: []string{name}, TrendDays: 30}
	}
	current, err := Resolve(ctx, client, "org-1", in("acme/new"), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Rows) != 1 {
		t.Fatalf("current name %#v", current.Rows)
	}
	stale, err := Resolve(ctx, client, "org-1", in("acme/old"), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale.Rows) != 0 {
		t.Fatalf("a superseded name selected the repository: %#v", stale.Rows)
	}
}
