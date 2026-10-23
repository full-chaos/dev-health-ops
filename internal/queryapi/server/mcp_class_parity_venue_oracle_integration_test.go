//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"

	"github.com/full-chaos/dev-health-ops/internal/fixturescli"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The MCP caller class against query-api's own /graphql document route (CHAOS-9802). Both are the real query-api,
// built by the real Build over one migrated ClickHouse and Postgres, served by the real listeners: the MCP listener
// (its own gqlgen server, its own ClickHouse client, its own caps and admission) and the public /graphql route. For
// every registered query document whose response root is an MCP class root, with the proof corpus's variables, the
// two answers are compared by the proof comparator. A difference is a defect of one of the two routes.
//
// This replaces the manual prove and repoint ceremony of the MCP class rows: the class decision no longer rests on a
// receipt written by hand, it rests on this comparison going green on every change and on main.
//
// The data is the frozen synthetic world with metrics and work graph, moved to end today, plus generated rows for
// the metric tables that world does not hold (mcpParityGeneratedTables). Every class root must have at least one
// shape where both answers hold data: a root measured only on empty answers is a failure, not a pass.

const (
	mcpParityOrg    = "9802a000-0000-4000-8000-00000000000a"
	mcpParityUserID = "9802a000-0000-4000-8000-0000000000b1"
)

// mcpParityGeneratedTables are the ClickHouse tables the class roots read that the frozen world leaves empty. Each
// is filled by mcpParitySeedTable from its own column types, so a migration that adds a column needs no change here.
var mcpParityGeneratedTables = []string{
	"capacity_forecasts",
	"compounding_risk_daily",
	"estimate_coverage_metrics_daily",
	"incident_metrics_daily",
	"repo_metrics_daily",
	"team_cognitive_load_daily",
	"team_metrics_daily",
	"team_repo_ownership",
	"testops_coverage_metrics_daily",
	"testops_pipeline_metrics_daily",
	"testops_test_metrics_daily",
	"user_metrics_daily",
	"work_item_completion_daily",
	"work_item_metrics_daily",
}

// mcpPolicyRefusalStatuses are the statuses of the MCP listener's own policy gates (a size or cost cap, an
// allowlist, an identity rule): a shape the class refuses by policy while the document route serves it is not a
// served shape, so it is excluded and named, never counted as a match.
var mcpPolicyRefusalStatuses = map[int]bool{http.StatusBadRequest: true, http.StatusForbidden: true, http.StatusUnprocessableEntity: true}

// mcpParityPolicyExcusals are the shapes the MCP listener refuses by its own policy while the document route serves
// them, by label, each with the typed refusal reason and the ticket that owns the difference. A refusal that is not
// declared here is a failure.
var mcpParityPolicyExcusals = map[string]struct{ reason, ticket string }{}

// mcpParityExcused is a corpus case the step does not measure, with the reason and the ticket that owns it. An
// excused shape is listed in the output and never counts as measured.
type mcpParityExcused struct{ label, root, reason, ticket string }

type mcpParityShape struct {
	legacy                   bool
	operation, variant, root string
	document                 string
	variables                map[string]any
	parity                   goapiproof.Options
}

func (s mcpParityShape) label() string {
	if s.variant == "" {
		return s.operation
	}
	return s.operation + ":" + s.variant
}

func TestMCPClassParityVenueOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	root := repoRootFromHere(t)

	user := edgeUser{
		name: "mcpparity", id: uuid.MustParse(mcpParityUserID), active: true,
		tokenOrg: mcpParityOrg, tokenRole: "admin", memberships: map[string]string{mcpParityOrg: "admin"},
	}
	schemaDigest := digest.Schema(schemav1.SDL)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, GoOnly: true, JWTKey: edgeOracleJWTKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'mcp-parity', 'MCP parity', 'community', 'stripe', true, now(), now())`, mcpParityOrg)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'mcpparity@example.com', true, true, false, 0, now(), now())`, user.id)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), mcpParityOrg, user.id)
			// Two sources of the org, one that last synced and one that failed: sourceHealth reads them from here.
			for i, source := range []struct {
				provider string
				success  bool
			}{{"github", true}, {"jira", false}} {
				exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, last_sync_at, last_sync_success, created_at, updated_at)
VALUES ($1, $2, $3, $4, '[]'::json, '{}'::json, true, true, now() - ($5 * interval '1 hour'), $6, now(), now())`,
					uuid.New(), mcpParityOrg, "parity-"+source.provider, source.provider, i+1, source.success)
			}
			// The class decision of every root: served. The decision is the listener's own gate; what it
			// serves is what is compared here.
			for _, rootField := range mcpclass.SortedRoots() {
				exec(`INSERT INTO go_api_class_decision (operation, mode, current_candidate_build, schema_digest, recorded_by)
VALUES ($1, 'primary', 'parity-oracle', $2, 'mcp-class-parity-oracle')`, mcpclass.Operation(rootField), schemaDigest)
			}
			return nil
		},
	})

	chURI := venue.AdminClickHouseURI(t, venue.GoClickHouseDB)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(chURI))
	if err != nil {
		t.Fatalf("open the venue ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	now := time.Now().UTC()
	loaded := mcpParityLoadWorld(ctx, t, conn, now)
	generated := mcpParitySeedGeneratedTables(ctx, t, conn, now)
	mcpParityThickenSecurityAlerts(ctx, t, conn)
	t.Logf("world rows by table: %v", loaded)
	t.Logf("generated rows by table: %v", generated)

	settings, goBase, mcpBase, leave := mcpParityStartPlane(t, ctx, venue, root, chURI)
	_ = settings
	defer leave()
	token := edgeOracleToken(t, edgeOracleJWTKey, user, time.Now().Add(time.Hour))
	shapes, excludedAtSource := mcpParityShapes(ctx, t, conn, root, now)

	type tally struct {
		executed, nonEmpty, excluded int
		failed                       []string
		excludedWhy                  []string
	}
	byRoot := map[string]*tally{}
	for _, rootField := range mcpclass.SortedRoots() {
		byRoot[rootField] = &tally{}
	}
	client := &http.Client{Timeout: 90 * time.Second}
	for _, shape := range shapes {
		tl := byRoot[shape.root]
		reference := mcpParityPost(t, client, goBase+"/graphql", map[string]string{"Authorization": "Bearer " + token},
			urqlBody(t, documentOperationName(shape.document), shape.document, shape.variables))
		class := mcpParityPost(t, client, mcpBase+"/query", mcpParityIdentityHeaders(),
			mcpBody(t, shape.document, shape.variables))

		if shape.legacy && (reference.status != http.StatusOK || mcpParityHasErrors(reference.body)) {
			// A legacy text (the dual-accept window, CHAOS-8000) may declare other variables than the corpus sends.
			// Both routes must then refuse it; one serving what the other refuses is a difference.
			if class.status == http.StatusOK && !mcpParityHasErrors(class.body) {
				tl.failed = append(tl.failed, shape.label()+": the MCP listener served a legacy text the /graphql document route refuses")
				t.Logf("FAILED  %-40s the MCP listener served a legacy text the /graphql route refuses", shape.label())
				continue
			}
			tl.excluded++
			tl.excludedWhy = append(tl.excludedWhy, shape.label())
			t.Logf("EXCUSED %-40s legacy text does not accept the corpus variables, refused by both routes (CHAOS-8001 removes it): %s", shape.label(), mcpParityTrim(reference.body))
			continue
		}
		if excusal, declared := mcpParityPolicyExcusals[shape.label()]; declared && class.status != http.StatusOK && mcpPolicyRefusalStatuses[class.status] && reference.status == http.StatusOK && strings.Contains(class.body, excusal.reason) {
			tl.excluded++
			tl.excludedWhy = append(tl.excludedWhy, fmt.Sprintf("%s=%d %s", shape.label(), class.status, mcpParityTrim(class.body)))
			t.Logf("EXCUSED %-40s MCP policy refusal %d: %s", shape.label(), class.status, mcpParityTrim(class.body))
			continue
		}
		if verdict := mcpParityJudge(shape, reference, class); verdict.failure != "" {
			tl.failed = append(tl.failed, shape.label()+": "+verdict.failure)
			t.Logf("FAILED  %-40s %s", shape.label(), verdict.failure)
			continue
		} else {
			tl.executed++
			if verdict.nonEmpty {
				tl.nonEmpty++
			}
			excused := make([]string, 0, len(shape.parity.VolatileFields)+len(shape.parity.FloatTierB))
			for path, reason := range shape.parity.VolatileFields {
				excused = append(excused, "volatile "+path+": "+reason)
			}
			for path, reason := range shape.parity.FloatTierB {
				excused = append(excused, "tier-b "+path+": "+reason)
			}
			sort.Strings(excused)
			t.Logf("MATCHED %-40s non_empty=%t excused=%d %s", shape.label(), verdict.nonEmpty, len(excused), strings.Join(excused, " | "))
		}
	}

	var problems []string
	for _, rootField := range mcpclass.SortedRoots() {
		tl := byRoot[rootField]
		excusedAtSource := 0
		for _, excused := range excludedAtSource {
			if excused.root == rootField {
				excusedAtSource++
			}
		}
		t.Logf("ROOT %-22s measured=%d matched=%d (non_empty=%d) excused=%d failed=%d", rootField,
			tl.executed+len(tl.failed), tl.executed, tl.nonEmpty, tl.excluded+excusedAtSource, len(tl.failed))
		for _, why := range tl.excludedWhy {
			t.Logf("  excluded %s", why)
		}
		for _, failure := range tl.failed {
			problems = append(problems, rootField+" "+failure)
		}
		switch {
		case tl.executed == 0 && len(tl.failed) == 0:
			problems = append(problems, rootField+": no shape was measured for this root (a shape that is excused is not measured)")
		case tl.nonEmpty == 0 && len(tl.failed) == 0:
			problems = append(problems, rootField+": every executed shape was empty on both routes, so nothing was compared")
		}
	}
	for _, excused := range excludedAtSource {
		t.Logf("EXCUSED %-40s root=%s not measured (%s): %s", excused.label, excused.root, excused.ticket, excused.reason)
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatalf("the MCP class and the /graphql document route disagree, or a root was not measured (%d):\n%s", len(problems), strings.Join(problems, "\n"))
	}
	venueoracle.WriteGoOnlyProof(t, "every MCP class root answers each registered document of its response root the same as query-api's /graphql document route, over one seeded ClickHouse")
}

func mcpParityLoadWorld(ctx context.Context, t *testing.T, conn driver.Conn, now time.Time) map[string]int {
	t.Helper()
	world, err := fixturescli.LoadFrozenWorld(fixturescli.GenerateParams{
		Provider: "synthetic", RepoName: "acme/live-e2e", RepoCount: 1, Days: 14, CommitsPerDay: 6, PRCount: 24, TeamCount: 10,
		Seed: 20260219, WithMetrics: true, WithWorkGraph: true,
	})
	if err != nil {
		t.Fatalf("load the frozen world: %v", err)
	}
	counts, err := fixturescli.LoadWorld(ctx, conn, world, mcpParityOrg, now)
	if err != nil {
		t.Fatalf("insert the frozen world: %v", err)
	}
	return counts
}

// mcpParitySeedGeneratedTables fills each table of mcpParityGeneratedTables that is still empty with 14 days of rows
// for three entities (the world's teams, and its repository), typed from the table's own columns.
func mcpParitySeedGeneratedTables(ctx context.Context, t *testing.T, conn driver.Conn, now time.Time) map[string]uint64 {
	t.Helper()
	queryStrings := func(query string) []string {
		rows, err := conn.Query(ctx, query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out = append(out, s)
		}
		return out
	}
	repos := queryStrings("SELECT toString(id) FROM repos ORDER BY id LIMIT 1")
	teams := queryStrings("SELECT toString(id) FROM teams ORDER BY id LIMIT 3")
	if len(repos) == 0 || len(teams) == 0 {
		t.Fatalf("the frozen world holds %d repositories and %d teams, want at least one of each", len(repos), len(teams))
	}
	out := map[string]uint64{}
	for _, table := range mcpParityGeneratedTables {
		var existing uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM `"+table+"`").Scan(&existing); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if existing > 0 {
			continue
		}
		columns, err := mcpParityColumns(ctx, conn, table)
		if err != nil {
			t.Fatal(err)
		}
		insert, err := mcpParityInsertSQL(table, columns, mcpParityOrg, repos[0], teams)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed %s: %v\n%s", table, err, insert)
		}
		var rows uint64
		_ = conn.QueryRow(ctx, "SELECT count() FROM `"+table+"`").Scan(&rows)
		out[table] = rows
	}
	return out
}

type mcpParityColumn struct{ name, typ string }

func mcpParityColumns(ctx context.Context, conn driver.Conn, table string) ([]mcpParityColumn, error) {
	rows, err := conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ? AND default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL') ORDER BY position", table)
	if err != nil {
		return nil, fmt.Errorf("columns of %s: %w", table, err)
	}
	defer rows.Close()
	var out []mcpParityColumn
	for rows.Next() {
		var c mcpParityColumn
		if err := rows.Scan(&c.name, &c.typ); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("table %s has no columns in the venue database", table)
	}
	return out, nil
}

// mcpParityInsertSQL builds an INSERT ... SELECT of 14 days x len(teams) rows. entity is the team (and the
// row's repository owner), dayoff the day offset from today.
func mcpParityInsertSQL(table string, columns []mcpParityColumn, org, repo string, teams []string) (string, error) {
	quoted := make([]string, len(teams))
	for i, team := range teams {
		quoted[i] = "'" + team + "'"
	}
	teamArray := "[" + strings.Join(quoted, ",") + "]"
	var names, exprs []string
	for _, c := range columns {
		expr, ok := mcpParityValue(c.name, c.typ, org, repo, teamArray)
		if override, has := mcpParityOverrides[table][c.name]; has {
			expr, ok = strings.NewReplacer("%REPO%", repo, "%TEAMS%", teamArray).Replace(override), true
		}
		if !ok {
			continue
		}
		names = append(names, "`"+c.name+"`")
		exprs = append(exprs, expr)
	}
	return fmt.Sprintf("INSERT INTO `%s` (%s) SELECT %s FROM (SELECT number AS n, intDiv(number, %d) %% 14 AS dayoff, number %% %d AS entity FROM numbers(%d))",
		table, strings.Join(names, ", "), strings.Join(exprs, ", "), len(teams), len(teams), 14*len(teams)), nil
}

// mcpParityOverrides are the columns whose meaning a type cannot give: the row's scope and its owner.
// entity 0 is the repository, 1 and 2 are teams.
var mcpParityOverrides = map[string]map[string]string{
	// row_view is the view of a row: the empty string is a legacy row, which every reader keeps (CHAOS-8863). A
	// generated string would be a view no reader asks for and the capacity reads would find no rows.
	"work_item_metrics_daily": {"row_view": "CAST('' AS LowCardinality(String))"},
	"compounding_risk_daily": {
		"scope":    "CAST(if(entity = 0, 'repo', 'team') AS Enum8('repo' = 1, 'team' = 2))",
		"scope_id": "if(entity = 0, '%REPO%', arrayElement(%TEAMS%, entity + 1))",
	},
}

func mcpParityValue(name, typ, org, repo, teamArray string) (string, bool) {
	base := typ
	for _, wrapper := range []string{"LowCardinality(", "Nullable("} {
		for strings.HasPrefix(base, wrapper) {
			base = strings.TrimSuffix(strings.TrimPrefix(base, wrapper), ")")
		}
	}
	cast := func(expr string) (string, bool) { return "CAST(" + expr + " AS " + typ + ")", true }
	lower := strings.ToLower(name)
	switch {
	case base == "UUID":
		if strings.Contains(lower, "repo") {
			return cast("toUUID('" + repo + "')")
		}
		return cast("generateUUIDv4()")
	case base == "Date" || base == "Date32":
		return cast("today() - dayoff")
	case strings.HasPrefix(base, "DateTime64"), strings.HasPrefix(base, "DateTime"):
		if strings.HasSuffix(lower, "computed_at") || strings.HasSuffix(lower, "_at") && !strings.Contains(lower, "date") {
			return cast("now()")
		}
		return cast("now() - toIntervalDay(dayoff)")
	case base == "String", strings.HasPrefix(base, "FixedString"):
		switch {
		case lower == "org_id":
			return cast("'" + org + "'")
		case lower == "team_id":
			return cast("arrayElement(" + teamArray + ", entity + 1)")
		case strings.Contains(lower, "repo"):
			return cast("'" + repo + "'")
		case strings.Contains(lower, "status"), strings.Contains(lower, "state"):
			return cast("'active'")
		case strings.HasSuffix(lower, "id"):
			return cast("concat('" + name + "-', toString(n))")
		}
		return cast("concat('" + name + "-', toString(entity))")
	case strings.HasPrefix(base, "Float"), strings.HasPrefix(base, "Decimal"):
		return cast("1.5 + entity + dayoff * 0.25")
	case strings.HasPrefix(base, "UInt"), strings.HasPrefix(base, "Int"), base == "Bool":
		if base == "Bool" || base == "UInt8" {
			return cast("1")
		}
		return cast("1 + entity + dayoff % 3")
	case strings.HasPrefix(base, "Array("):
		return cast("[]")
	case strings.HasPrefix(base, "Map("):
		return cast("map()")
	}
	return "", false
}

// mcpParityStartPlane builds query-api as prod does (the real Build over the real settings) with the MCP listener on
// its own port, and returns the bases of the public and the MCP listener.
func mcpParityStartPlane(t *testing.T, ctx context.Context, venue *venueoracle.Venue, root, chURI string) (map[string]string, string, string, func()) {
	t.Helper()
	jwksPath, _ := edgeOracleJWKS(t)
	settings := map[string]string{
		"CLICKHOUSE_URI":               chURI,
		"GO_API_REGISTRY_POSTGRES_URI": venue.AdminURI(t, venue.GoDB),
		"GO_API_ENVELOPE_JWKS_PATH":    jwksPath,
		"GO_API_ENVELOPE_ISSUER":       "mcp-parity-issuer",
		"GO_API_ENVELOPE_AUDIENCE":     "mcp-parity-audience",
		edgeJWTSecretEnvVar:            edgeOracleJWTKey,
	}
	leave := enterRepositoryRoot(t, root)
	plane, err := Build(func(key string) string { return settings[key] })
	if err != nil {
		t.Fatalf("build query-api: %v", err)
	}
	t.Cleanup(plane.Close)
	_, loopback, _ := net.ParseCIDR("127.0.0.1/32")
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil, []*net.IPNet{loopback})
	mcp := MCPListener("127.0.0.1:0", plane, []*net.IPNet{loopback})
	for _, listener := range []*Listener{public, internal, mcp} {
		if listener == nil {
			t.Fatal("a listener was not built")
		}
		if err := listener.Start(ctx); err != nil {
			t.Fatal(err)
		}
		listener := listener
		t.Cleanup(func() { _ = listener.Shutdown(context.Background()) })
	}
	return settings, "http://" + public.Address(), "http://" + mcp.Address(), leave
}

func mcpParityIdentityHeaders() map[string]string {
	headers := map[string]string{}
	for key, values := range mcpHeaders(mcpParityOrg, "member", "false", "false") {
		headers[key] = values[0]
	}
	return headers
}

type mcpParityAnswer struct {
	status int
	body   string
}

func mcpParityPost(t *testing.T, client *http.Client, url string, headers map[string]string, body string) mcpParityAnswer {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return mcpParityAnswer{status: response.StatusCode, body: string(raw)}
}

func mcpParityTrim(body string) string {
	body = strings.Join(strings.Fields(body), " ")
	if len(body) > 160 {
		return body[:160] + "..."
	}
	return body
}

type mcpParityVerdict struct {
	failure  string
	nonEmpty bool
}

// mcpParityJudge decides one shape: the reference (the /graphql document route) must have executed, the class must
// have served the same status, and the two bodies must compare as a match under the shape's own parity options.
func mcpParityJudge(shape mcpParityShape, reference, class mcpParityAnswer) mcpParityVerdict {
	if reference.status != http.StatusOK {
		return mcpParityVerdict{failure: fmt.Sprintf("the /graphql document route answered %d: %s", reference.status, mcpParityTrim(reference.body))}
	}
	if class.status != http.StatusOK {
		return mcpParityVerdict{failure: fmt.Sprintf("the MCP listener answered %d where /graphql served: %s", class.status, mcpParityTrim(class.body))}
	}
	baseline, err := goapiproof.DecodeSnapshot([]byte(reference.body))
	if err != nil {
		return mcpParityVerdict{failure: "the /graphql answer does not decode: " + err.Error()}
	}
	candidate, err := goapiproof.DecodeSnapshot([]byte(class.body))
	if err != nil {
		return mcpParityVerdict{failure: "the MCP answer does not decode: " + err.Error()}
	}
	if len(baseline.Errors) > 0 {
		return mcpParityVerdict{failure: "the /graphql answer carries errors: " + mcpParityTrim(reference.body)}
	}
	if len(candidate.Errors) > 0 {
		return mcpParityVerdict{failure: "the MCP answer carries errors: " + mcpParityTrim(class.body)}
	}
	result := goapiproof.Compare(baseline, candidate, shape.parity)
	if !result.IsMatch() && !mcpParityStochasticOnly(result) {
		return mcpParityVerdict{failure: mcpParityDescribe(result)}
	}
	return mcpParityVerdict{nonEmpty: mcpParityHoldsData(baseline.Data) && mcpParityHoldsData(candidate.Data)}
}

// mcpParityStochasticOnly: a shape under the stochastic leaf class is executed and checked for type, order and date
// offset instead of compared by value; it is never a match, and passes when the class found nothing to refuse.
func mcpParityStochasticOnly(result goapiproof.Result) bool {
	return result.StochasticLeafCitation != "" && len(result.StochasticLeafRefusals) == 0 && result.StructuralRefusal == "" &&
		len(result.UnusedExclusions) == 0 && len(result.UnusedTierB) == 0 && result.DifferencesOutsideBaselineDefect == 0
}

func mcpParityDescribe(result goapiproof.Result) string {
	parts := []string{"state " + result.TerminalState}
	for i, finding := range result.Findings {
		if i == 6 {
			parts = append(parts, fmt.Sprintf("... %d more", len(result.Findings)-i))
			break
		}
		parts = append(parts, fmt.Sprintf("%s %s: %s", finding.Kind, finding.Path, mcpParityTrim(finding.Detail)))
	}
	for label, list := range map[string][]string{
		"unused_exclusions": result.UnusedExclusions, "unused_tier_b": result.UnusedTierB,
		"undeclared_numeric_leaves": result.UndeclaredNumericLeaves, "unused_order_insensitive": result.UnusedOrderInsensitiveLists,
		"order_insensitive_refusals": result.OrderInsensitiveListRefusals,
	} {
		if len(list) > 0 {
			parts = append(parts, label+" "+strings.Join(list, ","))
		}
	}
	if result.StructuralRefusal != "" {
		parts = append(parts, "structural "+result.StructuralRefusal+": "+mcpParityTrim(result.StructuralDetail))
	}
	return strings.Join(parts, "; ")
}

// mcpParityWithoutAbsentFields drops the excusals and tolerances that name a field an older text does not select: an
// operation's declarations describe its current text, and an excusal that reaches nothing fails the comparison
// (unused exclusion). The older text still compares every field it does select, exactly.
func mcpParityWithoutAbsentFields(options goapiproof.Options, document string) goapiproof.Options {
	selects := func(path string) bool {
		segments := strings.Split(strings.TrimPrefix(path, "data."), ".")
		for _, segment := range segments[1:] {
			if !strings.Contains(document, segment) {
				return false
			}
		}
		return true
	}
	volatile := map[string]string{}
	for path, reason := range options.VolatileFields {
		if selects(path) {
			volatile[path] = reason
		}
	}
	options.VolatileFields = volatile
	tierB := map[string]string{}
	for path, reason := range options.FloatTierB {
		if selects(path) {
			tierB[path] = reason
		}
	}
	options.FloatTierB = tierB
	return options
}

func mcpParityHasErrors(body string) bool {
	var decoded struct {
		Errors []json.RawMessage `json:"errors"`
	}
	return json.Unmarshal([]byte(body), &decoded) != nil || len(decoded.Errors) > 0
}

// mcpParityHoldsData reports whether the answer carries at least one scalar other than a null, an empty string, a
// zero-length list or __typename: two empty answers agree without either route having read anything.
func mcpParityHoldsData(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case map[string]any:
		for key, child := range v {
			if key != "__typename" && mcpParityHoldsData(child) {
				return true
			}
		}
		return false
	case []any:
		for _, child := range v {
			if mcpParityHoldsData(child) {
				return true
			}
		}
		return false
	case string:
		return v != ""
	case json.Number:
		return true
	case float64, bool:
		return true
	}
	return true
}

// mcpParityRegisteredDocuments is every document registrydump lists, current and legacy texts both: the edge oracle's
// own selection keeps the oldest legacy text of an operation and drops the post-freeze and Go-born operations, and
// this oracle measures the current text of every one.
func mcpParityRegisteredDocuments(t *testing.T, root string) []registeredEdgeDocument {
	t.Helper()
	command := exec.Command("go", "run", "./cmd/registrydump", "-file", "internal/queryapi/server/query_route.go")
	command.Dir = root
	out, err := command.Output()
	if err != nil {
		t.Fatalf("registrydump: %v", err)
	}
	var docs []registeredEdgeDocument
	if err := json.Unmarshal(out, &docs); err != nil {
		t.Fatal(err)
	}
	if len(docs) == 0 {
		t.Fatal("registrydump listed no registered documents: nothing would be measured")
	}
	return docs
}

// mcpParityNodeTypeInput is the WorkGraphNodeTypeInput enum (contracts/graphql/v1/schema.graphql): the variable of a
// node-type filter holds an enum name, not the stored lower-case text.
const mcpParityNodeTypeInput = "('ISSUE','PR','COMMIT','FILE','RELEASE','FEATURE_FLAG','AI_WORKFLOW_RUN','DIFF','REVIEW_OUTCOME','DEPLOYMENT','INCIDENT')"

// mcpParityInstance finds, in the seeded data, the stored row a variant's kind of identifier names. A kind it cannot
// resolve is an error, never a skipped shape.
func mcpParityInstance(ctx context.Context, t *testing.T, conn driver.Conn, kind string) string {
	t.Helper()
	var query string
	switch {
	case strings.Contains(kind, "team id that has stored team-scope compounding-risk"):
		query = "SELECT scope_id FROM compounding_risk_daily WHERE scope = 'team' LIMIT 1"
	case strings.Contains(kind, "repository id that has stored compounding-risk"):
		query = "SELECT scope_id FROM compounding_risk_daily WHERE scope = 'repo' LIMIT 1"
	case strings.Contains(kind, "repository id that has open alerts"), strings.Contains(kind, "repository id that has alerts"):
		query = "SELECT toString(repo_id) FROM security_alerts GROUP BY repo_id ORDER BY count() DESC LIMIT 1"
	case strings.Contains(kind, "search term"):
		query = "SELECT cve_id FROM security_alerts WHERE cve_id IS NOT NULL AND cve_id != '' LIMIT 1"
	case strings.Contains(kind, "alert state"):
		query = "SELECT lower(state) FROM security_alerts WHERE state IS NOT NULL GROUP BY state ORDER BY count() DESC LIMIT 1"
	case strings.Contains(kind, "node type whose whole edge set as a source is below 200"):
		query = "SELECT upper(source_type) FROM work_graph_edges WHERE upper(source_type) IN " + mcpParityNodeTypeInput + " GROUP BY source_type HAVING count() < 200 ORDER BY count() DESC LIMIT 1"
	case strings.Contains(kind, "node type that has edges as a source"):
		query = "SELECT upper(source_type) FROM work_graph_edges WHERE upper(source_type) IN " + mcpParityNodeTypeInput + " GROUP BY source_type ORDER BY count() DESC LIMIT 1"
	case strings.Contains(kind, "node id that is the source or target of at least one edge and of fewer than 200"):
		query = "SELECT source_id FROM work_graph_edges GROUP BY source_id HAVING count() < 200 ORDER BY count() DESC LIMIT 1"
	case strings.Contains(kind, "node id that is the source or target of at least one edge"):
		query = "SELECT source_id FROM work_graph_edges LIMIT 1"
	default:
		t.Fatalf("no seeded value is known for the instance kind %q: add one to mcpParityInstance", kind)
	}
	var value string
	if err := conn.QueryRow(ctx, query).Scan(&value); err != nil || value == "" {
		t.Fatalf("no row for the instance kind %q (%v): the seeded data lacks it", kind, err)
	}
	return value
}

// mcpParityShapes lists every request the oracle sends: for each operation whose response root is an MCP class
// root, the registered document's current text with the corpus's base variables and each variant. A shape that needs
// a real instance id, and a variant marked as a known refusal, are returned as not measured with their reason.
func mcpParityShapes(ctx context.Context, t *testing.T, conn driver.Conn, root string, now time.Time) ([]mcpParityShape, []mcpParityExcused) {
	t.Helper()
	roots := mcpclass.AllowedRoots()
	documents := map[string]string{}
	legacyTexts := map[string][]registeredEdgeDocument{}
	for _, doc := range mcpParityRegisteredDocuments(t, root) {
		if doc.Kind != "query" {
			continue
		}
		if doc.Legacy {
			legacyTexts[doc.Operation] = append(legacyTexts[doc.Operation], doc)
			continue
		}
		documents[doc.Operation] = doc.Document
	}
	window := mcpParityWindow(now)
	var shapes []mcpParityShape
	var notMeasured []mcpParityExcused
	for _, operation := range goapiproof.KnownOperations() {
		spec, err := goapiproof.SpecFor(operation)
		if err != nil || !roots[spec.ResponseRoot] {
			continue
		}
		document, registered := documents[operation]
		if !registered {
			notMeasured = append(notMeasured, mcpParityExcused{label: operation, root: spec.ResponseRoot, reason: "not a registered query document", ticket: "CHAOS-9802"})
			continue
		}
		if spec.InstanceVariable != "" {
			notMeasured = append(notMeasured, mcpParityExcused{label: operation, root: spec.ResponseRoot, reason: "needs one stored row's id (" + spec.InstanceVariable + ")", ticket: "CHAOS-9802"})
		} else {
			shapes = append(shapes, mcpParityShape{
				operation: operation, root: spec.ResponseRoot, document: document,
				variables: spec.Variables(mcpParityOrg, window), parity: mcpParityOptions(operation, "", spec.Parity),
			})
			// The legacy texts of the operation are still accepted by /graphql (CHAOS-8000): each is sent with the base
			// variables too, so a regression limited to an old text is seen.
			for _, old := range legacyTexts[operation] {
				shapes = append(shapes, mcpParityShape{
					legacy: true, operation: operation, variant: "legacy-" + strings.TrimPrefix(old.Digest, "sha256:")[:8], root: spec.ResponseRoot,
					document: old.Document, variables: spec.Variables(mcpParityOrg, window),
					parity: mcpParityWithoutAbsentFields(mcpParityOptions(operation, "", spec.Parity), old.Document),
				})
			}
		}
		for _, variant := range mcpParityExtraVariants(operation, spec.Variants) {
			switch {
			case variant.KnownRefusal != nil:
				notMeasured = append(notMeasured, mcpParityExcused{label: operation + ":" + variant.Name, root: spec.ResponseRoot, reason: variant.KnownRefusal.Reason, ticket: variant.KnownRefusal.Ticket})
			default:
				variables, parity := variant.Variables(mcpParityOrg, window), variant.Parity
				if variant.Instance != nil {
					value := mcpParityInstance(ctx, t, conn, variant.Instance.Kind)
					variant.Instance.Bind(variables, value)
					parity.ScopeEcho = append(append([]goapiproof.ScopeEcho(nil), parity.ScopeEcho...), variant.Instance.Echo(value)...)
				}
				shapes = append(shapes, mcpParityShape{
					operation: operation, variant: variant.Name, root: spec.ResponseRoot, document: document,
					variables: variables, parity: mcpParityOptions(operation, variant.Name, parity),
				})
			}
		}
	}
	if len(shapes) == 0 {
		t.Fatal("no registered document has an MCP class root as its response root: nothing would be measured")
	}
	return shapes, notMeasured
}

// mcpParityWindow is the 14-day window the frozen world ends in: its last day is today.
func mcpParityWindow(now time.Time) goapiproof.Window {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	since := today.AddDate(0, 0, -13)
	weekStart := today.AddDate(0, 0, -(int(today.Weekday())+6)%7)
	return goapiproof.Window{
		SinceUTC: since.Format(time.RFC3339), UntilUTC: today.AddDate(0, 0, 1).Format(time.RFC3339),
		SinceDate: since.Format("2006-01-02"), UntilDate: today.Format("2006-01-02"), WeekStart: weekStart.Format("2006-01-02"),
	}
}

// mcpParityThickenSecurityAlerts adds open alerts created inside the corpus's fixed 2026-06-01..2026-08-31 range, of
// the severities its filters name, to the repository the world's alerts belong to: the frozen world's alerts are all
// dated to its own run.
func mcpParityThickenSecurityAlerts(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()
	statement := `INSERT INTO security_alerts (repo_id, alert_id, source, severity, state, package_name, cve_id, url, title, description, created_at, last_synced, org_id)
SELECT repo_id, concat('parity-', toString(number)), 'dependabot', arrayElement(['critical', 'high', 'unknown'], number % 3 + 1), 'open',
       concat('parity-package-', toString(number)), concat('CVE-2026-9', toString(number)), 'https://example.test/alert', concat('parity alert ', toString(number)),
       'seeded for the MCP class parity oracle', toDateTime64('2026-07-10 12:00:00', 3, 'UTC') + toIntervalDay(number), now64(3), org_id
FROM (SELECT repo_id, org_id FROM security_alerts LIMIT 1) ARRAY JOIN range(6) AS number`
	if err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("thicken security_alerts: %v", err)
	}
}
