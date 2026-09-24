//go:build integration

// Runs ResolveCatalog's generated SQL against a real ClickHouse engine: the
// repository-slug regex, the team roster join, the investment-path
// dimensions and the org scope, none of which a fake row scanner can
// execute.
package analytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const seededCatalogExtraDDL = `
CREATE TABLE repos (
    id UUID,
    repo String,
    ref Nullable(String),
    created_at DateTime64(3, 'UTC'),
    settings Nullable(String),
    tags Nullable(String),
    last_synced DateTime64(3, 'UTC'),
    org_id String,
    provider String,
    source_id Nullable(UUID)
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, id);

CREATE TABLE teams (
    id String,
    team_uuid UUID,
    name String,
    description Nullable(String),
    members Array(String),
    updated_at DateTime64(6),
    last_synced DateTime64(6) DEFAULT now(),
    org_id String DEFAULT 'default',
    is_active UInt8 DEFAULT 1
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, id);
`

func TestResolveCatalog_SeededRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, ddl := range []string{seededQualitySchemaDDL, investmentMetricsDailyDDL, seededCatalogExtraDDL} {
		for _, stmt := range splitSQLStatements(ddl) {
			if err := conn.Exec(ctx, stmt); err != nil {
				t.Fatalf("exec DDL %q: %v", stmt, err)
			}
		}
	}

	const orgID = "seeded-catalog"
	const otherOrgID = "seeded-catalog-other"

	repoRows := []struct{ id, repo, org string }{
		{"11111111-1111-1111-1111-111111111111", "Acme/Widget", orgID},
		{"22222222-2222-2222-2222-222222222222", " acme/widget ", orgID},
		{"33333333-3333-3333-3333-333333333333", "acme/other", orgID},
		{"44444444-4444-4444-4444-444444444444", "not-a-slug", orgID},
		{"55555555-5555-5555-5555-555555555555", "acme/toolong" + fmt.Sprintf("%0100d", 0), orgID},
		{"66666666-6666-6666-6666-666666666666", "foreign/repo", otherOrgID},
	}
	for _, r := range repoRows {
		if err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ('%s', '%s', now64(3), now64(3), '%s', 'github')",
			r.id, r.repo, r.org)); err != nil {
			t.Fatalf("seed repos: %v", err)
		}
	}
	for _, tm := range []struct {
		id, name, org string
		active        int
	}{
		{"team-b", "B", orgID, 1},
		{"team-a", "A", orgID, 1},
		{"team-off", "Off", orgID, 0},
		{"team-foreign", "F", otherOrgID, 1},
	} {
		if err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO teams (id, team_uuid, name, members, updated_at, org_id, is_active) VALUES ('%s', generateUUIDv4(), '%s', [], now64(6), '%s', %d)",
			tm.id, tm.name, tm.org, tm.active)); err != nil {
			t.Fatalf("seed teams: %v", err)
		}
	}
	seedSankeyOrderUnit(t, ctx, conn, orgID, "wu-1", "feature", "quality.testing", 1)
	seedSankeyOrderUnit(t, ctx, conn, orgID, "wu-2", "feature", "quality.review", 1)
	seedSankeyOrderUnit(t, ctx, conn, orgID, "wu-3", "bug", "risk.security", 1)
	seedSankeyOrderUnit(t, ctx, conn, otherOrgID, "wu-4", "foreign-type", "foreign.sub", 1)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	values := func(dim model.DimensionInput, filters *model.FilterInput) []model.CatalogValueItem {
		t.Helper()
		res, _, err := ResolveCatalog(ctx, client, orgID, &dim, filters)
		if err != nil {
			t.Fatalf("ResolveCatalog(%s): %v", dim, err)
		}
		return res.Values
	}

	if got, want := fmt.Sprint(values(model.DimensionInputRepo, nil)), fmt.Sprint([]model.CatalogValueItem{
		{Value: "acme/other", Count: 1}, {Value: "acme/widget", Count: 2},
	}); got != want {
		t.Errorf("repo values = %s, want %s", got, want)
	}
	if got, want := fmt.Sprint(values(model.DimensionInputTeam, nil)), fmt.Sprint([]model.CatalogValueItem{
		{Value: "team-a", Count: 0}, {Value: "team-b", Count: 0},
	}); got != want {
		t.Errorf("team values = %s, want %s", got, want)
	}
	if got, want := fmt.Sprint(values(model.DimensionInputWorkType, nil)), fmt.Sprint([]model.CatalogValueItem{
		{Value: "feature", Count: 2}, {Value: "bug", Count: 1},
	}); got != want {
		t.Errorf("work_type values = %s, want %s", got, want)
	}
	if got, want := fmt.Sprint(values(model.DimensionInputTheme, nil)), fmt.Sprint([]model.CatalogValueItem{
		{Value: "quality", Count: 2}, {Value: "risk", Count: 1},
	}); got != want {
		t.Errorf("theme values = %s, want %s", got, want)
	}
	if got := len(values(model.DimensionInputSubcategory, nil)); got != 3 {
		t.Errorf("subcategory values = %d, want 3", got)
	}
}
