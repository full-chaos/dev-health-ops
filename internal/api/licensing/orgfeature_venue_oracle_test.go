package licensing

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonOrgFeatureProgram runs each case through licensing/gating.py's own
// _check_org_feature_async over the real models on an in-memory async
// SQLite database: the organization row (when the case has one), and an
// optional org_licenses row whose features_override is the case's JSON.
const pythonOrgFeatureProgram = `
import asyncio, json, sys, uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.users import Organization
from dev_health_ops.licensing.gating import _check_org_feature_async
ORG = uuid.UUID("c6a38355-0000-4000-8000-000000000001")
async def run(case):
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for model in (Organization, OrgLicense):
            await conn.run_sync(model.__table__.create)
        if case["org"]:
            await conn.exec_driver_sql("INSERT INTO organizations (id, slug, name, settings, tier, managed_by, is_active, created_at, updated_at) VALUES (?, 's', 'n', '{}', ?, 'x', 1, '2026-01-01', '2026-01-01')", (ORG.hex, case["org_tier"]))
        if case["license"] is not None:
            await conn.exec_driver_sql("INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, features_override, created_at, updated_at) VALUES (?, ?, ?, 1, 'x', 'x', ?, '2026-01-01', '2026-01-01')", (uuid.uuid4().hex, ORG.hex, case["license"]["tier"], case["license"]["override"]))
    async with AsyncSession(engine) as session:
        allowed = await _check_org_feature_async(case["feature"], {"session": session, "org_id": case["org_id"]})
    await engine.dispose()
    return allowed
out = [asyncio.run(run(case)) for case in json.loads(sys.stdin.read())]
print(json.dumps(out))
`

type orgFeatureCase struct {
	OrgID   string             `json:"org_id"`
	Org     bool               `json:"org"`
	OrgTier *string            `json:"org_tier"`
	License *orgFeatureLicense `json:"license"`
	Feature string             `json:"feature"`
}

type orgFeatureLicense struct {
	Tier     string  `json:"tier"`
	Override *string `json:"override"`
}

// orgFeatureRows is the org_licenses and organizations rows a case stores,
// answered as the two queries OrgHasFeature makes read them.
type orgFeatureRows struct{ c orgFeatureCase }

func (rows orgFeatureRows) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic("OrgHasFeature reads single rows only")
}

func (rows orgFeatureRows) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if strings.Contains(sql, "org_licenses") {
		if rows.c.License == nil {
			return scanRow{err: pgx.ErrNoRows}
		}
		var override []byte
		if rows.c.License.Override != nil {
			override = []byte(*rows.c.License.Override)
		}
		tier := rows.c.License.Tier
		return scanRow{values: []any{&tier, override}}
	}
	if !rows.c.Org {
		return scanRow{err: pgx.ErrNoRows}
	}
	return scanRow{values: []any{rows.c.OrgTier}}
}

type scanRow struct {
	values []any
	err    error
}

func (row scanRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index, target := range dest {
		switch typed := target.(type) {
		case **string:
			*typed, _ = row.values[index].(*string)
		case *[]byte:
			*typed, _ = row.values[index].([]byte)
		}
	}
	return nil
}

// orgFeatureCases crosses the rows _check_org_feature_async reads: a
// license row of every tier (valid, other case, unknown) and every
// features_override shape, or the organization's own tier (valid, empty,
// unknown, no row; the column is NOT NULL), for a team feature, an enterprise feature, a
// community feature, an explicit-purchase feature and an unknown key, with
// a valid and an invalid org id.
func orgFeatureCases() []orgFeatureCase {
	org := "c6a38355-0000-4000-8000-000000000001"
	features := []string{"scheduled_jobs", "audit_log", "git_sync", "ask_dev", "no_such_feature"}
	overrides := []*string{nil, text(`null`), text(`{}`), text(`[]`), text(`"x"`), text(`{"scheduled_jobs": true}`),
		text(`{"scheduled_jobs": 1}`), text(`{"scheduled_jobs": 0}`), text(`{"scheduled_jobs": "yes"}`), text(`{"scheduled_jobs": ""}`),
		text(`{"audit_log": true, "ask_dev": [1]}`), text(`{"ask_dev": {}}`), text(`{"no_such_feature": true}`)}
	var cases []orgFeatureCase
	for _, feature := range features {
		for _, tier := range []string{"community", "team", "enterprise", "TEAM", "bogus", ""} {
			for _, override := range overrides {
				cases = append(cases, orgFeatureCase{OrgID: org, Org: true, OrgTier: text("enterprise"),
					License: &orgFeatureLicense{Tier: tier, Override: override}, Feature: feature})
			}
		}
		for _, orgTier := range []*string{text(""), text("community"), text("team"), text("enterprise"), text("Team"), text("gold")} {
			cases = append(cases, orgFeatureCase{OrgID: org, Org: true, OrgTier: orgTier, Feature: feature})
		}
		cases = append(cases,
			orgFeatureCase{OrgID: org, Org: false, Feature: feature},
			orgFeatureCase{OrgID: "not-a-uuid", Org: true, OrgTier: text("enterprise"), Feature: feature},
			orgFeatureCase{OrgID: "C6A38355000040008000000000000001", Org: true, OrgTier: text("enterprise"), Feature: feature},
		)
	}
	return cases
}

// TestOrgFeatureVenueOracleMatchesLivePython requires OrgHasFeature to
// allow exactly where the api's _check_org_feature_async does, for the
// same stored rows.
func TestOrgFeatureVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the org feature oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	cases := orgFeatureCases()
	input, _ := json.Marshal(cases)
	command := exec.Command(python, "-c", pythonOrgFeatureProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []bool
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(cases) {
		t.Fatalf("decode: %v (%d of %d)\n%s", err, len(want), len(cases), output)
	}
	allowed := 0
	for index, c := range cases {
		got, err := OrgHasFeature(context.Background(), orgFeatureRows{c}, c.OrgID, c.Feature)
		if err != nil {
			t.Fatalf("case %d: %v", index, err)
		}
		if got != want[index] {
			encoded, _ := json.Marshal(c)
			t.Errorf("case %d %s: go %v, python %v", index, encoded, got, want[index])
		}
		if got {
			allowed++
		}
	}
	if allowed == 0 || allowed == len(cases) {
		t.Errorf("the corpus did not reach both outcomes: %d of %d allowed", allowed, len(cases))
	}
	t.Logf("%d cases compared, %d allowed", len(cases), allowed)
	venueoracle.WriteProof(t)
}
