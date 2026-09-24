//go:build integration

// Team scope through the four production handlers -- GET and POST
// /api/v1/work-units and /api/v1/home -- against a real ClickHouse migrated by
// the real chain. One organization: a team owning two repositories, a second
// team owning one, a third owning none, and a repository owned by nobody. Each
// handler must answer, for each team, exactly the work its owned repositories
// hold.
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

const teamRoutesOrg = "teamroutes-org"

// seedTeamRoutesChurn writes one repo_metrics_daily row inside the home window
// (end_date 2026-08-25, 7 days): total_loc_touched is the churn metric's source.
func seedTeamRoutesChurn(t *testing.T, conn chdriver.Conn, repoID uuid.UUID, churn uint32) {
	t.Helper()
	if err := conn.Exec(context.Background(),
		`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
		repoID, time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), churn,
		time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), teamRoutesOrg); err != nil {
		t.Fatalf("seed repo_metrics_daily: %v", err)
	}
}

// seedTeamRoutesRework writes one investment_metrics_daily row keyed by team id:
// the source of home's rework allocation, which the team scope reads by column.
func seedTeamRoutesRework(t *testing.T, conn chdriver.Conn, teamID string) {
	t.Helper()
	if err := conn.Exec(context.Background(),
		`INSERT INTO investment_metrics_daily (repo_id, day, team_id, investment_area, project_stream, work_items_completed, prs_merged, churn_loc, computed_at, org_id)
		 VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), teamID, "feature_delivery", "core",
		uint32(5), uint32(2), uint64(40), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), teamRoutesOrg); err != nil {
		t.Fatalf("seed investment_metrics_daily: %v", err)
	}
}

func serveTeamRoutes(t *testing.T, handler http.HandlerFunc, method, target, body string) []byte {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	req := httptest.NewRequest(method, target, reader)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: teamRoutesOrg}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %s = HTTP %d: %s", method, target, rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes()
}

func workUnitIDsIn(t *testing.T, raw []byte) []string {
	t.Helper()
	var units []struct {
		WorkUnitID string `json:"work_unit_id"`
	}
	if err := json.Unmarshal(raw, &units); err != nil {
		t.Fatalf("decode work units: %v: %s", err, raw)
	}
	ids := make([]string, 0, len(units))
	for _, unit := range units {
		ids = append(ids, unit.WorkUnitID)
	}
	sort.Strings(ids)
	return ids
}

func homeChurnIn(t *testing.T, raw []byte) float64 {
	t.Helper()
	var body struct {
		Deltas []struct {
			Metric string  `json:"metric"`
			Value  float64 `json:"value"`
		} `json:"deltas"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode home: %v", err)
	}
	for _, delta := range body.Deltas {
		if delta.Metric == "churn" {
			return delta.Value
		}
	}
	t.Fatalf("home response carries no churn delta: %s", raw)
	return 0
}

func TestTeamScopeRoutes_OwnedRepositoriesOnly(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// team-two owns A (with its repo_id) and B (name only, the autoimport
	// shape); team-one owns C; team-none owns nothing; D is owned by nobody.
	repoA, repoB, repoC, repoD := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	seedTeamScopeRepo(t, conn, teamRoutesOrg, "acme/a", repoA)
	seedTeamScopeRepo(t, conn, teamRoutesOrg, "acme/b", repoB)
	seedTeamScopeRepo(t, conn, teamRoutesOrg, "acme/c", repoC)
	seedTeamScopeRepo(t, conn, teamRoutesOrg, "acme/d", repoD)
	seedTeamScopeOwnership(t, conn, teamRoutesOrg, "team-two", "acme/a", "exact", "inferred", &repoA, validFrom, nil, validFrom)
	seedTeamScopeOwnership(t, conn, teamRoutesOrg, "team-two", "acme/b", "exact", "provider_access", nil, validFrom, nil, validFrom)
	seedTeamScopeOwnership(t, conn, teamRoutesOrg, "team-one", "acme/c", "exact", "inferred", &repoC, validFrom, nil, validFrom)
	// Another organization's ownership row: same team id, and it names the
	// unowned repository D by id. It must not put D inside team-two's scope.
	seedTeamScopeOwnership(t, conn, "teamroutes-other-org", "team-two", "acme/d", "exact", "inferred", &repoD, validFrom, nil, validFrom)
	for unit, repo := range map[string]uuid.UUID{"wu-a": repoA, "wu-b": repoB, "wu-c": repoC, "wu-d": repoD} {
		seedTeamScopeWorkUnit(t, conn, teamRoutesOrg, unit, repo, 10)
	}
	for repo, churn := range map[uuid.UUID]uint32{repoA: 10, repoB: 20, repoC: 400, repoD: 8000} {
		seedTeamRoutesChurn(t, conn, repo, churn)
	}

	seedTeamRoutesRework(t, conn, "team-two")

	reader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	unitsGet, unitsPost := newWorkUnitsGetHandler(reader), newWorkUnitsPostHandler(reader)
	homeGet, homePost := newHomeGetHandler(client, nil), newHomePostHandler(client, nil)

	const window = "start_date=2026-08-01&end_date=2026-08-31"
	unitsPostBody := func(scope string) string {
		return `{"filters":{` + scope + `,"time":{"start_date":"2026-08-01","end_date":"2026-08-31"}}}`
	}
	homePostBody := func(scope string) string {
		return `{"filters":{` + scope + `,"time":{"range_days":7,"compare_days":7,"end_date":"2026-08-25"}}}`
	}
	teamScope := func(id string) string { return `"scope":{"level":"team","ids":["` + id + `"]}` }

	for _, cell := range []struct {
		name       string
		team       string // "" is the organization scope
		wantUnits  []string
		wantChurn  float64
		ownedCount int
		// wantHomeDeclaration/wantUnitsDeclaration: whether the corpus's own
		// BaselineTimeoutDeclared paths hold on the real handler's body. Home's
		// rework allocation is keyed by team column and is seeded for team-two
		// only, so a team without it reads as empty there and the declaration
		// would refuse instead of admitting on silence.
		wantHomeDeclaration, wantUnitsDeclaration bool
	}{
		{"two owned repositories, one unowned repository present", "team-two", []string{"wu-a", "wu-b"}, 30, 2, true, true},
		{"one owned repository", "team-one", []string{"wu-c"}, 400, 1, false, true},
		{"no owned repository", "team-none", []string{}, 0, 0, false, false},
		{"organization scope reads every repository", "", []string{"wu-a", "wu-b", "wu-c", "wu-d"}, 8430, 4, false, true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var unitsQuery, homeQuery, unitsScope, homeScope string
			if cell.team == "" {
				unitsQuery, homeQuery = "/api/v1/work-units?"+window, "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"
				unitsScope, homeScope = `"scope":{"level":"org"}`, `"scope":{"level":"org"}`
			} else {
				unitsQuery = "/api/v1/work-units?scope_type=team&scope_id=" + cell.team + "&" + window
				homeQuery = "/api/v1/home?scope_type=team&scope_id=" + cell.team + "&range_days=7&compare_days=7&end_date=2026-08-25"
				unitsScope, homeScope = teamScope(cell.team), teamScope(cell.team)
			}

			bodies := map[string][]byte{
				"REST:GET:/api/v1/work-units":  serveTeamRoutes(t, unitsGet, http.MethodGet, unitsQuery, ""),
				"REST:POST:/api/v1/work-units": serveTeamRoutes(t, unitsPost, http.MethodPost, "/api/v1/work-units", unitsPostBody(unitsScope)),
				"REST:GET:/api/v1/home":        serveTeamRoutes(t, homeGet, http.MethodGet, homeQuery, ""),
				"REST:POST:/api/v1/home":       serveTeamRoutes(t, homePost, http.MethodPost, "/api/v1/home", homePostBody(homeScope)),
			}
			for _, operation := range []string{"REST:GET:/api/v1/work-units", "REST:POST:/api/v1/work-units"} {
				if got := workUnitIDsIn(t, bodies[operation]); strings.Join(got, ",") != strings.Join(cell.wantUnits, ",") {
					t.Errorf("%s for %q = %v, want exactly %v", operation, cell.team, got, cell.wantUnits)
				}
			}
			for _, operation := range []string{"REST:GET:/api/v1/home", "REST:POST:/api/v1/home"} {
				if got := homeChurnIn(t, bodies[operation]); got != cell.wantChurn {
					t.Errorf("%s churn for %q = %v, want %v (the sum over exactly its %d owned repositories)", operation, cell.team, got, cell.wantChurn, cell.ownedCount)
				}
			}
			// The corpus's declared non-empty paths, evaluated on the real
			// handlers' own bodies (the four team-scoped requests only).
			if cell.team == "" {
				return
			}
			for operation, want := range map[string]bool{
				"REST:GET:/api/v1/work-units": cell.wantUnitsDeclaration, "REST:POST:/api/v1/work-units": cell.wantUnitsDeclaration,
				"REST:GET:/api/v1/home": cell.wantHomeDeclaration, "REST:POST:/api/v1/home": cell.wantHomeDeclaration,
			} {
				spec, err := goapiproof.SpecForREST(operation)
				if err != nil {
					t.Fatal(err)
				}
				var declared *goapiproof.BaselineTimeoutDeclaration
				for _, request := range spec.Requests {
					if request.BaselineTimeoutDeclared != nil {
						declared = request.BaselineTimeoutDeclared
					}
				}
				if declared == nil {
					t.Fatalf("%s carries no declaration", operation)
				}
				snapshot, err := goapiproof.DecodeRESTSnapshot(bodies[operation])
				if err != nil {
					t.Fatalf("%s: %v", operation, err)
				}
				holds := true
				for _, path := range declared.NonEmptyPaths {
					holds = holds && goapiproof.SnapshotHoldsNonEmpty(snapshot, path)
				}
				if holds != want {
					t.Errorf("%s for %q: declared NonEmptyPaths %v hold=%v, want %v", operation, cell.team, declared.NonEmptyPaths, holds, want)
				}
			}
		})
	}
}
