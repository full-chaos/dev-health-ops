//go:build integration

package adminstampvenue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// stamps are the stored updated_at values (ClickHouse DateTime64(6), naive).
// Each one is a rendering the two planes could disagree on: a trailing zero
// in the microseconds (Go's trimming layout drops it), a leading-zero
// microsecond, a single microsecond, a whole second (Python prints no
// fraction), and the largest fraction.
var stamps = []string{
	"2026-09-21 11:37:05.895620", // trailing zero: ...895620 vs ...89562
	"2026-09-21 11:37:05.895335", // plain six digits
	"2026-09-21 11:37:05.000001", // one microsecond
	"2026-09-21 11:37:05.100000", // five trailing zeros
	"2026-09-21 11:37:05.000000", // whole second: no fraction at all
	"2026-09-21 11:37:05.999999", // largest fraction
	"2026-01-02 03:04:05.050000", // leading zero + trailing zeros
}

// TestAdminTimestampVenueOracle sends GET /admin/teams and GET
// /admin/identities (and each team by id) to the real Python api and the Go
// api, over two ClickHouse databases seeded with the same rows carrying
// microsecond timestamps, and requires byte-identical answers: the compare
// is raw response text (R398), so a "Z" suffix or a trimmed zero is a diff.
func TestAdminTimestampVenueOracle(t *testing.T) {
	runAdminTimestampVenue(t)
}

// TestAdminTimestampServerZoneVenueOracle is the same comparison against a
// ClickHouse server whose zone is not UTC: clickhouse-connect then returns
// aware datetimes in that zone and the Python api prints their offset, so the
// Go route must too. Both a winter and a summer stamp are seeded, so the
// offset changes with daylight saving.
func TestAdminTimestampServerZoneVenueOracle(t *testing.T) {
	t.Setenv(containers.ClickHouseTimezoneEnv, "America/Los_Angeles")
	runAdminTimestampVenue(t)
}

func runAdminTimestampVenue(t *testing.T) {
	zone := os.Getenv(containers.ClickHouseTimezoneEnv)
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-admin-stamps-32b!"
	orgID, adminID, memberID := uuid.New(), uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at)
VALUES ($1, 'stamps', 'stamps', '{}', 'community', true, now(), now())`, orgID)
			for _, user := range []struct {
				id    uuid.UUID
				email string
				role  string
			}{{adminID, "admin-stamps@example.com", "admin"}, {memberID, "member-stamps@example.com", "member"}} {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, user.id, user.email)
				exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, now(), now())`,
					uuid.New(), user.id, orgID, user.role)
			}
			return map[string]map[string]any{
				"admin":  {"user_id": adminID.String(), "email": "admin-stamps@example.com", "org_id": orgID.String(), "role": "admin"},
				"member": {"user_id": memberID.String(), "email": "member-stamps@example.com", "org_id": orgID.String(), "role": "member"},
			}
		},
	})
	teamIDs := seedClickHouse(t, ctx, venue, orgID.String())
	base := startGoServer(t, ctx, venue, jwtKey)

	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	get := func(name, path string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: "/api/v1/admin" + path, Headers: bearer("admin")}
	}
	requests := []venueoracle.Request{
		get("teams", "/teams"), get("teams inactive too", "/teams?active_only=false"),
		get("identities", "/identities"), get("identities inactive too", "/identities?active_only=false"),
		{Name: "teams member refused", Method: "GET", Path: "/api/v1/admin/teams", Headers: bearer("member")},
	}
	for _, id := range teamIDs {
		requests = append(requests, get("team "+id, "/teams/"+id))
	}
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		// The seed must reach every stored shape, or a SAME could be two
		// empty lists agreeing: each stamp's Python rendering must appear
		// in the Go answer of the list routes.
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			if request.Name != "teams inactive too" && request.Name != "identities inactive too" {
				return
			}
			for _, want := range wantRendered(zone) {
				if !strings.Contains(goResponse.Body, `"updated_at":"`+want+`"`) {
					t.Errorf("%s: body lacks updated_at %s:\n%s", request.Name, want, goResponse.Body)
				}
			}
		},
	})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)
}

// wantRendered is Python's rendering of each stamp: a naive datetime (no
// zone, no fraction when the microseconds are zero) on a UTC server, and on a
// server in zone the stamp's wall clock with that zone's offset on that date.
func wantRendered(zone string) []string {
	var location *time.Location
	if zone != "" {
		var err error
		if location, err = time.LoadLocation(zone); err != nil {
			panic(err)
		}
	}
	out := make([]string, 0, len(stamps))
	for _, stamp := range stamps {
		text := strings.Replace(stamp, " ", "T", 1)
		text = strings.TrimSuffix(text, ".000000")
		if location != nil {
			at, err := time.ParseInLocation("2006-01-02 15:04:05.000000", stamp, location)
			if err != nil {
				panic(err)
			}
			text += at.Format("-07:00")
		}
		out = append(out, text)
	}
	return out
}

// seedClickHouse writes one team and one identity per stamp into BOTH
// planes' databases and returns the team ids.
func seedClickHouse(t *testing.T, ctx context.Context, venue *venueoracle.Venue, orgID string) []string {
	t.Helper()
	var statements []string
	var teamIDs []string
	for i, stamp := range stamps {
		teamID := fmt.Sprintf("team-%d", i)
		teamIDs = append(teamIDs, teamID)
		active := 1
		if i == len(stamps)-1 {
			active = 0 // one inactive row: only active_only=false shows it
		}
		statements = append(statements,
			fmt.Sprintf(`INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id)
VALUES ('%s', '%s', 'Team %d', NULL, [], [], [], [], %d, '%s', '%s', '%s')`, teamID, uuid.New(), i, active, stamp, stamp, orgID),
			fmt.Sprintf(`INSERT INTO identities (org_id, canonical_id, identity_uuid, display_name, email, provider_identities, team_ids, is_active, updated_at)
VALUES ('%s', 'user-%d@example.com', '%s', NULL, 'user-%d@example.com', '{}', ['%s'], %d, '%s')`, orgID, i, uuid.New(), i, teamID, active, stamp))
	}
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatalf("seed clickhouse %s: %v", database, err)
		}
		for _, statement := range statements {
			if err := conn.Exec(ctx, statement); err != nil {
				_ = conn.Close()
				t.Fatalf("seed clickhouse %s: %v\n%s", database, err, statement)
			}
		}
		_ = conn.Close()
	}
	return teamIDs
}

func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatalf("go pool: %v", err)
	}
	t.Cleanup(pool.Close)
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatalf("authenticator: %v", err)
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	clickHouse, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.GoAPIClickHouseURI(t)))
	if err != nil {
		t.Fatalf("go clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	routes := apiservice.Routes(apiservice.Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: policy.NewGuard(auth, logger)}, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// repoRoot walks up from this file to the directory holding src/dev_health_ops.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test's source file")
	}
	directory := filepath.Dir(file)
	for {
		if info, err := os.Stat(filepath.Join(directory, "src", "dev_health_ops")); err == nil && info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no src/dev_health_ops above %s", file)
		}
		directory = parent
	}
}
