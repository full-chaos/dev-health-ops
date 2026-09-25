//go:build integration

package backfillvenue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	venueKey       = "venue-oracle-integration-backfill-key-32b!!"
	venuePinnedNow = "2026-09-24T12:34:56.123456+00:00"
)

// TestIntegrationBackfillVenueOracle sends POST .../integrations/{id}/backfill
// to the real Python api (which plans the run in the request) and the Go api
// (which hands the run to the scheduler and waits for the real
// NativeMaterializer, running against the Go copy of the database, to plan
// it), on one pinned clock. It requires the same answers and the same
// sync_runs and sync_run_units rows (the backfill windows included). The rows'
// ids are random on the Python plane and derived from the occurrence on the Go
// plane, so ids are compared by their place of first appearance.
//
// The materializer stamps sync_runs.credential_fingerprint through a decryptor
// keyed like the Python plane's (the column is compared).
//
// Named divergences (CHAOS-6674): sync_runs.triggered_by is "backfill" where Python
// writes "admin-api"; an integration that is inactive, has no canonical
// configuration, or whose configuration is neither planner-managed nor pinned
// is planned by Python and refused with 409 by the hand-off (nothing
// written); a window with since not before before is refused by both, in
// different words (Python's planner only when it plans a unit); a plan the
// scheduler has not made within the wait answers 202 "pending" with the
// occurrence id. These are asserted below, not compared.
func TestIntegrationBackfillVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-integration-backfill!"
	v := newIDs()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + venueKey,
			"VENUE_PINNED_NOW=" + venuePinnedNow,
			"VENUE_PINNED_NOW_MODULES=dev_health_ops.sync.planner,dev_health_ops.sync.watermarks,dev_health_ops.models.integrations",
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seed(t, ctx, admin, venue, v)
			token := func(user, org uuid.UUID, email, role string) map[string]any {
				return map[string]any{"user_id": user.String(), "email": email, "org_id": org.String(), "role": role}
			}
			return map[string]map[string]any{
				"adminA":     token(v.adminA, v.orgA, "bf-admin-a@example.com", "admin"),
				"memberA":    token(v.memberA, v.orgA, "bf-member-a@example.com", "member"),
				"adminNoOrg": {"user_id": v.adminNoOrg.String(), "email": "bf-noorg@example.com", "org_id": "", "role": "admin"},
			}
		},
	})
	pinned, err := time.Parse(time.RFC3339Nano, venuePinnedNow)
	if err != nil {
		t.Fatal(err)
	}
	base := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Now = func() time.Time { return pinned }
		deps.IntegrationHandoffWait = 3 * time.Second
	})

	// The scheduler: the real materializer and occurrence reconciler over the
	// Go copy (as its superuser: the api role is the one under test).
	var paused atomic.Bool
	schedulerPool, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(schedulerPool.Close)
	// The credential fingerprint the run is stamped with is read through a
	// decryptor keyed like the Python plane's SETTINGS_ENCRYPTION_KEY (the
	// materializer refuses a credential-backed occurrence without one).
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(venueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := schedsync.NewNativeMaterializer(schedulerPool)
	if err != nil {
		t.Fatal(err)
	}
	materializer.WithCredentialFingerprint(decryptor)
	reconciler, err := schedsync.NewOccurrenceReconciler(schedulerPool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	loopCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for loopCtx.Err() == nil {
			if !paused.Load() {
				if _, err := reconciler.Reconcile(loopCtx, pinned.Add(time.Minute), 50); err != nil && loopCtx.Err() == nil {
					t.Logf("reconcile: %v", err)
				}
			}
			select {
			case <-loopCtx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() { stop(); <-done })

	same := sameRequests(venue, v)
	python := venue.ServePython(t, same)
	receipt := venueoracle.Diff(t, base, same, python, venueoracle.DiffOptions{Normalize: normalize})
	t.Logf("receipt (%d requests):\n%s", len(same), receipt)

	// The rows each plane planned, columns that are ids or a ruled difference
	// left out. The integrations the ruled requests name are excluded: Python
	// plans some of them, the hand-off refuses or fails them.
	excluded := []string{}
	for _, name := range append(append(append([]string{}, refusedNames...), windowNames...), otherNames...) {
		excluded = append(excluded, "'"+v.get(name).id.String()+"'")
	}
	notIn := strings.Join(excluded, ",")
	// minSeparators: rows are joined by " | ".
	compare := []struct {
		name, query   string
		minSeparators int
	}{
		{"sync_runs", `SELECT org_id, integration_id::text, mode, status, total_units, completed_units, failed_units, credential_id::text, credential_fingerprint,
auth_source, started_at, completed_at, result::text, error FROM sync_runs WHERE integration_id::text NOT IN (` + notIn + `)
ORDER BY integration_id::text, mode`, 8},
		{"sync_run_units", `SELECT u.org_id, u.integration_id::text, u.source_id::text, u.provider, u.dataset_key, u.cost_class, u.mode, u.since_at, u.before_at,
u.status, u.attempts, u.processor_flags::text FROM sync_run_units u WHERE u.integration_id::text NOT IN (` + notIn + `)
ORDER BY u.integration_id::text, u.source_id::text, u.dataset_key, u.since_at`, 40},
	}
	for _, table := range compare {
		pythonRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), table.query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), table.query)
		if pythonRows != goRows {
			t.Errorf("%s differ\n python:\n%.4000s\n go:\n%.4000s", table.name, pythonRows, goRows)
		}
		if strings.Count(pythonRows, " | ") < table.minSeparators {
			t.Errorf("%s: too few rows compared:\n%.2000s", table.name, pythonRows)
		}
	}

	// Ruled divergence: planned by Python, refused by the hand-off.
	refused := refusedRequests(venue, v)
	pythonRefused := venue.ServePython(t, refused)
	for index, request := range refused {
		goResponse := venueoracle.Do(t, base, request)
		if pythonRefused[index].Status != 202 || !strings.Contains(pythonRefused[index].Body, `"status":"accepted"`) {
			t.Errorf("%s: python %d %s (the divergence assumes Python plans it)", request.Name, pythonRefused[index].Status, pythonRefused[index].Body)
		}
		if goResponse.Status != 409 || goResponse.Body != `{"detail":"Integration has no sync configuration the scheduler can run"}` {
			t.Errorf("%s: go %d %s", request.Name, goResponse.Status, goResponse.Body)
		}
	}
	var refusedOccurrences int
	refusedIDs := []uuid.UUID{v.get("inactive").id, v.get("noconfig").id, v.get("unmanaged").id}
	if err := schedulerPool.QueryRow(ctx, `SELECT count(*) FROM scheduled_sync_occurrences o JOIN sync_configurations c ON c.id = o.sync_config_id
WHERE c.integration_id = ANY($1::uuid[])`, refusedIDs).Scan(&refusedOccurrences); err != nil || refusedOccurrences != 0 {
		t.Errorf("a refused hand-off wrote %d occurrences (err %v)", refusedOccurrences, err)
	}

	// Ruled divergence: a window whose since is not before its before. Both
	// planes refuse it with 400, in their own words; the scheduler's planner
	// quarantines the occurrence, which the route reports.
	windows := windowRequests(venue, v)
	pythonWindows := venue.ServePython(t, windows)
	for index, request := range windows {
		goResponse := venueoracle.Do(t, base, request)
		if pythonWindows[index].Status != 400 || pythonWindows[index].Body != `{"detail":"Backfill since must be before"}` {
			t.Errorf("%s: python %d %s", request.Name, pythonWindows[index].Status, pythonWindows[index].Body)
		}
		if goResponse.Status != 400 || !strings.HasPrefix(goResponse.Body, `{"detail":"Sync plan rejected: `) {
			t.Errorf("%s: go %d %s", request.Name, goResponse.Status, goResponse.Body)
		}
	}

	// The scheduler stopped: the wait ends with 202 "pending" and the
	// occurrence id, and the occurrence and its trigger are there for it.
	paused.Store(true)
	pending := venueoracle.Do(t, base, post(venue, "scheduler stopped", v.get("pending").id.String(), "adminA",
		venueoracle.B64(`{`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`}`)))
	if pending.Status != 202 || !regexp.MustCompile(`^\{"status":"pending","integration_id":"`+v.get("pending").id.String()+`","occurrence_id":"sha256:[0-9a-f]{64}"\}$`).MatchString(pending.Body) {
		t.Errorf("scheduler stopped: %d %s", pending.Status, pending.Body)
	}
	var status, mode, triggeredBy string
	var since, before time.Time
	if err := schedulerPool.QueryRow(ctx, `SELECT o.reconcile_status, m.mode, m.triggered_by, m.since, m.before FROM scheduled_sync_occurrences o
JOIN sync_manual_triggers m USING (occurrence_id) JOIN sync_configurations c ON c.id = o.sync_config_id WHERE c.integration_id = $1`, v.get("pending").id).Scan(&status, &mode, &triggeredBy, &since, &before); err != nil {
		t.Fatalf("pending occurrence: %v", err)
	}
	if status != "pending" || mode != "backfill" || triggeredBy != "backfill" ||
		!since.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)) || !before.Equal(time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("pending occurrence: status %s mode %s triggered_by %s since %s before %s", status, mode, triggeredBy, since, before)
	}
	paused.Store(false)
}

var anyUUID = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// normalize names each distinct uuid of a "clock: " request by its first
// appearance, so a run named twice reads as one run and two runs never do.
func normalize(request venueoracle.Request, body string) string {
	if !strings.HasPrefix(request.Name, "clock: ") {
		return body
	}
	seen := map[string]int{}
	return anyUUID.ReplaceAllStringFunc(body, func(id string) string {
		if _, ok := seen[id]; !ok {
			seen[id] = len(seen) + 1
		}
		return fmt.Sprintf("<uuid#%d>", seen[id])
	})
}

func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string, adjust func(*apiservice.Deps)) string {
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
	deps := apiservice.Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger)}
	adjust(&deps)
	routes := apiservice.Routes(deps, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

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
