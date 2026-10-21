//go:build integration

package syncvenue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	venueKey       = "venue-oracle-integration-sync-key-32-bytes!!"
	venuePinnedNow = "2026-09-24T12:34:56.123456+00:00"
)

// TestIntegrationSyncVenueOracle sends POST .../integrations/{id}/sync to the
// real Python api (which plans the run in the request) and the Go api (which
// hands the run to the scheduler and waits for the real NativeMaterializer,
// running against the Go copy of the database, to plan it), on one pinned
// clock. It requires the same answers and the same sync_runs and
// sync_run_units rows. The rows' ids are random on the Python plane and
// derived from the occurrence on the Go plane, so ids are compared by their
// place of first appearance.
//
// The materializer stamps sync_runs.credential_fingerprint through a decryptor
// keyed like the Python plane's (the column is compared).
//
// Named divergences (CHAOS-6673): sync_runs.triggered_by is "manual" (the
// occurrence table's CHECK) where Python writes "admin-api"; an integration
// that is inactive, has no canonical configuration, or whose configuration is
// neither planner-managed nor pinned is planned by Python and refused with 409
// by the hand-off (nothing written); a plan the scheduler has not made within
// the wait answers 202 "pending" with the occurrence id. The last two kinds
// are asserted below, not compared.
//
// Named divergence (CHAOS-10064, chris 2026-10-17: a trigger that names no
// sources plans the sources a scheduled run of the configuration plans): on
// the incremental integration, which also has an enabled source no
// configuration tagged, Python planned that source too (6 units), the hand-off
// plans the tagged ones (4). Its answer's total_units and that source's unit
// rows and run counts are asserted against the Python answer minus that
// source, not compared as equal.
func TestIntegrationSyncVenueOracle(t *testing.T) {
	golden := venueoracle.OpenGolden(t, venueGolden("integration-sync-venue-oracle", t.Name(), "c26ea95ab447e12f6bd230ee7651ed9ed5515e75c07f529ec8c2763293decd38"))
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-integration-sync!"
	v := newIDs()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden: golden, Root: root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + venueKey,
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seed(t, ctx, admin, venue, v)
			token := func(user, org uuid.UUID, email, role string) map[string]any {
				return map[string]any{"user_id": user.String(), "email": email, "org_id": org.String(), "role": role}
			}
			return map[string]map[string]any{
				"adminA":     token(v.adminA, v.orgA, "sync-admin-a@example.com", "admin"),
				"memberA":    token(v.memberA, v.orgA, "sync-member-a@example.com", "member"),
				"adminNoOrg": {"user_id": v.adminNoOrg.String(), "email": "sync-noorg@example.com", "org_id": "", "role": "admin"},
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
	python := golden.Python(t, venue, same)
	for index, request := range same {
		if request.Name == untaggedRequest && !strings.Contains(python[index].Body, `"total_units":6`) {
			t.Errorf("%s: python %s (the divergence assumes Python planned 6 units)", request.Name, python[index].Body)
		}
	}
	receipt := venueoracle.Diff(t, base, same, python, venueoracle.DiffOptions{Golden: golden, Normalize: normalizeRuled,
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			if request.Name == untaggedRequest && !strings.Contains(goResponse.Body, `"total_units":4`) {
				t.Errorf("%s: go %d %s, want 4 units (the two tagged enabled sources, two datasets each)", request.Name, goResponse.Status, goResponse.Body)
			}
		}})
	t.Logf("receipt (%d requests):\n%s", len(same), receipt)

	// The rows each plane planned, columns that are ids or a ruled difference
	// left out. The integrations the diverging requests name are excluded:
	// Python plans them, the hand-off refuses them.
	refused := fmt.Sprintf("'%s','%s','%s','%s'", v.inactive.id, v.noConfig.id, v.unmanaged.id, v.pending.id)
	// minSeparators: rows are joined by " | "; seven runs and fifteen units are planned.
	compare := []struct {
		name, query   string
		minSeparators int
	}{
		{"sync_runs", `SELECT org_id, integration_id::text, mode, status, total_units, completed_units, failed_units, credential_id::text, credential_fingerprint,
auth_source, started_at, completed_at, result::text, error FROM sync_runs WHERE integration_id NOT IN (` + refused + `)
ORDER BY integration_id::text, mode`, 6},
		{"sync_run_units", `SELECT u.org_id, u.integration_id::text, u.source_id::text, u.provider, u.dataset_key, u.cost_class, u.mode, u.since_at, u.before_at,
u.status, u.attempts, u.processor_flags::text FROM sync_run_units u WHERE u.integration_id NOT IN (` + refused + `)
ORDER BY u.integration_id::text, u.source_id::text, u.dataset_key`, 14},
	}
	var untaggedSource string
	if err := schedulerPool.QueryRow(ctx, `SELECT id::text FROM integration_sources WHERE external_id = 'acme/incremental-untagged'`).Scan(&untaggedSource); err != nil {
		t.Fatalf("the untagged source: %v", err)
	}
	for _, table := range compare {
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), table.query)
		pythonRows := golden.InspectRows(t, table.name)
		if want := withoutUntaggedSource(t, table.name, pythonRows, v.incremental.id.String(), untaggedSource); goRows != want {
			t.Errorf("%s differs after the requests (python minus the untagged source):\n python: %s\n go:     %s", table.name, want, goRows)
		}
		if strings.Count(pythonRows, " | ") < table.minSeparators {
			t.Errorf("%s: too few rows compared:\n%s", table.name, pythonRows)
		}
	}

	// Ruled divergences, both planes asked.
	diverging := divergingRequests(venue, v)
	pythonDiverging := golden.Python(t, venue, diverging)
	golden.Consumed(t, pythonDiverging...)
	for index, request := range diverging {
		goResponse := venueoracle.Do(t, base, request)
		if pythonDiverging[index].Status != 202 || !strings.Contains(pythonDiverging[index].Body, `"status":"accepted"`) {
			t.Errorf("%s: python %d %s (the divergence assumes Python plans it)", request.Name, pythonDiverging[index].Status, pythonDiverging[index].Body)
		}
		if goResponse.Status != 409 || goResponse.Body != `{"detail":"Integration has no sync configuration the scheduler can run"}` {
			t.Errorf("%s: go %d %s", request.Name, goResponse.Status, goResponse.Body)
		}
	}
	var refusedOccurrences int
	if err := schedulerPool.QueryRow(ctx, `SELECT count(*) FROM scheduled_sync_occurrences o JOIN sync_configurations c ON c.id = o.sync_config_id
WHERE c.integration_id = ANY($1::uuid[])`, []uuid.UUID{v.inactive.id, v.noConfig.id, v.unmanaged.id}).Scan(&refusedOccurrences); err != nil || refusedOccurrences != 0 {
		t.Errorf("a refused hand-off wrote %d occurrences (err %v)", refusedOccurrences, err)
	}

	// The scheduler stopped: the wait ends with 202 "pending" and the
	// occurrence id, and the occurrence and its trigger are there for it.
	paused.Store(true)
	pending := venueoracle.Do(t, base, post(venue, "scheduler stopped", v.pending.id.String(), "adminA", venueoracle.B64(`{"full_resync": true}`)))
	if pending.Status != 202 || !regexp.MustCompile(`^\{"status":"pending","integration_id":"`+v.pending.id.String()+`","occurrence_id":"sha256:[0-9a-f]{64}"\}$`).MatchString(pending.Body) {
		t.Errorf("scheduler stopped: %d %s", pending.Status, pending.Body)
	}
	var status, mode, triggeredBy string
	if err := schedulerPool.QueryRow(ctx, `SELECT o.reconcile_status, m.mode, m.triggered_by FROM scheduled_sync_occurrences o
JOIN sync_manual_triggers m USING (occurrence_id) JOIN sync_configurations c ON c.id = o.sync_config_id WHERE c.integration_id = $1`, v.pending.id).Scan(&status, &mode, &triggeredBy); err != nil {
		t.Fatalf("pending occurrence: %v", err)
	}
	if status != "pending" || mode != "full_resync" || triggeredBy != "manual" {
		t.Errorf("pending occurrence: status %s mode %s triggered_by %s", status, mode, triggeredBy)
	}
	paused.Store(false)
	golden.Finish(t)
}

var anyUUID = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// normalize names each distinct uuid of a "clock: " request by its first
// appearance, so a run named twice reads as one run and two runs never do.
// syncRunID is the run id field of an accepted answer, as the golden holds it
// (<id>) or as the Go api answers it (a uuid).
var syncRunID = regexp.MustCompile(`"sync_run_id":"(?:<id>|[0-9a-fA-F-]{36})"`)

func normalize(request venueoracle.Request, body string) string {
	if !strings.HasPrefix(request.Name, "clock: ") {
		return body
	}
	// The run id is a ruled difference: the Python plane draws a random one (the
	// scrub blanked it in the golden) and the Go api derives its own.
	body = syncRunID.ReplaceAllString(body, `"sync_run_id":"<id>"`)
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
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test's source file")
	}
	directory := filepath.Dir(file)
	for {
		if info, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil && !info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no go.mod above %s", file)
		}
		directory = parent
	}
}

// untaggedRequest is the request whose answer is the CHAOS-10064 divergence.
const untaggedRequest = "clock: incremental, every enabled source"

var totalUnits = regexp.MustCompile(`"total_units":\d+`)

// normalizeRuled is normalize, with the untagged request's total_units left
// out of the comparison (Inspect and the Python check pin both values).
func normalizeRuled(request venueoracle.Request, body string) string {
	body = normalize(request, body)
	if request.Name == untaggedRequest {
		body = totalUnits.ReplaceAllString(body, `"total_units":"<ruled CHAOS-10064>"`)
	}
	return body
}

// withoutUntaggedSource is the Python plane's rows as the hand-off writes them
// under CHAOS-10064: the untagged source's two units are not planned, and its
// run counts two units fewer (6 to 4). Every other row is unchanged.
func withoutUntaggedSource(t *testing.T, table, pythonRows, integration, source string) string {
	t.Helper()
	lines := strings.Split(pythonRows, " | ")
	switch table {
	case "sync_run_units":
		kept := make([]string, 0, len(lines))
		for _, line := range lines {
			if !strings.Contains(line, integration+source) {
				kept = append(kept, line)
			}
		}
		if len(lines)-len(kept) != 2 {
			t.Fatalf("sync_run_units: the untagged source has %d python units, want 2", len(lines)-len(kept))
		}
		return strings.Join(kept, " | ")
	case "sync_runs":
		from, to := integration+"incrementalplanned6 0 0", integration+"incrementalplanned4 0 0"
		if strings.Count(pythonRows, from) != 1 {
			t.Fatalf("sync_runs: python has %d runs of the incremental integration with 6 planned units, want 1", strings.Count(pythonRows, from))
		}
		return strings.Replace(pythonRows, from, to, 1)
	default:
		t.Fatalf("no CHAOS-10064 rule for %s", table)
		return ""
	}
}
