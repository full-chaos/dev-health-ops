package joboperator

import (
	"context"
	"os"
	"regexp"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
)

// TestEveryAuditedMutationIsInAuditedActions drives every Service method
// that writes an audit row, each on a fresh fixture, and collects the
// Action each one passes to Auditor.Begin. The set must equal
// serviceAuditedActions exactly: a new audited mutation fails here until it
// is listed (and the audit action check is widened to allow it). The
// direct-write verbs' Actions are pinned in workersctl, where the verbs are.
func TestEveryAuditedMutationIsInAuditedActions(t *testing.T) {
	ctx := context.Background()
	registry, err := jobruntime.Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal()
	calls := map[string]func(*serviceFixture) error{
		"Cancel": func(f *serviceFixture) error {
			f.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)
			_, err := f.service.Cancel(ctx, principal, 42, "operator_request", "corr-1")
			return err
		},
		"Retry": func(f *serviceFixture) error {
			f.backend.job = jobSummary(StateDiscarded, jobcontract.KindRetentionCleanup, "retention", 3)
			_, err := f.service.Retry(ctx, principal, 42, "operator_request", "corr-1")
			return err
		},
		"PauseQueue": func(f *serviceFixture) error {
			return f.service.PauseQueue(ctx, principal, "heartbeat", "incident_response", "corr-1")
		},
		"ResumeQueue": func(f *serviceFixture) error {
			return f.service.ResumeQueue(ctx, principal, "heartbeat", "incident_response", "corr-1")
		},
		"Drain": func(f *serviceFixture) error {
			_, err := f.service.Drain(ctx, principal, "ops", []string{"heartbeat", "retention"}, "deploy_drain", "corr-1")
			return err
		},
		"Undrain": func(f *serviceFixture) error {
			_, err := f.service.Undrain(ctx, principal, "ops", []string{"heartbeat", "retention"}, "deploy_resume", "corr-1")
			return err
		},
		"ApplyCheckedInRoute": func(f *serviceFixture) error {
			f.routes.state = syncroute.RouteState{Kind: "reference_discovery", Transport: "river", Generation: 2}
			_, err := f.service.ApplyCheckedInRoute(ctx, principal, "reference_discovery", "local_start", "corr-1")
			return err
		},
		"PauseRoute": func(f *serviceFixture) error {
			f.routes.state = syncroute.RouteState{Kind: "dispatch_sync_run", Transport: "celery", Generation: 2, Paused: true}
			_, err := f.service.PauseRoute(ctx, principal, "dispatch_sync_run", "cutover", "corr-1")
			return err
		},
		"DrainRoute": func(f *serviceFixture) error {
			f.routes.state = syncroute.RouteState{Kind: "dispatch_sync_run", Transport: "celery", Generation: 2}
			_, err := f.service.DrainRoute(ctx, principal, "dispatch_sync_run", "cutover", "corr-1")
			return err
		},
		"ResumeRoute": func(f *serviceFixture) error {
			f.routes.state = syncroute.RouteState{Kind: "dispatch_sync_run", Transport: "river", Generation: 2}
			_, err := f.service.ResumeRoute(ctx, principal, "dispatch_sync_run", "river", "cutover", "corr-1", time.Second)
			return err
		},
		"ApplyCheckedInJobRoute": func(f *serviceFixture) error {
			f.jobRoutes.state = jobroute.State{Kind: jobcontract.KindBillingNotification, Transport: "river_canary", Generation: 3}
			_, err := f.service.ApplyCheckedInJobRoute(ctx, principal, jobcontract.KindBillingNotification, "canary", "corr-1")
			return err
		},
		"RollbackJobRoute": func(f *serviceFixture) error {
			f.jobRoutes.state = jobroute.State{Kind: jobcontract.KindBillingNotification, Transport: "celery", Generation: 2}
			_, err := f.service.RollbackJobRoute(ctx, principal, jobcontract.KindBillingNotification, "rollback", "corr-1")
			return err
		},
	}
	seen := map[string]bool{}
	for name, call := range calls {
		fixture := newServiceFixtureWithRegistry(t, executableRegistry{RuntimeRegistry: registry})
		_ = call(fixture)
		if fixture.auditor.beginCalls != 1 {
			t.Errorf("%s: %d audit Begin calls, want 1 (order %v)", name, fixture.auditor.beginCalls, fixture.order)
			continue
		}
		seen[string(fixture.auditor.event.Action)] = true
	}
	want := map[string]bool{}
	for _, action := range serviceAuditedActions {
		want[string(action)] = true
	}
	if !maps(seen, want) {
		t.Fatalf("audited actions = %v, serviceAuditedActions = %v", sortedKeys(seen), sortedKeys(want))
	}
}

// TestAuditActionMigrationMatchesAuditedActions reads the migration that
// sets the audit table's action check and asserts its action tuple equals
// AuditedActions, so the Go list and the database check cannot drift. The
// Postgres integration test applies the real chain and runs one audited
// mutation per Action.
func TestAuditActionMigrationMatchesAuditedActions(t *testing.T) {
	source, err := os.ReadFile("../../src/dev_health_ops/alembic/versions/0138_worker_operator_audits_direct_write_actions.py")
	if err != nil {
		t.Fatal(err)
	}
	block := regexp.MustCompile(`(?ms)^_ACTIONS\s*=\s*\((.*?)\)`).FindSubmatch(source)
	if block == nil {
		t.Fatal("0138 has no _ACTIONS tuple")
	}
	migration := map[string]bool{}
	for _, match := range regexp.MustCompile(`"([^"]+)"`).FindAllSubmatch(block[1], -1) {
		migration[string(match[1])] = true
	}
	want := map[string]bool{}
	for _, action := range AuditedActions {
		want[string(action)] = true
	}
	if !maps(migration, want) {
		t.Fatalf("0138 actions = %v, AuditedActions = %v", sortedKeys(migration), sortedKeys(want))
	}
}

func maps(a, b map[string]bool) bool {
	return slices.Equal(sortedKeys(a), sortedKeys(b))
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
