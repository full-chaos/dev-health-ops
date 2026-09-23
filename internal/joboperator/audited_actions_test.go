package joboperator

import (
	"context"
	"os"
	"reflect"
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

// auditedMutationCalls drives every Service method that writes an audit row
// with a FIXED Action of its own, each on a fresh fixture. Package-level
// (not a local var inside one test) so TestEveryExportedServiceMethodIsAccountedFor
// can check its key set against the full reflected method set below.
var auditedMutationCalls = map[string]func(*serviceFixture) error{
	"Cancel": func(f *serviceFixture) error {
		f.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)
		_, err := f.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-1")
		return err
	},
	"Retry": func(f *serviceFixture) error {
		f.backend.job = jobSummary(StateDiscarded, jobcontract.KindRetentionCleanup, "retention", 3)
		_, err := f.service.Retry(context.Background(), testPrincipal(), 42, "operator_request", "corr-1")
		return err
	},
	"PauseQueue": func(f *serviceFixture) error {
		return f.service.PauseQueue(context.Background(), testPrincipal(), "heartbeat", "incident_response", "corr-1")
	},
	"ResumeQueue": func(f *serviceFixture) error {
		return f.service.ResumeQueue(context.Background(), testPrincipal(), "heartbeat", "incident_response", "corr-1")
	},
	"Drain": func(f *serviceFixture) error {
		_, err := f.service.Drain(context.Background(), testPrincipal(), "ops", []string{"heartbeat", "retention"}, "deploy_drain", "corr-1")
		return err
	},
	"Undrain": func(f *serviceFixture) error {
		_, err := f.service.Undrain(context.Background(), testPrincipal(), "ops", []string{"heartbeat", "retention"}, "deploy_resume", "corr-1")
		return err
	},
	"ApplyCheckedInRoute": func(f *serviceFixture) error {
		f.routes.state = syncroute.RouteState{Kind: "reference_discovery", Transport: "river", Generation: 2}
		_, err := f.service.ApplyCheckedInRoute(context.Background(), testPrincipal(), "reference_discovery", "local_start", "corr-1")
		return err
	},
	"PauseRoute": func(f *serviceFixture) error {
		f.routes.state = syncroute.RouteState{Kind: "dispatch_sync_run", Transport: "celery", Generation: 2, Paused: true}
		_, err := f.service.PauseRoute(context.Background(), testPrincipal(), "dispatch_sync_run", "cutover", "corr-1")
		return err
	},
	"DrainRoute": func(f *serviceFixture) error {
		f.routes.state = syncroute.RouteState{Kind: "dispatch_sync_run", Transport: "celery", Generation: 2}
		_, err := f.service.DrainRoute(context.Background(), testPrincipal(), "dispatch_sync_run", "cutover", "corr-1")
		return err
	},
	"ResumeRoute": func(f *serviceFixture) error {
		f.routes.state = syncroute.RouteState{Kind: "dispatch_sync_run", Transport: "river", Generation: 2}
		_, err := f.service.ResumeRoute(context.Background(), testPrincipal(), "dispatch_sync_run", "river", "cutover", "corr-1", time.Second)
		return err
	},
	"ApplyCheckedInJobRoute": func(f *serviceFixture) error {
		f.jobRoutes.state = jobroute.State{Kind: jobcontract.KindBillingNotification, Transport: "river_canary", Generation: 3}
		_, err := f.service.ApplyCheckedInJobRoute(context.Background(), testPrincipal(), jobcontract.KindBillingNotification, "canary", "corr-1")
		return err
	},
	"RollbackJobRoute": func(f *serviceFixture) error {
		f.jobRoutes.state = jobroute.State{Kind: jobcontract.KindBillingNotification, Transport: "celery", Generation: 2}
		_, err := f.service.RollbackJobRoute(context.Background(), testPrincipal(), jobcontract.KindBillingNotification, "rollback", "corr-1")
		return err
	},
}

// readOnlyServiceMethods lists every exported *Service method claimed to
// write nothing at all -- proven by TestReadOnlyServiceMethodsWriteNothing,
// which runs each one against a fresh fixture and asserts zero audit Begin
// calls, rather than trusting a comment nobody re-checks.
var readOnlyServiceMethods = map[string]func(*serviceFixture) error{
	"Inspect": func(f *serviceFixture) error {
		f.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)
		_, err := f.service.Inspect(context.Background(), testPrincipal(), 42)
		return err
	},
	"InspectRoute": func(f *serviceFixture) error {
		f.routes.state = syncroute.RouteState{Kind: "dispatch_sync_run", Transport: "celery", Generation: 2}
		_, err := f.service.InspectRoute(context.Background(), testPrincipal(), "dispatch_sync_run")
		return err
	},
	"InspectJobRoute": func(f *serviceFixture) error {
		f.jobRoutes.state = jobroute.State{Kind: jobcontract.KindBillingNotification, Transport: "celery", Generation: 2}
		_, err := f.service.InspectJobRoute(context.Background(), testPrincipal(), jobcontract.KindBillingNotification)
		return err
	},
	"Status": func(f *serviceFixture) error {
		return f.service.Status(context.Background(), testPrincipal())
	},
	"List": func(f *serviceFixture) error {
		f.backend.jobs = []JobSummary{jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 0)}
		_, err := f.service.List(context.Background(), testPrincipal(), ListFilter{States: []JobState{StateAvailable}, Limit: 25})
		return err
	},
	"Queues": func(f *serviceFixture) error {
		f.backend.queues = []QueueSummary{{Name: "heartbeat", Group: "ops"}}
		_, err := f.service.Queues(context.Background(), testPrincipal(), "ops", []string{"heartbeat"})
		return err
	},
	"Contracts": func(f *serviceFixture) error {
		_, err := f.service.Contracts(context.Background(), testPrincipal(), []string{"heartbeat", "retention"})
		return err
	},
	"Authorize": func(f *serviceFixture) error {
		return f.service.Authorize(context.Background(), testPrincipal(), ActionInspect, "job", "1")
	},
}

// parameterizedMutationServiceMethods lists every exported *Service method
// that DOES write, through the exact same mutate() path the fixed-Action
// methods above use, but takes its Action from the CALLER rather than
// hardcoding one of its own -- so it has no fixed Action to pin against
// serviceAuditedActions, and driving it here with a synthetic mutation would
// prove nothing beyond what its own coverage already does.
var parameterizedMutationServiceMethods = map[string]string{
	"Audited": "the generic direct-write passthrough for `dho workers` verbs " +
		"whose write bypasses this service's own backends; covered by " +
		"internal/workersctl's TestEveryDirectWriteVerbIsAuditedBeforeItWrites",
}

// TestEveryExportedServiceMethodIsAccountedFor reflects over *Service's
// exported method set -- the property this test exists to fix: a new
// audited mutation method used to be silently unpinned until someone
// remembered to add it to auditedMutationCalls by hand, and nothing failed
// in the meantime. Every exported method must now be claimed by EXACTLY one
// of auditedMutationCalls, readOnlyServiceMethods or
// parameterizedMutationServiceMethods, or this fails loud, by name.
func TestEveryExportedServiceMethodIsAccountedFor(t *testing.T) {
	serviceType := reflect.TypeOf((*Service)(nil))
	exported := map[string]bool{}
	for i := range serviceType.NumMethod() {
		exported[serviceType.Method(i).Name] = true
	}
	if len(exported) == 0 {
		t.Fatal("reflect found zero exported *Service methods; the walk has broken and every assertion below would pass vacuously")
	}

	var unaccounted []string
	for name := range exported {
		claims := 0
		if _, ok := auditedMutationCalls[name]; ok {
			claims++
		}
		if _, ok := readOnlyServiceMethods[name]; ok {
			claims++
		}
		if _, ok := parameterizedMutationServiceMethods[name]; ok {
			claims++
		}
		switch claims {
		case 0:
			unaccounted = append(unaccounted, name)
		case 1:
			// exactly one claim, as required
		default:
			t.Errorf("%s is claimed by %d of auditedMutationCalls/readOnlyServiceMethods/parameterizedMutationServiceMethods -- exactly one must claim it", name, claims)
		}
	}
	sort.Strings(unaccounted)
	if len(unaccounted) > 0 {
		t.Fatalf(
			"these exported *Service methods are in none of auditedMutationCalls "+
				"(a fixed-Action audited mutation), readOnlyServiceMethods (proven "+
				"zero-write) or parameterizedMutationServiceMethods (a caller-supplied "+
				"-Action mutation covered elsewhere): %v -- classify the new method "+
				"into exactly one of the three before this test can pass",
			unaccounted,
		)
	}

	// A name here that is not a real exported method is dead weight that
	// reads as coverage -- a renamed or deleted method left an excuse behind.
	for name := range readOnlyServiceMethods {
		if !exported[name] {
			t.Errorf("readOnlyServiceMethods names %q, which is not an exported *Service method (renamed or deleted?)", name)
		}
	}
	for name := range parameterizedMutationServiceMethods {
		if !exported[name] {
			t.Errorf("parameterizedMutationServiceMethods names %q, which is not an exported *Service method (renamed or deleted?)", name)
		}
	}
}

// TestReadOnlyServiceMethodsWriteNothing executes every method in
// readOnlyServiceMethods against a fresh fixture and asserts it never calls
// Auditor.Begin. A method wrongly excused as read-only (it actually writes)
// fails here, not just at the coverage check above.
func TestReadOnlyServiceMethodsWriteNothing(t *testing.T) {
	for name, call := range readOnlyServiceMethods {
		// The plain (unwrapped) registry, not executableRegistry: none of
		// these methods touch route/job-route executability, and Contracts
		// specifically needs the registry's own descriptorEnumerator method
		// set, which executableRegistry's embedding (RuntimeRegistry's
		// interface method set, not the concrete registry's) does not
		// forward.
		fixture := newServiceFixture(t)
		if callErr := call(fixture); callErr != nil {
			t.Errorf("%s: %v", name, callErr)
			continue
		}
		if fixture.auditor.beginCalls != 0 {
			t.Errorf(
				"%s: %d audit Begin call(s), want 0 -- excused as read-only in "+
					"readOnlyServiceMethods but actually wrote an audit row (order %v)",
				name, fixture.auditor.beginCalls, fixture.order,
			)
		}
	}
}

// TestEveryAuditedMutationIsInAuditedActions drives every Service method
// that writes an audit row, each on a fresh fixture, and collects the
// Action each one passes to Auditor.Begin. The set must equal
// serviceAuditedActions exactly: a new audited mutation fails here until it
// is listed (and the audit action check is widened to allow it). The
// direct-write verbs' Actions are pinned in workersctl, where the verbs are.
func TestEveryAuditedMutationIsInAuditedActions(t *testing.T) {
	registry, err := jobruntime.Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for name, call := range auditedMutationCalls {
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
