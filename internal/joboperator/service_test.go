package joboperator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
)

func TestCancelRequiresAuthorizationDomainGuardAndDurableAudit(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)

	updated, err := fixture.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-1")
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if updated.State != StateCancelled {
		t.Fatalf("updated state = %s", updated.State)
	}
	if strings.Join(fixture.order, ",") != "authorize,get,domain,audit_begin,cancel,audit_succeeded" {
		t.Fatalf("unexpected order: %v", fixture.order)
	}
	if fixture.backend.mutation.ExpectedState != StateAvailable || fixture.backend.mutation.ReasonCode != "operator_request" {
		t.Fatalf("mutation lost CAS/reason: %+v", fixture.backend.mutation)
	}
	if fixture.auditor.event.Action != ActionCancel || fixture.auditor.event.ResourceID != "42" {
		t.Fatalf("audit event: %+v", fixture.auditor.event)
	}
}

func TestCancelRunningFailsClosedWithoutPromptCancellationSupport(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateRunning, jobcontract.KindHeartbeat, "heartbeat", 1)
	fixture.backend.runningCancellation = false

	_, err := fixture.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-1")
	assertCode(t, err, CodeConflict)
	if fixture.backend.cancelCalls != 0 || fixture.auditor.beginCalls != 0 {
		t.Fatal("unsafe running cancellation reached mutation/audit")
	}
}

func TestRetryFailsClosedWhileMigrationRouteIsCelery(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateDiscarded, jobcontract.KindRetentionCleanup, "retention", 3)

	_, err := fixture.service.Retry(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-1")
	assertCode(t, err, CodeConflict)
	if fixture.backend.retryCalls != 0 || fixture.auditor.beginCalls != 0 {
		t.Fatal("Celery-routed job reached retry mutation")
	}
}

func TestRetryStateEligibilityExcludesActiveAndCompletedWork(t *testing.T) {
	t.Parallel()
	for _, state := range []JobState{StateRetryable, StateCancelled, StateDiscarded} {
		if !retryEligible(state) {
			t.Fatalf("state %s should be manually retryable", state)
		}
	}
	for _, state := range []JobState{StateAvailable, StateRunning, StateScheduled, StateCompleted} {
		if retryEligible(state) {
			t.Fatalf("state %s should not be manually retryable", state)
		}
	}
}

func TestEligibleRetryUsesStateCASDomainGuardAndAudit(t *testing.T) {
	t.Parallel()
	base, err := jobruntime.Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatalf("Load runtime registry: %v", err)
	}
	fixture := newServiceFixtureWithRegistry(t, executableRegistry{RuntimeRegistry: base})
	fixture.backend.job = jobSummary(StateDiscarded, jobcontract.KindRetentionCleanup, "retention", 3)

	updated, err := fixture.service.Retry(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-1")
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if updated.State != StateAvailable || updated.MaxAttempts != 4 {
		t.Fatalf("updated retry: %+v", updated)
	}
	if strings.Join(fixture.order, ",") != "authorize,get,domain,audit_begin,retry,audit_succeeded" {
		t.Fatalf("unexpected order: %v", fixture.order)
	}
	if fixture.backend.mutation.ExpectedState != StateDiscarded {
		t.Fatalf("retry lost state CAS: %+v", fixture.backend.mutation)
	}
}

func TestMutationDoesNotRunWithoutAuditIntent(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)
	fixture.auditor.beginErr = errors.New("audit-secret")

	_, err := fixture.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-1")
	assertCode(t, err, CodeAudit)
	if fixture.backend.cancelCalls != 0 {
		t.Fatal("mutation ran without durable audit intent")
	}
	if strings.Contains(err.Error(), "audit-secret") {
		t.Fatal("audit error text leaked")
	}
}

func TestMutationFailureCompletesAuditAsFailedAndRedactsCause(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)
	fixture.backend.cancelErr = errors.New("postgres-dsn-secret")

	_, err := fixture.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-1")
	assertCode(t, err, CodeBackend)
	if fixture.auditor.status != AuditFailed {
		t.Fatalf("audit status = %s", fixture.auditor.status)
	}
	if strings.Contains(err.Error(), "postgres-dsn-secret") {
		t.Fatal("backend error text leaked")
	}
}

func TestAmbiguousCommitIsAuditedAndReportedAsOutcomeUnknown(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)
	fixture.backend.cancelErr = ErrMutationOutcomeUnknown

	_, err := fixture.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-unknown")
	assertCode(t, err, CodeOutcomeUnknown)
	if fixture.auditor.status != AuditOutcomeUnknown {
		t.Fatalf("audit status = %s", fixture.auditor.status)
	}
}

func TestSuccessfulMutationWithIncompleteAuditReportsAuditPending(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)
	fixture.auditor.completeErr = errors.New("audit-completion-secret")

	_, err := fixture.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-pending")
	assertCode(t, err, CodeAuditPending)
	if fixture.backend.cancelCalls != 1 || fixture.auditor.status != AuditSucceeded ||
		strings.Contains(err.Error(), "audit-completion-secret") {
		t.Fatalf("successful mutation/audit pending state = calls:%d status:%s err:%v", fixture.backend.cancelCalls, fixture.auditor.status, err)
	}
}

func TestAuthorizationFailureHasNoReadOrMutationSideEffects(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.authorizer.err = errors.New("policy-secret")

	_, err := fixture.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-1")
	assertCode(t, err, CodeUnauthorized)
	if fixture.backend.getCalls != 0 || fixture.auditor.beginCalls != 0 {
		t.Fatal("unauthorized request reached backend or audit mutation")
	}
}

func TestListExposesOnlySanitizedSummaries(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.jobs = []JobSummary{
		jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 0),
		jobSummary(StateScheduled, jobcontract.KindRetentionCleanup, "retention", 0),
	}
	filter := ListFilter{States: []JobState{StateAvailable, StateScheduled}, Limit: 25}
	jobs, err := fixture.service.List(context.Background(), testPrincipal(), filter)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(jobs) != 2 || fixture.authorizer.request.Action != ActionInspect {
		t.Fatalf("jobs/auth: %d %+v", len(jobs), fixture.authorizer.request)
	}
	// JobSummary's type has no encoded-args or error-text return channel.
	if jobs[0].CorrelationID != "corr-safe" || jobs[0].Domain.ID == "" {
		t.Fatalf("safe summary fields missing: %+v", jobs[0])
	}
}

func TestStatusRequiresAuthorizedReadScope(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	if err := fixture.service.Status(context.Background(), testPrincipal()); err != nil {
		t.Fatalf("Status: %v", err)
	}
	if fixture.authorizer.request.Action != ActionInspect ||
		fixture.authorizer.request.ResourceType != "workers" ||
		fixture.authorizer.request.ResourceID != "status" {
		t.Fatalf("status authorization request = %+v", fixture.authorizer.request)
	}

	fixture.authorizer.err = errors.New("read-scope-denied")
	err := fixture.service.Status(context.Background(), testPrincipal())
	assertCode(t, err, CodeUnauthorized)
}

func TestRouteControlsPreserveAuthorizationAndDurableAudit(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.routes.state = syncroute.RouteState{
		Kind: "dispatch_sync_run", Transport: "celery", Generation: 2, Paused: true,
	}
	state, err := fixture.service.PauseRoute(
		context.Background(), testPrincipal(), "dispatch_sync_run", "cutover", "corr-route-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 2 ||
		strings.Join(fixture.order, ",") != "authorize,audit_begin,route_pause,audit_succeeded" {
		t.Fatalf("route pause state=%+v order=%v", state, fixture.order)
	}
	if fixture.auditor.event.Action != ActionPauseRoute ||
		fixture.auditor.event.ResourceType != "sync_route" {
		t.Fatalf("route audit=%+v", fixture.auditor.event)
	}
}

func TestRouteResumeFailsClosedWithoutCapabilityAndAuditsFailure(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.routes.err = syncroute.ErrCapabilityMissing
	_, err := fixture.service.ResumeRoute(
		context.Background(), testPrincipal(), "dispatch_sync_run", "river",
		"cutover", "corr-route-2", time.Second,
	)
	assertCode(t, err, CodePrecondition)
	if fixture.auditor.status != AuditFailed ||
		strings.Join(fixture.order, ",") != "authorize,audit_begin,route_resume,audit_failed" {
		t.Fatalf("route failure status=%s order=%v", fixture.auditor.status, fixture.order)
	}
}

func TestRouteDriftIsAnOperatorPreconditionFailure(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.routes.err = syncroute.ErrDrift
	_, err := fixture.service.PauseRoute(
		context.Background(), testPrincipal(), "dispatch_sync_run", "cutover", "corr-route-drift",
	)
	assertCode(t, err, CodePrecondition)
	if fixture.auditor.status != AuditFailed ||
		strings.Join(fixture.order, ",") != "authorize,audit_begin,route_pause,audit_failed" {
		t.Fatalf("route drift status=%s order=%v", fixture.auditor.status, fixture.order)
	}
}

func TestAmbiguousRouteCommitIsAuditedAsOutcomeUnknown(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.routes.err = syncroute.ErrMutationOutcomeUnknown
	_, err := fixture.service.PauseRoute(
		context.Background(), testPrincipal(), "dispatch_sync_run", "cutover", "corr-route-3",
	)
	assertCode(t, err, CodeOutcomeUnknown)
	if fixture.auditor.status != AuditOutcomeUnknown {
		t.Fatalf("route audit status=%s", fixture.auditor.status)
	}
}

func TestQueueInspectionRequiresExactSanitizedProfileCoverage(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.queues = []QueueSummary{
		{Name: "retention", Profile: "ops", Available: 2, Running: 1},
		{Name: "heartbeat", Profile: "ops", Scheduled: 1},
	}
	queues, err := fixture.service.Queues(context.Background(), testPrincipal(), "ops")
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if len(queues) != 2 || queues[0].Name != "heartbeat" || queues[1].Name != "retention" {
		t.Fatalf("queues were not sanitized/sorted: %+v", queues)
	}

	fixture.backend.queues = fixture.backend.queues[:1]
	if _, err := fixture.service.Queues(context.Background(), testPrincipal(), "ops"); err == nil {
		t.Fatal("incomplete queue coverage passed")
	}
}

func TestQueueAndDrainMutationsAreValidatedAndAudited(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	ctx := context.Background()
	principal := testPrincipal()

	if err := fixture.service.PauseQueue(ctx, principal, "heartbeat", "incident_response", "corr-op-2"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}
	if fixture.backend.pauseCalls != 1 || fixture.auditor.status != AuditSucceeded {
		t.Fatal("pause was not mutated and audited")
	}
	if err := fixture.service.ResumeQueue(ctx, principal, "unknown", "incident_response", "corr-op-3"); err == nil {
		t.Fatal("unknown queue resumed")
	}
	result, err := fixture.service.Drain(ctx, principal, "ops", "deploy_drain", "corr-op-4")
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if result.Profile != "ops" || result.QueuesPaused != 2 || fixture.backend.drainCalls != 1 {
		t.Fatalf("drain result: %+v", result)
	}
}

func TestOperatorInputBoundsAndDomainPreconditionsFailBeforeAudit(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)

	_, err := fixture.service.Cancel(context.Background(), testPrincipal(), 42, "contains spaces", "corr-op-1")
	assertCode(t, err, CodeInvalid)
	fixture.guard.err = errors.New("domain-terminal-secret")
	_, err = fixture.service.Cancel(context.Background(), testPrincipal(), 42, "operator_request", "corr-op-1")
	assertCode(t, err, CodePrecondition)
	if fixture.auditor.beginCalls != 0 || fixture.backend.cancelCalls != 0 {
		t.Fatal("invalid/precondition request reached audit mutation")
	}
}

func TestBackendCannotSmuggleRegistryOrDomainDrift(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	fixture.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "wrong", 1)
	_, err := fixture.service.Inspect(context.Background(), testPrincipal(), 42)
	assertCode(t, err, CodeBackend)

	fixture = newServiceFixture(t)
	fixture.backend.job = jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 1)
	fixture.backend.job.Domain.ID = "not-a-uuid"
	_, err = fixture.service.Inspect(context.Background(), testPrincipal(), 42)
	assertCode(t, err, CodeBackend)
}

type serviceFixture struct {
	service    *Service
	backend    *fakeBackend
	authorizer *fakeAuthorizer
	guard      *fakeDomainGuard
	auditor    *fakeAuditor
	routes     *fakeRouteController
	order      []string
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	registry, err := jobruntime.Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatalf("Load runtime registry: %v", err)
	}
	return newServiceFixtureWithRegistry(t, registry)
}

func newServiceFixtureWithRegistry(t *testing.T, registry RuntimeRegistry) *serviceFixture {
	t.Helper()
	fixture := &serviceFixture{}
	fixture.backend = &fakeBackend{order: &fixture.order, runningCancellation: true}
	fixture.authorizer = &fakeAuthorizer{order: &fixture.order}
	fixture.guard = &fakeDomainGuard{order: &fixture.order}
	fixture.auditor = &fakeAuditor{order: &fixture.order}
	fixture.routes = &fakeRouteController{order: &fixture.order}
	service, err := New(Dependencies{
		Registry: registry, Backend: fixture.backend, Authorizer: fixture.authorizer,
		DomainGuard: fixture.guard, Auditor: fixture.auditor,
		RouteController: fixture.routes,
		Clock:           func() time.Time { return time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fixture.service = service
	return fixture
}

type fakeRouteController struct {
	order *[]string
	state syncroute.RouteState
	err   error
}

func (controller *fakeRouteController) Inspect(context.Context, string) (syncroute.RouteState, error) {
	*controller.order = append(*controller.order, "route_inspect")
	return controller.state, controller.err
}
func (controller *fakeRouteController) Pause(context.Context, string) (syncroute.RouteState, error) {
	*controller.order = append(*controller.order, "route_pause")
	return controller.state, controller.err
}
func (controller *fakeRouteController) Drain(context.Context, string) (syncroute.RouteState, error) {
	*controller.order = append(*controller.order, "route_drain")
	return controller.state, controller.err
}
func (controller *fakeRouteController) Resume(context.Context, string, string, time.Duration) (syncroute.RouteState, error) {
	*controller.order = append(*controller.order, "route_resume")
	return controller.state, controller.err
}

type executableRegistry struct{ RuntimeRegistry }

func (registry executableRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := registry.RuntimeRegistry.Descriptor(kind)
	if ok && kind == jobcontract.KindRetentionCleanup {
		descriptor.MigrationState = "canary"
		descriptor.Route = "river_canary"
	}
	return descriptor, ok
}

func testPrincipal() Principal { return Principal{Type: "operator", ID: "user-123"} }

func jobSummary(state JobState, kind, queue string, attempt int) JobSummary {
	maxAttempts := 1
	domainType := "schedule_occurrence"
	if kind == jobcontract.KindRetentionCleanup {
		maxAttempts = 3
		domainType = "maintenance_run"
	}
	return JobSummary{
		ID: 42, Kind: kind, Queue: queue, State: state, Attempt: attempt, MaxAttempts: maxAttempts,
		CreatedAt: time.Date(2026, 7, 21, 11, 0, 0, 0, time.UTC), CorrelationID: "corr-safe",
		Domain: jobcontract.DomainLink{Type: domainType, ID: "11111111-1111-4111-8111-111111111111"},
	}
}

func assertCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var serviceErr *ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.Code != code {
		t.Fatalf("error = %v, want code %s", err, code)
	}
}

type fakeAuthorizer struct {
	order   *[]string
	err     error
	request AuthorizationRequest
}

func (authorizer *fakeAuthorizer) Authorize(_ context.Context, request AuthorizationRequest) error {
	*authorizer.order = append(*authorizer.order, "authorize")
	authorizer.request = request
	return authorizer.err
}

type fakeDomainGuard struct {
	order *[]string
	err   error
}

func (guard *fakeDomainGuard) Check(context.Context, Action, JobSummary) error {
	*guard.order = append(*guard.order, "domain")
	return guard.err
}

type fakeAuditHandle struct{ auditor *fakeAuditor }

func (handle *fakeAuditHandle) Complete(_ context.Context, status AuditStatus) error {
	handle.auditor.status = status
	*handle.auditor.order = append(*handle.auditor.order, "audit_"+string(status))
	return handle.auditor.completeErr
}

type fakeAuditor struct {
	order       *[]string
	beginCalls  int
	event       AuditEvent
	status      AuditStatus
	beginErr    error
	completeErr error
}

func (auditor *fakeAuditor) Begin(_ context.Context, event AuditEvent) (AuditHandle, error) {
	auditor.beginCalls++
	auditor.event = event
	*auditor.order = append(*auditor.order, "audit_begin")
	if auditor.beginErr != nil {
		return nil, auditor.beginErr
	}
	return &fakeAuditHandle{auditor: auditor}, nil
}

type fakeBackend struct {
	order               *[]string
	job                 JobSummary
	jobs                []JobSummary
	queues              []QueueSummary
	mutation            Mutation
	getCalls            int
	cancelCalls         int
	retryCalls          int
	pauseCalls          int
	resumeCalls         int
	drainCalls          int
	runningCancellation bool
	cancelErr           error
}

func (backend *fakeBackend) Get(context.Context, int64) (JobSummary, error) {
	backend.getCalls++
	*backend.order = append(*backend.order, "get")
	return backend.job, nil
}

func (backend *fakeBackend) List(context.Context, ListFilter) ([]JobSummary, error) {
	*backend.order = append(*backend.order, "list")
	return backend.jobs, nil
}

func (backend *fakeBackend) Queues(context.Context, string) ([]QueueSummary, error) {
	*backend.order = append(*backend.order, "queues")
	return backend.queues, nil
}

func (backend *fakeBackend) Cancel(_ context.Context, _ int64, mutation Mutation) (JobSummary, error) {
	backend.cancelCalls++
	backend.mutation = mutation
	*backend.order = append(*backend.order, "cancel")
	if backend.cancelErr != nil {
		return JobSummary{}, backend.cancelErr
	}
	updated := backend.job
	updated.State = StateCancelled
	return updated, nil
}

func (backend *fakeBackend) Retry(_ context.Context, _ int64, mutation Mutation) (JobSummary, error) {
	backend.retryCalls++
	backend.mutation = mutation
	*backend.order = append(*backend.order, "retry")
	updated := backend.job
	updated.State = StateAvailable
	updated.MaxAttempts++
	return updated, nil
}

func (backend *fakeBackend) PauseQueue(_ context.Context, _ string, mutation Mutation) error {
	backend.pauseCalls++
	backend.mutation = mutation
	*backend.order = append(*backend.order, "pause")
	return nil
}

func (backend *fakeBackend) ResumeQueue(_ context.Context, _ string, mutation Mutation) error {
	backend.resumeCalls++
	backend.mutation = mutation
	*backend.order = append(*backend.order, "resume")
	return nil
}

func (backend *fakeBackend) Drain(_ context.Context, profile string, mutation Mutation) (DrainResult, error) {
	backend.drainCalls++
	backend.mutation = mutation
	*backend.order = append(*backend.order, "drain")
	return DrainResult{Profile: profile, QueuesPaused: 2, RunningAtStart: 1}, nil
}

func (backend *fakeBackend) SupportsRunningCancellation() bool {
	return backend.runningCancellation
}
