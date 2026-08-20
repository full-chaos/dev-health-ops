package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
)

type commandAuthorizer struct{ err error }

func (authorizer commandAuthorizer) Authorize(context.Context, joboperator.AuthorizationRequest) error {
	return authorizer.err
}

type commandBackend struct {
	queues          map[string]joboperator.QueueSummary
	queuesRequests  [][]string
	drainRequests   [][]string
	undrainRequests [][]string
	drainResult     joboperator.DrainResult
	undrainResult   joboperator.DrainResult
}

func (commandBackend) Get(context.Context, int64) (joboperator.JobSummary, error) {
	return joboperator.JobSummary{}, errors.New("unused")
}
func (commandBackend) List(context.Context, joboperator.ListFilter) ([]joboperator.JobSummary, error) {
	return nil, errors.New("unused")
}
func (backend *commandBackend) Queues(_ context.Context, queues []string) ([]joboperator.QueueSummary, error) {
	backend.queuesRequests = append(backend.queuesRequests, append([]string(nil), queues...))
	result := make([]joboperator.QueueSummary, 0, len(queues))
	for _, queue := range queues {
		summary, ok := backend.queues[queue]
		if !ok {
			continue
		}
		summary.Name = queue
		result = append(result, summary)
	}
	return result, nil
}
func (commandBackend) Cancel(context.Context, int64, joboperator.Mutation) (joboperator.JobSummary, error) {
	return joboperator.JobSummary{}, errors.New("unused")
}
func (commandBackend) Retry(context.Context, int64, joboperator.Mutation) (joboperator.JobSummary, error) {
	return joboperator.JobSummary{}, errors.New("unused")
}
func (commandBackend) PauseQueue(context.Context, string, joboperator.Mutation) error {
	return errors.New("unused")
}
func (commandBackend) ResumeQueue(context.Context, string, joboperator.Mutation) error {
	return errors.New("unused")
}
func (backend *commandBackend) Drain(_ context.Context, queues []string, _ joboperator.Mutation) (joboperator.DrainResult, error) {
	backend.drainRequests = append(backend.drainRequests, append([]string(nil), queues...))
	if backend.drainResult.Group == "" {
		backend.drainResult.QueuesPaused = len(queues)
		backend.drainResult.RunningAtStart = 1
	}
	return backend.drainResult, nil
}
func (backend *commandBackend) Undrain(_ context.Context, queues []string, _ joboperator.Mutation) (joboperator.DrainResult, error) {
	backend.undrainRequests = append(backend.undrainRequests, append([]string(nil), queues...))
	if backend.undrainResult.Group == "" {
		backend.undrainResult.QueuesPaused = len(queues)
		backend.undrainResult.RunningAtStart = 1
	}
	return backend.undrainResult, nil
}
func (commandBackend) SupportsRunningCancellation() bool { return false }

type commandDomainGuard struct{}

func (commandDomainGuard) Check(context.Context, joboperator.Action, joboperator.JobSummary) error {
	return errors.New("unused")
}

type commandAuditor struct{}

type commandAuditHandle struct{}

func (commandAuditHandle) Complete(context.Context, joboperator.AuditStatus) error { return nil }

func (commandAuditor) Begin(context.Context, joboperator.AuditEvent) (joboperator.AuditHandle, error) {
	return commandAuditHandle{}, nil
}

type commandRouteController struct {
	state syncroute.RouteState
	err   error
}

type commandJobRouteController struct{}

func (commandJobRouteController) Inspect(_ context.Context, kind string) (jobroute.State, error) {
	return jobroute.State{
		Kind: kind, Transport: "celery", Generation: 1,
		UpdatedAt: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
	}, nil
}

func (commandJobRouteController) Rollback(_ context.Context, kind string) (jobroute.State, error) {
	return jobroute.State{
		Kind: kind, Transport: "celery", Generation: 2,
		UpdatedAt: time.Date(2026, 7, 21, 12, 1, 0, 0, time.UTC),
	}, nil
}

func (commandJobRouteController) ApplyCheckedIn(_ context.Context, kind string) (jobroute.State, error) {
	return jobroute.State{
		Kind: kind, Transport: "river_canary", Generation: 2,
		UpdatedAt: time.Date(2026, 7, 21, 12, 1, 0, 0, time.UTC),
	}, nil
}

func (controller commandRouteController) Inspect(context.Context, string) (syncroute.RouteState, error) {
	return controller.state, controller.err
}
func (controller commandRouteController) ApplyCheckedIn(context.Context, string) (syncroute.RouteState, error) {
	if controller.state.Kind == "" && controller.err == nil {
		return syncroute.RouteState{
			Kind: "reference_discovery", Transport: "river", Generation: 2,
			RollbackTransport: "celery",
		}, nil
	}
	return controller.state, controller.err
}
func (controller commandRouteController) Pause(context.Context, string) (syncroute.RouteState, error) {
	if controller.state.Kind == "" && controller.err == nil {
		return syncroute.RouteState{
			Kind: "post_sync", Transport: "celery", Generation: 2, Paused: true,
			RollbackTransport: "celery",
		}, nil
	}
	return controller.state, controller.err
}
func (controller commandRouteController) Drain(context.Context, string) (syncroute.RouteState, error) {
	return controller.state, controller.err
}
func (controller commandRouteController) Resume(context.Context, string, string, time.Duration) (syncroute.RouteState, error) {
	if controller.state.Kind == "" && controller.err == nil {
		return syncroute.RouteState{
			Kind: "post_sync", Transport: "celery", Generation: 3,
			RollbackTransport: "celery",
		}, nil
	}
	return controller.state, controller.err
}

func TestDispatchStatusRequiresReadAuthorizationAndEmitsBoundedJSON(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	var stdout, stderr bytes.Buffer
	if code := dispatch(context.Background(), runtime, []string{"status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("dispatch status code = %d, stderr=%s", code, stderr.String())
	}
	if strings.Contains(stdout.String(), "secret") || stdout.String() != "{\"queue_control_mode\":\"direct\",\"river_schema_version\":7,\"status\":\"ready\"}\n" {
		t.Fatalf("status output = %q", stdout.String())
	}

	runtime = commandRuntime(t, commandAuthorizer{err: errors.New("credential-secret")})
	stdout.Reset()
	stderr.Reset()
	if code := dispatch(context.Background(), runtime, []string{"status"}, &stdout, &stderr); code != 1 {
		t.Fatalf("unauthorized status code = %d", code)
	}
	if stdout.Len() != 0 || stderr.String() != "{\"error\":{\"code\":\"unauthorized\"}}\n" ||
		strings.Contains(stderr.String(), "credential-secret") {
		t.Fatalf("unauthorized output stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestDispatchMutationRequiresReasonAndCorrelationBeforeService(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	var stdout, stderr bytes.Buffer
	code := dispatch(context.Background(), runtime, []string{"jobs", "cancel", "42"}, &stdout, &stderr)
	if code != 1 || stderr.String() != "{\"error\":{\"code\":\"invalid_request\"}}\n" {
		t.Fatalf("cancel validation code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchRoutesCanPauseAndResumePostSyncOnCelery(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	var stdout, stderr bytes.Buffer
	if code := dispatch(context.Background(), runtime, []string{
		"routes", "pause", "--reason", "maintenance", "--correlation-id", "route-cli-1", "post_sync",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("routes pause code=%d stderr=%s", code, stderr.String())
	}
	if stdout.String() != "{\"kind\":\"post_sync\",\"transport\":\"celery\",\"generation\":2,\"paused\":true,\"rollback_transport\":\"celery\",\"live_claims\":0}\n" {
		t.Fatalf("routes pause output=%q", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := dispatch(context.Background(), runtime, []string{
		"routes", "resume", "--reason", "maintenance", "--correlation-id", "route-cli-2",
		"--transport", "celery", "post_sync",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("routes resume code=%d stderr=%s", code, stderr.String())
	}
	if stdout.String() != "{\"kind\":\"post_sync\",\"transport\":\"celery\",\"generation\":3,\"paused\":false,\"rollback_transport\":\"celery\",\"live_claims\":0}\n" {
		t.Fatalf("routes resume output=%q", stdout.String())
	}
}

func TestDispatchRoutesApplyCheckedInRiverTransport(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	var stdout, stderr bytes.Buffer
	if code := dispatch(context.Background(), runtime, []string{
		"routes", "apply", "--reason", "local_start", "--correlation-id", "route-cli-apply",
		"reference_discovery",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("routes apply code=%d stderr=%s", code, stderr.String())
	}
	if stdout.String() != "{\"kind\":\"reference_discovery\",\"transport\":\"river\",\"generation\":2,\"paused\":false,\"rollback_transport\":\"celery\",\"live_claims\":0}\n" {
		t.Fatalf("routes apply output=%q", stdout.String())
	}
}

func TestDispatchStreamsStatusIsAuthorizedBoundedCoexistenceState(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	runtime.streamDeploymentState = "coexistence_disabled"
	runtime.streams = []streamProfileStatus{
		{Profile: "stream-external", Owner: "celery", MaxReplicas: 1},
		{Profile: "stream-ingest", Owner: "celery", MaxReplicas: 1},
	}
	var stdout, stderr bytes.Buffer
	if code := dispatch(context.Background(), runtime, []string{"streams", "status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("streams status code=%d stderr=%s", code, stderr.String())
	}
	want := "{\"deployment_state\":\"coexistence_disabled\",\"profiles\":[{\"profile\":\"stream-external\",\"owner\":\"celery\",\"enabled_by_default\":false,\"min_replicas\":0,\"max_replicas\":1},{\"profile\":\"stream-ingest\",\"owner\":\"celery\",\"enabled_by_default\":false,\"min_replicas\":0,\"max_replicas\":1}]}\n"
	if stdout.String() != want || strings.Contains(stdout.String(), "secret") {
		t.Fatalf("streams status output=%q", stdout.String())
	}
}

func TestDispatchQueuesStatusIncludesExactReplicaAndBudgetTelemetry(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	runtime.queueStatusSource = workerQueueStatusSourceFunc(func(context.Context) (workerQueueStatusResponse, error) {
		return workerQueueStatusResponse{
			DeploymentState: "coexistence_disabled",
			ConnectionBudget: workerConnectionBudgetStatus{
				QueueSession:       connectionBudgetStatus{Used: 22, Limit: 22, Headroom: 0},
				CoordinatorSession: connectionBudgetStatus{Used: 10, Limit: 10, Headroom: 0},
				DomainTransaction:  connectionBudgetStatus{Used: 58, Limit: 1000, Headroom: 942},
				Server:             connectionBudgetStatus{Used: 87, Limit: 100, Headroom: 13},
			},
			Groups: []workerQueueStatus{{
				Group: "sync", Queues: []string{"sync.provider_unit", "sync.outbox"},
				ConfiguredInManifest: true,
				DesiredReplicas:      2, LiveReplicas: 2, MaxReplicas: 2,
				QueueBacklog: 7, ActiveJobs: 2, DrainState: "active",
			}},
		}, nil
	})
	var stdout, stderr bytes.Buffer
	if code := dispatch(context.Background(), runtime, []string{"queues", "status"}, &stdout, &stderr); code != 0 {
		t.Fatalf("queues status code=%d stderr=%s", code, stderr.String())
	}
	want := "{\"deployment_state\":\"coexistence_disabled\",\"connection_budget\":{\"queue_session\":{\"used\":22,\"limit\":22,\"headroom\":0},\"coordinator_session\":{\"used\":10,\"limit\":10,\"headroom\":0},\"domain_transaction\":{\"used\":58,\"limit\":1000,\"headroom\":942},\"server\":{\"used\":87,\"limit\":100,\"headroom\":13}},\"groups\":[{\"group\":\"sync\",\"queues\":[\"sync.provider_unit\",\"sync.outbox\"],\"configured_in_manifest\":true,\"desired_replicas\":2,\"live_replicas\":2,\"max_replicas\":2,\"queue_backlog\":7,\"active_jobs\":2,\"drain_state\":\"active\"}]}\n"
	if stdout.String() != want || stderr.Len() != 0 {
		t.Fatalf("queues status stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestManifestQueueStatusSourceCombinesFreshQueueAndPresenceState(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	contractRegistry, err := jobcontract.LoadRegistry("contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	manifest, budget, err := deploymentcontract.Load("deploy/go-workers/deployment.json", contractRegistry)
	if err != nil {
		t.Fatal(err)
	}
	var syncProcess deploymentcontract.Process
	for _, process := range manifest.Processes {
		if process.Name == "sync" {
			syncProcess = process
			syncProcess.DesiredReplicas = 2
			break
		}
	}
	manifest.Processes = []deploymentcontract.Process{syncProcess}
	queueSummaries := make(map[string]joboperator.QueueSummary, len(syncProcess.Queues))
	for _, queue := range syncProcess.Queues {
		queueSummaries[queue] = joboperator.QueueSummary{Name: queue, Group: "sync"}
	}
	firstQueue := syncProcess.Queues[0]
	firstSummary := queueSummaries[firstQueue]
	firstSummary.Available = 4
	firstSummary.Retryable = 2
	firstSummary.Scheduled = 1
	firstSummary.Running = 2
	queueSummaries[firstQueue] = firstSummary
	queueSummaries["heartbeat"] = joboperator.QueueSummary{Name: "heartbeat", Group: "latency"}
	runtimeRegistry, err := jobruntime.Load("contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: runtimeRegistry, Backend: &commandBackend{queues: queueSummaries},
		Authorizer: commandAuthorizer{}, DomainGuard: commandDomainGuard{}, Auditor: commandAuditor{},
		RouteController: commandRouteController{}, JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := joboperator.Principal{Type: "service_credential", ID: "00000000-0000-4000-8000-000000000303"}
	source := manifestQueueStatusSource{
		service: service, principal: principal, manifest: manifest, budget: budget,
		presence: func(context.Context) ([]jobruntime.WorkerPresenceSummary, error) {
			return []jobruntime.WorkerPresenceSummary{
				{WorkerGroup: "sync", Queues: append([]string(nil), syncProcess.Queues...), Live: 2, Draining: 1},
				{WorkerGroup: "latency", Queues: []string{"heartbeat"}, Live: 3},
			}, nil
		},
	}
	status, err := source.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Groups) != 2 || !reflect.DeepEqual(status.Groups[0], workerQueueStatus{
		Group: "sync", Queues: append([]string(nil), syncProcess.Queues...),
		ConfiguredInManifest: true,
		DesiredReplicas:      2, LiveReplicas: 2, MaxReplicas: 2,
		QueueBacklog: 7, ActiveJobs: 2, DrainState: "draining",
	}) {
		t.Fatalf("queue status = %#v", status.Groups)
	}
	if !reflect.DeepEqual(status.Groups[1], workerQueueStatus{
		Group: "latency", Queues: []string{"heartbeat"}, LiveReplicas: 3,
		DrainState: "active",
	}) {
		t.Fatalf("custom queue status = %#v", status.Groups[1])
	}
	// sync-provider is its own declared process (CHAOS-3926), so its
	// max_replicas x per-process connections count separately from sync.
	//
	// CHAOS-3945 then raised the queue-session figure again, from 26 to 34:
	// a started River client holds one notifier LISTEN session outside its
	// queue-control pool, so each of the four "river" runtime processes costs
	// 3 per replica, not 2. The pool went 27 -> 37, which is that declared 34
	// plus one whole replica of rolling-restart overlap, and the server
	// requirement 100 -> 200. Every process ships at zero replicas, so runtime
	// usage is unchanged; this is the declared worst case the budget covers.
	if status.ConnectionBudget.QueueSession != (connectionBudgetStatus{Used: 34, Limit: 37, Headroom: 3}) ||
		status.ConnectionBudget.CoordinatorSession != (connectionBudgetStatus{Used: 10, Limit: 11, Headroom: 1}) ||
		status.ConnectionBudget.Server != (connectionBudgetStatus{Used: 103, Limit: 200, Headroom: 97}) {
		t.Fatalf("connection budget = %#v", status.ConnectionBudget)
	}
}

func TestDispatchQueuesDrainAndUndrainRequireExplicitQueues(t *testing.T) {
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	backend := &commandBackend{
		queues: map[string]joboperator.QueueSummary{
			"heartbeat": {Group: "ops"},
			"retention": {Group: "ops"},
		},
	}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: backend, Authorizer: commandAuthorizer{},
		DomainGuard: commandDomainGuard{}, Auditor: commandAuditor{},
		RouteController:    commandRouteController{},
		JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &operatorRuntime{
		service:          service,
		queueControlMode: "direct",
		principal: joboperator.Principal{
			Type: "service_credential",
			ID:   "00000000-0000-4000-8000-000000000303",
		},
	}
	var stdout, stderr bytes.Buffer
	code := dispatch(context.Background(), runtime, []string{
		"queues", "drain", "--group", "ops",
		"--queue", "heartbeat", "--queue", "retention",
		"--reason", "deploy_drain", "--correlation-id", "queue-cli-1",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("queues drain code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"group":"ops"`) || !strings.Contains(stdout.String(), `"queues_paused":2`) {
		t.Fatalf("queues drain output=%q", stdout.String())
	}
	if len(backend.drainRequests) != 1 || strings.Join(backend.drainRequests[0], ",") != "heartbeat,retention" {
		t.Fatalf("drain requests = %#v", backend.drainRequests)
	}

	stdout.Reset()
	stderr.Reset()
	code = dispatch(context.Background(), runtime, []string{
		"queues", "undrain", "--group", "ops",
		"--queue", "heartbeat", "--queue", "retention",
		"--reason", "deploy_resume", "--correlation-id", "queue-cli-2",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("queues undrain code=%d stderr=%s", code, stderr.String())
	}
	if len(backend.undrainRequests) != 1 || strings.Join(backend.undrainRequests[0], ",") != "heartbeat,retention" {
		t.Fatalf("undrain requests = %#v", backend.undrainRequests)
	}
}

func TestDispatchContractsAcceptsExplicitQueues(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	var stdout, stderr bytes.Buffer
	code := dispatch(context.Background(), runtime, []string{
		"contracts", "--queue", "heartbeat", "--queue", "retention",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("contracts code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"queue":"heartbeat"`) ||
		!strings.Contains(stdout.String(), `"queue":"retention"`) ||
		strings.Contains(stdout.String(), `"profile":`) {
		t.Fatalf("contracts output=%q", stdout.String())
	}
}

func TestDispatchJobRouteRollbackIsOneBoundedAuthenticatedCommand(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	var stdout, stderr bytes.Buffer
	code := dispatch(context.Background(), runtime, []string{
		"job-routes", "rollback", "--reason", "provider_failure",
		"--correlation-id", "job-route-cli-1",
		"operational.billing_notification",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("rollback code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"kind":"operational.billing_notification"`) ||
		!strings.Contains(stdout.String(), `"transport":"celery"`) ||
		!strings.Contains(stdout.String(), `"generation":2`) {
		t.Fatalf("rollback output=%q", stdout.String())
	}
}

func TestDispatchJobRouteApplyIsOneBoundedAuthenticatedCommand(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{})
	var stdout, stderr bytes.Buffer
	code := dispatch(context.Background(), runtime, []string{
		"job-routes", "apply", "--reason", "canary_start",
		"--correlation-id", "job-route-cli-apply-1",
		"operational.billing_notification",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("apply code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"transport":"river_canary"`) ||
		!strings.Contains(stdout.String(), `"generation":2`) {
		t.Fatalf("apply output=%q", stdout.String())
	}
}

func commandRuntime(t *testing.T, authorizer joboperator.Authorizer) *operatorRuntime {
	t.Helper()
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	backend := &commandBackend{queues: map[string]joboperator.QueueSummary{}}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: backend, Authorizer: authorizer,
		DomainGuard: commandDomainGuard{}, Auditor: commandAuditor{},
		RouteController:    commandRouteController{},
		JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &operatorRuntime{
		service:          service,
		queueControlMode: "direct",
		principal: joboperator.Principal{
			Type: "service_credential",
			ID:   "00000000-0000-4000-8000-000000000303",
		},
	}
}

func TestSessionSafeModeRejectsTransactionAndUnknownModes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		mode string
		want bool
	}{
		{mode: "direct", want: true},
		{mode: "session", want: true},
		{mode: "transaction", want: false},
		{mode: "invalid", want: false},
	} {
		if got := sessionSafeMode(databaseMode(func(key string) (string, bool) { return test.mode, true }, "ignored")); got != test.want {
			t.Fatalf("mode %q allowed=%t, want %t", test.mode, got, test.want)
		}
	}
}
