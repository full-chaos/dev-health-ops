package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
)

type commandAuthorizer struct{ err error }

func (authorizer commandAuthorizer) Authorize(context.Context, joboperator.AuthorizationRequest) error {
	return authorizer.err
}

type commandBackend struct{}

func (commandBackend) Get(context.Context, int64) (joboperator.JobSummary, error) {
	return joboperator.JobSummary{}, errors.New("unused")
}
func (commandBackend) List(context.Context, joboperator.ListFilter) ([]joboperator.JobSummary, error) {
	return nil, errors.New("unused")
}
func (commandBackend) Queues(context.Context, string) ([]joboperator.QueueSummary, error) {
	return nil, errors.New("unused")
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
func (commandBackend) Drain(context.Context, string, joboperator.Mutation) (joboperator.DrainResult, error) {
	return joboperator.DrainResult{}, errors.New("unused")
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
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: commandBackend{}, Authorizer: authorizer,
		DomainGuard: commandDomainGuard{}, Auditor: commandAuditor{},
		RouteController:    commandRouteController{},
		JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &operatorRuntime{
		service: service,
		principal: joboperator.Principal{
			Type: "service_credential",
			ID:   "00000000-0000-4000-8000-000000000303",
		},
	}
}
