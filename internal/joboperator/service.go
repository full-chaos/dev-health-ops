// Package joboperator defines the sanitized, authenticated operator boundary
// for River jobs and queues. Database implementations remain behind Backend;
// this package never exposes encoded arguments or River error payloads.
package joboperator

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
)

type JobState string

const (
	StateAvailable JobState = "available"
	StatePending   JobState = "pending"
	StateRunning   JobState = "running"
	StateRetryable JobState = "retryable"
	StateScheduled JobState = "scheduled"
	StateCancelled JobState = "cancelled"
	StateCompleted JobState = "completed"
	StateDiscarded JobState = "discarded"
)

type Action string

const (
	ActionInspect      Action = "jobs.inspect"
	ActionCancel       Action = "jobs.cancel"
	ActionRetry        Action = "jobs.retry"
	ActionPauseQueue   Action = "queues.pause"
	ActionResumeQueue  Action = "queues.resume"
	ActionDrain        Action = "workers.drain"
	ActionInspectRoute Action = "routes.inspect"
	ActionPauseRoute   Action = "routes.pause"
	ActionDrainRoute   Action = "routes.drain"
	ActionResumeRoute  Action = "routes.resume"
)

// JobSummary is intentionally incapable of carrying encoded_args, exception
// text, or credentials. Backends must decode only the safe envelope metadata
// they need to populate correlation/domain fields.
type JobSummary struct {
	ID             int64                  `json:"id"`
	Kind           string                 `json:"kind"`
	Queue          string                 `json:"queue"`
	State          JobState               `json:"state"`
	Attempt        int                    `json:"attempt"`
	MaxAttempts    int                    `json:"max_attempts"`
	CreatedAt      time.Time              `json:"created_at"`
	ScheduledAt    time.Time              `json:"scheduled_at"`
	AttemptedAt    *time.Time             `json:"attempted_at,omitempty"`
	FinalizedAt    *time.Time             `json:"finalized_at,omitempty"`
	CorrelationID  string                 `json:"correlation_id"`
	OrganizationID *string                `json:"organization_id,omitempty"`
	Domain         jobcontract.DomainLink `json:"domain"`
}

type ListFilter struct {
	States []JobState
	Kind   string
	Queue  string
	Limit  int
}

// QueueSummary is safe operational telemetry. It contains counts and bounded
// queue identity only, never jobs or serialized arguments.
type QueueSummary struct {
	Name              string     `json:"name"`
	Profile           string     `json:"profile"`
	Paused            bool       `json:"paused"`
	Available         int64      `json:"available"`
	Running           int64      `json:"running"`
	Retryable         int64      `json:"retryable"`
	Scheduled         int64      `json:"scheduled"`
	OldestAvailableAt *time.Time `json:"oldest_available_at,omitempty"`
}

type Principal struct {
	Type string
	ID   string
}

type AuthorizationRequest struct {
	Principal    Principal
	Action       Action
	ResourceType string
	ResourceID   string
}

type Authorizer interface {
	Authorize(context.Context, AuthorizationRequest) error
}

// DomainGuard verifies authoritative domain state immediately before a
// cancel/retry mutation. Payload tenancy is never trusted for this decision.
type DomainGuard interface {
	Check(context.Context, Action, JobSummary) error
}

type Mutation struct {
	Principal     Principal
	Action        Action
	ResourceType  string
	ResourceID    string
	ReasonCode    string
	CorrelationID string
	ExpectedState JobState
}

type DrainResult struct {
	Profile        string `json:"profile"`
	QueuesPaused   int    `json:"queues_paused"`
	RunningAtStart int    `json:"running_at_start"`
}

type ContractSummary struct {
	Kind           string `json:"kind"`
	CurrentVersion int    `json:"current_version"`
	Profile        string `json:"profile"`
	Queue          string `json:"queue"`
	MigrationState string `json:"migration_state"`
	Route          string `json:"route"`
	RollbackRoute  string `json:"rollback_route"`
	Executable     bool   `json:"executable"`
}

// Backend performs compare-and-set mutations using ExpectedState. A backend
// backed by a PollOnly River client must return false from
// SupportsRunningCancellation because no prompt running-context signal exists.
type Backend interface {
	Get(context.Context, int64) (JobSummary, error)
	List(context.Context, ListFilter) ([]JobSummary, error)
	Queues(context.Context, string) ([]QueueSummary, error)
	Cancel(context.Context, int64, Mutation) (JobSummary, error)
	Retry(context.Context, int64, Mutation) (JobSummary, error)
	PauseQueue(context.Context, string, Mutation) error
	ResumeQueue(context.Context, string, Mutation) error
	Drain(context.Context, string, Mutation) (DrainResult, error)
	SupportsRunningCancellation() bool
}

type RouteController interface {
	Inspect(context.Context, string) (syncroute.RouteState, error)
	Pause(context.Context, string) (syncroute.RouteState, error)
	Drain(context.Context, string) (syncroute.RouteState, error)
	Resume(context.Context, string, string, time.Duration) (syncroute.RouteState, error)
}

type AuditEvent struct {
	Principal     Principal
	Action        Action
	ResourceType  string
	ResourceID    string
	ReasonCode    string
	CorrelationID string
	CreatedAt     time.Time
}

type AuditStatus string

const (
	AuditSucceeded      AuditStatus = "succeeded"
	AuditFailed         AuditStatus = "failed"
	AuditOutcomeUnknown AuditStatus = "outcome_unknown"
)

// AuditHandle represents a durably recorded intent. Begin must commit the
// intent before returning; Complete records only bounded success/failure.
type AuditHandle interface {
	Complete(context.Context, AuditStatus) error
}

type Auditor interface {
	Begin(context.Context, AuditEvent) (AuditHandle, error)
}

type Clock func() time.Time

const auditCompletionTimeout = 5 * time.Second

type Dependencies struct {
	Registry        RuntimeRegistry
	Backend         Backend
	Authorizer      Authorizer
	DomainGuard     DomainGuard
	Auditor         Auditor
	Clock           Clock
	RouteController RouteController
}

type Service struct {
	registry    RuntimeRegistry
	backend     Backend
	authorizer  Authorizer
	domainGuard DomainGuard
	auditor     Auditor
	clock       Clock
	routes      RouteController
}

// RuntimeRegistry is the read-only subset of jobruntime.Registry needed by
// operator policy. Keeping it as an interface allows deployment validation and
// operator transport to evolve independently.
type RuntimeRegistry interface {
	Descriptor(string) (jobruntime.Descriptor, bool)
	Profile(string) []jobruntime.Descriptor
	HasProfile(string) bool
	HasQueue(string) bool
}

func New(dependencies Dependencies) (*Service, error) {
	if dependencies.Registry == nil || dependencies.Backend == nil || dependencies.Authorizer == nil ||
		dependencies.DomainGuard == nil || dependencies.Auditor == nil || dependencies.RouteController == nil {
		return nil, errors.New("complete operator dependencies are required")
	}
	clock := dependencies.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Service{
		registry:    dependencies.Registry,
		backend:     dependencies.Backend,
		authorizer:  dependencies.Authorizer,
		domainGuard: dependencies.DomainGuard,
		auditor:     dependencies.Auditor,
		clock:       clock,
		routes:      dependencies.RouteController,
	}, nil
}

func (service *Service) InspectRoute(ctx context.Context, principal Principal, kind string) (syncroute.RouteState, error) {
	if err := validatePrincipal(principal); err != nil || kind == "" {
		return syncroute.RouteState{}, serviceError(CodeInvalid, err)
	}
	if err := service.authorize(ctx, principal, ActionInspectRoute, "sync_route", kind); err != nil {
		return syncroute.RouteState{}, err
	}
	state, err := service.routes.Inspect(ctx, kind)
	if err != nil {
		return syncroute.RouteState{}, mapRouteError(err)
	}
	return state, nil
}

func (service *Service) PauseRoute(
	ctx context.Context,
	principal Principal,
	kind, reasonCode, correlationID string,
) (syncroute.RouteState, error) {
	return service.routeMutation(ctx, principal, ActionPauseRoute, kind, reasonCode, correlationID,
		func() (syncroute.RouteState, error) { return service.routes.Pause(ctx, kind) })
}

func (service *Service) DrainRoute(
	ctx context.Context,
	principal Principal,
	kind, reasonCode, correlationID string,
) (syncroute.RouteState, error) {
	return service.routeMutation(ctx, principal, ActionDrainRoute, kind, reasonCode, correlationID,
		func() (syncroute.RouteState, error) { return service.routes.Drain(ctx, kind) })
}

func (service *Service) ResumeRoute(
	ctx context.Context,
	principal Principal,
	kind, transport, reasonCode, correlationID string,
	quiescenceTimeout time.Duration,
) (syncroute.RouteState, error) {
	return service.routeMutation(ctx, principal, ActionResumeRoute, kind, reasonCode, correlationID,
		func() (syncroute.RouteState, error) {
			return service.routes.Resume(ctx, kind, transport, quiescenceTimeout)
		})
}

func (service *Service) routeMutation(
	ctx context.Context,
	principal Principal,
	action Action,
	kind, reasonCode, correlationID string,
	operation func() (syncroute.RouteState, error),
) (syncroute.RouteState, error) {
	if err := validateMutationInput(principal, reasonCode, correlationID); err != nil || kind == "" {
		return syncroute.RouteState{}, serviceError(CodeInvalid, err)
	}
	if err := service.authorize(ctx, principal, action, "sync_route", kind); err != nil {
		return syncroute.RouteState{}, err
	}
	mutation := Mutation{
		Principal: principal, Action: action, ResourceType: "sync_route", ResourceID: kind,
		ReasonCode: reasonCode, CorrelationID: correlationID,
	}
	var state syncroute.RouteState
	err := service.mutate(ctx, mutation, func() error {
		var operationErr error
		state, operationErr = operation()
		return operationErr
	})
	return state, err
}

type ErrorCode string

const (
	CodeInvalid        ErrorCode = "invalid_request"
	CodeUnauthorized   ErrorCode = "unauthorized"
	CodeNotFound       ErrorCode = "not_found"
	CodeConflict       ErrorCode = "state_conflict"
	CodePrecondition   ErrorCode = "domain_precondition"
	CodeAudit          ErrorCode = "audit_unavailable"
	CodeAuditPending   ErrorCode = "audit_pending"
	CodeBackend        ErrorCode = "backend_unavailable"
	CodeOutcomeUnknown ErrorCode = "outcome_unknown"
)

type ServiceError struct {
	Code  ErrorCode
	cause error
}

func (err *ServiceError) Error() string {
	return "job operator request failed [" + string(err.Code) + "]"
}

func (err *ServiceError) Unwrap() error { return err.cause }

var (
	ErrNotFound      = errors.New("job not found")
	ErrStateConflict = errors.New("job state conflict")
	// ErrMutationOutcomeUnknown means PostgreSQL returned an error while
	// committing a queue mutation. The transaction may have committed before
	// the connection failure became observable, so callers must inspect state
	// and must never label the action failed or retry it blindly.
	ErrMutationOutcomeUnknown = errors.New("worker mutation outcome is unknown")
)

func (service *Service) Inspect(ctx context.Context, principal Principal, jobID int64) (JobSummary, error) {
	if err := validatePrincipal(principal); err != nil || jobID < 1 {
		return JobSummary{}, serviceError(CodeInvalid, err)
	}
	if err := service.authorize(ctx, principal, ActionInspect, "job", strconv.FormatInt(jobID, 10)); err != nil {
		return JobSummary{}, err
	}
	job, err := service.backend.Get(ctx, jobID)
	if err != nil {
		return JobSummary{}, mapBackendError(err)
	}
	if err := service.validateSummary(job); err != nil {
		return JobSummary{}, serviceError(CodeBackend, err)
	}
	return job, nil
}

// Status authorizes the top-level runtime status view. Composition performs
// the database and River-schema probes before constructing Service, so this
// method owns only the read-scope decision and exposes no backend detail.
func (service *Service) Status(ctx context.Context, principal Principal) error {
	if err := validatePrincipal(principal); err != nil {
		return serviceError(CodeInvalid, err)
	}
	return service.authorize(ctx, principal, ActionInspect, "workers", "status")
}

func (service *Service) List(ctx context.Context, principal Principal, filter ListFilter) ([]JobSummary, error) {
	if err := validatePrincipal(principal); err != nil {
		return nil, serviceError(CodeInvalid, err)
	}
	if err := service.validateFilter(filter); err != nil {
		return nil, serviceError(CodeInvalid, err)
	}
	if err := service.authorize(ctx, principal, ActionInspect, "jobs", "list"); err != nil {
		return nil, err
	}
	jobs, err := service.backend.List(ctx, filter)
	if err != nil {
		return nil, mapBackendError(err)
	}
	for _, job := range jobs {
		if err := service.validateSummary(job); err != nil {
			return nil, serviceError(CodeBackend, err)
		}
	}
	return jobs, nil
}

func (service *Service) Queues(ctx context.Context, principal Principal, profile string) ([]QueueSummary, error) {
	if err := validatePrincipal(principal); err != nil || !service.registry.HasProfile(profile) {
		return nil, serviceError(CodeInvalid, err)
	}
	if err := service.authorize(ctx, principal, ActionInspect, "profile_queues", profile); err != nil {
		return nil, err
	}
	summaries, err := service.backend.Queues(ctx, profile)
	if err != nil {
		return nil, mapBackendError(err)
	}
	expected := make(map[string]struct{})
	for _, descriptor := range service.registry.Profile(profile) {
		expected[descriptor.Queue] = struct{}{}
	}
	seen := make(map[string]struct{}, len(summaries))
	for _, summary := range summaries {
		if summary.Profile != profile || !service.registry.HasQueue(summary.Name) ||
			summary.Available < 0 || summary.Running < 0 || summary.Retryable < 0 || summary.Scheduled < 0 {
			return nil, serviceError(CodeBackend, errors.New("invalid queue summary"))
		}
		if _, ok := expected[summary.Name]; !ok {
			return nil, serviceError(CodeBackend, errors.New("queue summary is outside profile"))
		}
		if _, duplicate := seen[summary.Name]; duplicate {
			return nil, serviceError(CodeBackend, errors.New("duplicate queue summary"))
		}
		seen[summary.Name] = struct{}{}
	}
	if len(seen) != len(expected) {
		return nil, serviceError(CodeBackend, errors.New("queue summary coverage is incomplete"))
	}
	sort.Slice(summaries, func(left, right int) bool { return summaries[left].Name < summaries[right].Name })
	return summaries, nil
}

func (service *Service) Contracts(ctx context.Context, principal Principal, profile string) ([]ContractSummary, error) {
	if err := validatePrincipal(principal); err != nil || !service.registry.HasProfile(profile) {
		return nil, serviceError(CodeInvalid, err)
	}
	if err := service.authorize(ctx, principal, ActionInspect, "profile_contracts", profile); err != nil {
		return nil, err
	}
	descriptors := service.registry.Profile(profile)
	result := make([]ContractSummary, 0, len(descriptors))
	for _, descriptor := range descriptors {
		result = append(result, ContractSummary{
			Kind: descriptor.Kind, CurrentVersion: descriptor.CurrentVersion,
			Profile: descriptor.Profile, Queue: descriptor.Queue,
			MigrationState: descriptor.MigrationState, Route: descriptor.Route,
			RollbackRoute: descriptor.RollbackRoute, Executable: descriptor.Executable(),
		})
	}
	return result, nil
}

func (service *Service) Cancel(ctx context.Context, principal Principal, jobID int64, reasonCode, correlationID string) (JobSummary, error) {
	job, mutation, err := service.prepareJobMutation(ctx, principal, ActionCancel, jobID, reasonCode, correlationID)
	if err != nil {
		return JobSummary{}, err
	}
	if !cancelEligible(job.State) || (job.State == StateRunning && !service.backend.SupportsRunningCancellation()) {
		return JobSummary{}, serviceError(CodeConflict, errors.New("job is not safely cancellable"))
	}
	if err := service.domainGuard.Check(ctx, ActionCancel, job); err != nil {
		return JobSummary{}, serviceError(CodePrecondition, err)
	}
	var updated JobSummary
	err = service.mutate(ctx, mutation, func() error {
		var mutationErr error
		updated, mutationErr = service.backend.Cancel(ctx, jobID, mutation)
		return mutationErr
	})
	if err != nil {
		return JobSummary{}, err
	}
	if err := service.validateSummary(updated); err != nil {
		return JobSummary{}, serviceError(CodeBackend, err)
	}
	if updated.State != StateCancelled && !(job.State == StateRunning && updated.State == StateRunning) {
		return JobSummary{}, serviceError(CodeBackend, errors.New("cancel returned an invalid state"))
	}
	return updated, nil
}

func (service *Service) Retry(ctx context.Context, principal Principal, jobID int64, reasonCode, correlationID string) (JobSummary, error) {
	job, mutation, err := service.prepareJobMutation(ctx, principal, ActionRetry, jobID, reasonCode, correlationID)
	if err != nil {
		return JobSummary{}, err
	}
	descriptor, _ := service.registry.Descriptor(job.Kind)
	if !retryEligible(job.State) || !descriptor.Executable() {
		return JobSummary{}, serviceError(CodeConflict, errors.New("job is not safely retryable"))
	}
	if err := service.domainGuard.Check(ctx, ActionRetry, job); err != nil {
		return JobSummary{}, serviceError(CodePrecondition, err)
	}
	var updated JobSummary
	err = service.mutate(ctx, mutation, func() error {
		var mutationErr error
		updated, mutationErr = service.backend.Retry(ctx, jobID, mutation)
		return mutationErr
	})
	if err != nil {
		return JobSummary{}, err
	}
	if err := service.validateSummary(updated); err != nil {
		return JobSummary{}, serviceError(CodeBackend, err)
	}
	if updated.State != StateAvailable {
		return JobSummary{}, serviceError(CodeBackend, errors.New("retry returned an invalid state"))
	}
	return updated, nil
}

func (service *Service) PauseQueue(ctx context.Context, principal Principal, queue, reasonCode, correlationID string) error {
	return service.queueMutation(ctx, principal, ActionPauseQueue, queue, reasonCode, correlationID, service.backend.PauseQueue)
}

func (service *Service) ResumeQueue(ctx context.Context, principal Principal, queue, reasonCode, correlationID string) error {
	return service.queueMutation(ctx, principal, ActionResumeQueue, queue, reasonCode, correlationID, service.backend.ResumeQueue)
}

func (service *Service) Drain(ctx context.Context, principal Principal, profile, reasonCode, correlationID string) (DrainResult, error) {
	if err := validateMutationInput(principal, reasonCode, correlationID); err != nil || !service.registry.HasProfile(profile) {
		return DrainResult{}, serviceError(CodeInvalid, err)
	}
	mutation := Mutation{
		Principal: principal, Action: ActionDrain, ResourceType: "profile", ResourceID: profile,
		ReasonCode: reasonCode, CorrelationID: correlationID,
	}
	if err := service.authorize(ctx, principal, mutation.Action, mutation.ResourceType, mutation.ResourceID); err != nil {
		return DrainResult{}, err
	}
	var result DrainResult
	err := service.mutate(ctx, mutation, func() error {
		var mutationErr error
		result, mutationErr = service.backend.Drain(ctx, profile, mutation)
		return mutationErr
	})
	if err != nil {
		return DrainResult{}, err
	}
	if result.Profile != profile || result.QueuesPaused < 0 || result.RunningAtStart < 0 {
		return DrainResult{}, serviceError(CodeBackend, errors.New("invalid drain result"))
	}
	return result, nil
}

func (service *Service) prepareJobMutation(ctx context.Context, principal Principal, action Action, jobID int64, reasonCode, correlationID string) (JobSummary, Mutation, error) {
	if err := validateMutationInput(principal, reasonCode, correlationID); err != nil || jobID < 1 {
		return JobSummary{}, Mutation{}, serviceError(CodeInvalid, err)
	}
	resourceID := strconv.FormatInt(jobID, 10)
	if err := service.authorize(ctx, principal, action, "job", resourceID); err != nil {
		return JobSummary{}, Mutation{}, err
	}
	job, err := service.backend.Get(ctx, jobID)
	if err != nil {
		return JobSummary{}, Mutation{}, mapBackendError(err)
	}
	if err := service.validateSummary(job); err != nil {
		return JobSummary{}, Mutation{}, serviceError(CodeBackend, err)
	}
	return job, Mutation{
		Principal: principal, Action: action, ResourceType: "job", ResourceID: resourceID,
		ReasonCode: reasonCode, CorrelationID: correlationID, ExpectedState: job.State,
	}, nil
}

func (service *Service) queueMutation(ctx context.Context, principal Principal, action Action, queue, reasonCode, correlationID string, operation func(context.Context, string, Mutation) error) error {
	if err := validateMutationInput(principal, reasonCode, correlationID); err != nil {
		return serviceError(CodeInvalid, err)
	}
	if !service.hasQueue(queue) {
		return serviceError(CodeInvalid, errors.New("queue is not registered"))
	}
	mutation := Mutation{
		Principal: principal, Action: action, ResourceType: "queue", ResourceID: queue,
		ReasonCode: reasonCode, CorrelationID: correlationID,
	}
	if err := service.authorize(ctx, principal, action, "queue", queue); err != nil {
		return err
	}
	return service.mutate(ctx, mutation, func() error { return operation(ctx, queue, mutation) })
}

func (service *Service) mutate(ctx context.Context, mutation Mutation, operation func() error) error {
	handle, err := service.auditor.Begin(ctx, AuditEvent{
		Principal: mutation.Principal, Action: mutation.Action,
		ResourceType: mutation.ResourceType, ResourceID: mutation.ResourceID,
		ReasonCode: mutation.ReasonCode, CorrelationID: mutation.CorrelationID,
		CreatedAt: service.clock().UTC(),
	})
	if err != nil || handle == nil {
		return serviceError(CodeAudit, err)
	}
	if err := operation(); err != nil {
		status := AuditFailed
		if errors.Is(err, ErrMutationOutcomeUnknown) || errors.Is(err, syncroute.ErrMutationOutcomeUnknown) {
			status = AuditOutcomeUnknown
		}
		completeErr := completeAudit(ctx, handle, status)
		if status == AuditOutcomeUnknown {
			return serviceError(CodeOutcomeUnknown, errors.Join(err, completeErr))
		}
		if completeErr != nil {
			return serviceError(CodeAuditPending, errors.Join(err, completeErr))
		}
		if errors.Is(err, syncroute.ErrUnknownRoute) ||
			errors.Is(err, syncroute.ErrRouteStateConflict) ||
			errors.Is(err, syncroute.ErrLiveClaims) ||
			errors.Is(err, syncroute.ErrCapabilityMissing) ||
			errors.Is(err, syncroute.ErrQuiescenceMissing) ||
			errors.Is(err, syncroute.ErrInvalidConfiguration) {
			return mapRouteError(err)
		}
		return mapBackendError(err)
	}
	if err := completeAudit(ctx, handle, AuditSucceeded); err != nil {
		return serviceError(CodeAuditPending, err)
	}
	return nil
}

func completeAudit(ctx context.Context, handle AuditHandle, status AuditStatus) error {
	completionContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), auditCompletionTimeout)
	defer cancel()
	return handle.Complete(completionContext, status)
}

func (service *Service) authorize(ctx context.Context, principal Principal, action Action, resourceType, resourceID string) error {
	if err := service.authorizer.Authorize(ctx, AuthorizationRequest{
		Principal: principal, Action: action, ResourceType: resourceType, ResourceID: resourceID,
	}); err != nil {
		return serviceError(CodeUnauthorized, err)
	}
	return nil
}

func (service *Service) validateFilter(filter ListFilter) error {
	if filter.Limit < 1 || filter.Limit > 500 {
		return errors.New("list limit must be between 1 and 500")
	}
	if len(filter.States) == 0 {
		return errors.New("at least one state is required")
	}
	states := make([]string, 0, len(filter.States))
	seen := make(map[JobState]struct{}, len(filter.States))
	for _, state := range filter.States {
		if state != StateAvailable && state != StateRunning && state != StateRetryable && state != StateScheduled {
			return errors.New("state is not inspectable")
		}
		if _, duplicate := seen[state]; duplicate {
			return errors.New("duplicate state")
		}
		seen[state] = struct{}{}
		states = append(states, string(state))
	}
	if !sort.StringsAreSorted(states) {
		return errors.New("states must be sorted")
	}
	if filter.Kind != "" {
		descriptor, ok := service.registry.Descriptor(filter.Kind)
		if !ok {
			return errors.New("kind is not registered")
		}
		if filter.Queue != "" && descriptor.Queue != filter.Queue {
			return errors.New("kind and queue do not match")
		}
	}
	if filter.Queue != "" && !service.hasQueue(filter.Queue) {
		return errors.New("queue is not registered")
	}
	return nil
}

func (service *Service) validateSummary(job JobSummary) error {
	if job.ID < 1 || job.Attempt < 0 || job.MaxAttempts < 1 || job.Attempt > job.MaxAttempts {
		return errors.New("job summary has invalid execution identity")
	}
	descriptor, ok := service.registry.Descriptor(job.Kind)
	if !ok || descriptor.Queue != job.Queue || job.MaxAttempts < descriptor.MaxAttempts {
		return errors.New("job summary drifts from registry")
	}
	if !knownState(job.State) {
		return errors.New("job summary has invalid state")
	}
	if !safeIdentifier.MatchString(job.CorrelationID) || len(job.CorrelationID) > 128 {
		return errors.New("job summary correlation is invalid")
	}
	if descriptor.OrganizationScope == "tenant" {
		if job.OrganizationID == nil || !uuidIdentifier.MatchString(*job.OrganizationID) {
			return errors.New("job summary organization is invalid")
		}
	} else if job.OrganizationID != nil {
		return errors.New("global job summary carries an organization")
	}
	if job.Domain.Type != descriptor.DomainLink || !uuidIdentifier.MatchString(job.Domain.ID) {
		return errors.New("job summary domain link drifts from registry")
	}
	return nil
}

func (service *Service) hasQueue(queue string) bool {
	return service.registry.HasQueue(queue)
}

var (
	safeIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$`)
	reasonCode     = regexp.MustCompile(`^[a-z][a-z0-9_.-]{2,63}$`)
	principalType  = regexp.MustCompile(`^[a-z][a-z0-9_]{1,31}$`)
	uuidIdentifier = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

func validatePrincipal(principal Principal) error {
	if !principalType.MatchString(principal.Type) || !safeIdentifier.MatchString(principal.ID) || len(principal.ID) > 128 {
		return errors.New("principal is invalid")
	}
	return nil
}

func validateMutationInput(principal Principal, reason, correlation string) error {
	if err := validatePrincipal(principal); err != nil {
		return err
	}
	if !reasonCode.MatchString(reason) || !safeIdentifier.MatchString(correlation) || len(correlation) > 128 {
		return errors.New("reason or correlation is invalid")
	}
	return nil
}

func knownState(state JobState) bool {
	return state == StateAvailable || state == StatePending || state == StateRunning || state == StateRetryable || state == StateScheduled ||
		state == StateCancelled || state == StateCompleted || state == StateDiscarded
}

func cancelEligible(state JobState) bool {
	return state == StateAvailable || state == StateRunning || state == StateRetryable || state == StateScheduled
}

func retryEligible(state JobState) bool {
	return state == StateRetryable || state == StateCancelled || state == StateDiscarded
}

func mapBackendError(err error) error {
	if errors.Is(err, ErrNotFound) {
		return serviceError(CodeNotFound, err)
	}
	if errors.Is(err, ErrStateConflict) {
		return serviceError(CodeConflict, err)
	}
	return serviceError(CodeBackend, err)
}

func mapRouteError(err error) error {
	switch {
	case errors.Is(err, syncroute.ErrUnknownRoute):
		return serviceError(CodeNotFound, err)
	case errors.Is(err, syncroute.ErrRouteStateConflict), errors.Is(err, syncroute.ErrLiveClaims):
		return serviceError(CodeConflict, err)
	case errors.Is(err, syncroute.ErrCapabilityMissing), errors.Is(err, syncroute.ErrQuiescenceMissing):
		return serviceError(CodePrecondition, err)
	case errors.Is(err, syncroute.ErrInvalidConfiguration):
		return serviceError(CodeInvalid, err)
	default:
		return serviceError(CodeBackend, err)
	}
}

func serviceError(code ErrorCode, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("%s", code)
	}
	return &ServiceError{Code: code, cause: cause}
}
