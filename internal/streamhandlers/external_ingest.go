package streamhandlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
)

const externalSchemaVersion = "external-ingest.v1"

var externalSystems = map[string]struct{}{
	"github": {}, "gitlab": {}, "jira": {}, "linear": {}, "custom": {}, "pagerduty": {}, "atlassian": {},
}

var operationalExternalKinds = map[string]struct{}{
	"operational_service.v1": {}, "operational_incident.v1": {}, "operational_alert.v1": {},
	"incident_timeline_event.v1": {}, "incident_note.v1": {}, "incident_responder.v1": {},
	"escalation_policy.v1": {}, "on_call_schedule.v1": {}, "on_call_assignment.v1": {},
	"operational_team.v1": {}, "operational_user.v1": {}, "service_repository_mapping.v1": {},
}

var externalAllowedKinds = map[string]map[string]struct{}{
	"github":    kindSet("repository.v1", "identity.v1", "team.v1", "work_item.v1", "work_item_transition.v1", "work_item_dependency.v1", "pull_request.v1", "review.v1", "commit.v1"),
	"gitlab":    kindSet("repository.v1", "identity.v1", "team.v1", "work_item.v1", "work_item_transition.v1", "work_item_dependency.v1", "pull_request.v1", "review.v1", "commit.v1"),
	"jira":      kindSet("identity.v1", "team.v1", "work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"),
	"linear":    kindSet("identity.v1", "team.v1", "work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"),
	"custom":    kindSet("repository.v1", "identity.v1", "team.v1", "pull_request.v1", "review.v1", "commit.v1"),
	"pagerduty": kindSet("identity.v1", "team.v1"),
	"atlassian": kindSet("identity.v1", "team.v1"),
}

func init() {
	for system := range externalAllowedKinds {
		if system == "jira" || system == "linear" {
			continue
		}
		for kind := range operationalExternalKinds {
			externalAllowedKinds[system][kind] = struct{}{}
		}
	}
}

type externalPointer struct {
	IngestionID    uuid.UUID
	OrgID          string
	SourceSystem   string
	SourceInstance string
	SchemaVersion  string
}

type externalBatch struct {
	Pointer         externalPointer
	SourceID        uuid.UUID
	EntityFamily    string
	ItemsReceived   int
	WindowStartedAt *time.Time
	WindowEndedAt   *time.Time
	Payload         []byte
	Skip            bool
}

type externalRejection struct {
	Index                                 int
	Kind, ExternalID, Code, Message, Path string
}

type externalCompletion struct {
	Accepted, Rejected int
	RecordCounts       map[string]int
	Rejections         []externalRejection
	Scope              ExternalRecomputeScope
}

type ExternalRecomputeScope struct {
	OrgID, SourceSystem, SourceInstance string
	IngestionID                         uuid.UUID
	RepoIDs, TeamIDs, RecordKinds       []string
	WindowStart, WindowEnd              *time.Time
}

type externalBatchRepository interface {
	LoadForProcessing(context.Context, externalPointer) (externalBatch, error)
	OperationalAllowed(context.Context, string) (bool, error)
	Complete(context.Context, externalBatch, externalCompletion) error
	Fail(context.Context, externalPointer, string) error
}

type externalSinkRecord struct {
	Index      int
	Kind       string
	ExternalID string
	Payload    map[string]any
}

type externalSinkBatch struct {
	Pointer  externalPointer
	SourceID uuid.UUID
	Records  []externalSinkRecord
}

type externalBatchSink interface {
	Write(context.Context, externalSinkBatch) (ExternalRecomputeScope, error)
}

type externalRecomputeScheduler interface {
	Schedule(context.Context, ExternalRecomputeScope) error
}

type ExternalIngestHandler struct {
	repository externalBatchRepository
	sink       externalBatchSink
	scheduler  externalRecomputeScheduler
	backoff    []time.Duration
}

func NewExternalIngestHandler(repository externalBatchRepository, sink externalBatchSink, scheduler externalRecomputeScheduler) (*ExternalIngestHandler, error) {
	if repository == nil || sink == nil {
		return nil, streamrunner.ErrInvalidConfig
	}
	return &ExternalIngestHandler{
		repository: repository, sink: sink, scheduler: scheduler,
		backoff: []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second},
	}, nil
}

func (h *ExternalIngestHandler) Handle(ctx context.Context, message streamrunner.Message) error {
	pointer, err := parseExternalPointer(message)
	if err != nil {
		return err
	}
	batch, err := h.repository.LoadForProcessing(ctx, pointer)
	if err != nil {
		return err
	}
	if batch.Skip {
		return nil
	}
	envelope, err := parseExternalEnvelope(batch.Payload)
	if err != nil {
		return err
	}
	if envelope.SchemaVersion != externalSchemaVersion || envelope.Source.System != pointer.SourceSystem ||
		envelope.Source.Instance != pointer.SourceInstance || envelope.Source.EntityFamily != batch.EntityFamily {
		return &streamrunner.PermanentError{Reason: "external_pointer_payload_mismatch"}
	}
	if envelope.Source.EntityFamily == "operational" {
		allowed, err := h.repository.OperationalAllowed(ctx, pointer.OrgID)
		if err != nil {
			return fmt.Errorf("evaluate external operational entitlement: %w", err)
		}
		if !allowed {
			return &streamrunner.PermanentError{Reason: "feature_disabled"}
		}
	}
	if len(envelope.Records) != batch.ItemsReceived {
		return &streamrunner.PermanentError{Reason: "external_record_count_mismatch"}
	}
	accepted, rejections, counts := normalizeExternalRecords(pointer, envelope)
	scope := ExternalRecomputeScope{
		OrgID: pointer.OrgID, SourceSystem: pointer.SourceSystem,
		SourceInstance: pointer.SourceInstance, IngestionID: pointer.IngestionID,
		WindowStart: batch.WindowStartedAt, WindowEnd: batch.WindowEndedAt,
	}
	if len(accepted) > 0 {
		scope, err = h.writeWithRetries(ctx, externalSinkBatch{
			Pointer: pointer, SourceID: batch.SourceID, Records: accepted,
		})
		if err != nil {
			return err
		}
		if scope.OrgID == "" {
			scope.OrgID = pointer.OrgID
		}
		if scope.SourceSystem == "" {
			scope.SourceSystem = pointer.SourceSystem
		}
		if scope.SourceInstance == "" {
			scope.SourceInstance = pointer.SourceInstance
		}
		if scope.IngestionID == uuid.Nil {
			scope.IngestionID = pointer.IngestionID
		}
		if scope.WindowStart == nil {
			scope.WindowStart = batch.WindowStartedAt
		}
		if scope.WindowEnd == nil {
			scope.WindowEnd = batch.WindowEndedAt
		}
	}
	completion := externalCompletion{
		Accepted: len(accepted), Rejected: len(rejections),
		RecordCounts: counts, Rejections: rejections, Scope: scope,
	}
	if err := h.repository.Complete(ctx, batch, completion); err != nil {
		return err
	}
	if h.scheduler != nil && len(accepted) > 0 {
		// Completion persisted the pending scope transactionally. Scheduling
		// is best-effort here; a crash or outage is recovered by the scheduler's
		// pending-scope scan rather than replaying already-terminal sink writes.
		_ = h.scheduler.Schedule(ctx, scope)
	}
	return nil
}

func (h *ExternalIngestHandler) writeWithRetries(ctx context.Context, batch externalSinkBatch) (ExternalRecomputeScope, error) {
	scope, err := h.sink.Write(ctx, batch)
	for _, delay := range h.backoff {
		if err == nil {
			return scope, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ExternalRecomputeScope{}, ctx.Err()
		case <-timer.C:
		}
		scope, err = h.sink.Write(ctx, batch)
	}
	if err != nil {
		return ExternalRecomputeScope{}, fmt.Errorf("external ingest durable sink retries exhausted: %w", err)
	}
	return scope, nil
}

func (h *ExternalIngestHandler) FinalizePermanent(ctx context.Context, message streamrunner.Message, reason string) error {
	pointer, err := parseExternalPointer(message)
	if err != nil {
		// A malformed UUID has no addressable status row. Its durable DLQ row
		// is the only possible terminal record.
		return nil
	}
	return h.repository.Fail(ctx, pointer, reason)
}

func parseExternalPointer(message streamrunner.Message) (externalPointer, error) {
	parts := strings.Split(message.Stream, ":")
	if len(parts) != 3 || parts[0] != "external-ingest" || parts[2] != "batches" {
		return externalPointer{}, &streamrunner.PermanentError{Reason: "invalid_external_stream"}
	}
	id, err := uuid.Parse(message.Fields["ingestion_id"])
	if err != nil {
		return externalPointer{}, &streamrunner.PermanentError{Reason: "invalid_external_ingestion_id"}
	}
	pointer := externalPointer{
		IngestionID: id, OrgID: strings.TrimSpace(message.Fields["org_id"]),
		SourceSystem:   strings.TrimSpace(message.Fields["source_system"]),
		SourceInstance: strings.TrimSpace(message.Fields["source_instance"]),
		SchemaVersion:  strings.TrimSpace(message.Fields["schema_version"]),
	}
	if pointer.OrgID == "" || pointer.OrgID != parts[1] || pointer.SourceInstance == "" ||
		pointer.SchemaVersion != externalSchemaVersion {
		return externalPointer{}, &streamrunner.PermanentError{Reason: "invalid_external_pointer"}
	}
	if _, ok := externalSystems[pointer.SourceSystem]; !ok {
		return externalPointer{}, &streamrunner.PermanentError{Reason: "invalid_external_source"}
	}
	return pointer, nil
}

type externalEnvelope struct {
	SchemaVersion  string
	IdempotencyKey string
	Source         struct {
		Type, System, Instance, EntityFamily string
	}
	Records []externalRecord
}

type externalRecord struct {
	Kind, ExternalID string
	Payload          map[string]any
}

func parseExternalEnvelope(raw []byte) (externalEnvelope, error) {
	var wire struct {
		SchemaVersion  string `json:"schemaVersion"`
		IdempotencyKey string `json:"idempotencyKey"`
		Source         struct {
			Type         string  `json:"type"`
			System       string  `json:"system"`
			Instance     string  `json:"instance"`
			EntityFamily string  `json:"entityFamily"`
			Producer     *string `json:"producer"`
			Version      *string `json:"producerVersion"`
		} `json:"source"`
		Window *struct {
			StartedAt string `json:"startedAt"`
			EndedAt   string `json:"endedAt"`
		} `json:"window"`
		Records []struct {
			Kind       string         `json:"kind"`
			ExternalID string         `json:"externalId"`
			Payload    map[string]any `json:"payload"`
		} `json:"records"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return externalEnvelope{}, &streamrunner.PermanentError{Reason: "invalid_external_payload"}
	}
	if err := ensureJSONEOF(decoder); err != nil || wire.SchemaVersion == "" || wire.IdempotencyKey == "" ||
		wire.Source.System == "" || wire.Source.Instance == "" || len(wire.Records) == 0 || len(wire.Records) > 1000 {
		return externalEnvelope{}, &streamrunner.PermanentError{Reason: "invalid_external_payload"}
	}
	entityFamily := wire.Source.EntityFamily
	if entityFamily == "" {
		entityFamily = "legacy"
	}
	if wire.Source.Type == "" {
		wire.Source.Type = "customer_push"
	}
	if wire.Source.Type != "customer_push" || (entityFamily != "legacy" && entityFamily != "operational") {
		return externalEnvelope{}, &streamrunner.PermanentError{Reason: "invalid_external_payload"}
	}
	if wire.Window != nil {
		started, startErr := time.Parse(time.RFC3339Nano, wire.Window.StartedAt)
		ended, endErr := time.Parse(time.RFC3339Nano, wire.Window.EndedAt)
		if startErr != nil || endErr != nil || ended.Before(started) {
			return externalEnvelope{}, &streamrunner.PermanentError{Reason: "invalid_external_payload"}
		}
	}
	envelope := externalEnvelope{SchemaVersion: wire.SchemaVersion, IdempotencyKey: wire.IdempotencyKey, Records: make([]externalRecord, 0, len(wire.Records))}
	envelope.Source.Type, envelope.Source.System, envelope.Source.Instance, envelope.Source.EntityFamily = wire.Source.Type, wire.Source.System, wire.Source.Instance, entityFamily
	for _, record := range wire.Records {
		if record.Kind == "" || record.ExternalID == "" || record.Payload == nil {
			return externalEnvelope{}, &streamrunner.PermanentError{Reason: "invalid_external_payload"}
		}
		envelope.Records = append(envelope.Records, externalRecord{Kind: record.Kind, ExternalID: record.ExternalID, Payload: record.Payload})
	}
	return envelope, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("extra JSON value")
	}
	return nil
}

func normalizeExternalRecords(pointer externalPointer, envelope externalEnvelope) ([]externalSinkRecord, []externalRejection, map[string]int) {
	accepted := make([]externalSinkRecord, 0, len(envelope.Records))
	rejections := make([]externalRejection, 0)
	counts := make(map[string]int)
	for index, record := range envelope.Records {
		if _, allowed := externalAllowedKinds[pointer.SourceSystem][record.Kind]; !allowed {
			rejections = append(rejections, rejection(index, record, "unsupported_kind_for_system", "record kind is not accepted for source system", "kind"))
			continue
		}
		_, operational := operationalExternalKinds[record.Kind]
		if (envelope.Source.EntityFamily == "operational") != operational {
			rejections = append(rejections, rejection(index, record, "entity_family_mismatch", "record kind does not match source entity family", "kind"))
			continue
		}
		if err := validateExternalRecord(record.Kind, record.Payload); err != nil {
			rejections = append(rejections, rejection(index, record, "invalid_field", err.Error(), "payload"))
			continue
		}
		if isGitFamilyKind(record.Kind) && (pointer.SourceSystem == "github" || pointer.SourceSystem == "gitlab" || pointer.SourceSystem == "custom") {
			repo := stringField(record.Payload, "repositoryExternalId")
			if record.Kind == "repository.v1" {
				repo = stringField(record.Payload, "externalId")
			}
			if !strings.EqualFold(repo, pointer.SourceInstance) {
				rejections = append(rejections, rejection(index, record, "record_outside_source_instance", "repository identifier does not match source instance", "payload"))
				continue
			}
		}
		accepted = append(accepted, externalSinkRecord{Index: index, Kind: record.Kind, ExternalID: record.ExternalID, Payload: record.Payload})
		counts[record.Kind]++
	}
	return accepted, rejections, counts
}

func rejection(index int, record externalRecord, code, message, path string) externalRejection {
	return externalRejection{
		Index: index, Kind: record.Kind, ExternalID: record.ExternalID,
		Code: code, Message: message, Path: fmt.Sprintf("records[%d].%s", index, path),
	}
}

func isGitFamilyKind(kind string) bool {
	return slices.Contains([]string{"repository.v1", "pull_request.v1", "review.v1", "commit.v1"}, kind)
}

func kindSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
