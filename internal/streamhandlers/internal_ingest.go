package streamhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
)

type InternalIngestHandler struct {
	conn productClickHouse
	now  func() time.Time
}

func NewInternalIngestHandler(conn productClickHouse) (*InternalIngestHandler, error) {
	if conn == nil {
		return nil, streamrunner.ErrInvalidConfig
	}
	return &InternalIngestHandler{conn: conn, now: time.Now}, nil
}

// Handle ports the existing `/api/v1/ingest` envelope. ReplacingMergeTree
// natural keys make the crash-after-write/before-ack replay idempotent.
func (h *InternalIngestHandler) Handle(ctx context.Context, message streamrunner.Message) error {
	parts := strings.Split(message.Stream, ":")
	if len(parts) != 3 || parts[0] != "ingest" {
		return &streamrunner.PermanentError{Reason: "invalid_ingest_stream"}
	}
	entity, raw := parts[2], message.Fields["payload"]
	if raw == "" {
		return &streamrunner.PermanentError{Reason: "missing_ingest_payload"}
	}
	var envelope struct {
		OrgID   string           `json:"org_id"`
		RepoURL string           `json:"repo_url"`
		Items   []map[string]any `json:"items"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil || envelope.OrgID == "" || len(envelope.Items) == 0 || len(envelope.Items) > 1000 {
		return &streamrunner.PermanentError{Reason: "invalid_ingest_payload"}
	}
	if envelope.OrgID != parts[1] {
		return &streamrunner.PermanentError{Reason: "ingest_org_stream_mismatch"}
	}
	if entity != "work-items" && entity != "incidents" && envelope.RepoURL == "" {
		return &streamrunner.PermanentError{Reason: "missing_ingest_repo"}
	}
	repoID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(envelope.RepoURL))
	now := h.now().UTC()
	switch entity {
	case "commits":
		batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO git_commits (org_id,repo_id,hash,message,author_name,author_email,author_when,committer_name,committer_email,committer_when,parents,last_synced)")
		if err != nil {
			return fmt.Errorf("prepare commits: %w", err)
		}
		for _, item := range envelope.Items {
			author, ok := itemTime(item, "author_when")
			if !ok || stringValue(item, "hash") == "" || stringValue(item, "message") == "" || stringValue(item, "author_name") == "" || stringValue(item, "author_email") == "" {
				return &streamrunner.PermanentError{Reason: "invalid_commit"}
			}
			committer, _ := itemTime(item, "committer_when")
			if committer.IsZero() {
				committer = author
			}
			if err := batch.Append(envelope.OrgID, repoID, stringValue(item, "hash"), nullable(item, "message"), nullable(item, "author_name"), nullable(item, "author_email"), author, nullable(item, "committer_name"), nullable(item, "committer_email"), committer, uint32(number(item, "parents", 1)), now); err != nil {
				return fmt.Errorf("append commit: %w", err)
			}
		}
		return sendBatch(batch)
	case "pull-requests":
		batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO git_pull_requests (org_id,repo_id,number,title,body,state,author_name,author_email,created_at,merged_at,closed_at,head_branch,base_branch,additions,deletions,changed_files,last_synced)")
		if err != nil {
			return fmt.Errorf("prepare pull requests: %w", err)
		}
		type reviewRow struct {
			number      uint32
			reviewID    string
			reviewer    string
			state       string
			submittedAt time.Time
		}
		reviews := make([]reviewRow, 0)
		for _, item := range envelope.Items {
			created, ok := itemTime(item, "created_at")
			if !ok || number(item, "number", 0) < 1 || stringValue(item, "title") == "" || stringValue(item, "state") == "" || stringValue(item, "author_name") == "" {
				return &streamrunner.PermanentError{Reason: "invalid_pull_request"}
			}
			merged, _ := itemTime(item, "merged_at")
			closed, _ := itemTime(item, "closed_at")
			prNumber := uint32(number(item, "number", 0))
			if err := batch.Append(envelope.OrgID, repoID, prNumber, nullable(item, "title"), nullable(item, "body"), nullable(item, "state"), nullable(item, "author_name"), nullable(item, "author_email"), created, nullableTime(merged), nullableTime(closed), nullable(item, "head_branch"), nullable(item, "base_branch"), nullableUint(item, "additions"), nullableUint(item, "deletions"), nullableUint(item, "changed_files"), now); err != nil {
				return fmt.Errorf("append pull request: %w", err)
			}
			rawReviews, ok := item["reviews"].([]any)
			if !ok && item["reviews"] != nil {
				return &streamrunner.PermanentError{Reason: "invalid_pull_request_review"}
			}
			for _, rawReview := range rawReviews {
				review, ok := rawReview.(map[string]any)
				if !ok {
					return &streamrunner.PermanentError{Reason: "invalid_pull_request_review"}
				}
				submittedAt, validTime := itemTime(review, "submitted_at")
				if !validTime || stringValue(review, "review_id") == "" || stringValue(review, "reviewer") == "" || stringValue(review, "state") == "" {
					return &streamrunner.PermanentError{Reason: "invalid_pull_request_review"}
				}
				reviews = append(reviews, reviewRow{
					number:      prNumber,
					reviewID:    stringValue(review, "review_id"),
					reviewer:    stringValue(review, "reviewer"),
					state:       stringValue(review, "state"),
					submittedAt: submittedAt,
				})
			}
		}
		if err := sendBatch(batch); err != nil {
			return err
		}
		if len(reviews) == 0 {
			return nil
		}
		reviewBatch, err := h.conn.PrepareBatch(ctx, "INSERT INTO git_pull_request_reviews (org_id,repo_id,number,review_id,reviewer,state,submitted_at,last_synced)")
		if err != nil {
			return fmt.Errorf("prepare pull request reviews: %w", err)
		}
		for _, review := range reviews {
			if err := reviewBatch.Append(envelope.OrgID, repoID, review.number, review.reviewID, review.reviewer, review.state, review.submittedAt, now); err != nil {
				return fmt.Errorf("append pull request review: %w", err)
			}
		}
		return sendBatch(reviewBatch)
	case "deployments":
		batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO deployments (org_id,repo_id,deployment_id,status,environment,started_at,finished_at,deployed_at,pull_request_number,release_ref,release_ref_confidence,last_synced)")
		if err != nil {
			return fmt.Errorf("prepare deployments: %w", err)
		}
		for _, item := range envelope.Items {
			if stringValue(item, "deployment_id") == "" || stringValue(item, "status") == "" || stringValue(item, "environment") == "" {
				return &streamrunner.PermanentError{Reason: "invalid_deployment"}
			}
			started, _ := itemTime(item, "started_at")
			finished, _ := itemTime(item, "finished_at")
			deployed, _ := itemTime(item, "deployed_at")
			if err := batch.Append(envelope.OrgID, repoID, stringValue(item, "deployment_id"), nullable(item, "status"), nullable(item, "environment"), nullableTime(started), nullableTime(finished), nullableTime(deployed), nullableUint(item, "pull_request_number"), stringValue(item, "release_ref"), floatValue(item, "release_ref_confidence"), now); err != nil {
				return fmt.Errorf("append deployment: %w", err)
			}
		}
		return sendBatch(batch)
	case "work-items":
		batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO work_items (org_id,repo_id,work_item_id,provider,title,description,type,status,status_raw,project_key,project_id,native_team_key,project_name,assignees,reporter,created_at,updated_at,started_at,completed_at,closed_at,labels,story_points,sprint_id,sprint_name,parent_id,epic_id,url,priority_raw,service_class,due_at,last_synced)")
		if err != nil {
			return fmt.Errorf("prepare work items: %w", err)
		}
		for _, item := range envelope.Items {
			created, ok := itemTime(item, "created_at")
			if !ok || stringValue(item, "work_item_id") == "" || stringValue(item, "provider") == "" || stringValue(item, "title") == "" || !validWorkItem(item) {
				return &streamrunner.PermanentError{Reason: "invalid_work_item"}
			}
			updated, _ := itemTime(item, "updated_at")
			if updated.IsZero() {
				updated = created
			}
			started, _ := itemTime(item, "started_at")
			completed, _ := itemTime(item, "completed_at")
			if err := batch.Append(envelope.OrgID, uuid.Nil, stringValue(item, "work_item_id"), stringValue(item, "provider"), stringValue(item, "title"), nullable(item, "description"), stringValue(item, "type"), stringValue(item, "status"), stringValue(item, "status_raw"), stringValue(item, "project_key"), "", "", "", stringsValue(item, "assignees"), stringValue(item, "reporter"), created, updated, nullableTime(started), nullableTime(completed), nil, stringsValue(item, "labels"), nullableFloat(item, "story_points"), "", "", "", "", stringValue(item, "url"), stringValue(item, "priority_raw"), "", nil, now); err != nil {
				return fmt.Errorf("append work item: %w", err)
			}
		}
		return sendBatch(batch)
	case "incidents":
		return h.persistIncidents(ctx, envelope.OrgID, envelope.RepoURL, repoID, envelope.Items, now)
	default:
		return &streamrunner.PermanentError{Reason: "unsupported_ingest_entity"}
	}
}

const operationalBaseColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at,source_revision,source_conflict_key,ingest_revision,ordering_contract,id,source_id,source_url,source_event_at,source_event_id,observed_at,last_synced,raw_status,raw_severity,raw_priority,normalized_status,normalized_severity,normalized_priority,relationship_provenance,relationship_confidence"

func (h *InternalIngestHandler) persistIncidents(ctx context.Context, orgID, repoURL string, repoID uuid.UUID, items []map[string]any, now time.Time) error {
	type incidentSource struct {
		id, status    string
		started       time.Time
		resolved      *time.Time
		sourceVersion time.Time
	}
	sources := make([]incidentSource, 0, len(items))
	for _, item := range items {
		started, ok := itemTime(item, "started_at")
		if !ok || stringValue(item, "incident_id") == "" || stringValue(item, "status") == "" {
			return &streamrunner.PermanentError{Reason: "invalid_incident"}
		}
		resolvedValue, hasResolved := item["resolved_at"]
		var resolved *time.Time
		if hasResolved && resolvedValue != nil {
			parsed, ok := itemTime(item, "resolved_at")
			if !ok {
				return &streamrunner.PermanentError{Reason: "invalid_incident"}
			}
			resolved = &parsed
		}
		sourceVersion := started
		if resolved != nil {
			sourceVersion = *resolved
		}
		sources = append(sources, incidentSource{
			id: stringValue(item, "incident_id"), status: stringValue(item, "status"),
			started: started, resolved: resolved, sourceVersion: sourceVersion,
		})
	}
	derivedVersion := sources[0].sourceVersion
	for _, source := range sources[1:] {
		if source.sourceVersion.After(derivedVersion) {
			derivedVersion = source.sourceVersion
		}
	}
	provider, providerInstance := "external", "legacy-repository-ingest"
	serviceID, err := canonicalOperationalID(orgID, provider, providerInstance, "operational_service", repoURL)
	if err != nil {
		return &streamrunner.PermanentError{Reason: "invalid_incident_identity"}
	}
	serviceBase := operationalBase{
		orgID: orgID, provider: provider, providerInstanceID: providerInstance,
		sourceEntityType: "repository", externalID: repoURL, sourceVersionAt: derivedVersion,
		observedAt: now, lastSynced: now,
	}
	serviceValues, err := operationalValues("operational_service", serviceBase, []operationalField{
		{"name", repoURL}, {"description", nil}, {"service_type", "repository"},
		{"owning_team_id", nil}, {"escalation_policy_id", nil}, {"is_deleted", false}, {"deleted_at", nil},
	})
	if err != nil {
		return &streamrunner.PermanentError{Reason: "invalid_incident_ordering"}
	}
	serviceBatch, err := h.conn.PrepareBatch(ctx, "INSERT INTO operational_services ("+operationalBaseColumns+",name,description,service_type,owning_team_id,escalation_policy_id,is_deleted,deleted_at)")
	if err != nil {
		return fmt.Errorf("prepare operational service: %w", err)
	}
	if err := serviceBatch.Append(serviceValues...); err != nil {
		return fmt.Errorf("append operational service: %w", err)
	}
	if err := sendBatch(serviceBatch); err != nil {
		return err
	}

	provenance, confidence := "native_repository_context", 1.0
	mappingExternalID := repoURL + ":" + repoID.String()
	mappingBase := operationalBase{
		orgID: orgID, provider: provider, providerInstanceID: providerInstance,
		sourceEntityType: "repository_mapping", externalID: mappingExternalID,
		sourceVersionAt: derivedVersion, observedAt: now, lastSynced: now,
		relationshipProvenance: &provenance, relationshipConfidence: &confidence,
	}
	mappingValues, err := operationalValues("operational_service_repository_mapping", mappingBase, []operationalField{
		{"service_id", serviceID}, {"repo_id", repoID}, {"repo_full_name", repoURL},
		{"repo_provider", provider}, {"mapping_kind", "repository_derived"}, {"rule_id", nil},
		{"valid_from", nil}, {"valid_to", nil}, {"is_active", true},
	})
	if err != nil {
		return &streamrunner.PermanentError{Reason: "invalid_incident_ordering"}
	}
	mappingBatch, err := h.conn.PrepareBatch(ctx, "INSERT INTO operational_service_repository_mappings ("+operationalBaseColumns+",service_id,repo_id,repo_full_name,repo_provider,mapping_kind,rule_id,valid_from,valid_to,is_active)")
	if err != nil {
		return fmt.Errorf("prepare operational service repository mapping: %w", err)
	}
	if err := mappingBatch.Append(mappingValues...); err != nil {
		return fmt.Errorf("append operational service repository mapping: %w", err)
	}
	if err := sendBatch(mappingBatch); err != nil {
		return err
	}

	incidentBatch, err := h.conn.PrepareBatch(ctx, "INSERT INTO operational_incidents ("+operationalBaseColumns+",service_id,service_external_id,escalation_policy_id,title,description,started_at,resolved_at,is_deleted,deleted_at)")
	if err != nil {
		return fmt.Errorf("prepare operational incidents: %w", err)
	}
	for _, source := range sources {
		rawStatus := source.status
		normalized := normalizedOperationalStatus(source.status)
		incidentBase := operationalBase{
			orgID: orgID, provider: provider, providerInstanceID: providerInstance,
			sourceEntityType: "issue", externalID: source.id, sourceVersionAt: source.sourceVersion,
			observedAt: now, lastSynced: now, rawStatus: &rawStatus, normalizedStatus: normalized,
		}
		incidentValues, orderingErr := operationalValues("operational_incident", incidentBase, []operationalField{
			{"service_id", serviceID}, {"service_external_id", repoURL}, {"escalation_policy_id", nil},
			{"title", source.id}, {"description", nil}, {"started_at", source.started},
			{"resolved_at", nullableTimePointer(source.resolved)}, {"is_deleted", false}, {"deleted_at", nil},
		})
		if orderingErr != nil {
			return &streamrunner.PermanentError{Reason: "invalid_incident_ordering"}
		}
		if err := incidentBatch.Append(incidentValues...); err != nil {
			return fmt.Errorf("append operational incident: %w", err)
		}
	}
	return sendBatch(incidentBatch)
}

func normalizedOperationalStatus(raw string) *string {
	normalized, ok := map[string]string{
		"active": "active", "acknowledged": "acknowledged", "closed": "resolved",
		"open": "open", "opened": "open", "resolved": "resolved", "suppressed": "suppressed",
	}[strings.ToLower(strings.TrimSpace(raw))]
	if !ok {
		return nil
	}
	return &normalized
}

func sendBatch(batch driver.Batch) error {
	if err := batch.Send(); err != nil {
		return fmt.Errorf("persist ingest: %w", err)
	}
	return nil
}
func stringValue(item map[string]any, key string) string {
	value, _ := item[key].(string)
	return value
}
func nullable(item map[string]any, key string) any {
	if value := stringValue(item, key); value != "" {
		return value
	}
	return nil
}
func number(item map[string]any, key string, fallback float64) float64 {
	value, ok := item[key].(float64)
	if !ok {
		return fallback
	}
	return value
}
func floatValue(item map[string]any, key string) float64 { return number(item, key, 0) }
func nullableUint(item map[string]any, key string) any {
	if _, ok := item[key]; !ok {
		return nil
	}
	return uint32(number(item, key, 0))
}
func nullableFloat(item map[string]any, key string) any {
	if _, ok := item[key]; !ok {
		return nil
	}
	return number(item, key, 0)
}
func stringsValue(item map[string]any, key string) []string {
	raw, _ := item[key].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
func itemTime(item map[string]any, key string) (time.Time, bool) {
	raw := stringValue(item, key)
	if raw == "" {
		return time.Time{}, false
	}
	value, err := time.Parse(time.RFC3339, raw)
	return value, err == nil
}
func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func validWorkItem(item map[string]any) bool {
	_, provider := map[string]struct{}{"jira": {}, "github": {}, "gitlab": {}, "linear": {}}[stringValue(item, "provider")]
	_, kind := map[string]struct{}{"story": {}, "task": {}, "bug": {}, "epic": {}, "issue": {}, "incident": {}, "chore": {}, "unknown": {}}[stringValue(item, "type")]
	_, status := map[string]struct{}{"backlog": {}, "todo": {}, "in_progress": {}, "in_review": {}, "blocked": {}, "done": {}, "canceled": {}, "unknown": {}}[stringValue(item, "status")]
	return provider && kind && status
}
