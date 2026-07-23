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

type InternalIngestHandler struct{ conn productClickHouse }

func NewInternalIngestHandler(conn productClickHouse) (*InternalIngestHandler, error) {
	if conn == nil {
		return nil, streamrunner.ErrInvalidConfig
	}
	return &InternalIngestHandler{conn: conn}, nil
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
	now := time.Now().UTC()
	switch entity {
	case "commits":
		batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO git_commits (repo_id,hash,message,author_name,author_email,author_when,committer_name,committer_email,committer_when,parents,last_synced)")
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
			if err := batch.Append(repoID, stringValue(item, "hash"), nullable(item, "message"), nullable(item, "author_name"), nullable(item, "author_email"), author, nullable(item, "committer_name"), nullable(item, "committer_email"), committer, uint32(number(item, "parents", 1)), now); err != nil {
				return fmt.Errorf("append commit: %w", err)
			}
		}
		return sendBatch(batch)
	case "pull-requests":
		batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO git_pull_requests (repo_id,number,title,body,state,author_name,author_email,created_at,merged_at,closed_at,head_branch,base_branch,additions,deletions,changed_files,last_synced)")
		if err != nil {
			return fmt.Errorf("prepare pull requests: %w", err)
		}
		for _, item := range envelope.Items {
			created, ok := itemTime(item, "created_at")
			if !ok || number(item, "number", 0) < 1 || stringValue(item, "title") == "" || stringValue(item, "state") == "" || stringValue(item, "author_name") == "" {
				return &streamrunner.PermanentError{Reason: "invalid_pull_request"}
			}
			merged, _ := itemTime(item, "merged_at")
			closed, _ := itemTime(item, "closed_at")
			if err := batch.Append(repoID, uint32(number(item, "number", 0)), nullable(item, "title"), nullable(item, "body"), nullable(item, "state"), nullable(item, "author_name"), nullable(item, "author_email"), created, nullableTime(merged), nullableTime(closed), nullable(item, "head_branch"), nullable(item, "base_branch"), nullableUint(item, "additions"), nullableUint(item, "deletions"), nullableUint(item, "changed_files"), now); err != nil {
				return fmt.Errorf("append pull request: %w", err)
			}
		}
		return sendBatch(batch)
	case "deployments":
		batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO deployments (repo_id,deployment_id,status,environment,started_at,finished_at,deployed_at,pull_request_number,release_ref,release_ref_confidence,last_synced)")
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
			if err := batch.Append(repoID, stringValue(item, "deployment_id"), nullable(item, "status"), nullable(item, "environment"), nullableTime(started), nullableTime(finished), nullableTime(deployed), nullableUint(item, "pull_request_number"), stringValue(item, "release_ref"), floatValue(item, "release_ref_confidence"), now); err != nil {
				return fmt.Errorf("append deployment: %w", err)
			}
		}
		return sendBatch(batch)
	case "work-items":
		batch, err := h.conn.PrepareBatch(ctx, "INSERT INTO work_items (repo_id,work_item_id,provider,title,description,type,status,status_raw,project_key,project_id,assignees,reporter,created_at,updated_at,started_at,completed_at,closed_at,labels,story_points,sprint_id,sprint_name,parent_id,epic_id,url,last_synced)")
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
			if err := batch.Append(uuid.Nil, stringValue(item, "work_item_id"), stringValue(item, "provider"), stringValue(item, "title"), nullable(item, "description"), stringValue(item, "type"), stringValue(item, "status"), stringValue(item, "status_raw"), stringValue(item, "project_key"), "", stringsValue(item, "assignees"), stringValue(item, "reporter"), created, updated, nullableTime(started), nullableTime(completed), nil, stringsValue(item, "labels"), nullableFloat(item, "story_points"), "", "", "", "", stringValue(item, "url"), now); err != nil {
				return fmt.Errorf("append work item: %w", err)
			}
		}
		return sendBatch(batch)
	default:
		return &streamrunner.PermanentError{Reason: "unsupported_ingest_entity"}
	}
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
