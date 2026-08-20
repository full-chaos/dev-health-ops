package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

const githubWorkItemsIncompleteResultKey = "incomplete"

// applyGitHubWorkItemsIncompletePolicy is the single reader for D17's durable
// optional-enrichment evidence. It accepts both the live typed slice emitted
// by GitHubWorkItemsRouteHandler and the []any/map[string]any shape produced
// when a prepared snapshot is decoded from JSON.
//
// A non-empty, well-formed optional set keeps every effect eligible to land
// but suppresses the completion watermark. A blocking cause, an unknown
// component, or malformed durable evidence fails closed instead of converting
// a required-data failure into a successful incomplete batch.
func applyGitHubWorkItemsIncompletePolicy(
	provider string,
	dataset string,
	result map[string]any,
	watermark *time.Time,
) (map[string]any, *time.Time, error) {
	if strings.ToLower(strings.TrimSpace(provider)) != "github" {
		return result, watermark, nil
	}
	if strings.ToLower(strings.TrimSpace(dataset)) != "work-items" {
		return result, watermark, nil
	}
	value := result[githubWorkItemsIncompleteResultKey]
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, nil, ErrInvalidConfiguration
	}
	if bytes.Equal(encoded, []byte("null")) {
		return nil, nil, ErrInvalidConfiguration
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var incomplete []GitHubWorkItemsIncomplete
	if err := decoder.Decode(&incomplete); err != nil {
		return nil, nil, ErrInvalidConfiguration
	}
	if len(incomplete) > maxEffectRows {
		return nil, nil, ErrInvalidConfiguration
	}
	for _, partial := range incomplete {
		if partial.Cause == "" {
			return nil, nil, ErrInvalidConfiguration
		}
		if partial.Cause != strings.TrimSpace(partial.Cause) {
			return nil, nil, ErrInvalidConfiguration
		}
		if partial.SubjectID != strings.TrimSpace(partial.SubjectID) {
			return nil, nil, ErrInvalidConfiguration
		}
		if !githubWorkItemsIncompleteIsOptional(partial) {
			return nil, nil, ErrInvalidConfiguration
		}
	}

	normalized := cloneCompletionResult(result)
	// The normalized entry MUST be a non-nil slice. This policy is applied
	// twice on every healthy GitHub work-items unit -- once by
	// CompleteRouteExecutor.Execute (complete_route.go) on the collected batch,
	// and again by PostgresRepository.Complete (repository_postgres.go) on the
	// payload that batch produced -- and the prepared snapshot re-reads it a
	// third time through JSON on recovery. json.Marshal renders a nil slice as
	// `null`, which is exactly the "the route emitted no durable evidence"
	// shape the reader above fails closed on. Writing `append(nil)` therefore
	// made the empty-evidence case (no optional enrichment failed: the healthy
	// path) accepted on the first application and refused on the second, so
	// github/work-items committed its effects and then failed completion on
	// every attempt, forever (CHAOS-3940). Normalization must be a fixed point.
	entries := make([]GitHubWorkItemsIncomplete, 0, len(incomplete))
	entries = append(entries, incomplete...)
	normalized[githubWorkItemsIncompleteResultKey] = entries
	if len(incomplete) != 0 {
		return normalized, nil, nil
	}
	if watermark == nil {
		return normalized, nil, nil
	}
	valueUTC := watermark.UTC()
	return normalized, &valueUTC, nil
}
