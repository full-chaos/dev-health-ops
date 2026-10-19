package providersync

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// jiraStoryPointsFieldNames is the preference order of the Jira story point
// custom fields: the classic "Story Points" field first, then the
// next-gen/team-managed "Story point estimate" field.
var jiraStoryPointsFieldNames = []string{"story points", "story point estimate"}

type jiraFieldMetadata struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Custom bool   `json:"custom"`
	Schema struct {
		Type string `json:"type"`
	} `json:"schema"`
}

// discoverJiraStoryPointsFields reads GET /rest/api/3/field once and returns
// the ids of the custom number fields named like a story point field, in
// preference order. A non-number field with a matching name is never used.
// A failed lookup is an error: the caller stops the run, because persisting
// NULL story points would overwrite the stored values.
func discoverJiraStoryPointsFields(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
) ([]string, error) {
	var fields []jiraFieldMetadata
	err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/api/3/field", "/rest/api/3/field", nil, &fields)
	// Jira always lists its system fields: a null or empty body is a broken
	// lookup, never "this site has no points field".
	if err == nil && len(fields) == 0 {
		err = providerfoundation.ErrNormalizationInvalid
	}
	if err != nil {
		slog.WarnContext(ctx, "providersync.jira.story_points_field_discovery_failed")
		return nil, err
	}
	return pickJiraStoryPointsFields(ctx, fields), nil
}

func pickJiraStoryPointsFields(ctx context.Context, fields []jiraFieldMetadata) []string {
	type candidate struct {
		rank int
		id   string
	}
	found := make([]candidate, 0, 2)
	for _, field := range fields {
		if !field.Custom || field.ID == "" || field.Schema.Type != "number" {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(field.Name))
		for rank, want := range jiraStoryPointsFieldNames {
			if name == want {
				found = append(found, candidate{rank: rank, id: field.ID})
			}
		}
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].rank != found[j].rank {
			return found[i].rank < found[j].rank
		}
		return found[i].id < found[j].id
	})
	ids := make([]string, 0, len(found))
	for _, item := range found {
		ids = append(ids, item.id)
	}
	if len(ids) == 0 {
		slog.WarnContext(ctx, "providersync.jira.story_points_field_not_found", "fields_seen", len(fields))
	} else {
		slog.InfoContext(ctx, "providersync.jira.story_points_field_discovered", "field_ids", ids)
	}
	return ids
}

// jiraStoryPoints reads the issue size. A configured story_points_field wins
// and is the only field read; otherwise the discovered fields are tried in
// preference order and the first non-null value is used.
func jiraStoryPoints(claim Claim, fields map[string]any, discovered []string, objectShape bool) *float64 {
	if configured := jiraOptionString(claim, "story_points_field", ""); configured != "" {
		return jiraFloat(jiraField(fields, configured, objectShape))
	}
	for _, id := range discovered {
		if value := jiraDiscoveredPoints(jiraField(fields, id, objectShape)); value != nil {
			return value
		}
	}
	return nil
}

// jiraStoryPointsDiscoveryNeeded reports whether the field list must be read.
func jiraStoryPointsDiscoveryNeeded(claim Claim) bool {
	return jiraOptionString(claim, "story_points_field", "") == ""
}

type jiraStoryPointsStats struct{ sized, unsized int }

func (stats *jiraStoryPointsStats) observe(item jiraWorkItemRow) {
	if item.StoryPoints != nil {
		stats.sized++
	} else {
		stats.unsized++
	}
}

func (stats jiraStoryPointsStats) log(ctx context.Context) {
	slog.InfoContext(ctx, "providersync.jira.story_points_sized",
		"sized", stats.sized, "unsized", stats.unsized)
}

// jiraDiscoveredPoints is a number or a numeric string, else nil: a value
// that does not parse must fall through to the next candidate instead of
// becoming 0.
func jiraDiscoveredPoints(value any) *float64 {
	switch typed := value.(type) {
	case json.Number:
		if _, err := strconv.ParseFloat(string(typed), 64); err != nil {
			return nil
		}
	case string:
		if _, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err != nil {
			return nil
		}
	}
	return jiraFloat(value)
}
