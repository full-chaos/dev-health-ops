package rest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
	"atlassian/atlassian/rest/mappers"
)

var defaultJiraSearchFields = []string{
	"project",
	"issuetype",
	"status",
	"created",
	"updated",
	"resolutiondate",
	"assignee",
	"reporter",
	"labels",
	"components",
}

func (c *JiraRESTClient) ListIssuesViaREST(ctx context.Context, cloudID string, jql string, pageSize int) ([]atlassian.JiraIssue, error) {
	storyPointsField := strings.TrimSpace(os.Getenv("ATLASSIAN_JIRA_STORY_POINTS_FIELD"))
	sprintIDsField := strings.TrimSpace(os.Getenv("ATLASSIAN_JIRA_SPRINT_IDS_FIELD"))
	return c.ListIssuesViaRESTWithFields(ctx, cloudID, jql, pageSize, storyPointsField, sprintIDsField)
}

func buildJiraSearchFields(storyPointsField string, sprintIDsField string) ([]string, error) {
	fields := make([]string, 0, len(defaultJiraSearchFields)+2)
	fields = append(fields, defaultJiraSearchFields...)
	for _, raw := range []string{storyPointsField, sprintIDsField} {
		if raw == "" {
			continue
		}
		clean := strings.TrimSpace(raw)
		if clean == "" {
			return nil, errors.New("custom field names must be non-empty when provided")
		}
		already := false
		for _, existing := range fields {
			if existing == clean {
				already = true
				break
			}
		}
		if !already {
			fields = append(fields, clean)
		}
	}
	return fields, nil
}

func (c *JiraRESTClient) ListIssuesViaRESTWithFields(
	ctx context.Context,
	cloudID string,
	jql string,
	pageSize int,
	storyPointsField string,
	sprintIDsField string,
) ([]atlassian.JiraIssue, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return nil, errors.New("cloudID is required")
	}
	jqlClean := strings.TrimSpace(jql)
	if jqlClean == "" {
		return nil, errors.New("jql is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	fieldList, err := buildJiraSearchFields(storyPointsField, sprintIDsField)
	if err != nil {
		return nil, err
	}
	fields := strings.Join(fieldList, ",")
	startAt := 0
	seenStart := map[int]struct{}{}
	var out []atlassian.JiraIssue

	for {
		if _, ok := seenStart[startAt]; ok {
			return nil, errors.New("pagination startAt repeated; aborting to prevent infinite loop")
		}
		seenStart[startAt] = struct{}{}

		payload, err := c.GetJSON(ctx, "/rest/api/3/search", map[string]string{
			"jql":        jqlClean,
			"startAt":    strconv.Itoa(startAt),
			"maxResults": strconv.Itoa(pageSize),
			"fields":     fields,
		})
		if err != nil {
			return nil, err
		}
		page, err := gen.DecodeSearchResults(payload)
		if err != nil {
			return nil, fmt.Errorf("decode issue search response: %w", err)
		}

		for _, it := range page.Issues {
			mapped, err := mappers.JiraIssueFromRESTWithFields(cloud, it, storyPointsField, sprintIDsField)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}

		if page.Total != nil && *page.Total >= 0 {
			if startAt+len(page.Issues) >= *page.Total {
				break
			}
		} else if len(page.Issues) < pageSize {
			break
		}

		if len(page.Issues) == 0 {
			break
		}
		startAt += len(page.Issues)
	}

	return out, nil
}
