package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
	"atlassian/atlassian/rest/mappers"
)

func normalizeProjectTypeFilter(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	for _, raw := range values {
		v := strings.ToUpper(strings.TrimSpace(raw))
		if v != "" {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("projectTypes must be non-empty")
	}
	return out, nil
}

func (c *JiraRESTClient) ListProjectsViaREST(ctx context.Context, cloudID string, projectTypes []string, pageSize int) ([]atlassian.CanonicalProjectWithOpsgenieTeams, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return nil, errors.New("cloudID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	types, err := normalizeProjectTypeFilter(projectTypes)
	if err != nil {
		return nil, err
	}
	typeSet := map[string]struct{}{}
	for _, t := range types {
		typeSet[t] = struct{}{}
	}

	startAt := 0
	seenStart := map[int]struct{}{}
	var out []atlassian.CanonicalProjectWithOpsgenieTeams

	for {
		if _, ok := seenStart[startAt]; ok {
			return nil, errors.New("pagination startAt repeated; aborting to prevent infinite loop")
		}
		seenStart[startAt] = struct{}{}

		payload, err := c.GetJSON(ctx, "/rest/api/3/project/search", map[string]string{
			"startAt":    strconv.Itoa(startAt),
			"maxResults": strconv.Itoa(pageSize),
		})
		if err != nil {
			return nil, err
		}
		page, err := gen.DecodePageBeanProject(payload)
		if err != nil {
			return nil, fmt.Errorf("decode project search response: %w", err)
		}

		values := page.Values
		for _, item := range values {
			project, err := mappers.JiraProjectFromRESTProject(cloud, item)
			if err != nil {
				return nil, err
			}
			if project.Type == nil {
				continue
			}
			if _, ok := typeSet[strings.TrimSpace(*project.Type)]; !ok {
				continue
			}
			out = append(out, atlassian.CanonicalProjectWithOpsgenieTeams{
				Project:       project,
				OpsgenieTeams: []atlassian.OpsgenieTeamRef{},
			})
		}

		hasIsLast := false
		isLast := false
		if page.IsLast != nil {
			hasIsLast = true
			isLast = *page.IsLast
		}
		if hasIsLast && isLast {
			break
		}

		hasTotal := false
		total := 0
		if page.Total != nil && *page.Total >= 0 {
			hasTotal = true
			total = *page.Total
		}
		if hasTotal {
			if startAt+len(values) >= total {
				break
			}
		} else if len(values) < pageSize {
			break
		}

		if len(values) == 0 {
			if hasIsLast && !isLast {
				return nil, fmt.Errorf("received empty page with isLast=false at startAt=%d", startAt)
			}
			break
		}
		startAt += len(values)
	}

	return out, nil
}

func (c *JiraRESTClient) CreateProject(ctx context.Context, cloudID string, p atlassian.JiraProject) (atlassian.JiraProject, error) {
	data := map[string]any{
		"key":            p.Key,
		"name":           p.Name,
		"projectTypeKey": "software",
		"leadAccountId":  "...", // Should be handled via caller or terraform
	}
	if p.Type != nil {
		data["projectTypeKey"] = *p.Type
	}

	payload, err := c.PostJSON(ctx, "/rest/api/3/project", data)
	if err != nil {
		return atlassian.JiraProject{}, err
	}

	var genP gen.Project
	b, _ := json.Marshal(payload)
	json.Unmarshal(b, &genP)

	return mappers.JiraProjectFromRESTProject(cloudID, genP)
}

func (c *JiraRESTClient) DeleteProject(ctx context.Context, projectKeyOrID string) error {
	if projectKeyOrID == "" {
		return fmt.Errorf("projectKeyOrID is required for delete")
	}
	return c.Delete(ctx, fmt.Sprintf("/rest/api/3/project/%s", projectKeyOrID))
}
