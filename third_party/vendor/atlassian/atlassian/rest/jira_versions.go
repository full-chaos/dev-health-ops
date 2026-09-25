package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
	"atlassian/atlassian/rest/mappers"
)

func (c *JiraRESTClient) ListVersions(ctx context.Context, projectKeyOrID string, pageSize int) ([]atlassian.JiraVersion, error) {
	project := strings.TrimSpace(projectKeyOrID)
	if project == "" {
		return nil, fmt.Errorf("projectKeyOrID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	var out []atlassian.JiraVersion
	startAt := 0
	seenStartAt := map[int]struct{}{}

	for {
		if _, ok := seenStartAt[startAt]; ok {
			return nil, fmt.Errorf("pagination startAt repeated; aborting to prevent infinite loop")
		}
		seenStartAt[startAt] = struct{}{}

		params := map[string]string{
			"startAt":    fmt.Sprintf("%d", startAt),
			"maxResults": fmt.Sprintf("%d", pageSize),
		}
		path := fmt.Sprintf("/rest/api/3/project/%s/version", project)
		payload, err := c.GetJSON(ctx, path, params)
		if err != nil {
			return nil, err
		}

		page, err := gen.DecodePageBeanVersion(payload)
		if err != nil {
			return nil, fmt.Errorf("decode PageBeanVersion: %w", err)
		}

		for _, item := range page.Values {
			out = append(out, mappers.MapRESTVersion(project, item))
		}

		if page.IsLast != nil && *page.IsLast {
			break
		}

		if page.Total != nil {
			if startAt+len(page.Values) >= *page.Total {
				break
			}
		} else {
			if len(page.Values) < pageSize {
				break
			}
		}

		if len(page.Values) == 0 {
			break
		}
		startAt += len(page.Values)
	}

	return out, nil
}

func (c *JiraRESTClient) CreateVersion(ctx context.Context, projectKey string, v atlassian.JiraVersion) (atlassian.JiraVersion, error) {
	data := map[string]any{
		"name":     v.Name,
		"project":  projectKey,
		"released": v.Released,
	}
	if v.ReleaseDate != nil {
		data["releaseDate"] = *v.ReleaseDate
	}

	payload, err := c.PostJSON(ctx, "/rest/api/3/version", data)
	if err != nil {
		return atlassian.JiraVersion{}, err
	}

	var genV gen.Version
	b, _ := json.Marshal(payload)
	json.Unmarshal(b, &genV)

	return mappers.MapRESTVersion(projectKey, genV), nil
}

func (c *JiraRESTClient) UpdateVersion(ctx context.Context, projectKey string, v atlassian.JiraVersion) (atlassian.JiraVersion, error) {
	if v.ID == "" {
		return atlassian.JiraVersion{}, fmt.Errorf("version ID is required for update")
	}

	data := map[string]any{
		"name":     v.Name,
		"released": v.Released,
	}
	if v.ReleaseDate != nil {
		data["releaseDate"] = *v.ReleaseDate
	}

	path := fmt.Sprintf("/rest/api/3/version/%s", v.ID)
	payload, err := c.PutJSON(ctx, path, data)
	if err != nil {
		return atlassian.JiraVersion{}, err
	}

	var genV gen.Version
	b, _ := json.Marshal(payload)
	json.Unmarshal(b, &genV)

	return mappers.MapRESTVersion(projectKey, genV), nil
}

func (c *JiraRESTClient) DeleteVersion(ctx context.Context, versionID string) error {
	if versionID == "" {
		return fmt.Errorf("version ID is required for delete")
	}
	return c.Delete(ctx, fmt.Sprintf("/rest/api/3/version/%s", versionID))
}
