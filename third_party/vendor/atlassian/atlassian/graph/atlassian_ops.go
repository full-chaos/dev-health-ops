package graph

import (
	"context"

	"atlassian/atlassian"
)

func (c *Client) IterIssueIncidentsViaGraphQL(
	ctx context.Context,
	cloudID string,
	issueKey string,
) ([]*atlassian.AtlassianOpsIncident, error) {
	return []*atlassian.AtlassianOpsIncident{}, nil
}

func (c *Client) IterProjectAlertsViaGraphQL(
	ctx context.Context,
	cloudID string,
	projectKey string,
) ([]*atlassian.AtlassianOpsAlert, error) {
	return []*atlassian.AtlassianOpsAlert{}, nil
}

func (c *Client) IterProjectSchedulesViaGraphQL(
	ctx context.Context,
	cloudID string,
	projectKey string,
) ([]*atlassian.AtlassianOpsSchedule, error) {
	return []*atlassian.AtlassianOpsSchedule{}, nil
}
