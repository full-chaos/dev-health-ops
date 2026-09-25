package rest

import (
	"context"
	"fmt"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/mappers"
)

// JsmOpsClient provides methods for calling the JSM Operations REST API.
// Base URL: https://api.atlassian.com/jsm/ops/api/{cloudId}
// OAuth scope required: read:ops-config:jira-service-management
type JsmOpsClient struct {
	JiraRESTClient
	CloudID string
}

func (c *JsmOpsClient) baseURL() string {
	cloudID := strings.TrimSpace(c.CloudID)
	return fmt.Sprintf("https://api.atlassian.com/jsm/ops/api/%s", cloudID)
}

// ListSchedules retrieves all on-call schedules with offset/size pagination.
func (c *JsmOpsClient) ListSchedules(ctx context.Context, offset, size int) ([]atlassian.AtlassianOpsSchedule, error) {
	if size <= 0 {
		size = 50
	}

	origBase := c.BaseURL
	c.BaseURL = c.baseURL()
	defer func() { c.BaseURL = origBase }()

	currentOffset := offset
	seenOffsets := map[int]struct{}{}
	var out []atlassian.AtlassianOpsSchedule

	for {
		if _, ok := seenOffsets[currentOffset]; ok {
			return nil, fmt.Errorf("pagination offset %d repeated; aborting to prevent infinite loop", currentOffset)
		}
		seenOffsets[currentOffset] = struct{}{}

		body, err := c.GetJSON(ctx, "/v1/schedules", map[string]string{
			"offset": fmt.Sprintf("%d", currentOffset),
			"size":   fmt.Sprintf("%d", size),
		})
		if err != nil {
			return nil, err
		}

		values, _ := body["values"].([]any)
		for _, v := range values {
			item, ok := v.(map[string]any)
			if !ok {
				continue
			}
			schedule, err := mappers.JsmOpsScheduleFromREST(item)
			if err != nil {
				return nil, err
			}
			out = append(out, schedule)
		}

		links, _ := body["links"].(map[string]any)
		if next, _ := links["next"].(string); next == "" {
			break
		}
		if len(values) < size {
			break
		}
		if len(values) == 0 {
			break
		}
		currentOffset += len(values)
	}

	return out, nil
}

// GetSchedule retrieves a single schedule by ID.
func (c *JsmOpsClient) GetSchedule(ctx context.Context, scheduleID string) (atlassian.AtlassianOpsSchedule, error) {
	scheduleID = strings.TrimSpace(scheduleID)
	if scheduleID == "" {
		return atlassian.AtlassianOpsSchedule{}, fmt.Errorf("scheduleID is required")
	}

	origBase := c.BaseURL
	c.BaseURL = c.baseURL()
	defer func() { c.BaseURL = origBase }()

	body, err := c.GetJSON(ctx, fmt.Sprintf("/v1/schedules/%s", scheduleID), nil)
	if err != nil {
		return atlassian.AtlassianOpsSchedule{}, err
	}
	return mappers.JsmOpsScheduleFromREST(body)
}

// ListEscalations retrieves all escalations for a team with offset/size pagination.
func (c *JsmOpsClient) ListEscalations(ctx context.Context, teamID string, offset, size int) ([]atlassian.AtlassianOpsEscalation, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, fmt.Errorf("teamID is required")
	}
	if size <= 0 {
		size = 50
	}

	origBase := c.BaseURL
	c.BaseURL = c.baseURL()
	defer func() { c.BaseURL = origBase }()

	currentOffset := offset
	seenOffsets := map[int]struct{}{}
	var out []atlassian.AtlassianOpsEscalation

	for {
		if _, ok := seenOffsets[currentOffset]; ok {
			return nil, fmt.Errorf("pagination offset %d repeated; aborting to prevent infinite loop", currentOffset)
		}
		seenOffsets[currentOffset] = struct{}{}

		body, err := c.GetJSON(ctx, fmt.Sprintf("/v1/teams/%s/escalations", teamID), map[string]string{
			"offset": fmt.Sprintf("%d", currentOffset),
			"size":   fmt.Sprintf("%d", size),
		})
		if err != nil {
			return nil, err
		}

		values, _ := body["values"].([]any)
		for _, v := range values {
			item, ok := v.(map[string]any)
			if !ok {
				continue
			}
			escalation, err := mappers.JsmOpsEscalationFromREST(item)
			if err != nil {
				return nil, err
			}
			out = append(out, escalation)
		}

		links, _ := body["links"].(map[string]any)
		if next, _ := links["next"].(string); next == "" {
			break
		}
		if len(values) < size {
			break
		}
		if len(values) == 0 {
			break
		}
		currentOffset += len(values)
	}

	return out, nil
}

// ListAlertPolicies retrieves global alert policies with offset/size pagination.
func (c *JsmOpsClient) ListAlertPolicies(ctx context.Context, offset, size int) ([]atlassian.AtlassianOpsAlertPolicy, error) {
	if size <= 0 {
		size = 50
	}

	origBase := c.BaseURL
	c.BaseURL = c.baseURL()
	defer func() { c.BaseURL = origBase }()

	currentOffset := offset
	seenOffsets := map[int]struct{}{}
	var out []atlassian.AtlassianOpsAlertPolicy

	for {
		if _, ok := seenOffsets[currentOffset]; ok {
			return nil, fmt.Errorf("pagination offset %d repeated; aborting to prevent infinite loop", currentOffset)
		}
		seenOffsets[currentOffset] = struct{}{}

		body, err := c.GetJSON(ctx, "/v1/alerts/policies", map[string]string{
			"offset": fmt.Sprintf("%d", currentOffset),
			"size":   fmt.Sprintf("%d", size),
		})
		if err != nil {
			return nil, err
		}

		values, _ := body["values"].([]any)
		for _, v := range values {
			item, ok := v.(map[string]any)
			if !ok {
				continue
			}
			policy, err := mappers.JsmOpsAlertPolicyFromREST(item)
			if err != nil {
				return nil, err
			}
			out = append(out, policy)
		}

		links, _ := body["links"].(map[string]any)
		if next, _ := links["next"].(string); next == "" {
			break
		}
		if len(values) < size {
			break
		}
		if len(values) == 0 {
			break
		}
		currentOffset += len(values)
	}

	return out, nil
}

// GetOnCall retrieves the current on-call participants for a schedule.
func (c *JsmOpsClient) GetOnCall(ctx context.Context, scheduleID string) ([]atlassian.AtlassianOpsOnCallParticipant, error) {
	scheduleID = strings.TrimSpace(scheduleID)
	if scheduleID == "" {
		return nil, fmt.Errorf("scheduleID is required")
	}

	origBase := c.BaseURL
	c.BaseURL = c.baseURL()
	defer func() { c.BaseURL = origBase }()

	body, err := c.GetJSON(ctx, fmt.Sprintf("/v1/schedules/%s/on-calls", scheduleID), nil)
	if err != nil {
		return nil, err
	}

	participantsRaw, _ := body["onCallParticipants"].([]any)
	var out []atlassian.AtlassianOpsOnCallParticipant
	for _, v := range participantsRaw {
		item, ok := v.(map[string]any)
		if !ok {
			continue
		}
		p, err := mappers.JsmOpsOnCallParticipantFromREST(item, scheduleID)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// ListHeartbeats retrieves heartbeats for a team with offset/size pagination.
func (c *JsmOpsClient) ListHeartbeats(ctx context.Context, teamID string, offset, size int) ([]atlassian.AtlassianOpsHeartbeat, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, fmt.Errorf("teamID is required")
	}
	if size <= 0 {
		size = 50
	}

	origBase := c.BaseURL
	c.BaseURL = c.baseURL()
	defer func() { c.BaseURL = origBase }()

	currentOffset := offset
	seenOffsets := map[int]struct{}{}
	var out []atlassian.AtlassianOpsHeartbeat

	for {
		if _, ok := seenOffsets[currentOffset]; ok {
			return nil, fmt.Errorf("pagination offset %d repeated; aborting to prevent infinite loop", currentOffset)
		}
		seenOffsets[currentOffset] = struct{}{}

		body, err := c.GetJSON(ctx, fmt.Sprintf("/v1/teams/%s/heartbeats", teamID), map[string]string{
			"offset": fmt.Sprintf("%d", currentOffset),
			"size":   fmt.Sprintf("%d", size),
		})
		if err != nil {
			return nil, err
		}

		values, _ := body["values"].([]any)
		for _, v := range values {
			item, ok := v.(map[string]any)
			if !ok {
				continue
			}
			heartbeat, err := mappers.JsmOpsHeartbeatFromREST(item)
			if err != nil {
				return nil, err
			}
			out = append(out, heartbeat)
		}

		links, _ := body["links"].(map[string]any)
		if next, _ := links["next"].(string); next == "" {
			break
		}
		if len(values) < size {
			break
		}
		if len(values) == 0 {
			break
		}
		currentOffset += len(values)
	}

	return out, nil
}
