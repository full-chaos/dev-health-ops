package mappers

import (
	"errors"
	"fmt"
	"strings"

	"atlassian/atlassian"
)

func strPtrFromRaw(raw any) *string {
	if raw == nil {
		return nil
	}
	s := strings.TrimSpace(fmt.Sprintf("%v", raw))
	if s == "" {
		return nil
	}
	return &s
}

func boolFromRaw(raw any, defaultVal bool) bool {
	if raw == nil {
		return defaultVal
	}
	if b, ok := raw.(bool); ok {
		return b
	}
	return defaultVal
}

func intPtrFromRaw(raw any) *int {
	if raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case float64:
		i := int(v)
		return &i
	case int:
		return &v
	case int64:
		i := int(v)
		return &i
	}
	return nil
}

// JsmOpsScheduleFromREST maps a raw JSM Ops API schedule dict to AtlassianOpsSchedule.
func JsmOpsScheduleFromREST(raw map[string]any) (atlassian.AtlassianOpsSchedule, error) {
	if raw == nil {
		return atlassian.AtlassianOpsSchedule{}, errors.New("raw schedule map is required")
	}

	idRaw, ok := raw["id"]
	if !ok {
		return atlassian.AtlassianOpsSchedule{}, errors.New("schedule.id is required")
	}
	id := strings.TrimSpace(fmt.Sprintf("%v", idRaw))
	if id == "" {
		return atlassian.AtlassianOpsSchedule{}, errors.New("schedule.id is required")
	}

	nameRaw, ok := raw["name"]
	if !ok {
		return atlassian.AtlassianOpsSchedule{}, errors.New("schedule.name is required")
	}
	name := strings.TrimSpace(fmt.Sprintf("%v", nameRaw))
	if name == "" {
		return atlassian.AtlassianOpsSchedule{}, errors.New("schedule.name is required")
	}

	return atlassian.AtlassianOpsSchedule{
		ID:          id,
		Name:        name,
		Description: strPtrFromRaw(raw["description"]),
		Timezone:    strPtrFromRaw(raw["timezone"]),
		Enabled:     boolFromRaw(raw["enabled"], true),
		TeamID:      strPtrFromRaw(raw["teamId"]),
	}, nil
}

// JsmOpsEscalationFromREST maps a raw JSM Ops API escalation dict to AtlassianOpsEscalation.
func JsmOpsEscalationFromREST(raw map[string]any) (atlassian.AtlassianOpsEscalation, error) {
	if raw == nil {
		return atlassian.AtlassianOpsEscalation{}, errors.New("raw escalation map is required")
	}

	idRaw, ok := raw["id"]
	if !ok {
		return atlassian.AtlassianOpsEscalation{}, errors.New("escalation.id is required")
	}
	id := strings.TrimSpace(fmt.Sprintf("%v", idRaw))
	if id == "" {
		return atlassian.AtlassianOpsEscalation{}, errors.New("escalation.id is required")
	}

	nameRaw, ok := raw["name"]
	if !ok {
		return atlassian.AtlassianOpsEscalation{}, errors.New("escalation.name is required")
	}
	name := strings.TrimSpace(fmt.Sprintf("%v", nameRaw))
	if name == "" {
		return atlassian.AtlassianOpsEscalation{}, errors.New("escalation.name is required")
	}

	return atlassian.AtlassianOpsEscalation{
		ID:          id,
		Name:        name,
		Description: strPtrFromRaw(raw["description"]),
		TeamID:      strPtrFromRaw(raw["teamId"]),
	}, nil
}

// JsmOpsAlertPolicyFromREST maps a raw JSM Ops API alert policy dict to AtlassianOpsAlertPolicy.
func JsmOpsAlertPolicyFromREST(raw map[string]any) (atlassian.AtlassianOpsAlertPolicy, error) {
	if raw == nil {
		return atlassian.AtlassianOpsAlertPolicy{}, errors.New("raw alertPolicy map is required")
	}

	idRaw, ok := raw["id"]
	if !ok {
		return atlassian.AtlassianOpsAlertPolicy{}, errors.New("alertPolicy.id is required")
	}
	id := strings.TrimSpace(fmt.Sprintf("%v", idRaw))
	if id == "" {
		return atlassian.AtlassianOpsAlertPolicy{}, errors.New("alertPolicy.id is required")
	}

	nameRaw, ok := raw["name"]
	if !ok {
		return atlassian.AtlassianOpsAlertPolicy{}, errors.New("alertPolicy.name is required")
	}
	name := strings.TrimSpace(fmt.Sprintf("%v", nameRaw))
	if name == "" {
		return atlassian.AtlassianOpsAlertPolicy{}, errors.New("alertPolicy.name is required")
	}

	return atlassian.AtlassianOpsAlertPolicy{
		ID:      id,
		Name:    name,
		Enabled: boolFromRaw(raw["enabled"], true),
		TeamID:  strPtrFromRaw(raw["teamId"]),
		Type:    strPtrFromRaw(raw["type"]),
	}, nil
}

// JsmOpsOnCallParticipantFromREST maps a raw JSM Ops API on-call participant dict.
func JsmOpsOnCallParticipantFromREST(raw map[string]any, scheduleID string) (atlassian.AtlassianOpsOnCallParticipant, error) {
	if raw == nil {
		return atlassian.AtlassianOpsOnCallParticipant{}, errors.New("raw onCallParticipant map is required")
	}

	idRaw, ok := raw["id"]
	if !ok {
		return atlassian.AtlassianOpsOnCallParticipant{}, errors.New("onCallParticipant.id is required")
	}
	id := strings.TrimSpace(fmt.Sprintf("%v", idRaw))
	if id == "" {
		return atlassian.AtlassianOpsOnCallParticipant{}, errors.New("onCallParticipant.id is required")
	}

	participantType := "user"
	if typeRaw, ok := raw["type"]; ok {
		if t := strings.TrimSpace(fmt.Sprintf("%v", typeRaw)); t != "" {
			participantType = t
		}
	}

	scheduleID = strings.TrimSpace(scheduleID)
	if scheduleID == "" {
		return atlassian.AtlassianOpsOnCallParticipant{}, errors.New("scheduleID is required")
	}

	return atlassian.AtlassianOpsOnCallParticipant{
		ID:         id,
		Type:       participantType,
		ScheduleID: scheduleID,
	}, nil
}

// JsmOpsHeartbeatFromREST maps a raw JSM Ops API heartbeat dict to AtlassianOpsHeartbeat.
// The JSM Ops Heartbeat schema uses name as its primary identifier; we fall back to name for id.
func JsmOpsHeartbeatFromREST(raw map[string]any) (atlassian.AtlassianOpsHeartbeat, error) {
	if raw == nil {
		return atlassian.AtlassianOpsHeartbeat{}, errors.New("raw heartbeat map is required")
	}

	nameRaw, ok := raw["name"]
	if !ok {
		return atlassian.AtlassianOpsHeartbeat{}, errors.New("heartbeat.name is required")
	}
	name := strings.TrimSpace(fmt.Sprintf("%v", nameRaw))
	if name == "" {
		return atlassian.AtlassianOpsHeartbeat{}, errors.New("heartbeat.name is required")
	}

	heartbeatID := name
	if idRaw, ok := raw["id"]; ok {
		if id := strings.TrimSpace(fmt.Sprintf("%v", idRaw)); id != "" {
			heartbeatID = id
		}
	}

	return atlassian.AtlassianOpsHeartbeat{
		ID:           heartbeatID,
		Name:         name,
		Enabled:      boolFromRaw(raw["enabled"], true),
		Interval:     intPtrFromRaw(raw["interval"]),
		IntervalUnit: strPtrFromRaw(raw["intervalUnit"]),
	}, nil
}
