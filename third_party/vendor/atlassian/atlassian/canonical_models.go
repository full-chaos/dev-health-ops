package atlassian

type JiraUser struct {
	AccountID   string  `json:"accountId"`
	DisplayName string  `json:"displayName"`
	Email       *string `json:"email,omitempty"`
}

type JiraProject struct {
	CloudID string  `json:"cloudId"`
	Key     string  `json:"key"`
	Name    string  `json:"name"`
	Type    *string `json:"type,omitempty"`
}

type JiraSprint struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	State      string  `json:"state"`
	StartAt    *string `json:"startAt,omitempty"`
	EndAt      *string `json:"endAt,omitempty"`
	CompleteAt *string `json:"completeAt,omitempty"`
}

type JiraIssue struct {
	CloudID     string    `json:"cloudId"`
	Key         string    `json:"key"`
	ProjectKey  string    `json:"projectKey"`
	IssueType   string    `json:"issueType"`
	Status      string    `json:"status"`
	CreatedAt   string    `json:"createdAt"`
	UpdatedAt   string    `json:"updatedAt"`
	ResolvedAt  *string   `json:"resolvedAt,omitempty"`
	Assignee    *JiraUser `json:"assignee,omitempty"`
	Reporter    *JiraUser `json:"reporter,omitempty"`
	Labels      []string  `json:"labels"`
	Components  []string  `json:"components"`
	StoryPoints *float64  `json:"storyPoints,omitempty"`
	SprintIDs   []string  `json:"sprintIds"`
}

type JiraChangelogItem struct {
	Field      string  `json:"field"`
	From       *string `json:"from,omitempty"`
	To         *string `json:"to,omitempty"`
	FromString *string `json:"fromString,omitempty"`
	ToString   *string `json:"toString,omitempty"`
}

type JiraChangelogEvent struct {
	IssueKey  string              `json:"issueKey"`
	EventID   string              `json:"eventId"`
	Author    *JiraUser           `json:"author,omitempty"`
	CreatedAt string              `json:"createdAt"`
	Items     []JiraChangelogItem `json:"items"`
}

type JiraWorklog struct {
	IssueKey         string    `json:"issueKey"`
	WorklogID        string    `json:"worklogId"`
	Author           *JiraUser `json:"author,omitempty"`
	StartedAt        string    `json:"startedAt"`
	TimeSpentSeconds int       `json:"timeSpentSeconds"`
	CreatedAt        string    `json:"createdAt"`
	UpdatedAt        string    `json:"updatedAt"`
}

type OpsgenieTeamRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type CanonicalProjectWithOpsgenieTeams struct {
	Project       JiraProject       `json:"project"`
	OpsgenieTeams []OpsgenieTeamRef `json:"opsgenieTeams"`
}

type JiraBoard struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type JiraVersion struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	ProjectKey  string  `json:"projectKey"`
	Released    bool    `json:"released"`
	ReleaseDate *string `json:"releaseDate,omitempty"`
}

type AtlassianTeam struct {
	ID          string  `json:"id"`
	DisplayName string  `json:"displayName"`
	State       string  `json:"state"`
	Description *string `json:"description,omitempty"`
	AvatarURL   *string `json:"avatarUrl,omitempty"`
	MemberCount *int    `json:"memberCount,omitempty"`
}

type AtlassianTeamMember struct {
	TeamID      string  `json:"teamId"`
	AccountID   string  `json:"accountId"`
	DisplayName *string `json:"displayName,omitempty"`
	Role        *string `json:"role,omitempty"`
}

type AtlassianOpsIncident struct {
	ID          string  `json:"id"`
	URL         *string `json:"url,omitempty"`
	Summary     string  `json:"summary"`
	Description *string `json:"description,omitempty"`
	Status      string  `json:"status"`
	Severity    string  `json:"severity"`
	CreatedAt   string  `json:"createdAt"`
	ProviderID  *string `json:"providerId,omitempty"`
}

type AtlassianOpsAlert struct {
	ID             string  `json:"id"`
	Status         string  `json:"status"`
	Priority       string  `json:"priority"`
	CreatedAt      string  `json:"createdAt"`
	AcknowledgedAt *string `json:"acknowledgedAt,omitempty"`
	SnoozedAt      *string `json:"snoozedAt,omitempty"`
	ClosedAt       *string `json:"closedAt,omitempty"`
}

type AtlassianOpsSchedule struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
	Timezone    *string `json:"timezone,omitempty"`
	Enabled     bool    `json:"enabled"`
	TeamID      *string `json:"teamId,omitempty"`
}

type AtlassianOpsRotation struct {
	ID         string  `json:"id"`
	ScheduleID string  `json:"scheduleId"`
	Name       string  `json:"name"`
	Type       *string `json:"type,omitempty"`
	StartDate  *string `json:"startDate,omitempty"`
	EndDate    *string `json:"endDate,omitempty"`
	Length     *int    `json:"length,omitempty"`
}

type AtlassianOpsEscalation struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
	TeamID      *string `json:"teamId,omitempty"`
}

type AtlassianOpsAlertPolicy struct {
	ID      string  `json:"id"`
	Name    string  `json:"name"`
	Enabled bool    `json:"enabled"`
	TeamID  *string `json:"teamId,omitempty"`
	Type    *string `json:"type,omitempty"`
}

type AtlassianOpsOnCallParticipant struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	ScheduleID string `json:"scheduleId"`
}

type AtlassianOpsHeartbeat struct {
	ID           string  `json:"id"`
	Name         string  `json:"name"`
	Enabled      bool    `json:"enabled"`
	Interval     *int    `json:"interval,omitempty"`
	IntervalUnit *string `json:"intervalUnit,omitempty"`
}

type TeamworkProject struct {
	TeamID      string  `json:"teamId"`
	ProjectID   string  `json:"projectId"`
	ProjectKey  *string `json:"projectKey,omitempty"`
	ProjectName *string `json:"projectName,omitempty"`
}

type TeamworkUserRelation struct {
	SubjectUserID string  `json:"subjectUserId"`
	RelationType  string  `json:"relationType"` // TEAM_MEMBER, REPORTS_TO, MANAGES
	TeamID        *string `json:"teamId,omitempty"`
	RelatedUserID *string `json:"relatedUserId,omitempty"`
}

type CompassComponent struct {
	ID            string   `json:"id"`
	CloudID       string   `json:"cloudId"`
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	Description   *string  `json:"description,omitempty"`
	OwnerTeamID   *string  `json:"ownerTeamId,omitempty"`
	OwnerTeamName *string  `json:"ownerTeamName,omitempty"`
	Labels        []string `json:"labels"`
	CreatedAt     *string  `json:"createdAt,omitempty"`
	UpdatedAt     *string  `json:"updatedAt,omitempty"`
}

type CompassRelationship struct {
	ID               string `json:"id"`
	Type             string `json:"type"`
	StartComponentID string `json:"startComponentId"`
	EndComponentID   string `json:"endComponentId"`
}

type CompassScorecardScore struct {
	ComponentID   string   `json:"componentId"`
	ScorecardID   string   `json:"scorecardId"`
	ScorecardName *string  `json:"scorecardName,omitempty"`
	Score         float64  `json:"score"`
	MaxScore      *float64 `json:"maxScore,omitempty"`
	EvaluatedAt   *string  `json:"evaluatedAt,omitempty"`
}
