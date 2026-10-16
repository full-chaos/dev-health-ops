package aiimpact

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

// IncidentWorkType is the work_type of an incident that has no triggering pull
// request. "incident" is a value of the work item type enum the schema already
// carries (external_schema.go, status_mapping.go); no enum value is added.
const IncidentWorkType = "incident"

// IncidentTrigger is the pull request that triggered an incident: the pull
// request of the deployment the incident is linked to (the deployment-incident
// edge of best tier, then deployments.pull_request_number). The executor reads
// it, including a pull request merged on an earlier day than the incident.
type IncidentTrigger struct {
	// RepoID and Number name the triggering pull request.
	RepoID uuid.UUID
	Number int64
	// TeamID is the team that owned the pull request: the primary team of its
	// linked issue (work_graph_issue_pr, then work_item_team_attributions).
	// nil when no team owned it.
	TeamID *string
	// Bucket and WorkType are the pull request's own (an AI attribution, or
	// BucketUnknown and "pull_request" for one without AI evidence).
	Bucket   AttributionBucket
	WorkType string
}

// MergedPRTeam is one merged pull request of a repository with the team that
// owned it. It breaks a tie between main owners.
type MergedPRTeam struct {
	TeamID   string
	MergedAt time.Time
}

type incidentAnchor struct {
	team     groupKey
	bucket   AttributionBucket
	workType string
}

type anchorKey struct {
	group  groupKey
	bucket AttributionBucket
}

// incidentTeam returns the one team an incident counts for (D6040, D6041):
//
//  1. the team its source names (IncidentRow.SourceTeamID);
//  2. else the team that owned the triggering pull request;
//  3. else the main owner of the repository. Among tied main owners (D5990):
//     the tied team that owned the most recently merged pull request of the
//     repository before the incident started, else the lowest team id.
//
// ok is false when no team applies; the incident then counts in the
// repository's no-team rows.
func incidentTeam(params Params, incident IncidentRow) (team string, ok bool) {
	if incident.SourceTeamID != nil && *incident.SourceTeamID != "" {
		return *incident.SourceTeamID, true
	}
	if incident.Trigger != nil && incident.Trigger.TeamID != nil && *incident.Trigger.TeamID != "" {
		return *incident.Trigger.TeamID, true
	}
	if params.MainOwners == nil {
		return "", false
	}
	owners := params.MainOwners(incident.RepoID)
	switch len(owners) {
	case 0:
		return "", false
	case 1:
		return owners[0], true
	}
	tied := make(map[string]struct{}, len(owners))
	for _, owner := range owners {
		tied[owner] = struct{}{}
	}
	var best string
	var bestAt time.Time
	found := false
	for _, merged := range params.RepoMergedPRTeams[incident.RepoID] {
		if _, isTied := tied[merged.TeamID]; !isTied || !merged.MergedAt.Before(incident.StartedAt) {
			continue
		}
		if !found || merged.MergedAt.After(bestAt) || (merged.MergedAt.Equal(bestAt) && merged.TeamID < best) {
			best, bestAt, found = merged.TeamID, merged.MergedAt, true
		}
	}
	if found {
		return best, true
	}
	sorted := append([]string(nil), owners...)
	sort.Strings(sorted)
	return sorted[0], true
}

// anchorIncident places an incident on exactly one (team, repository,
// work_type, bucket) row. Its work_type and bucket come only from the
// triggering pull request; without one there is no AI evidence, so the incident
// sits on the human (baseline) bucket with work_type IncidentWorkType.
func anchorIncident(params Params, incident IncidentRow) incidentAnchor {
	anchor := incidentAnchor{bucket: BucketHuman, workType: IncidentWorkType}
	if incident.Trigger != nil {
		anchor.bucket, anchor.workType = incident.Trigger.Bucket, incident.Trigger.WorkType
	}
	anchor.team = groupKey{repoID: incident.RepoID, workType: anchor.workType}
	if team, ok := incidentTeam(params, incident); ok {
		anchor.team.teamID, anchor.team.hasTeam = team, true
	}
	return anchor
}

// SafeBucket is the bucket of an attribution kind: the AI kinds and human, else
// BucketUnknown. A pull request without an attribution is BucketUnknown.
func SafeBucket(kind *string) AttributionBucket { return safeBucket(kind) }
