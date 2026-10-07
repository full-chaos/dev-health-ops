package model

import "strings"

// TeamAttributionSourceFromStored maps a stored attribution source to the
// public GraphQL enum. The producer's fallback contract is unrecognized or
// empty -> UNASSIGNED, so this mapping must not make the read path stricter.
func TeamAttributionSourceFromStored(raw string) TeamAttributionSource {
	switch strings.ToLower(raw) {
	case "native_team":
		return TeamAttributionSourceNativeTeam
	case "issue_project":
		return TeamAttributionSourceIssueProject
	case "project_ownership":
		return TeamAttributionSourceProjectOwnership
	case "repo_ownership":
		return TeamAttributionSourceRepoOwnership
	case "assignee_membership":
		return TeamAttributionSourceAssigneeMembership
	case "linked_issue":
		return TeamAttributionSourceLinkedIssue
	case "author_membership":
		return TeamAttributionSourceAuthorMembership
	case "manual_fallback":
		return TeamAttributionSourceManualFallback
	default:
		return TeamAttributionSourceUnassigned
	}
}

// TeamAttributionConfidenceFromStored maps a stored attribution confidence to
// the public GraphQL enum. The producer's fallback contract is unrecognized
// or empty -> NONE.
func TeamAttributionConfidenceFromStored(raw string) TeamAttributionConfidence {
	switch strings.ToLower(raw) {
	case "high":
		return TeamAttributionConfidenceHigh
	case "medium":
		return TeamAttributionConfidenceMedium
	case "low":
		return TeamAttributionConfidenceLow
	case "manual":
		return TeamAttributionConfidenceManual
	default:
		return TeamAttributionConfidenceNone
	}
}
