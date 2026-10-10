package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The four catalog writers of team_memberships and their accessors for
// ReuseFirstSeenMembershipValidFrom (CHAOS-9007). Each writer calls its
// function below on the rows it is about to write, so the rows of every run
// carry the valid_from their fact was first seen with.

func reuseLinearMembershipFirstSeen(ctx context.Context, conn driver.Conn, orgID string, rows []linearReferenceMembershipRow) ([]linearReferenceMembershipRow, error) {
	return ReuseFirstSeenMembershipValidFrom(ctx, conn, orgID, rows, MembershipFirstSeenAccessors[linearReferenceMembershipRow]{
		Provider: func(r linearReferenceMembershipRow) string { return r.Provider }, Source: func(r linearReferenceMembershipRow) string { return r.Source },
		TeamID: func(r linearReferenceMembershipRow) string { return r.TeamID }, MemberID: func(r linearReferenceMembershipRow) string { return r.MemberID },
		ValidFrom: func(r linearReferenceMembershipRow) time.Time { return r.ValidFrom },
		SetFrom:   func(r *linearReferenceMembershipRow, at time.Time) { r.ValidFrom = at },
	})
}

func reuseGitHubMembershipFirstSeen(ctx context.Context, conn driver.Conn, orgID string, rows []githubMembershipRow) ([]githubMembershipRow, error) {
	return ReuseFirstSeenMembershipValidFrom(ctx, conn, orgID, rows, MembershipFirstSeenAccessors[githubMembershipRow]{
		Provider: func(r githubMembershipRow) string { return r.Provider }, Source: func(r githubMembershipRow) string { return r.Source },
		TeamID: func(r githubMembershipRow) string { return r.TeamID }, MemberID: func(r githubMembershipRow) string { return r.MemberID },
		ValidFrom: func(r githubMembershipRow) time.Time { return r.ValidFrom },
		SetFrom:   func(r *githubMembershipRow, at time.Time) { r.ValidFrom = at },
	})
}

func reuseGitLabMembershipFirstSeen(ctx context.Context, conn driver.Conn, orgID string, rows []gitlabTeamCatalogMembershipRow) ([]gitlabTeamCatalogMembershipRow, error) {
	return ReuseFirstSeenMembershipValidFrom(ctx, conn, orgID, rows, MembershipFirstSeenAccessors[gitlabTeamCatalogMembershipRow]{
		Provider: func(r gitlabTeamCatalogMembershipRow) string { return r.Provider }, Source: func(r gitlabTeamCatalogMembershipRow) string { return r.Source },
		TeamID: func(r gitlabTeamCatalogMembershipRow) string { return r.TeamID }, MemberID: func(r gitlabTeamCatalogMembershipRow) string { return r.MemberID },
		ValidFrom: func(r gitlabTeamCatalogMembershipRow) time.Time { return r.ValidFrom },
		SetFrom:   func(r *gitlabTeamCatalogMembershipRow, at time.Time) { r.ValidFrom = at },
	})
}

func reuseJiraMembershipFirstSeen(ctx context.Context, conn driver.Conn, orgID string, rows []jiraTeamCatalogMembershipRow) ([]jiraTeamCatalogMembershipRow, error) {
	return ReuseFirstSeenMembershipValidFrom(ctx, conn, orgID, rows, MembershipFirstSeenAccessors[jiraTeamCatalogMembershipRow]{
		Provider: func(r jiraTeamCatalogMembershipRow) string { return r.Provider }, Source: func(r jiraTeamCatalogMembershipRow) string { return r.Source },
		TeamID: func(r jiraTeamCatalogMembershipRow) string { return r.TeamID }, MemberID: func(r jiraTeamCatalogMembershipRow) string { return r.MemberID },
		ValidFrom: func(r jiraTeamCatalogMembershipRow) time.Time { return r.ValidFrom },
		SetFrom:   func(r *jiraTeamCatalogMembershipRow, at time.Time) { r.ValidFrom = at },
	})
}
