package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The four catalog writers of team_memberships and their accessors for
// ReuseFirstSeenMembershipValidFrom (CHAOS-9007). Each writer calls its
// function below on the rows it is about to write, so the rows of every run
// carry the valid_from their fact was first seen with.

func reuseJiraMembershipFirstSeen(ctx context.Context, conn driver.Conn, orgID string, rows []jiraTeamCatalogMembershipRow) ([]jiraTeamCatalogMembershipRow, error) {
	return ReuseFirstSeenMembershipValidFrom(ctx, conn, orgID, rows, MembershipFirstSeenAccessors[jiraTeamCatalogMembershipRow]{
		Provider: func(r jiraTeamCatalogMembershipRow) string { return r.Provider }, Source: func(r jiraTeamCatalogMembershipRow) string { return r.Source },
		TeamID: func(r jiraTeamCatalogMembershipRow) string { return r.TeamID }, MemberID: func(r jiraTeamCatalogMembershipRow) string { return r.MemberID },
		ValidFrom: func(r jiraTeamCatalogMembershipRow) time.Time { return r.ValidFrom },
		SetFrom:   func(r *jiraTeamCatalogMembershipRow, at time.Time) { r.ValidFrom = at },
	})
}

// The snapshot writers of the three catalogs that also close a member the
// provider no longer lists (CHAOS-9079). Each is the ONE place that says where
// that catalog's memberships live and how a closed row is made.

var linearMembershipWriter = MembershipSnapshotWriter[linearReferenceMembershipRow]{
	Provider: "linear", Source: "native",
	// Linear keys a member by its email when it has one (linearMemberID), and
	// raw_provider_user_id holds the first identity facet, not Linear's user id:
	// no stable user id is stored. A member whose id comes from an email is
	// never closed, because a changed or hidden email would read as a departure.
	KeyedByEmail: func(open openMembership) bool {
		return open.RawEmail != nil && strings.TrimSpace(*open.RawEmail) != ""
	},
	TeamID:    func(r linearReferenceMembershipRow) string { return r.TeamID },
	MemberID:  func(r linearReferenceMembershipRow) string { return r.MemberID },
	ValidFrom: func(r linearReferenceMembershipRow) time.Time { return r.ValidFrom },
	SetFrom:   func(r *linearReferenceMembershipRow, at time.Time) { r.ValidFrom = at },
	Closed: func(open openMembership, closedAt, updatedAt time.Time) linearReferenceMembershipRow {
		return linearReferenceMembershipRow(closedMembershipColumns(open, closedAt, updatedAt))
	},
}

var githubMembershipWriter = MembershipSnapshotWriter[githubMembershipRow]{
	Provider: githubTeamCatalogProvider, Source: githubTeamCatalogSource,
	TeamID:    func(r githubMembershipRow) string { return r.TeamID },
	MemberID:  func(r githubMembershipRow) string { return r.MemberID },
	ValidFrom: func(r githubMembershipRow) time.Time { return r.ValidFrom },
	SetFrom:   func(r *githubMembershipRow, at time.Time) { r.ValidFrom = at },
	Closed: func(open openMembership, closedAt, updatedAt time.Time) githubMembershipRow {
		return githubMembershipRow(closedMembershipColumns(open, closedAt, updatedAt))
	},
}

var gitlabMembershipWriter = MembershipSnapshotWriter[gitlabTeamCatalogMembershipRow]{
	Provider: gitlabTeamCatalogProvider, Source: gitlabTeamCatalogSource,
	TeamID:    func(r gitlabTeamCatalogMembershipRow) string { return r.TeamID },
	MemberID:  func(r gitlabTeamCatalogMembershipRow) string { return r.MemberID },
	ValidFrom: func(r gitlabTeamCatalogMembershipRow) time.Time { return r.ValidFrom },
	SetFrom:   func(r *gitlabTeamCatalogMembershipRow, at time.Time) { r.ValidFrom = at },
	Closed: func(open openMembership, closedAt, updatedAt time.Time) gitlabTeamCatalogMembershipRow {
		return gitlabTeamCatalogMembershipRow(closedMembershipColumns(open, closedAt, updatedAt))
	},
}
