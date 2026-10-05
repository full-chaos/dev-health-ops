package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// -----------------------------------------------------------------------
// CHAOS-8512: the team of a TestOps daily row is the team that OWNS the
// repository (team_repo_ownership), not a match of teams.repo_patterns.
//
// # Why
//
// No provider sync writes teams.repo_patterns, and no provider sync sets
// team_id on a pipeline run, a suite or a coverage snapshot. So every row of
// the four TestOps families (testops_pipeline, testops_test, testops_coverage,
// testops_risk) had a NULL team on real data, and a read grouped by team (the
// TestOps "Failure patterns" card) showed one group, "Unattributed".
// Ownership rows are the source for this repository-scoped metric.
//
// # The rule (the same for the four families)
//
//  1. A team_id on the source row itself wins (testops.resolveRepoTeam). This
//     is unchanged.
//  2. Else the repository's AUTHORITATIVE owner as of the END of the target
//     day: teamownership.AuthoritativeOwnerByRepo. For a repository that more
//     than one team owns, that function keeps ONE team: the first by
//     is_primary DESC, specificity DESC, updated_at DESC, team_id ASC. One
//     team, not all of them, because each TestOps daily table holds one row
//     per (org, repo, day): a second team has no row to live in. It is the
//     rule team_cognitive_load_daily and compounding_risk_daily already use.
//  3. Else NULL. A repository with no owner row is not guessed: there is no
//     fallback to teams.repo_patterns and none to a person's team membership.
//
// # Why the END of the day
//
// The rows describe the whole target day, and valid_from is the time a sync
// first wrote the claim. With the start of the day, an organization whose
// ownership is first synced during day D gets a NULL team for D. Days before
// the first ownership sync keep a NULL team with either instant: a claim is not
// moved back in time.
// -----------------------------------------------------------------------

// testopsRepoOwnerResolver resolves a repository to its authoritative owning
// team. Its key is the repository id text: the four TestOps families pass
// repoID.String() as the name they resolve, so no repository name is read.
type testopsRepoOwnerResolver struct {
	ownerByRepoID map[string]string
}

// ResolveRepo returns the owning team id of the repository id text, or ""
// when the repository has no owner row. The team name is not read by any
// TestOps compute, so it is always "".
func (resolver testopsRepoOwnerResolver) ResolveRepo(repoID string) (string, string) {
	return resolver.ownerByRepoID[repoID], ""
}

// newTestopsRepoOwnerResolver wraps an owner map (repository id text -> team
// id) as the resolver the testops compute takes.
func newTestopsRepoOwnerResolver(ownerByRepoID map[string]string) testops.RepoTeamResolver {
	return testopsRepoOwnerResolver{ownerByRepoID: ownerByRepoID}
}

// loadTestopsRepoOwnerResolver reads the authoritative owner of every
// repository of the organization as of dayEnd (the exclusive end of the target
// day). A failed read is returned, never taken as "no owner": a row written
// with a NULL team by a failed read would look the same as an unowned
// repository.
func loadTestopsRepoOwnerResolver(
	ctx context.Context, conn driver.Conn, organizationID string, dayEnd time.Time,
) (testops.RepoTeamResolver, error) {
	owners, err := teamownership.AuthoritativeOwnerByRepo(ctx, conn, organizationID, dayEnd)
	if err != nil {
		return nil, fmt.Errorf("load testops repository owners: %w", err)
	}
	return newTestopsRepoOwnerResolver(owners), nil
}
