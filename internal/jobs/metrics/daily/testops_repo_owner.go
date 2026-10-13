package daily

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

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
//  2. Else the repository's AUTHORITATIVE owners as of the END of the target
//     day, as teamownership.AuthoritativeOwnersByRepo gives them, by
//     (is_primary = 1) DESC, specificity DESC, updated_at DESC, team_id ASC.
//     A repository with several main owners (a full ownership tie, D5990)
//     gets one row per main owner in the three metrics tables
//     (testops_pipeline_metrics_daily, testops_test_metrics_daily,
//     testops_coverage_metrics_daily): their sorting key holds team_id
//     (migration 124), and each owner counts the repository whole, like the
//     other team families. The risk families (testops_risk) still keep the
//     first owner: their tables are keyed by repository and day.
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

// testopsRepoOwners maps a repository id text to its ranked main owners.
type testopsRepoOwners map[string][]string

// testopsRepoOwnerResolver resolves a repository to its authoritative owning
// team: the first owner by rank. Its key is the repository id text: the
// TestOps families pass repoID.String() as the name they resolve, so no
// repository name is read.
type testopsRepoOwnerResolver struct {
	owners testopsRepoOwners
}

// ResolveRepo returns the first owning team id of the repository id text, or ""
// when the repository has no owner row. The team name is not read by any
// TestOps compute, so it is always "".
func (resolver testopsRepoOwnerResolver) ResolveRepo(repoID string) (string, string) {
	owners := resolver.owners[repoID]
	if len(owners) == 0 {
		return "", ""
	}
	return owners[0], ""
}

// loadTestopsRepoOwnerResolver reads the authoritative owners of every
// repository of the organization as of dayEnd (the exclusive end of the target
// day). A failed read is returned, never taken as "no owner": a row written
// with a NULL team by a failed read would look the same as an unowned
// repository.
func loadTestopsRepoOwnerResolver(
	ctx context.Context, conn driver.Conn, organizationID string, dayEnd time.Time,
) (testops.RepoTeamResolver, testopsRepoOwners, error) {
	owners, err := teamownership.AuthoritativeOwnersByRepo(ctx, conn, organizationID, dayEnd)
	if err != nil {
		return nil, nil, fmt.Errorf("load testops repository owners: %w", err)
	}
	return testopsRepoOwnerResolver{owners: owners}, owners, nil
}

// fanOutOwners gives a metric with no team of its own one copy per main owner
// of its repository. A metric that carries a team (a team_id on the source
// row) is kept as it is, and so is a metric of a repository with no owner. A
// repository with several main owners logs one line: the tie is visible there
// and nowhere else (D5990).
func fanOutOwners[M any](
	ctx context.Context, family string, metrics []M, repoOf func(M) uuid.UUID,
	teamOf func(M) *string, setTeam func(*M, *string), owners testopsRepoOwners,
) []M {
	out := make([]M, 0, len(metrics))
	for _, metric := range metrics {
		if team := teamOf(metric); team != nil && *team != "" {
			out = append(out, metric)
			continue
		}
		repoID := repoOf(metric)
		mains := owners[repoID.String()]
		if len(mains) > 1 {
			slog.DebugContext(ctx, "testops tied main owners: one row per owner",
				"family", family, "repo_id", repoID.String(), "owners", len(mains))
		}
		if len(mains) == 0 {
			out = append(out, metric)
			continue
		}
		for _, owner := range mains {
			owner := owner
			copied := metric
			setTeam(&copied, &owner)
			out = append(out, copied)
		}
	}
	return out
}
