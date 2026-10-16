package daily

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiimpact"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
	"github.com/full-chaos/dev-health-ops/internal/teamresolve"
)

// AIImpactExecutor is the NATIVE implementation of the ai_impact
// metrics.daily family (CHAOS-4280). It is a thin ClickHouse adapter over the
// pure internal/jobs/metrics/aiimpact kernel; the compute's fidelity notes
// live on that package and the query notes on
// ai_impact_native_clickhouse.go.
//
// Unlike ai_governance (org-scoped), this family IS repo-scoped: Python's
// compute takes pull_request_rows/review_rows already scoped to the
// partition's repos, and groups its output by (team_id, repo_id, work_type).
// So the usual `len(repoIDs) == 0 -> return 0, nil` guard applies here and
// the org-scope exception does NOT.
type AIImpactExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errAIImpactUnavailable = fmt.Errorf("ai_impact native executor unavailable")

// NewAIImpactExecutor fails closed on a nil connection, matching every other
// native family's construction-time policy.
func NewAIImpactExecutor(conn driver.Conn) (*AIImpactExecutor, error) {
	if conn == nil {
		return nil, errAIImpactUnavailable
	}
	return &AIImpactExecutor{
		conn:   conn,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *AIImpactExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errAIImpactUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	targetDay := run.TargetDay.UTC()
	dayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)
	computedAt := executor.nowUTC()

	// The stale-key rule (stale_team_keys.go): a key of the day and of the
	// partition's repositories that this compute did not produce gets a row of
	// zeros. It also runs when the compute has no row to write.
	supersede := func(written int, records []aiimpact.Record) (int, error) {
		produced := make([]staleKey, 0, len(records))
		for _, record := range records {
			teamID := ""
			if record.TeamID != nil {
				teamID = *record.TeamID
			}
			produced = append(produced, staleKey{
				teamID, record.RepoID.String(), record.WorkType, string(record.AttributionBucket),
			})
		}
		repositories := make([][]string, 0, len(repoIDs))
		for _, repoID := range repoIDs {
			repositories = append(repositories, []string{repoID.String()})
		}
		superseded, err := supersedeStaleTeamKeys(
			ctx, executor.conn, teamkeytables.AIImpactMetricsDaily, run.OrganizationID, dayStart,
			newStaleKeyScope(repositories...), produced, computedAt,
		)
		written += superseded
		if err != nil {
			return wrapWorkItemScopePartialWrite("ai_impact", written, partition, err)
		}
		return written, nil
	}

	pullRequests, err := LoadAIImpactPullRequests(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}

	// REUSES the incident family's loader rather than re-implementing the
	// mapping join -- the brief's "two implementations of one concept that
	// disagree" rule. Python feeds ai_impact from the SAME load_incidents call
	// that feeds compute_incident_metrics_daily, so sharing the reader here is
	// the faithful shape, not a shortcut.
	//
	// INHERITED DIVERGENCE, stated so it is not rediscovered as a parity bug:
	// LoadIncidentsStarted carries CHAOS-4269's port-with-fix guard
	// (`valid_from IS NULL OR valid_from <= as_of`), which Python lacks. For an
	// org with repository-derived incident mappings, Python silently sees ZERO
	// incidents and this executor sees the real ones -- so incidents_count and
	// incident_drag_rate will differ, in the fixed direction. That is the same
	// accepted divergence the incident family already ships.
	incidentRows, err := LoadIncidentsStarted(
		ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd, computedAt, nil,
	)
	if err != nil {
		return 0, err
	}

	if len(pullRequests) == 0 && len(incidentRows) == 0 {
		// No pull requests and no incidents in the window: no facts, and compute
		// would produce no rows.
		return supersede(0, nil)
	}

	reviews, err := LoadAIImpactReviews(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}
	attributions, err := LoadAIImpactAttributions(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}

	prNumbers := make([]uint32, 0, len(pullRequests))
	for _, pullRequest := range pullRequests {
		prNumbers = append(prNumbers, uint32(pullRequest.Number))
	}

	// nil vs empty is LOAD-BEARING (CHAOS-2183). Python wraps its linkage build
	// in try/except and leaves pr_commit_stats as None on ANY failure, which
	// makes has_test_change unknown for every PR and test_gap_rate null rather
	// than 100%. A hard error here would instead fail the whole partition, so
	// the failure is swallowed to unavailable exactly as Python does -- the one
	// place in this executor where an error is not propagated.
	//
	// codex round chaos-4280-r1, finding 5: the swallow itself is correct
	// parity (see above), but it used to be INDISTINGUISHABLE from a healthy
	// day with no linkage rows -- the outer handler's outcome=computed label
	// is the same either way, and this executor logs nothing. Python at least
	// logs a warning here (job_daily.py's pr_commit_stats build). Rather than
	// bolt ad-hoc logging onto a package that has none, this increments a
	// dedicated counter (aiimpact.RecordLinkageUnavailable) so the condition
	// is observable in the SAME place every other native-family signal lives,
	// without changing what gets computed or written.
	linkage, linkageErr := LoadAIImpactPRCommitLinkage(
		ctx, executor.conn, run.OrganizationID, repoIDs, prNumbers,
	)
	hasLinkage := linkageErr == nil
	if !hasLinkage {
		linkage = nil
		aiimpact.RecordLinkageUnavailable()
	}

	// Team resolution goes through the same authoritative owner resolver
	// team_cognitive_load and compounding_risk_team already consume
	// (teamownership.AuthoritativeOwnersByRepo: one owner, or every main owner
	// of a full ownership tie, D5990, ranked
	// is_primary/specificity/updated_at/team_id), with this family's own
	// repo-pattern resolver as the fallback for a repo team_repo_ownership
	// does not resolve -- see resolveAIImpactRepoToTeam.
	teams, err := LoadAIImpactTeams(ctx, executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	repoNames, err := LoadAIImpactRepoNames(ctx, executor.conn, run.OrganizationID, repoIDs)
	if err != nil {
		return 0, err
	}
	repoNamesByIDText := make(map[string]string, len(repoNames))
	for repoID, repoName := range repoNames {
		repoNamesByIDText[repoID.String()] = repoName
	}
	patternResolver := aiimpact.BuildRepoPatternResolver(teams)
	repoToTeam, err := resolveAIImpactRepoToTeam(
		ctx, executor.conn, run.OrganizationID, dayStart, repoIDs, repoNamesByIDText, patternResolver,
	)
	if err != nil {
		return 0, fmt.Errorf("ai_impact: %w", err)
	}
	teamResolver := func(repoID uuid.UUID, _ string) []string {
		owners := repoToTeam[repoID.String()]
		teamIDs := make([]string, 0, len(owners))
		for _, owner := range owners {
			teamIDs = append(teamIDs, owner.TeamID)
		}
		return teamIDs
	}
	ownerShare := func(repoID uuid.UUID, teamID string) float64 {
		for _, owner := range repoToTeam[repoID.String()] {
			if owner.TeamID == teamID {
				return owner.Share
			}
		}
		return 1
	}

	mainOwners := func(repoID uuid.UUID) []string {
		var main []string
		for _, owner := range repoToTeam[repoID.String()] {
			if owner.Main {
				main = append(main, owner.TeamID)
			}
		}
		return main
	}
	incidents, repoMergedPRTeams, err := executor.resolveIncidents(
		ctx, run.OrganizationID, incidentRows, teams, mainOwners, dayEnd,
	)
	if err != nil {
		return 0, err
	}

	records := aiimpact.Compute(aiimpact.Params{
		Day: dayStart, OrgID: run.OrganizationID,
		PullRequests: pullRequests, Reviews: reviews,
		Attributions: attributions, Incidents: incidents,
		PRCommitStats: linkage, HasCommitStats: hasLinkage,
		TeamResolver: teamResolver, OwnerShare: ownerShare, RepoNamesByID: repoNames,
		MainOwners: mainOwners, RepoMergedPRTeams: repoMergedPRTeams,
	})
	if len(records) == 0 {
		return supersede(0, nil)
	}
	written, err := WriteAIImpactMetrics(ctx, executor.conn, records, computedAt)
	if err != nil {
		return written, err
	}
	return supersede(written, records)
}

var _ NativeFamilyExecutor = (*AIImpactExecutor)(nil)

// resolveIncidents gives each incident the facts that place it on one team
// (D6040, D6041): the team its source names, its triggering pull request, and,
// for a repository whose main owners tie, the repository's team-owned merged
// pull requests. The source's team reference is mapped through the team catalog
// (provider and native_team_key); an unmapped reference is dropped, never
// matched by raw equality with a team id.
func (executor *AIImpactExecutor) resolveIncidents(
	ctx context.Context, organizationID string, rows []IncidentRow, teams []aiimpact.Team,
	mainOwners func(uuid.UUID) []string, before time.Time,
) ([]aiimpact.IncidentRow, map[uuid.UUID][]aiimpact.MergedPRTeam, error) {
	if len(rows) == 0 {
		return nil, nil, nil
	}
	knownTeams := make(map[string]struct{}, len(teams))
	for _, team := range teams {
		knownTeams[team.ID] = struct{}{}
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.IncidentID)
	}
	sourceTeams, err := LoadAIImpactIncidentSourceTeams(ctx, executor.conn, organizationID, ids)
	if err != nil {
		return nil, nil, err
	}
	var catalog map[AIImpactProviderTeamRef][]string
	if len(sourceTeams) > 0 {
		catalog, err = LoadAIImpactTeamsByNativeKey(ctx, executor.conn, organizationID)
		if err != nil {
			return nil, nil, err
		}
	}
	triggers, err := LoadAIImpactIncidentTriggers(ctx, executor.conn, organizationID, ids)
	if err != nil {
		return nil, nil, err
	}
	incidents := make([]aiimpact.IncidentRow, 0, len(rows))
	tiedRepos := make(map[uuid.UUID]struct{})
	var sourceNamed, sourceDropped, triggered int
	for _, row := range rows {
		incident := aiimpact.IncidentRow{ID: row.IncidentID, RepoID: row.RepoID, StartedAt: row.StartedAt}
		if ref, ok := sourceTeams[row.IncidentID]; ok {
			if team, mapped := MapAIImpactProviderTeam(ref, catalog, knownTeams); mapped {
				incident.SourceTeamID = &team
				sourceNamed++
			} else {
				sourceDropped++
			}
		}
		if trigger, ok := triggers[row.IncidentID]; ok {
			if trigger.TeamID != nil {
				if _, known := knownTeams[*trigger.TeamID]; !known {
					trigger.TeamID = nil
				}
			}
			incident.Trigger = &trigger
			triggered++
		}
		incidents = append(incidents, incident)
		if len(mainOwners(row.RepoID)) > 1 {
			tiedRepos[row.RepoID] = struct{}{}
		}
	}
	var repoMergedPRTeams map[uuid.UUID][]aiimpact.MergedPRTeam
	if len(tiedRepos) > 0 {
		repos := make([]uuid.UUID, 0, len(tiedRepos))
		for repoID := range tiedRepos {
			repos = append(repos, repoID)
		}
		sort.Slice(repos, func(i, j int) bool { return repos[i].String() < repos[j].String() })
		repoMergedPRTeams, err = LoadAIImpactRepoMergedPRTeams(ctx, executor.conn, organizationID, repos, before)
		if err != nil {
			return nil, nil, err
		}
	}
	slog.Info("ai_impact incident team resolution",
		"organization_id", organizationID, "incidents", len(rows),
		"source_team_named", sourceNamed, "source_team_unmapped", sourceDropped,
		"triggering_pull_request", triggered, "tied_main_owner_repos", len(tiedRepos))
	return incidents, repoMergedPRTeams, nil
}

// resolveAIImpactRepoToTeam builds {repo_id_str: owners with their linked
// shares} through teamownership.OwnerSharesByRepo (CHAOS-9604, D5988): the main
// owner (every tied team of a full tie, D5990) and every lesser owner of a
// repository, each at its own share, with patternResolver as the fallback for
// a repo that resolver leaves unresolved (one team holding the repository
// whole). Ownership always wins over a pattern match for the same repo, and a
// repo repoNamesByID does not carry is never guessed by either path
// (teamresolve.ResolveSharesFromOwnershipMap's own guard).
//
// A resolution failure is logged at ERROR and propagated, never swallowed: a
// ClickHouse error resolving ownership must fail this partition rather than
// silently degrade to a patterns-only (or empty) attribution.
func resolveAIImpactRepoToTeam(
	ctx context.Context, conn driver.Conn, organizationID string, asOf time.Time,
	repoIDs []uuid.UUID, repoNamesByID map[string]string,
	patternResolver numerical.RepoTeamResolver,
) (map[string][]teamownership.OwnerShare, error) {
	ownershipMap, err := teamownership.OwnerSharesByRepo(ctx, conn, organizationID, asOf)
	if err != nil {
		slog.Error("ai_impact team resolution: authoritative repo ownership query failed",
			"organization_id", organizationID, "error", err)
		return nil, fmt.Errorf("resolve authoritative repo ownership: %w", err)
	}

	repoToTeam := teamresolve.ResolveSharesFromOwnershipMap(ownershipMap, repoIDs, repoNamesByID, patternResolver)

	ownershipWins, patternWins := 0, 0
	for _, repoID := range repoIDs {
		key := repoID.String()
		owners, resolved := repoToTeam[key]
		if !resolved {
			continue
		}
		if len(ownershipMap[key]) > 0 {
			ownershipWins++
			slog.Debug("ai_impact team resolution: repository resolved via team_repo_ownership",
				"organization_id", organizationID, "repo_id", key, "owners", len(owners))
			continue
		}
		patternWins++
		slog.Debug("ai_impact team resolution: repository resolved via repo_patterns fallback",
			"organization_id", organizationID, "repo_id", key, "owners", len(owners))
	}
	slog.Info("ai_impact team resolution summary",
		"organization_id", organizationID, "repos_total", len(repoIDs),
		"resolved_via_ownership", ownershipWins, "resolved_via_patterns", patternWins,
		"unresolved", len(repoIDs)-ownershipWins-patternWins,
	)

	return repoToTeam, nil
}
