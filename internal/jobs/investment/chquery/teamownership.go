package chquery

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/full-chaos/dev-health-ops/internal/ownershipwindow"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/google/uuid"
)

// TeamRepoDonor is persisted ownership evidence for one member issue. A repo
// can have multiple donor teams; callers must union repo IDs before allocation.
type TeamRepoDonor struct {
	WorkItemID string
	TeamID     string
	RepoID     uuid.UUID
	// Share is the team's stored linked share of the repository
	// (team_repo_ownership.share, D5988): the largest non-NULL share of its rows
	// that hold. nil = not measured (NULL is not zero). Only the donor set fills it.
	Share *float64
}

// teamRepoDonorsQuery joins repos to ownership on plain equality keys: an
// ownership row names its repository either by id or, when repo_id is unset,
// by provider+name, so the two cases are two equi-join branches unioned
// together rather than one join with an OR predicate. An OR across two
// unrelated columns is not a hash-join key, so ClickHouse previously fell
// back to a filtered cross product between repos and ownership.
//
// Every dedup here is load-bearing, because the outer DISTINCT removes
// duplicate tuples but cannot remove a tuple that only a stale version
// produces: a newer teams version deactivates a team, a newer ownership
// version retracts it, a newer repos version renames the repository away from
// a name-only ownership row, a same-instant attributions correction ties on
// computed_at, and an older attribution generation lives under a different
// sorting key. So the query does not shed FINALs; it evaluates each one once.
// ClickHouse inlines a WITH subquery at every reference, so only the ownership
// and repository resolution -- small, and genuinely needed once per branch --
// sits inside the UNION ALL. The attributions and teams reads are joined once
// to its result.
//
// The latest generation is picked with a window over the FINAL rows instead
// of a second scan feeding an IN set. The two agree: FINAL keeps every sorting
// key's highest computed_at, so the highest computed_at among the FINAL rows
// of a work item is the highest among all its physical rows.
//
// Both sides of the team_id join carry legitimate duplicates: a work item can
// hold several eligible primary rows for one team, and a team can reach one
// repository through several ownership rows (sources, validity windows, the id
// arm and the name arm). Collapsing them only after the join made the join
// emit the product of the two multiplicities, hundreds of millions of rows in
// production for a far smaller result. So each side is reduced to its distinct
// join keys first: (work item, team) after the team filter, and (team,
// repository) after both arms. A key is then unique on each side, every joined
// tuple is already distinct, and no DISTINCT is needed after the join. The
// ownership rows are also collapsed to their distinct matching columns before
// the arms join repositories, for the same reason one level down.
//
// The requested work item ids arrive as an external table, not an
// Array(String) parameter. The server substitutes a parameter into the query
// text before logging it, so hundreds of long ids made the logged query about
// 100 kB, cut at log_queries_cut_to_length before its first CTE ended: every
// form of this query then shared one normalized_query_hash and no text match
// could find it. An external table keeps the logged text to the query itself.
const teamRepoDonorsQuery = `
WITH attributions AS (
    SELECT work_item_id, team_id, source, is_primary
    FROM (
        SELECT work_item_id, team_id, source, is_primary, computed_at,
               max(computed_at) OVER (PARTITION BY work_item_id) AS latest_computed_at
        FROM work_item_team_attributions FINAL
        WHERE org_id = {org_id:String} AND work_item_id IN (SELECT work_item_id FROM donor_work_items)
    )
    WHERE computed_at = latest_computed_at
),
active_teams AS (
    SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1
),
donor_teams AS (
    SELECT DISTINCT a.work_item_id AS work_item_id, assumeNotNull(a.team_id) AS team_id
    FROM attributions AS a
    INNER JOIN active_teams AS t ON t.id = a.team_id
    WHERE a.is_primary = 1 AND a.source IN ('native_team', 'issue_project', 'project_ownership', 'repo_ownership')
      AND a.team_id IS NOT NULL AND a.team_id != ''
),
ownership AS (
    SELECT DISTINCT team_id, provider, repo_id, lower(repo_full_name) AS repo_name_lower
    FROM team_repo_ownership FINAL
    WHERE org_id = {org_id:String}
      AND source IN ('native', 'jira_legacy', 'provider_access', 'inferred')
      AND valid_from <= {as_of:DateTime64(3, 'UTC')}
      AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
),
scoped_repos AS (
    SELECT id, provider, lower(repo) AS repo_lower FROM repos FINAL WHERE org_id = {org_id:String}
),
team_repos AS (
    SELECT DISTINCT team_id, repo_id
    FROM (
        SELECT o.team_id AS team_id, r.id AS repo_id
        FROM ownership AS o
        INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.id = o.repo_id
        WHERE o.repo_id IS NOT NULL

        UNION ALL

        SELECT o.team_id AS team_id, r.id AS repo_id
        FROM ownership AS o
        INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.repo_lower = o.repo_name_lower
        WHERE o.repo_id IS NULL
    )
    WHERE repo_id != toUUID('00000000-0000-0000-0000-000000000000')
)
SELECT d.work_item_id, d.team_id, tr.repo_id
FROM donor_teams AS d
INNER JOIN team_repos AS tr ON tr.team_id = d.team_id
ORDER BY d.work_item_id, tr.repo_id, d.team_id`

// TeamRepoInterval is one ownership row of a team over a repository of this
// organization's catalog, with the time it holds: from ValidFrom, and until
// ValidTo when the row is closed.
type TeamRepoInterval struct {
	TeamID    string
	RepoID    uuid.UUID
	ValidFrom time.Time
	ValidTo   *time.Time
	Share     *float64
}

// ValidAt reports whether the ownership row held at the instant: it began at or
// before it and is open or ends after it. The rule of the as-of query.
func (interval TeamRepoInterval) ValidAt(at time.Time) bool {
	return !interval.ValidFrom.After(at) && (interval.ValidTo == nil || interval.ValidTo.After(at))
}

// TeamRepoDonorSet is what the donors of a set of work items are made of, read
// without an as-of: the donor team(s) of each work item, every ownership row of
// those teams, and the first record of each repository. A work unit takes the
// ownership valid at its OWN end time (at the first record of its repository
// when that is later), and one run holds units of many end times, so the parts
// are read once and each unit selects its donors with DonorsAt.
//
// The two parts are kept apart on purpose. Joined, the result has one row per
// work item, repository and ownership row, which grows with every closed
// ownership row a team's repository ever had.
type TeamRepoDonorSet struct {
	// TeamsByWorkItem holds the donor teams of a work item, sorted.
	TeamsByWorkItem map[string][]string
	// IntervalsByTeam holds the ownership rows of a team, by repository then start.
	IntervalsByTeam map[string][]TeamRepoInterval
	// FirstByRepo holds the first record of each repository of the organization (package ownershipwindow): a row is
	// tested at the later of the instant and the first record of its repository. A repository with no entry has no
	// record and is tested at the instant.
	FirstByRepo map[uuid.UUID]time.Time
	// Rows is the number of rows the reads returned.
	Rows int
}

// DonorsAt returns the donors of the work item whose ownership row held at the
// instant: each (team, repository) once, by repository then team.
func (set TeamRepoDonorSet) DonorsAt(workItemID string, at time.Time) []TeamRepoDonor {
	var donors []TeamRepoDonor
	seen := map[TeamRepoDonor]int{}
	for _, team := range set.TeamsByWorkItem[workItemID] {
		for _, interval := range set.IntervalsByTeam[team] {
			if !interval.ValidAt(ownershipwindow.ClampAsOf(at, set.FirstByRepo[interval.RepoID])) {
				continue
			}
			key := TeamRepoDonor{WorkItemID: workItemID, TeamID: team, RepoID: interval.RepoID}
			if index, ok := seen[key]; ok {
				if interval.Share != nil && (donors[index].Share == nil || *interval.Share > *donors[index].Share) {
					share := *interval.Share
					donors[index].Share = &share
				}
				continue
			}
			seen[key] = len(donors)
			donor := key
			if interval.Share != nil {
				share := *interval.Share
				donor.Share = &share
			}
			donors = append(donors, donor)
		}
	}
	sort.SliceStable(donors, func(i, j int) bool {
		if donors[i].RepoID != donors[j].RepoID {
			return donors[i].RepoID.String() < donors[j].RepoID.String()
		}
		return donors[i].TeamID < donors[j].TeamID
	})
	return donors
}

// donorTeamsQuery is the first half of teamRepoDonorsQuery: the donor teams of
// the work items, from the latest primary attribution snapshot, active teams
// and the four eligible sources only.
const donorTeamsQuery = `
WITH attributions AS (
    SELECT work_item_id, team_id, source, is_primary
    FROM (
        SELECT work_item_id, team_id, source, is_primary, computed_at,
               max(computed_at) OVER (PARTITION BY work_item_id) AS latest_computed_at
        FROM work_item_team_attributions FINAL
        WHERE org_id = {org_id:String} AND work_item_id IN (SELECT work_item_id FROM donor_work_items)
    )
    WHERE computed_at = latest_computed_at
),
active_teams AS (
    SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1
)
SELECT DISTINCT a.work_item_id AS work_item_id, assumeNotNull(a.team_id) AS team_id
FROM attributions AS a
INNER JOIN active_teams AS t ON t.id = a.team_id
WHERE a.is_primary = 1 AND a.source IN ('native_team', 'issue_project', 'project_ownership', 'repo_ownership')
  AND a.team_id IS NOT NULL AND a.team_id != ''
ORDER BY work_item_id, team_id`

// teamRepoIntervalsQuery is the second half of teamRepoDonorsQuery with the
// validity window of each ownership row in the result and the "still open at
// instant" test left to the caller: every sync-derived ownership row of the donor
// teams (the external table donor_teams), joined to this organization's
// repository catalog by id or, when the row has no id, by provider and name.
// No row is cut at the run's bound: before the first record of a repository its
// first owners apply (D5987 option C), so the row that holds at that first
// record is needed even when it begins after the bound. The rows of a team that is no donor of the run's work
// items are not read: the read grows with the donor teams' ownership, not with
// the organization's.
const teamRepoIntervalsQuery = `
WITH ownership AS (
    SELECT DISTINCT team_id, provider, repo_id, lower(repo_full_name) AS repo_name_lower, valid_from, valid_to, share
    FROM team_repo_ownership FINAL
    WHERE org_id = {org_id:String}
      AND source IN ('native', 'jira_legacy', 'provider_access', 'inferred')
      AND team_id IN (SELECT team_id FROM donor_teams)
),
scoped_repos AS (
    SELECT id, provider, lower(repo) AS repo_lower FROM repos FINAL WHERE org_id = {org_id:String}
)
SELECT DISTINCT team_id, repo_id, valid_from, valid_to, share
FROM (
    SELECT o.team_id AS team_id, r.id AS repo_id, o.valid_from AS valid_from, o.valid_to AS valid_to, o.share AS share
    FROM ownership AS o
    INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.id = o.repo_id
    WHERE o.repo_id IS NOT NULL

    UNION ALL

    SELECT o.team_id AS team_id, r.id AS repo_id, o.valid_from AS valid_from, o.valid_to AS valid_to, o.share AS share
    FROM ownership AS o
    INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.repo_lower = o.repo_name_lower
    WHERE o.repo_id IS NULL
)
WHERE repo_id != toUUID('00000000-0000-0000-0000-000000000000')
ORDER BY team_id, repo_id, valid_from`

// FetchTeamRepoDonorSet reads the donor teams of the work items, their ownership
// rows and the first record of each repository of the organization. upTo is the
// run's bound: a unit asks DonorsAt for an instant at or before it. For every
// instant at or after the first record of a repository, DonorsAt gives what
// FetchTeamRepoDonors gives for that instant (held by
// TestTheDonorSetGivesTheDonorsOfEveryInstant); before it the first owners apply
// (TestTheDonorSetAppliesTheFirstRecordedOwnersBackward).
func (reader *Reader) FetchTeamRepoDonorSet(ctx context.Context, workItemIDs []string, organizationID string, upTo time.Time) (TeamRepoDonorSet, error) {
	set := TeamRepoDonorSet{TeamsByWorkItem: map[string][]string{}, IntervalsByTeam: map[string][]TeamRepoInterval{}}
	if reader == nil || reader.conn == nil || strings.TrimSpace(organizationID) == "" || upTo.IsZero() {
		return set, ErrUnavailable
	}
	ids := dedupeStrings(workItemIDs)
	if len(ids) == 0 {
		return set, nil
	}
	queryCtx, err := withDonorWorkItems(ctx, ids)
	if err != nil {
		return set, err
	}
	teamRows, err := reader.conn.Query(queryCtx, donorTeamsQuery, clickhouse.Named("org_id", organizationID))
	if err != nil {
		return set, fmt.Errorf("query donor teams: %w", err)
	}
	defer func() { _ = teamRows.Close() }()
	storedTeamIDs := map[string]bool{}
	for teamRows.Next() {
		var workItemID, teamID string
		if err := teamRows.Scan(&workItemID, &teamID); err != nil {
			return set, fmt.Errorf("scan donor team: %w", err)
		}
		// The second read names the teams as they are stored.
		storedTeamIDs[teamID] = true
		// The same decode as FetchTeamRepoDonors: an undecoded id matches no
		// component node and the unit loses its allocation with no error.
		workItemID = pythonparity.DecodeClickHouseStringValue(workItemID)
		teamID = pythonparity.DecodeClickHouseStringValue(teamID)
		set.TeamsByWorkItem[workItemID] = append(set.TeamsByWorkItem[workItemID], teamID)
		set.Rows++
	}
	if err := teamRows.Err(); err != nil {
		return set, fmt.Errorf("iterate donor teams: %w", err)
	}
	if len(set.TeamsByWorkItem) == 0 {
		return set, nil
	}
	intervalCtx, err := withDonorTeams(ctx, storedTeamIDs)
	if err != nil {
		return set, err
	}
	intervalRows, err := reader.conn.Query(intervalCtx, teamRepoIntervalsQuery, clickhouse.Named("org_id", organizationID))
	if err != nil {
		return set, fmt.Errorf("query team repository intervals: %w", err)
	}
	defer func() { _ = intervalRows.Close() }()
	for intervalRows.Next() {
		var interval TeamRepoInterval
		if err := intervalRows.Scan(&interval.TeamID, &interval.RepoID, &interval.ValidFrom, &interval.ValidTo, &interval.Share); err != nil {
			return set, fmt.Errorf("scan team repository interval: %w", err)
		}
		interval.TeamID = pythonparity.DecodeClickHouseStringValue(interval.TeamID)
		set.IntervalsByTeam[interval.TeamID] = append(set.IntervalsByTeam[interval.TeamID], interval)
		set.Rows++
	}
	if err := intervalRows.Err(); err != nil {
		return set, fmt.Errorf("iterate team repository intervals: %w", err)
	}
	first, err := reader.readFirstRecords(ctx, ownershipwindow.FirstRecordsQuery(ownershipwindow.Names{Org: "org_id", AsOf: "as_of"}), organizationID)
	if err != nil {
		return set, err
	}
	set.FirstByRepo = first
	return set, nil
}

// readFirstRecords runs a first-record statement of package ownershipwindow (FirstRecordsQuery) and keys its rows by the
// repository id. A repo_key that is no id (a repository the catalog does not hold) is left out.
func (reader *Reader) readFirstRecords(ctx context.Context, query, organizationID string) (map[uuid.UUID]time.Time, error) {
	rows, err := reader.conn.Query(ctx, query, clickhouse.Named("org_id", organizationID))
	if err != nil {
		return nil, fmt.Errorf("query first ownership records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	first := map[uuid.UUID]time.Time{}
	for rows.Next() {
		var key string
		var from time.Time
		if err := rows.Scan(&key, &from); err != nil {
			return nil, fmt.Errorf("scan first ownership record: %w", err)
		}
		id, err := uuid.Parse(key)
		if err != nil {
			continue
		}
		first[id] = from
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate first ownership records: %w", err)
	}
	return first, nil
}

// FetchTeamRepoDonors tests the ownership window at the instant alone, with no first-owners-backward clamp (no
// production caller: the materializer reads FetchTeamRepoDonorSet).
// FetchTeamRepoDonors reads only the latest primary attribution snapshot and
// live, sync-derived repository ownership. It never resolves person membership
// or accepts a manual mapping. linked_issue is excluded because its persisted
// evidence does not establish whether the donor was itself membership-derived.
// See docs/reference/data-models/investment.md for the allocation boundary.
func (reader *Reader) FetchTeamRepoDonors(ctx context.Context, workItemIDs []string, organizationID string, asOf time.Time) ([]TeamRepoDonor, error) {
	if reader == nil || reader.conn == nil || strings.TrimSpace(organizationID) == "" || asOf.IsZero() {
		return nil, ErrUnavailable
	}
	ids := dedupeStrings(workItemIDs)
	if len(ids) == 0 {
		return []TeamRepoDonor{}, nil
	}
	queryCtx, err := withDonorWorkItems(ctx, ids)
	if err != nil {
		return nil, err
	}
	rows, err := reader.conn.Query(queryCtx, teamRepoDonorsQuery,
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("as_of", asOf.UTC().Format("2006-01-02 15:04:05.000")))
	if err != nil {
		return nil, fmt.Errorf("query team repository donors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	donors := make([]TeamRepoDonor, 0)
	for rows.Next() {
		var donor TeamRepoDonor
		if err := rows.Scan(&donor.WorkItemID, &donor.TeamID, &donor.RepoID); err != nil {
			return nil, fmt.Errorf("scan team repository donor: %w", err)
		}
		// Same decode every other fetcher in this package applies. The donor's
		// work_item_id is matched against component node ids that arrive from
		// the edge fetcher already hexed, so an undecoded donor spells the same
		// bytes differently, matches nothing, and the unit loses its allocation
		// with no error raised anywhere.
		donor.WorkItemID = pythonparity.DecodeClickHouseStringValue(donor.WorkItemID)
		donor.TeamID = pythonparity.DecodeClickHouseStringValue(donor.TeamID)
		donors = append(donors, donor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate team repository donors: %w", err)
	}
	return donors, nil
}

// withDonorTeams attaches the stored ids of the donor teams the interval query
// reads as its donor_teams external table, in a fixed order.
func withDonorTeams(ctx context.Context, teamIDs map[string]bool) (context.Context, error) {
	table, err := ext.NewTable("donor_teams", ext.Column("team_id", "String"))
	if err != nil {
		return nil, fmt.Errorf("build donor team table: %w", err)
	}
	ids := make([]string, 0, len(teamIDs))
	for id := range teamIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := table.Append(id); err != nil {
			return nil, fmt.Errorf("build donor team table: %w", err)
		}
	}
	return clickhouse.Context(ctx, clickhouse.WithExternalTable(table)), nil
}

// withDonorWorkItems attaches the work item ids the donor query reads as its
// donor_work_items external table.
func withDonorWorkItems(ctx context.Context, workItemIDs []string) (context.Context, error) {
	table, err := ext.NewTable("donor_work_items", ext.Column("work_item_id", "String"))
	if err != nil {
		return nil, fmt.Errorf("build donor work item table: %w", err)
	}
	for _, id := range workItemIDs {
		if err := table.Append(id); err != nil {
			return nil, fmt.Errorf("build donor work item table: %w", err)
		}
	}
	return clickhouse.Context(ctx, clickhouse.WithExternalTable(table)), nil
}
