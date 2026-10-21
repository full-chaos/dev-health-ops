package providersync

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// CHAOS-10521: a catalog writer (GitHub, GitLab, Jira, Atlassian Teams, Linear)
// stamps valid_from with the run that first saw the link. The provider gives no
// time for a team grant or a membership, so the start of a link is its earliest
// evidence (team-attribution.md section 0.4c): the earliest pull request of a
// repository, the earliest work item of a project, the earliest work item a
// member reported or is assigned. valid_from = min(evidence, first seen, run).
//
// valid_from is a key column of the three tables, so moving a start is a new
// row: the first row of a fact is written again at the evidence and its old key
// is retired as an empty interval (valid_to = valid_from), the supersede of the
// inferred rows (CHAOS-9396 F1). It runs before the writer reads its open rows,
// so the writer's first-seen rule then takes the evidence start and a repeat run
// writes nothing. Only the FIRST row of a fact moves: a later row (a member who
// came back) keeps the start the run that saw the return gave it. A read or a
// write that fails moves nothing and never fails the sync.

// CatalogEvidenceTable is a table the catalog writers stamp valid_from on.
type CatalogEvidenceTable string

const (
	CatalogEvidenceMemberships      CatalogEvidenceTable = "team_memberships"
	CatalogEvidenceProjectOwnership CatalogEvidenceTable = "team_project_ownership"
	CatalogEvidenceRepoOwnership    CatalogEvidenceTable = "team_repo_ownership"
)

// CatalogEvidenceScope is the rows of one writer: a table, a provider and a
// source, and, for a writer that owns only the rows of its own teams, those
// teams (nil: every team of the scope).
type CatalogEvidenceScope struct {
	Table            CatalogEvidenceTable
	Provider, Source string
	TeamIDs          []string
}

// CatalogEvidenceFact names one link of a writer's row: its team, the key column
// (member_id, project_id or repo_full_name), the repository id of a repository
// row, and the other identities of a member (raw ids, email, identity facets).
type CatalogEvidenceFact struct {
	TeamID, Key string
	RepoID      *string
	Identities  []string
}

// Skipped reasons of the WARN catalog_evidence_start_skipped.
const (
	catalogEvidenceSkipRowsRead     = "rows_read_failed"
	catalogEvidenceSkipEvidenceRead = "evidence_read_failed"
	catalogEvidenceSkipWrite        = "carry_write_failed"
	catalogEvidenceSkipNewerRow     = "row_newer_than_run"
)

// CatalogEvidenceStarts is what one carry learned: the facts the scope already
// holds a row of and the evidence of the org. Start gives a fresh row of a fact
// the scope holds no row of its start.
type CatalogEvidenceStarts struct {
	at       time.Time
	known    map[string]bool
	evidence catalogEvidence
	usable   bool
}

// Start is the valid_from to write a fresh row of fact with, when the writer
// gives it current (the run). A fact the scope already holds keeps current: the
// writer's first-seen rule gives it the start of its open row. A new fact starts
// at its evidence when the evidence is earlier.
func (starts CatalogEvidenceStarts) Start(fact CatalogEvidenceFact, current time.Time) time.Time {
	if !starts.usable || starts.known[catalogEvidenceFactKey(fact.TeamID, fact.Key)] {
		return current
	}
	if from, ok := starts.evidence.of(fact, starts.at); ok && from.Before(current) {
		return from
	}
	return current
}

// CarryCatalogEvidenceStarts moves the first row of every fact of scope whose
// evidence is earlier than its valid_from to the evidence, and returns what a
// writer needs to start its new facts there.
func CarryCatalogEvidenceStarts(ctx context.Context, conn driver.Conn, orgID string, scope CatalogEvidenceScope, at time.Time) CatalogEvidenceStarts {
	at = at.UTC().Truncate(time.Millisecond)
	spec, ok := catalogEvidenceSpecs[scope.Table]
	if conn == nil || !ok || strings.TrimSpace(orgID) == "" || at.IsZero() || (scope.TeamIDs != nil && len(scope.TeamIDs) == 0) {
		return CatalogEvidenceStarts{}
	}
	skipped := func(reason string, rows int) {
		slog.Default().WarnContext(ctx, "catalog_evidence_start_skipped",
			"org_id", orgID, "provider", scope.Provider, "table", string(scope.Table), "source", scope.Source,
			"reason", reason, "rows", rows)
	}
	rows, err := readCatalogEvidenceRows(ctx, conn, spec, orgID, scope)
	if err != nil {
		skipped(catalogEvidenceSkipRowsRead, 0)
		return CatalogEvidenceStarts{}
	}
	evidence, err := spec.readEvidence(ctx, conn, orgID)
	if err != nil {
		skipped(catalogEvidenceSkipEvidenceRead, len(rows))
		return CatalogEvidenceStarts{}
	}
	plan := planCatalogEvidenceCarry(rows, evidence, at)
	starts := CatalogEvidenceStarts{at: at, known: plan.known, evidence: evidence, usable: true}
	if plan.newer > 0 {
		skipped(catalogEvidenceSkipNewerRow, plan.newer)
	}
	if len(plan.moves) == 0 {
		return starts
	}
	if err := writeCatalogEvidenceCarry(ctx, conn, spec, rows, plan.moves, at); err != nil {
		skipped(catalogEvidenceSkipWrite, len(plan.moves))
		return starts
	}
	slog.Default().InfoContext(ctx, "catalog_evidence_start_carried",
		"org_id", orgID, "provider", scope.Provider, "table", string(scope.Table), "source", scope.Source,
		"rows", len(plan.moves))
	return starts
}

// catalogEvidenceRow is one stored row of a scope: the link it names, its
// interval and version, and every column as the insert takes it.
type catalogEvidenceRow struct {
	Fact      CatalogEvidenceFact
	ValidFrom time.Time
	ValidTo   *time.Time
	UpdatedAt time.Time
	values    []any
}

func (row catalogEvidenceRow) empty() bool {
	return row.ValidTo != nil && !row.ValidTo.After(row.ValidFrom)
}

// catalogEvidenceMove writes rows[Row] again at From and retires its old key.
type catalogEvidenceMove struct {
	Row  int
	From time.Time
}

type catalogEvidencePlan struct {
	moves []catalogEvidenceMove
	known map[string]bool
	// newer counts the facts left alone because a row of them is newer than the run.
	newer int
}

// planCatalogEvidenceCarry is the pure rule: for each fact, its first row that
// is not an empty interval moves to the fact's evidence when the evidence is
// earlier than its start and not after the run. A fact with a row written after
// the run is another run's to move.
func planCatalogEvidenceCarry(rows []catalogEvidenceRow, evidence catalogEvidence, at time.Time) catalogEvidencePlan {
	plan := catalogEvidencePlan{known: map[string]bool{}}
	first := map[string]int{}
	newer := map[string]bool{}
	var order []string
	for index, row := range rows {
		key := catalogEvidenceFactKey(row.Fact.TeamID, row.Fact.Key)
		if row.UpdatedAt.After(at) {
			newer[key] = true
		}
		if row.empty() {
			continue
		}
		plan.known[key] = true
		current, seen := first[key]
		if !seen {
			order = append(order, key)
		}
		if !seen || row.ValidFrom.Before(rows[current].ValidFrom) {
			first[key] = index
		}
	}
	sort.Strings(order)
	for _, key := range order {
		index := first[key]
		from, ok := evidence.of(rows[index].Fact, at)
		if !ok || !from.Before(rows[index].ValidFrom) {
			continue
		}
		if newer[key] {
			plan.newer++
			continue
		}
		plan.moves = append(plan.moves, catalogEvidenceMove{Row: index, From: from})
	}
	return plan
}

func catalogEvidenceFactKey(teamID, key string) string { return teamID + "\x00" + key }

// catalogEvidence is the earliest evidence of an org: by project id, by
// repository id and name, and by the people of the work items.
type catalogEvidence struct {
	byKey  map[string]time.Time
	byRepo map[string]time.Time
	people catalogPeople
}

func (evidence catalogEvidence) of(fact CatalogEvidenceFact, at time.Time) (time.Time, bool) {
	var found time.Time
	take := func(t time.Time) {
		t = evidenceTime(t)
		if !t.IsZero() && (found.IsZero() || t.Before(found)) {
			found = t
		}
	}
	if evidence.byKey != nil {
		take(evidence.byKey[fact.Key])
	}
	if evidence.byRepo != nil {
		if fact.RepoID != nil {
			take(evidence.byRepo[strings.ToLower(strings.TrimSpace(*fact.RepoID))])
		}
		take(evidence.byRepo[catalogRepoNameKey(fact.Key)])
	}
	if evidence.people.earliest != nil {
		take(evidence.people.of(fact))
	}
	if found.IsZero() || found.After(at) {
		return time.Time{}, false
	}
	return found, true
}

func catalogRepoNameKey(name string) string {
	return "name:" + strings.ToLower(strings.TrimSpace(name))
}

// catalogPeople is the earliest created_at of a work item per person (its
// reporter or an assignee), indexed for the attribution matrix's member rule
// and for an identity equal after NormalizeDerivationIdentity.
type catalogPeople struct {
	earliest map[string]time.Time // lower-cased person
	// bySuffix: a person "x:<facet>" for every ':' of it; byAtPrefix: a person
	// "<facet>@..." for every '@' of it.
	bySuffix, byAtPrefix map[string]time.Time
	normalized           map[string]time.Time
}

func newCatalogPeople(earliest map[string]time.Time) catalogPeople {
	people := catalogPeople{earliest: map[string]time.Time{}, bySuffix: map[string]time.Time{},
		byAtPrefix: map[string]time.Time{}, normalized: map[string]time.Time{}}
	keep := func(index map[string]time.Time, key string, at time.Time) {
		if current, ok := index[key]; key != "" && (!ok || at.Before(current)) {
			index[key] = at
		}
	}
	for person, at := range earliest {
		at = evidenceTime(at)
		if at.IsZero() {
			continue
		}
		lower := strings.ToLower(person)
		keep(people.earliest, lower, at)
		keep(people.normalized, teamattribution.NormalizeDerivationIdentity(person), at)
		for index := 0; index < len(lower); index++ {
			switch lower[index] {
			case ':':
				keep(people.bySuffix, lower[index+1:], at)
			case '@':
				keep(people.byAtPrefix, lower[:index], at)
			}
		}
	}
	return people
}

// of is the earliest work of a member: a person equal to the member's facet
// (the member id after its provider prefix), ending in ":"+facet or starting
// with facet+"@" (the rule of the attribution matrix's membership cell), or
// equal to one of its identities.
func (people catalogPeople) of(fact CatalogEvidenceFact) time.Time {
	var found time.Time
	take := func(index map[string]time.Time, key string) {
		if key == "" {
			return
		}
		if at, ok := index[key]; ok && (found.IsZero() || at.Before(found)) {
			found = at
		}
	}
	member := strings.ToLower(fact.Key)
	facet := member[strings.Index(member, ":")+1:]
	take(people.earliest, facet)
	take(people.bySuffix, facet)
	take(people.byAtPrefix, facet)
	for _, identity := range append([]string{fact.Key}, fact.Identities...) {
		take(people.normalized, teamattribution.NormalizeDerivationIdentity(identity))
	}
	return found
}

// catalogEvidenceSpec is how one table's rows are read and written again.
type catalogEvidenceSpec struct {
	table        string
	columns      string
	selectList   string
	scan         func(driver.Rows) (catalogEvidenceRow, error)
	validFrom    int
	validTo      int
	updatedAt    int
	readEvidence func(context.Context, driver.Conn, string) (catalogEvidence, error)
}

var catalogEvidenceSpecs = map[CatalogEvidenceTable]catalogEvidenceSpec{
	CatalogEvidenceMemberships: {
		table:      "team_memberships",
		columns:    "org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, provider_user_id",
		selectList: "org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, toString(source), is_primary, specificity, priority, valid_from, valid_to, updated_at, provider_user_id",
		validFrom:  11, validTo: 12, updatedAt: 13,
		scan: func(rows driver.Rows) (catalogEvidenceRow, error) {
			var org, provider, team, member, source string
			var rawID, rawEmail, providerUserID *string
			var facets []string
			var isPrimary uint8
			var specificity uint16
			var priority int32
			var from, updated time.Time
			var to *time.Time
			if err := rows.Scan(&org, &provider, &team, &member, &rawID, &rawEmail, &facets, &source, &isPrimary, &specificity, &priority, &from, &to, &updated, &providerUserID); err != nil {
				return catalogEvidenceRow{}, err
			}
			identities := append([]string(nil), facets...)
			for _, value := range []*string{rawID, rawEmail} {
				if value != nil {
					identities = append(identities, *value)
				}
			}
			return catalogEvidenceRow{
				Fact:   CatalogEvidenceFact{TeamID: team, Key: member, Identities: identities},
				values: []any{org, provider, team, member, rawID, rawEmail, facets, source, isPrimary, specificity, priority, from, to, updated, providerUserID},
			}, nil
		},
		readEvidence: readCatalogPeopleEvidence,
	},
	CatalogEvidenceProjectOwnership: {
		table:      "team_project_ownership",
		columns:    "org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at",
		selectList: "org_id, provider, team_id, project_id, project_key, toString(source), is_primary, specificity, priority, valid_from, valid_to, updated_at",
		validFrom:  9, validTo: 10, updatedAt: 11,
		scan: func(rows driver.Rows) (catalogEvidenceRow, error) {
			var org, provider, team, project, source string
			var key *string
			var isPrimary uint8
			var specificity uint16
			var priority int32
			var from, updated time.Time
			var to *time.Time
			if err := rows.Scan(&org, &provider, &team, &project, &key, &source, &isPrimary, &specificity, &priority, &from, &to, &updated); err != nil {
				return catalogEvidenceRow{}, err
			}
			return catalogEvidenceRow{
				Fact:   CatalogEvidenceFact{TeamID: team, Key: project},
				values: []any{org, provider, team, project, key, source, isPrimary, specificity, priority, from, to, updated},
			}, nil
		},
		readEvidence: readCatalogProjectEvidence,
	},
	CatalogEvidenceRepoOwnership: {
		table:      "team_repo_ownership",
		columns:    "org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share",
		selectList: "org_id, provider, team_id, repo_id, repo_full_name, toString(match_type), toString(source), is_primary, specificity, priority, valid_from, valid_to, updated_at, share",
		validFrom:  10, validTo: 11, updatedAt: 12,
		scan: func(rows driver.Rows) (catalogEvidenceRow, error) {
			var org, provider, team, name, matchType, source string
			var repoID *uuid.UUID
			var isPrimary uint8
			var specificity uint16
			var priority int32
			var from, updated time.Time
			var to *time.Time
			var share *float64
			if err := rows.Scan(&org, &provider, &team, &repoID, &name, &matchType, &source, &isPrimary, &specificity, &priority, &from, &to, &updated, &share); err != nil {
				return catalogEvidenceRow{}, err
			}
			fact := CatalogEvidenceFact{TeamID: team, Key: name}
			if repoID != nil {
				text := repoID.String()
				fact.RepoID = &text
			}
			return catalogEvidenceRow{
				Fact:   fact,
				values: []any{org, provider, team, repoID, name, matchType, source, isPrimary, specificity, priority, from, to, updated, share},
			}, nil
		},
		readEvidence: readCatalogRepoEvidence,
	},
}

func readCatalogEvidenceRows(ctx context.Context, conn driver.Conn, spec catalogEvidenceSpec, orgID string, scope CatalogEvidenceScope) ([]catalogEvidenceRow, error) {
	query := "SELECT " + spec.selectList + " FROM " + spec.table + " FINAL WHERE org_id = ? AND provider = ? AND source = ?"
	args := []any{orgID, scope.Provider, scope.Source}
	if scope.TeamIDs != nil {
		query += " AND team_id IN ?"
		args = append(args, scope.TeamIDs)
	}
	result, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var rows []catalogEvidenceRow
	for result.Next() {
		row, err := spec.scan(result)
		if err != nil {
			return nil, err
		}
		row.ValidFrom = row.values[spec.validFrom].(time.Time).UTC()
		if to := row.values[spec.validTo].(*time.Time); to != nil {
			closed := to.UTC()
			row.ValidTo = &closed
		}
		row.UpdatedAt = row.values[spec.updatedAt].(time.Time).UTC()
		rows = append(rows, row)
	}
	return rows, result.Err()
}

// writeCatalogEvidenceCarry writes the carry rows in one insert.
func writeCatalogEvidenceCarry(ctx context.Context, conn driver.Conn, spec catalogEvidenceSpec, rows []catalogEvidenceRow, moves []catalogEvidenceMove, at time.Time) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+spec.table+" ("+spec.columns+")")
	if err != nil {
		return err
	}
	defer func() {
		if !batch.IsSent() {
			_ = batch.Abort()
		}
	}()
	for _, values := range catalogEvidenceCarryRows(spec, rows, moves, at) {
		if err := batch.Append(values...); err != nil {
			return err
		}
	}
	return batch.Send()
}

// catalogEvidenceCarryRows is, for each move, the row at its evidence start
// (updated_at = the run) and its old key as an empty interval with an
// updated_at past the stored one, so the retired version wins the merge.
func catalogEvidenceCarryRows(spec catalogEvidenceSpec, rows []catalogEvidenceRow, moves []catalogEvidenceMove, at time.Time) [][]any {
	out := make([][]any, 0, 2*len(moves))
	for _, move := range moves {
		row := rows[move.Row]
		moved := append([]any(nil), row.values...)
		moved[spec.validFrom] = move.From
		moved[spec.updatedAt] = at
		retired := append([]any(nil), row.values...)
		start := row.ValidFrom
		retired[spec.validTo] = &start
		version := at
		if !version.After(row.UpdatedAt) {
			version = row.UpdatedAt.Add(time.Millisecond)
		}
		retired[spec.updatedAt] = version
		out = append(out, moved, retired)
	}
	return out
}

// readCatalogPeopleEvidence is the earliest work item per person of the org.
func readCatalogPeopleEvidence(ctx context.Context, conn driver.Conn, orgID string) (catalogEvidence, error) {
	result, err := conn.Query(ctx, `SELECT person, min(created_at)
FROM (SELECT arrayJoin(arrayConcat([reporter], assignees)) AS person, created_at FROM work_items FINAL WHERE org_id = ?)
WHERE person != ''
GROUP BY person`, orgID)
	if err != nil {
		return catalogEvidence{}, err
	}
	earliest, err := scanCatalogEvidenceTimes(result)
	if err != nil {
		return catalogEvidence{}, err
	}
	return catalogEvidence{people: newCatalogPeople(earliest)}, nil
}

// readCatalogProjectEvidence is the earliest work item per project id of the org.
func readCatalogProjectEvidence(ctx context.Context, conn driver.Conn, orgID string) (catalogEvidence, error) {
	result, err := conn.Query(ctx, `SELECT project_id, min(created_at) FROM work_items FINAL
WHERE org_id = ? AND project_id != '' GROUP BY project_id`, orgID)
	if err != nil {
		return catalogEvidence{}, err
	}
	byKey, err := scanCatalogEvidenceTimes(result)
	if err != nil {
		return catalogEvidence{}, err
	}
	return catalogEvidence{byKey: byKey}, nil
}

// readCatalogRepoEvidence is the earliest pull request per repository of the
// org, by its id and by its name (a row with no repo_id names it by full name).
func readCatalogRepoEvidence(ctx context.Context, conn driver.Conn, orgID string) (catalogEvidence, error) {
	result, err := conn.Query(ctx, `SELECT p.repo, p.first, ifNull(r.name, '') FROM (
  SELECT toString(repo_id) AS repo, min(created_at) AS first FROM git_pull_requests FINAL WHERE org_id = ? GROUP BY repo_id
) AS p
LEFT JOIN (SELECT toString(id) AS id, argMax(repo, last_synced) AS name FROM repos WHERE org_id = ? GROUP BY id) AS r ON r.id = p.repo`,
		orgID, orgID)
	if err != nil {
		return catalogEvidence{}, err
	}
	defer result.Close()
	byRepo := map[string]time.Time{}
	keep := func(key string, at time.Time) {
		if current, ok := byRepo[key]; !ok || at.Before(current) {
			byRepo[key] = at
		}
	}
	for result.Next() {
		var repo, name string
		var first time.Time
		if err := result.Scan(&repo, &first, &name); err != nil {
			return catalogEvidence{}, err
		}
		keep(strings.ToLower(repo), first.UTC())
		if strings.TrimSpace(name) != "" {
			keep(catalogRepoNameKey(name), first.UTC())
		}
	}
	if err := result.Err(); err != nil {
		return catalogEvidence{}, err
	}
	return catalogEvidence{byRepo: byRepo}, nil
}

func scanCatalogEvidenceTimes(result driver.Rows) (map[string]time.Time, error) {
	defer result.Close()
	out := map[string]time.Time{}
	for result.Next() {
		var key string
		var first time.Time
		if err := result.Scan(&key, &first); err != nil {
			return nil, err
		}
		out[key] = first.UTC()
	}
	return out, result.Err()
}

// MembershipEvidenceFact names a membership row for Start: its team, its member
// id and the member's other identities.
func MembershipEvidenceFact(teamID, memberID string, rawProviderUserID, rawEmail *string, facets []string) CatalogEvidenceFact {
	identities := append([]string(nil), facets...)
	for _, value := range []*string{rawProviderUserID, rawEmail} {
		if value != nil {
			identities = append(identities, *value)
		}
	}
	return CatalogEvidenceFact{TeamID: teamID, Key: memberID, Identities: identities}
}

// ProjectEvidenceFact names a project ownership row for Start.
func ProjectEvidenceFact(teamID string, project ProjectID) CatalogEvidenceFact {
	return CatalogEvidenceFact{TeamID: teamID, Key: project.String()}
}

// RepoEvidenceFact names a repository ownership row for Start.
func RepoEvidenceFact(teamID, repoFullName string, repoID *string) CatalogEvidenceFact {
	return CatalogEvidenceFact{TeamID: teamID, Key: repoFullName, RepoID: repoID}
}

// StartCatalogRows returns a copy of rows with each row's valid_from set by
// starts.Start.
func StartCatalogRows[R any](starts CatalogEvidenceStarts, rows []R, fact func(R) CatalogEvidenceFact, validFrom func(*R) *time.Time) []R {
	if !starts.usable || len(rows) == 0 {
		return rows
	}
	out := append([]R(nil), rows...)
	for index := range out {
		from := validFrom(&out[index])
		*from = starts.Start(fact(out[index]), *from)
	}
	return out
}

func (r githubMembershipRow) evidenceFact() CatalogEvidenceFact {
	return MembershipEvidenceFact(r.TeamID, r.MemberID, r.RawProviderUserID, r.RawEmail, r.IdentityFacets)
}

func (r gitlabTeamCatalogMembershipRow) evidenceFact() CatalogEvidenceFact {
	return MembershipEvidenceFact(r.TeamID, r.MemberID, r.RawProviderUserID, r.RawEmail, r.IdentityFacets)
}

func (r linearReferenceMembershipRow) evidenceFact() CatalogEvidenceFact {
	return MembershipEvidenceFact(r.TeamID, r.MemberID, r.RawProviderUserID, r.RawEmail, r.IdentityFacets)
}
