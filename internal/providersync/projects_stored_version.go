package providersync

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

// Values of projects.target_state. 'unknown' is the column default, so a row
// written before the column existed reads as unknown, never as "none dated".
//
//	stated      the run read the project's versions or milestones: target_date and
//	            the columns beside it are that answer
//	none_dated  the run read them and no open entry has a date (target_date NULL)
//	held        the run did not read them (the read failed, or none was made): every
//	            target column is the value of the last run that did
//	unknown     no run ever read them: no target value
const (
	projectTargetStated   = "stated"
	projectTargetNoneDate = "none_dated"
	projectTargetHeld     = "held"
	projectTargetUnknown  = "unknown"
)

// Values of projects.target_kind: what supplied the date.
const (
	projectTargetKindProject   = "project"
	projectTargetKindVersion   = "version"
	projectTargetKindMilestone = "milestone"
)

// projectsCatalogInsert is the one insert of the provider-sync writers of the
// `projects` catalog (linear, jira, gitlab).
const projectsCatalogInsert = `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, state_source, target_date, target_state, target_kind, target_source, target_ref, target_ref_name, target_other_open, target_fetched_at, url, team_ids, team_keys, lead_id, lead_name, lead_email, updated_at, last_synced)`

// projectTargetColumns are the columns a fetch of the project's target
// states together: they are written by one answer or kept together.
var projectTargetColumns = []string{
	"target_date", "target_state", "target_kind", "target_source", "target_ref", "target_ref_name",
	"target_other_open", "target_fetched_at",
}

// projectsContract is the contract table of the provider-sync writers of
// `projects`. Every column is stated by the provider row except the target
// columns, which are R2: a writer that read the provider's answer for the
// project states them (a date or an explicit none, D6004's class); a writer
// whose read failed this run, or never ran for the row, leaves them unstated
// and keeps the values the key already holds. applyProjectsContract then
// turns the kept state into 'held' (or leaves 'unknown'): the held date is
// stale, and target_fetched_at says by how much.
var projectsContract = storedversion.Contract{Writer: "provider_sync", Table: "projects", Columns: joinColumns(
	contractColumns(storedversion.Identity, "provider", "id"),
	contractColumns(storedversion.Writer, "org_id"),
	contractColumns(storedversion.Stated, "project_key", "name", "is_active", "state", "state_source"),
	projectTargetContractColumns(),
	contractColumns(storedversion.Stated, "url", "team_ids", "team_keys", "lead_id", "lead_name", "lead_email"),
	contractColumns(storedversion.Writer, "updated_at", "last_synced"),
)}

func projectTargetContractColumns() []storedversion.Column {
	columns := make([]storedversion.Column, 0, len(projectTargetColumns))
	for _, name := range projectTargetColumns {
		columns = append(columns, storedversion.Column{Name: name, Rule: storedversion.Unstated, Fields: []string{"target"}})
	}
	return columns
}

// projectTargetFacts is what a fetch of a project's target answered, beyond
// the date: the provenance of the date. The zero value is "not read this run".
type projectTargetFacts struct {
	// TargetState is stated or none_dated when the run read the project's
	// versions or milestones, and empty when it did not.
	TargetState string `json:"target_state,omitempty"`
	// TargetKind is what supplied the date: project, version or milestone.
	TargetKind string `json:"target_kind,omitempty"`
	// TargetSource is the provider fact the date came from, in the shape of
	// state_source: <provider>.<kind>.<field>.
	TargetSource    string     `json:"target_source,omitempty"`
	TargetRef       string     `json:"target_ref,omitempty"`
	TargetRefName   string     `json:"target_ref_name,omitempty"`
	TargetOtherOpen uint32     `json:"target_other_open,omitempty"`
	TargetFetchedAt *time.Time `json:"target_fetched_at,omitempty"`
}

func (facts projectTargetFacts) read() bool {
	return facts.TargetState == projectTargetStated || facts.TargetState == projectTargetNoneDate
}

// projectTargetCandidate is one version or milestone of a project.
type projectTargetCandidate struct {
	// ID is the provider's own id of the entry; Ref is the key written as
	// target_ref (the id itself, or the sprints row key built from it).
	ID, Ref, Name string
	// Date is the provider's date text; empty or unreadable is undated.
	Date string
	// Open is true while the provider has not released, closed or archived it.
	Open bool
}

// providerIDLess orders two provider ids: as numbers when both are decimal
// numbers (so 9 comes before 10), else as text.
func providerIDLess(left, right string) bool {
	l, lErr := strconv.ParseUint(left, 10, 64)
	r, rErr := strconv.ParseUint(right, 10, 64)
	if lErr == nil && rErr == nil && l != r {
		return l < r
	}
	return left < right
}

// pickProjectTarget is the one rule for a project's target date, shared by
// every provider that has no project date of its own (D6009): the earliest
// OPEN dated candidate, a past date included. An overdue target stays the
// target until a provider act (release, close, archive) moves it to the next
// open dated one; the passing of time alone never does. Equal dates pick the
// lower provider id, compared as numbers when both are numbers (9 before 10),
// so the pick does not depend on the order the provider listed them in. A
// project with no open dated candidate has no target.
//
// The result is the answer of a fetch that succeeded: the date, or nil, with
// its provenance. kind is project, version or milestone and source is
// <provider>.<kind>.<field>.
func pickProjectTarget(kind, source string, fetchedAt time.Time, candidates []projectTargetCandidate) (*time.Time, projectTargetFacts) {
	type dated struct {
		day time.Time
		projectTargetCandidate
	}
	open := make([]dated, 0, len(candidates))
	for _, candidate := range candidates {
		if !candidate.Open {
			continue
		}
		if day, ok := providerDateOnly(candidate.Date); ok {
			open = append(open, dated{day: day, projectTargetCandidate: candidate})
		}
	}
	fetched := fetchedAt.UTC()
	if len(open) == 0 {
		return nil, projectTargetFacts{TargetState: projectTargetNoneDate, TargetFetchedAt: &fetched}
	}
	sort.SliceStable(open, func(i, j int) bool {
		if !open[i].day.Equal(open[j].day) {
			return open[i].day.Before(open[j].day)
		}
		return providerIDLess(open[i].ID, open[j].ID)
	})
	day := open[0].day
	return &day, projectTargetFacts{
		TargetState: projectTargetStated, TargetKind: kind, TargetSource: source, TargetRef: open[0].Ref, TargetRefName: open[0].Name,
		TargetOtherOpen: uint32(len(open) - 1), TargetFetchedAt: &fetched,
	}
}

// projectStoredRow is one `projects` row in projectsCatalogInsert column
// order. TargetUnread is true when the run did not read the project's target.
type projectStoredRow struct {
	Values       []any
	TargetUnread bool
}

// newProjectStoredRow builds the row of a project in insert column order.
func newProjectStoredRow(
	id, orgID, provider string, projectKey *string, name string, isActive uint8, state, stateSource string,
	targetDate *time.Time, facts projectTargetFacts, url string, teamIDs, teamKeys []string,
	leadID, leadName, leadEmail *string, updatedAt, lastSynced time.Time,
) projectStoredRow {
	targetState := facts.TargetState
	var otherOpen *uint32
	switch facts.TargetState {
	case projectTargetStated:
		otherOpen = &facts.TargetOtherOpen
	case projectTargetNoneDate:
	default:
		targetState = projectTargetUnknown
	}
	return projectStoredRow{
		Values: []any{
			id, orgID, provider, projectKey, name, isActive, state, stateSource,
			targetDate, targetState, facts.TargetKind, facts.TargetSource, facts.TargetRef, facts.TargetRefName, otherOpen, facts.TargetFetchedAt,
			url, teamIDs, teamKeys, leadID, leadName, leadEmail, updatedAt, lastSynced,
		},
		TargetUnread: !facts.read(),
	}
}

// projectStoredVersionRows turns rows into contract rows: a row whose target
// was not read carries the held target columns.
func projectStoredVersionRows(rows []projectStoredRow) []storedversion.Row {
	stored := make([]storedversion.Row, len(rows))
	for i, row := range rows {
		unread := row.TargetUnread
		stored[i] = storedversion.Row{
			Values: row.Values,
			Carry:  projectsContract.Carry(func(string) bool { return unread }),
		}
	}
	return stored
}

// applyProjectsContract runs the contract over rows, rewriting each row's
// Values in place: the stated target columns, or the held ones for a row whose
// target was not read. The state of a kept target is 'held' when the held
// version had been read by some run (stated, none_dated or held itself), and
// 'unknown' when it never had. A failed read of the held versions fails the
// call before any insert.
func applyProjectsContract(
	ctx context.Context, conn storedversion.Querier, orgID string, rows []projectStoredRow, log bool,
) error {
	stored := projectStoredVersionRows(rows)
	outcomes, err := projectsContract.Apply(ctx, conn, orgID, projectsCatalogInsert, stored)
	if err != nil {
		return err
	}
	markHeldTargets(rows, stored)
	if log {
		projectsContract.Log(ctx, orgID, storedVersionCarriedEvent, storedVersionRefusedEvent, outcomes)
	}
	return nil
}

// markHeldTargets states 'held' on every unread row whose kept target had been
// read by some run.
func markHeldTargets(rows []projectStoredRow, stored []storedversion.Row) {
	position := projectsInsertPositions["target_state"]
	for i, row := range rows {
		if !row.TargetUnread {
			continue
		}
		switch stored[i].Values[position] {
		case projectTargetStated, projectTargetNoneDate, projectTargetHeld:
			stored[i].Values[position] = projectTargetHeld
		default:
			stored[i].Values[position] = projectTargetUnknown
		}
	}
}

var projectsInsertPositions = func() map[string]int {
	positions, err := storedversion.Positions(projectsCatalogInsert)
	if err != nil {
		panic(err)
	}
	return positions
}()

// projectTargetDate is the target_date of a row's Values after the contract.
func projectTargetDate(values []any) (*time.Time, error) {
	switch value := values[projectsInsertPositions["target_date"]].(type) {
	case nil:
		return nil, nil
	case *time.Time:
		return value, nil
	case time.Time:
		return &value, nil
	default:
		return nil, fmt.Errorf("stored version: projects target_date %T is not a date", value)
	}
}

// providerDateOnly reads a provider's date or date-time text as the UTC day it
// names. A date-only value keeps its day: it is never read in a local zone.
func providerDateOnly(value string) (time.Time, bool) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		utc := parsed.UTC()
		return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC), true
	}
	if len(raw) > len("2006-01-02") {
		raw = raw[:len("2006-01-02")]
	}
	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

// projectTargetColumnsMatch reads the target columns of a stored project row
// and compares them with the values the contract gave the expected row, for
// the recovery readback.
func projectTargetColumnsMatch(ctx context.Context, conn storedversion.Querier, orgID, provider, id string, values []any) (bool, error) {
	result, err := conn.Query(ctx, `SELECT target_state, target_kind, target_source, target_ref, target_ref_name, target_other_open, target_fetched_at
		FROM projects FINAL WHERE org_id = ? AND provider = ? AND id = ?`, orgID, provider, id)
	if err != nil {
		return false, err
	}
	defer result.Close()
	matched := false
	for result.Next() {
		var state, kind, source, ref, name string
		var otherOpen *uint32
		var fetchedAt *time.Time
		if err := result.Scan(&state, &kind, &source, &ref, &name, &otherOpen, &fetchedAt); err != nil {
			return false, err
		}
		position := projectsInsertPositions
		matched = fmt.Sprint(values[position["target_state"]], values[position["target_kind"]], values[position["target_source"]],
			values[position["target_ref"]], values[position["target_ref_name"]]) == fmt.Sprint(state, kind, source, ref, name) &&
			sameOtherOpen(values[position["target_other_open"]], otherOpen) &&
			sameFetchedAt(values[position["target_fetched_at"]], fetchedAt)
	}
	return matched, result.Err()
}

func sameOtherOpen(expected any, stored *uint32) bool {
	switch value := expected.(type) {
	case nil:
		return stored == nil
	case uint32:
		return stored != nil && *stored == value
	case *uint32:
		return value == nil && stored == nil || value != nil && stored != nil && *value == *stored
	}
	return false
}

func sameFetchedAt(expected any, stored *time.Time) bool {
	switch value := expected.(type) {
	case nil:
		return stored == nil
	case time.Time:
		return stored != nil && stored.Equal(value)
	case *time.Time:
		return value == nil && stored == nil || value != nil && stored != nil && stored.Equal(*value)
	}
	return false
}

// projectsStoredVersionSpecs lists the writers of `projects` under the
// contract for the invariant enumeration: the Linear, Jira and GitLab catalog
// writers share one insert and one contract. A row whose payload carries no
// target field is one whose target was not read.
func projectsStoredVersionSpecs() []storedversion.Spec {
	return []storedversion.Spec{{
		Name: "provider sync projects", Contract: projectsContract, Insert: projectsCatalogInsert,
		Carry: func(payload map[string]any) map[string]bool {
			_, read := payload["target"]
			return projectsContract.Carry(func(string) bool { return !read })
		},
	}}
}
