// Package projectmembership owns the ONE shaping of a project-membership row
// and of the `projects` catalogue row that makes it resolvable.
//
// It exists because two different writers put rows into the same two tables and
// neither is the other's caller: `streamhandlers`' external-ingest sink writes
// what a customer pushed, and `providersync`' GitHub Projects V2 effect adapter
// writes what the Go sync route fetched. Left to themselves each would carry
// its own column list and its own value ordering, and the two would agree only
// for as long as nobody edited one of them -- a divergence that produces
// perfectly well-formed rows with the project id in the project key column.
//
// So the column order and the row-to-values projection live here, once. A
// writer that wants to insert into these tables asks this package for the
// statement AND for the values; it may not spell either itself.
//
// What does NOT live here is anything either side can decide alone: the
// external path's payload validation and refusal codes stay in streamhandlers,
// where the batch pointer is visible, and the producer's fetch semantics stay
// in providersync. This package is the shared vocabulary, not a third writer.
package projectmembership

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SubjectKind values. The table's subject_kind column admits exactly these two,
// and both writers branch their identity derivation on them, so they are spelled
// once rather than as string literals at four call sites.
const (
	SubjectWorkItem    = "work_item"
	SubjectPullRequest = "pull_request"
)

// TransitionsInsert is the ONLY statement that may write
// project_membership_transitions. Its column order and Row.Values must be read
// together -- they are the two halves of one contract, which is exactly why
// they are adjacent here instead of in two packages.
const TransitionsInsert = `INSERT INTO project_membership_transitions ` +
	`(org_id,source_id,repo_id,subject_kind,subject_id,provider,` +
	`from_project_id,to_project_id,from_project_key,to_project_key,` +
	`actor,occurred_at,last_synced,event_id)`

// Row is one project_membership_transitions row, provider-neutral and
// pre-coercion. Both writers build this and hand it to Values.
type Row struct {
	OrgID          string     `json:"org_id"`
	SourceID       *uuid.UUID `json:"source_id"`
	RepoID         uuid.UUID  `json:"repo_id"`
	SubjectKind    string     `json:"subject_kind"`
	SubjectID      string     `json:"subject_id"`
	Provider       string     `json:"provider"`
	FromProjectID  string     `json:"from_project_id"`
	ToProjectID    string     `json:"to_project_id"`
	FromProjectKey string     `json:"from_project_key"`
	ToProjectKey   string     `json:"to_project_key"`
	Actor          string     `json:"actor"`
	OccurredAt     time.Time  `json:"occurred_at"`
	LastSynced     time.Time  `json:"last_synced"`
	EventID        string     `json:"event_id"`
}

// Values returns the row in TransitionsInsert's column order.
//
// source_id is the one column that may be NULL, so it is the one field passed
// as a typed pointer rather than a value; every other column is a bare type
// because the schema mirrors work_item_transitions column-for-column.
func (row Row) Values() []any {
	return []any{
		row.OrgID, row.SourceID, row.RepoID, row.SubjectKind, row.SubjectID,
		row.Provider, row.FromProjectID, row.ToProjectID,
		row.FromProjectKey, row.ToProjectKey, row.Actor,
		row.OccurredAt, row.LastSynced, row.EventID,
	}
}

// SortingKey is the table's ORDER BY, as a comparable value. A writer that
// dedupes a batch before sending must dedupe on exactly this, or it will either
// collapse two distinct events or let a contradicting pair through to a FINAL
// that picks between them arbitrarily.
func (row Row) SortingKey() string {
	return strings.Join([]string{
		row.OrgID, row.SubjectKind, row.RepoID.String(), row.SubjectID,
		row.OccurredAt.UTC().Format(time.RFC3339Nano), row.EventID,
	}, "\x00")
}

// EventID is the ruled fallback event_id: a content-determined hash, for the
// case where the provider exposes no native per-change id.
//
// Every input is CONTENT, never observation. That is what makes it safe as a
// sorting-key member: re-syncing one unchanged membership recomputes the same
// value and ReplacingMergeTree collapses it, whereas mixing in a sync timestamp
// would mint a new key per sync and accumulate one row per sync of a single
// membership.
//
// The input list and order mirror the formula Context Fabric recorded on
// CHAOS-4193. repo_id is included because it is a sorting-key member and two
// repositories could otherwise share a subject_id string -- every PR number 42
// in an org would hash identically without it.
func EventID(row Row) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		row.OrgID, row.SubjectKind, row.RepoID.String(), row.SubjectID,
		row.FromProjectID, row.ToProjectID,
		row.OccurredAt.UTC().Format(time.RFC3339Nano),
	}, "\x00")))
	return hex.EncodeToString(digest[:])[:32]
}

// ProjectsInsert writes the BASE 051 columns of `projects` and no others.
//
// 073 and 074 added linear-specific lifecycle columns (state, target_date, url,
// team_ids, lead_*). They are deliberately left at their defaults here: a
// producer ensuring a project row so its membership can resolve has no honest
// value for them, and writing a fabricated one would out-version a real linear
// catalogue row for the same project on a ReplacingMergeTree keyed
// (org_id, provider, id).
const ProjectsInsert = `INSERT INTO projects ` +
	`(id,org_id,provider,project_key,name,is_active,updated_at,last_synced)`

// CatalogRow is a `projects` row in its base 051 columns.
type CatalogRow struct {
	ID         string    `json:"id"`
	OrgID      string    `json:"org_id"`
	Provider   string    `json:"provider"`
	ProjectKey string    `json:"project_key"`
	Name       string    `json:"name"`
	IsActive   uint8     `json:"is_active"`
	UpdatedAt  time.Time `json:"updated_at"`
	LastSynced time.Time `json:"last_synced"`
}

// Values returns the catalogue row in ProjectsInsert's column order.
func (row CatalogRow) Values() []any {
	return []any{
		row.ID, row.OrgID, row.Provider, row.ProjectKey, row.Name,
		row.IsActive, row.UpdatedAt, row.LastSynced,
	}
}

// SortingKey is `projects`' ORDER BY (org_id, provider, id).
func (row CatalogRow) SortingKey() string {
	return strings.Join([]string{row.OrgID, row.Provider, row.ID}, "\x00")
}

// EnsureProjectsRow builds the `projects` row that makes a membership row's
// destination resolvable.
//
// It exists because of a gap the CHAOS-4194 inventory turned up: GitHub
// Projects V2 wrote NO `projects` row anywhere. The fetcher stamped
// `project_id = ghprojv2:<org>#<n>` onto work items and nothing ever created
// the entity that id names, so under the resolve-to-`projects` constraint every
// github membership would have been filtered out -- correct rows dropped for a
// reason nothing in the membership path would explain.
//
// "Ensure" rather than "insert" is literal: `projects` is a ReplacingMergeTree
// keyed (org_id, provider, id), so re-emitting the same row every sync
// converges instead of accumulating. That is why a producer can call this
// unconditionally without knowing whether the project already exists.
//
// This is also the load-bearing half of the ruled vocabulary contract: because
// a producer ensures the row and mints its membership ids from the same value,
// resolution holds BY CONSTRUCTION rather than by a check on the write path.
func EnsureProjectsRow(orgID, provider, id, key, name string, normalizedAt time.Time) CatalogRow {
	return CatalogRow{
		ID: id, OrgID: orgID, Provider: provider, ProjectKey: key, Name: name,
		IsActive: 1, UpdatedAt: normalizedAt.UTC(), LastSynced: normalizedAt.UTC(),
	}
}
