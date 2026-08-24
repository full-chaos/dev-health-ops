package providersync

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/google/uuid"
)

// projectMembershipRow is one row of `project_membership_transitions`
// (CHAOS-4194) as a PROVIDER emits it, before any sink coercion -- the same
// convention the sibling githubWorkItem* rows follow.
//
// It is provider-agnostic on purpose. The table takes rows from github, jira
// and linear, and CHAOS-4193's jira and linear producers build the same shape;
// duplicating it per provider would let three copies drift apart while each
// looked locally correct.
type projectMembershipRow struct {
	OrgID          string    `json:"org_id"`
	SubjectKind    string    `json:"subject_kind"`
	SubjectID      string    `json:"subject_id"`
	RepoID         uuid.UUID `json:"repo_id"`
	Provider       string    `json:"provider"`
	FromProjectID  string    `json:"from_project_id"`
	ToProjectID    string    `json:"to_project_id"`
	FromProjectKey string    `json:"from_project_key"`
	ToProjectKey   string    `json:"to_project_key"`
	Actor          string    `json:"actor"`
	OccurredAt     time.Time `json:"occurred_at"`
	EventID        string    `json:"event_id"`
	// LastSynced is the ReplacingMergeTree version column; see
	// githubWorkItemRow.LastSynced for why the unit's normalizedAt is carried
	// in the row rather than stamped inside the writer.
	LastSynced time.Time `json:"last_synced"`
}

// projectMembershipEventID is the ruled fallback event_id: a content-determined
// hash, for the case where the provider exposes no native per-change id.
//
// GitHub Projects V2 is exactly that case. Its `changes` connection covers
// field-value edits WITHIN a board and carries no "item joined project X"
// event at all, so board membership has no provider event id to borrow. What
// makes the hash safe as a sorting-key member is that every input is
// content, never observation: re-syncing one unchanged membership recomputes
// the same value and ReplacingMergeTree collapses it, whereas mixing in a sync
// timestamp would mint a new key per sync and accumulate one row per sync of a
// single membership -- the defect occurred_at's required-ness already closed
// once on the sink side.
//
// The input list and its order mirror the formula Context Fabric recorded on
// CHAOS-4193 (org, subject_kind, repo_id, subject_id, from, to, occurred_at).
// repo_id is in it because it is a sorting-key member and two repositories
// could otherwise share a subject_id string -- every PR number 42 in the org
// would hash identically without it.
func projectMembershipEventID(row projectMembershipRow) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		row.OrgID, row.SubjectKind, row.RepoID.String(), row.SubjectID,
		row.FromProjectID, row.ToProjectID,
		row.OccurredAt.UTC().Format(time.RFC3339Nano),
	}, "\x00")))
	return hex.EncodeToString(digest[:])[:32]
}

// projectCatalogRow is a `projects` row in its base 051 columns.
type projectCatalogRow struct {
	ID         string    `json:"id"`
	OrgID      string    `json:"org_id"`
	Provider   string    `json:"provider"`
	ProjectKey string    `json:"project_key"`
	Name       string    `json:"name"`
	IsActive   uint8     `json:"is_active"`
	UpdatedAt  time.Time `json:"updated_at"`
	LastSynced time.Time `json:"last_synced"`
}

// ensureProjectsRow builds the `projects` row that makes a membership row's
// destination resolvable.
//
// It exists because of a gap the CHAOS-4194 inventory turned up: GitHub
// Projects V2 wrote NO `projects` row anywhere. The fetcher stamped
// `project_id = ghprojv2:<org>#<n>` onto work items and nothing ever created
// the entity that id names. The vocabulary constraint requires (provider,
// project_id) to resolve to a `projects` row, so without this every github
// membership row would be filtered out downstream -- correct rows, dropped,
// for a reason nothing in the membership path would explain.
//
// "ensure" rather than "insert" is literal: `projects` is a
// ReplacingMergeTree keyed (org_id, provider, id) (051_team_attribution_
// dimensions.sql), so re-emitting the same row on every sync converges instead
// of accumulating. That is why the target loop can call this unconditionally
// and does not need to know whether the project already exists.
//
// Only the BASE 051 columns are written. 073/074 added linear-specific
// lifecycle columns (state, target_date, url, team_ids, lead_*) that this
// helper deliberately leaves at their defaults: it has no honest value for
// them, and writing a fabricated one would out-version a real linear catalogue
// row for the same project. The jira producer built under CHAOS-4193 reuses
// this call for the same reason -- JQL is per-project-scoped, so id and key are
// on hand there too.
func ensureProjectsRow(orgID, provider, id, key, name string, normalizedAt time.Time) projectCatalogRow {
	return projectCatalogRow{
		ID: id, OrgID: orgID, Provider: provider, ProjectKey: key, Name: name,
		IsActive: 1, UpdatedAt: normalizedAt.UTC(), LastSynced: normalizedAt.UTC(),
	}
}
