package providersync

import (
	"context"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CHAOS-4530 shipped a per-sync "tombstone" (is_active=0, project_key=nil)
// for the {org_id}:linear:{team_key} pseudo-project identity, believing
// is_active=0 would be enough for a reader to treat it as retired. CF (acr
// owner), live on org 70d529e0, found that assumption wrong on two counts:
// acr's identity resolution does not filter on `projects.is_active` at all,
// AND `is_active=0` already legitimately marks two REAL completed Linear
// projects for an unrelated reason -- so it can never be a safe, unambiguous
// "this is the synthetic row" signal for any reader. The row must be
// physically GONE from `projects`, not soft-deleted.
//
// This file is the one-time, operator-invoked cleanup for rows ALREADY
// written before that correction (linear_reference_catalog_route.go no
// longer writes ANY row -- active or tombstone -- for this identity, see
// that file's CollectReferenceCatalog). It is NOT a per-sync mutation:
// nothing in this package calls it; it is wired to a dev-health-workerctl
// verb an operator runs by hand, once per environment (or once per org, if
// scoped), same class of action as `metrics partition-recompute`.

// linearPseudoProjectIdentityPredicate is the producer-owned shape check for
// this artifact: id = "{org_id}:linear:{team_key}" (formerly built by
// linear_reference_catalog_route.go's team-derived-project construction --
// see that file's CollectReferenceCatalog doc comment for the full history).
// team_key varies per org/team, so rather than enumerate every team key,
// this matches the SHAPE: provider='linear' AND id starts with THIS ROW'S
// OWN org_id followed by the literal ":linear:" marker. A real Linear
// project's id is always the raw provider UUID (standard 8-4-4-4-12 hex,
// normalizeLinearReferenceProject), which can never start with its own
// org_id plus that marker -- confirmed org-agnostic and team-key-agnostic.
//
// Deliberately tied to the row's OWN org_id (startsWith(id,
// concat(org_id, ':linear:'))), not a bare substring test anywhere in id
// (codex review, 2026-08-29, P2): normalizeLinearReferenceProject and
// linearEnsureProjectsRow accept any non-empty id from the wire, so a
// substring-only check could in principle match a row whose id merely
// CONTAINS ":linear:" without being shaped like ITS OWN org's pseudo-
// project identity (e.g. a different org's identity string embedded
// inside a differently-owned id) -- this predicate can only ever match a
// row that reconstructs its own org_id as a literal prefix, which the
// known producer's real project ids (raw UUIDs) can never do.
const linearPseudoProjectIdentityPredicate = `provider = 'linear' AND startsWith(id, concat(org_id, ':linear:'))`

const linearPseudoProjectCleanupSelectQuery = `SELECT org_id, id, name, is_active FROM projects FINAL WHERE ` + linearPseudoProjectIdentityPredicate

const linearPseudoProjectCleanupSelectQueryScoped = linearPseudoProjectCleanupSelectQuery + ` AND org_id = {org_id:String}`

// mutations_sync=1 makes this ALTER TABLE DELETE block until ClickHouse has
// actually applied it, rather than merely queuing an async mutation --
// deliberate for a rare, deliberate operator action: the caller's reported
// row count must describe rows that are ACTUALLY gone by the time this
// returns, not a mutation still in system.mutations (same synchronous-write
// discipline sinks.go's clickHouseGenerationContext already uses for insert
// visibility, applied here to a delete instead).
const linearPseudoProjectCleanupDeleteMutation = `ALTER TABLE projects DELETE WHERE ` + linearPseudoProjectIdentityPredicate

const linearPseudoProjectCleanupDeleteMutationScoped = linearPseudoProjectCleanupDeleteMutation + ` AND org_id = {org_id:String}`

// LinearPseudoProjectCleanupRow is one pseudo-project identity found (and,
// unless dry-run, deleted) by RetireLinearPseudoProjectRows.
type LinearPseudoProjectCleanupRow struct {
	OrgID    string `json:"org_id"`
	ID       string `json:"id"`
	Name     string `json:"name"`
	IsActive uint8  `json:"is_active"`
}

// LinearPseudoProjectCleanupOutcome is RetireLinearPseudoProjectRows' full
// report: DryRun records which mode produced it, Rows is every matching
// identity found (present regardless of dry-run), and DeletedRows mirrors
// len(Rows) only when a real (non-dry-run) delete actually ran -- 0 in
// dry-run mode, so a caller can never mistake "found" for "deleted".
type LinearPseudoProjectCleanupOutcome struct {
	DryRun      bool                            `json:"dry_run"`
	Rows        []LinearPseudoProjectCleanupRow `json:"rows"`
	DeletedRows int                             `json:"deleted_rows"`
}

// RetireLinearPseudoProjectRows finds every {org_id}:linear:{team_key}
// pseudo-project row still present in `projects` -- across every org unless
// orgID scopes it -- and, unless dryRun, physically deletes them via a
// synchronous ALTER TABLE DELETE. Idempotent: a second call after a real
// delete finds (and would delete) nothing, since the SELECT and the DELETE
// share the exact same predicate.
//
// Never touches a REAL project row: the identity predicate matches only the
// synthetic id shape a real Linear project id (a raw provider UUID) cannot
// produce -- confirmed by construction, not by is_active (which CF's finding
// makes explicitly unsafe to use here: it also marks real completed
// projects).
func RetireLinearPseudoProjectRows(ctx context.Context, conn driver.Conn, orgID string, dryRun bool) (LinearPseudoProjectCleanupOutcome, error) {
	if conn == nil {
		return LinearPseudoProjectCleanupOutcome{}, ErrInvalidConfiguration
	}
	orgID = strings.TrimSpace(orgID)
	query := linearPseudoProjectCleanupSelectQuery
	mutation := linearPseudoProjectCleanupDeleteMutation
	var namedArgs []any
	if orgID != "" {
		query = linearPseudoProjectCleanupSelectQueryScoped
		mutation = linearPseudoProjectCleanupDeleteMutationScoped
		namedArgs = []any{clickhouse.Named("org_id", orgID)}
	}
	rows, err := conn.Query(ctx, query, namedArgs...)
	if err != nil {
		return LinearPseudoProjectCleanupOutcome{}, err
	}
	defer rows.Close()
	outcome := LinearPseudoProjectCleanupOutcome{DryRun: dryRun, Rows: make([]LinearPseudoProjectCleanupRow, 0)}
	for rows.Next() {
		var row LinearPseudoProjectCleanupRow
		if err := rows.Scan(&row.OrgID, &row.ID, &row.Name, &row.IsActive); err != nil {
			return LinearPseudoProjectCleanupOutcome{}, err
		}
		outcome.Rows = append(outcome.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return LinearPseudoProjectCleanupOutcome{}, err
	}
	if dryRun || len(outcome.Rows) == 0 {
		return outcome, nil
	}
	syncCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"mutations_sync": "1",
	}))
	if err := conn.Exec(syncCtx, mutation, namedArgs...); err != nil {
		return outcome, err
	}
	outcome.DeletedRows = len(outcome.Rows)
	return outcome, nil
}
