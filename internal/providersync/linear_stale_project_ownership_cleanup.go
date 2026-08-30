package providersync

import (
	"context"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CHAOS-4548: before CHAOS-4530, every Linear sync cycle stamped the owning
// TEAM's key onto team_project_ownership.project_key for REAL project
// ownership rows too (project_id = the real Linear project UUID), not just
// the {org_id}:linear:{team_key} pseudo-identity row. CHAOS-4530 fixed the
// writer (linear_reference_catalog_route.go's real-project ownership rows
// now carry ProjectKey: nil) and CHAOS-4537 moved every reader off
// project_key for real projects entirely -- acr's projectOwnershipJoinSQL
// (acr/internal/contextfabric/devhealthfacts/shared.go) only ever matches a
// `projects` row through project_key, and every real Linear project's
// `projects.project_key` has been NULL since CHAOS-4530 (never 'CHAOS', a
// value only the now-deleted pseudo-project row ever carried) -- so these
// rows were never reachable through that join, before or after. The Go
// project_id-keyed readers (team_repo_ownership_derivation_clickhouse.go's
// loadTeamRepoOwnershipProjectLinks, and the Python
// metrics/loaders/clickhouse.py load_team_attribution_context) GROUP BY
// (provider, project_id, team_id) directly and never select project_key at
// all, so they are equally indifferent to its value. Confirmed empirically,
// local org 70d529e0 (2026-08-30): every project_id's CHAOS-keyed and
// NULL-keyed rows agree on the same team_id (zero rows with
// uniqExact(team_id) > 1 grouped by project_id), so any reader keyed on
// project_id gets an identical answer whether it reads the stale row or the
// current one.
//
// This file is the one-time, operator-invoked cleanup for the stale rows
// left behind by that historical writer bug -- pure hygiene, no reader
// depends on them being gone, same class of action as
// RetireLinearPseudoProjectRows (linear_pseudo_project_cleanup.go). Nothing
// in this package calls it; it is wired to a dev-health-workerctl verb an
// operator runs by hand.
//
// Deliberately excludes the {org_id}:linear:{team_key} pseudo-identity row:
// that row's OWN team_project_ownership entry is still load-bearing (kept on
// purpose by CHAOS-4530, still read by nothing today per CHAOS-4537, but not
// yet safe to stop WRITING per CHAOS-4560's own live-verification ordering
// constraint) -- this cleanup is scoped to CHAOS-4548 only and must never
// touch CHAOS-4560's still-open concern.

// linearStaleProjectOwnershipPredicate is the producer-owned shape check for
// this artifact: provider='linear', a non-null project_key (the historical
// team-key stamp), on a REAL project_id -- never the
// "{org_id}:linear:{team_key}" pseudo-identity shape linear_pseudo_project_
// cleanup.go's linearPseudoProjectIdentityPredicate already owns. Mirrors
// that predicate's own-org_id prefix-match discipline (codex review on
// linear_pseudo_project_cleanup.go, 2026-08-29, P2): the exclusion ties the
// ":linear:" marker to THIS ROW'S OWN org_id, never a bare substring test.
//
// The trailing (org_id, project_id) IN (...) clause is a codex review P1
// (2026-08-30): an org that has never re-synced since CHAOS-4530's writer
// fix went live has ONLY the stale CHAOS-keyed row for a real project --
// deleting it unconditionally would remove the ONLY team_project_ownership
// signal loadTeamRepoOwnershipProjectLinks (and load_team_attribution_
// context) read for that project, since those readers select by
// (project_id, team_id) regardless of project_key. Requiring a sibling row
// for the SAME (org_id, project_id) with project_key IS NULL to already
// exist proves the fixed writer has already produced a replacement before
// the old row is removed -- never delete a project's only ownership
// signal.
//
// This makes the predicate self-referencing (used inside an ALTER TABLE
// team_project_ownership DELETE against team_project_ownership itself).
// Codex review round 2 (2026-08-30, P2) flagged this as unsupported --
// disproven by direct execution on both engines that could plausibly run
// it: a real ALTER TABLE ... DELETE with this exact predicate
// (mutations_sync=1) against a scratch org, on (1) the local dev stack's
// ClickHouse 26.7.5.10 (the version local + prod both run, per AGENTS.md's
// data vocabulary) and (2) a throwaway container from the exact digest
// internal/testsupport/containers/harness.go pins for Go integration tests
// (resolves to 26.6.1.1193, distinct from the Python-CI-gate 25.1 pinned in
// .github/workflows/test.yml/live-e2e.yml) -- both completed with no error
// and deleted exactly the CHAOS-keyed row, leaving its NULL-keyed sibling
// untouched.
const linearStaleProjectOwnershipPredicate = `provider = 'linear' AND project_key IS NOT NULL AND NOT startsWith(project_id, concat(org_id, ':linear:')) AND (org_id, project_id) IN (SELECT org_id, project_id FROM team_project_ownership WHERE provider = 'linear' AND project_key IS NULL)`

const linearStaleProjectOwnershipSelectQuery = `SELECT org_id, project_id, project_key, team_id FROM team_project_ownership WHERE ` + linearStaleProjectOwnershipPredicate

const linearStaleProjectOwnershipSelectQueryScoped = linearStaleProjectOwnershipSelectQuery + ` AND org_id = {org_id:String}`

// mutations_sync=1, same synchronous-write discipline
// RetireLinearPseudoProjectRows uses: the caller's reported row count must
// describe rows ACTUALLY gone by the time this returns.
const linearStaleProjectOwnershipDeleteMutation = `ALTER TABLE team_project_ownership DELETE WHERE ` + linearStaleProjectOwnershipPredicate

const linearStaleProjectOwnershipDeleteMutationScoped = linearStaleProjectOwnershipDeleteMutation + ` AND org_id = {org_id:String}`

// LinearStaleProjectOwnershipRow is one stale team_project_ownership row
// found (and, unless dry-run, deleted) by RetireStaleLinearProjectOwnershipRows.
type LinearStaleProjectOwnershipRow struct {
	OrgID      string `json:"org_id"`
	ProjectID  string `json:"project_id"`
	ProjectKey string `json:"project_key"`
	TeamID     string `json:"team_id"`
}

// LinearStaleProjectOwnershipOutcome is
// RetireStaleLinearProjectOwnershipRows' full report: DryRun records which
// mode produced it, Rows is every matching row found (present regardless of
// dry-run), and DeletedRows mirrors len(Rows) only when a real (non-dry-run)
// delete actually ran.
type LinearStaleProjectOwnershipOutcome struct {
	DryRun      bool                             `json:"dry_run"`
	Rows        []LinearStaleProjectOwnershipRow `json:"rows"`
	DeletedRows int                              `json:"deleted_rows"`
}

// RetireStaleLinearProjectOwnershipRows finds every team_project_ownership
// row for a REAL Linear project (project_id is the provider UUID, not the
// {org_id}:linear:{team_key} pseudo-identity) that still carries the
// historical team-key project_key stamp -- across every org unless orgID
// scopes it -- and, unless dryRun, physically deletes them via a synchronous
// ALTER TABLE DELETE. Idempotent: a second call after a real delete finds
// (and would delete) nothing, since the SELECT and the DELETE share the
// exact same predicate.
//
// Never deletes a project's ONLY ownership row: the predicate requires a
// sibling project_key IS NULL row for the same (org_id, project_id) to
// already exist -- proof the fixed writer has re-synced that project --
// before a stale row is even considered a candidate. An org that has not
// re-synced since CHAOS-4530 keeps its only signal untouched.
//
// Never touches the {org_id}:linear:{team_key} pseudo-identity row: the
// predicate explicitly excludes any project_id shaped that way, so
// CHAOS-4560's still-open, still-load-bearing writer concern is untouched by
// this verb.
func RetireStaleLinearProjectOwnershipRows(ctx context.Context, conn driver.Conn, orgID string, dryRun bool) (LinearStaleProjectOwnershipOutcome, error) {
	if conn == nil {
		return LinearStaleProjectOwnershipOutcome{}, ErrInvalidConfiguration
	}
	orgID = strings.TrimSpace(orgID)
	query := linearStaleProjectOwnershipSelectQuery
	mutation := linearStaleProjectOwnershipDeleteMutation
	var namedArgs []any
	if orgID != "" {
		query = linearStaleProjectOwnershipSelectQueryScoped
		mutation = linearStaleProjectOwnershipDeleteMutationScoped
		namedArgs = []any{clickhouse.Named("org_id", orgID)}
	}
	rows, err := conn.Query(ctx, query, namedArgs...)
	if err != nil {
		return LinearStaleProjectOwnershipOutcome{}, err
	}
	defer rows.Close()
	outcome := LinearStaleProjectOwnershipOutcome{DryRun: dryRun, Rows: make([]LinearStaleProjectOwnershipRow, 0)}
	for rows.Next() {
		var row LinearStaleProjectOwnershipRow
		// project_key is Nullable(String); the predicate already guarantees
		// IS NOT NULL, but the driver still requires a pointer destination
		// to scan a Nullable(String) column (same shape linear_reference_
		// catalog.go's ProjectKey *string uses for this same column).
		var projectKey *string
		if err := rows.Scan(&row.OrgID, &row.ProjectID, &projectKey, &row.TeamID); err != nil {
			return LinearStaleProjectOwnershipOutcome{}, err
		}
		if projectKey != nil {
			row.ProjectKey = *projectKey
		}
		outcome.Rows = append(outcome.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return LinearStaleProjectOwnershipOutcome{}, err
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
