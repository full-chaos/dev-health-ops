# Team-Dimension Catalog: Source of Truth

**Status:** Authoritative (CHAOS-1751)

## Contract

The semantic `teams` ClickHouse table is the **source of truth** for the
`TEAM` dimension in the GraphQL `catalog` query. Activity counts are
looked up from an event table via `LEFT JOIN`, so a team with no recorded
activity in the window still appears in the picker with `count = 0`.

This contract applies to the GraphQL `catalog(orgId, dimension: TEAM)`
field. Other dimensions (`REPO`, `AUTHOR`, `WORK_TYPE`, `THEME`,
`SUBCATEGORY`) continue to derive distinct values from the event table
directly, because their identifiers are not first-class semantic
entities the same way teams are.

## Why

`investment_metrics_daily.team_id` and other `_metrics_daily.team_id`
columns are event-time analytics columns. Their values are populated by
sinks during metric computation and may reflect:

- Resolved team identifiers (`teams.id`) when the resolver matched.
- The sentinel `"unassigned"` when no team could be resolved.
- Synthetic literals from fixture generators (e.g. the historical
  `"alpha"` fallback in `fixtures/generators/investments.py`).

Using these columns as the source of truth for the picker conflates the
event-time namespace with the semantic roster:

- Active teams with no activity in the window disappear from the picker.
- Synthetic / sentinel values leak into the UX.
- The picker silently diverges from any other surface that reads from
  `teams`.

Surfacing the roster from `teams` and joining counts from an event
table keeps the two namespaces aligned and makes data gaps observable
(a real team showing `count = 0` is a fact about activity, not a UX bug).

## Query Shape

The compiled SQL for `catalog(orgId, dimension: TEAM)` is roughly:

```sql
SELECT
    t.id AS value,
    COALESCE(activity.count, 0) AS count
FROM (
    SELECT id, name
    FROM teams FINAL
    WHERE org_id = %(org_id)s
      AND is_active = 1
      AND id != ''
) AS t
LEFT JOIN (
    SELECT toString(team_id) AS team_id, COUNT(*) AS count
    FROM investment_metrics_daily
    WHERE team_id IS NOT NULL
      AND investment_metrics_daily.org_id = %(org_id)s
      AND toString(team_id) != ''
    GROUP BY team_id
) AS activity ON activity.team_id = t.id
ORDER BY count DESC, t.name ASC
LIMIT %(limit)s
```

The count source is determined by the existing source-selection logic
in `compile_catalog_values` and matches the event table the rest of the
GraphQL analytics surface reads from for the same query mode
(investment vs. non-investment).

## Filters

Scope/category filters in `FilterInput` target event-table columns
(e.g. `team_id`, `repo_id`, `work_unit_type`). They are intentionally
**not** applied to the team picker query: the picker exposes the full
active roster regardless of what scope is selected, so users can switch
scope freely. Activity counts narrow with the count-source query if a
follow-up surface re-queries with the chosen filters; the catalog
itself stays roster-complete.

## Divergence with `/api/v1/filters/options`

The legacy REST endpoint `GET /api/v1/filters/options` builds a
Python-side `UNION ALL` across `teams FINAL`, `user_metrics_daily`, and
`work_item_user_metrics_daily`. It is **not** the source of truth for
the GraphQL surface and should be converged onto the same shared helper
in a follow-up. New consumers should use the GraphQL `catalog` field.

Developer values from this endpoint are intentionally limited to
email-shaped `user_metrics_daily.author_email` values. The underlying
metrics table can contain fallback identities such as provider handles
or display names for issue-only contributors, but `who.developers` is an
exact git author-email filter and the picker must not offer identities
that cannot match that predicate.

## Files

- `src/dev_health_ops/api/graphql/sql/templates.py` : `catalog_values_team_template()`
- `src/dev_health_ops/api/graphql/sql/compiler.py` : `compile_catalog_values()` (TEAM branch)
- `src/dev_health_ops/api/graphql/sql/validate.py` : `Dimension.TEAM`
- `tests/graphql/test_compiler.py` : `TestCompileCatalogValues::test_team_catalog_uses_teams_table_as_source_of_truth`

## Sync Flow — ClickHouse is the system of record (CHAOS-2600 CS5)

ClickHouse is the system of record for the team catalog AND identity→team membership. There is no PostgreSQL team control plane and no Postgres team projection.

The sync flow is structured as follows:
1. **Direct ClickHouse writes**: `dev-hops sync teams` writes the ClickHouse `teams` table directly via `insert_teams` — org-scoped (`--org`) rows are tagged with `org_id`, no-org rows are untagged. Team auto-import and the admin team CRUD surface (`ClickHouseTeamAdminService`) are native ClickHouse writers too.
2. **ClickHouse-native identities**: Identity records live in the ClickHouse `identities` table (`ClickHouseIdentityStore`); the admin identity surface updates `teams.members` surgically by facet. The old Postgres→ClickHouse member-reconcile path (which used `members_by_team`) was deleted in CS6 (CHAOS-2607); live admin/identity writes never touch Postgres `identity_mappings`.
3. **No Postgres bridge / projection**: `bridge_teams_to_clickhouse`, `providers/team_bridge.py`, `providers/team_reconcile.py` (and `dev-hops teams reconcile`), and the `sync_teams_to_analytics` task are **deleted** in CS5. The org-scoped path does not project to Postgres via `TeamDriftSyncService`. The Postgres `team_mappings` / `identity_mappings` models are dropped in CS6.
## History

- CHAOS-1751: Established `teams` as the source of truth for the TEAM
  dimension catalog; `LEFT JOIN`-based counts surfaced honestly,
  including teams with `count = 0`. Fixture runner aligned to use the
  semantic `teams.id` namespace for event-table writes.
