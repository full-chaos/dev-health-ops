// Identity resolution for GET /api/v1/people/{person_id}/summary and
// GET /api/v1/people/{person_id}/metric -- ports:
//   - api/services/people.py's _resolve_identity_context (274-298) and
//     _identity_inputs (301-303)
//   - api/queries/people.py's resolve_person_identity (35-45), backed by
//     sql/people/person_lookup.sql, inlined verbatim below with one
//     declared fix (see resolvePersonIdentity's own doc comment)
package people

import (
	"context"
	"fmt"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/quadrant"
)

// resolvePersonIdentity ports resolve_person_identity (queries/people.py:
// 35-45) and its person_lookup.sql, with one declared fix: the
// reference's load_sql call bypasses dedup_from for this read (a
// duplicate identity/user_identity string collapses via UNION DISTINCT
// regardless of which physical row it came from, so the VALUE was never
// wrong), but user_metrics_daily is ReplacingMergeTree(computed_at)
// (migration 096) and this package's own class ruling is one dedup shape
// for every ReplacingMergeTree read, not a per-query exception -- so this
// branch reads FINAL too, matching work_item_user_metrics_daily's branch
// two lines below, which already carried it. Identical in shape to
// cmd/query-api/internal/quadrant/identity.go's own resolvePersonIdentity
// (same Python source, same fix, different package -- see this file's own
// package doc comment for why it is not shared).
//
// UNSAFE STATEMENT: this was also a `WITH identities AS (...) SELECT ...`
// CTE until this fix: dev-health-go's client-side read-only guard
// (clickhouse/client.go's validateReadOnlyStatement) requires a
// statement's FIRST token to be the literal "SELECT", so a query
// beginning "WITH ..." is rejected before it ever reaches ClickHouse
// (ErrUnsafeStatement, "clickhouse runtime: unsafe statement") -- the
// swallowed cause of the 503 every /people/{person_id}/* "person not
// found" request degraded to (this function returning an error instead
// of "", the not-found sentinel resolveIdentityContext's caller expects,
// turns every 404 this route should answer into a 503). The "identities"
// CTE is referenced exactly once (in the outer FROM), so inlining it as
// an ordinary derived-table subquery is a purely mechanical, semantically
// identical rewrite.
func resolvePersonIdentity(ctx context.Context, client QueryClient, personID, orgID string) (string, error) {
	query := fmt.Sprintf(`
        SELECT
            identity AS identity_id
        FROM (
            SELECT identity_id AS identity
            FROM user_metrics_daily FINAL
            WHERE identity_id != ''
              AND org_id = {org_id:String}

            UNION DISTINCT

            SELECT user_identity AS identity
            FROM work_item_user_metrics_daily FINAL
            WHERE user_identity != ''
              AND org_id = {org_id:String}
        ) AS identities
        WHERE lower(hex(MD5(identity))) = {person_id:String}
        LIMIT 1
        %s
    `, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "person_id", Value: personID},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return "", fmt.Errorf("people: resolve_person_identity query: %w", err)
	}
	defer rows.Close()

	var identity string
	if rows.Next() {
		if err := rows.Scan(&identity); err != nil {
			return "", fmt.Errorf("people: resolve_person_identity scan: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("people: resolve_person_identity: %w", err)
	}
	return identity, nil
}

// resolveIdentityContext ports _resolve_identity_context (services/
// people.py:274-298): resolve a person_id (an md5 hash) to its canonical
// identity plus alias list, via a ClickHouse lookup and the (in-practice-
// empty, see identity.go's own doc comment) alias config. "" canonical
// with a nil alias list means no identity was found for person_id --
// callers translate that into the 404 "Person not found" build_person_
// summary_response/build_person_metric_response raise.
func resolveIdentityContext(ctx context.Context, client QueryClient, personID, orgID string) (canonical string, aliasList []string, err error) {
	aliases := loadIdentityAliases()
	reverse := buildReverseAliasMap(aliases)

	identity, err := resolvePersonIdentity(ctx, client, personID, orgID)
	if err != nil {
		return "", nil, err
	}
	if identity != "" {
		canon, ok := reverse[normalizeAlias(identity)]
		if !ok {
			canon = identity
		}
		list := append([]string(nil), aliases[canon]...)
		inList := false
		for _, a := range list {
			if a == identity {
				inList = true
				break
			}
		}
		if !inList && identity != canon {
			list = append(list, identity)
		}
		return canon, list, nil
	}

	for canon, list := range aliases {
		if quadrant.PersonIDForIdentity(canon) == personID {
			return canon, append([]string(nil), list...), nil
		}
		for _, alias := range list {
			if quadrant.PersonIDForIdentity(alias) == personID {
				return canon, append([]string(nil), list...), nil
			}
		}
	}
	return "", nil, nil
}
