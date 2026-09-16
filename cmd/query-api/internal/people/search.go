package people

import (
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/quadrant"
)

// maxSearchLimit ports _MAX_SEARCH_LIMIT (services/people.py:66).
const maxSearchLimit = 50

// SearchParams is the already-resolved request shape GET /api/v1/people's
// route file reduces to before calling BuildSearchResponse -- the route
// file owns query-param parsing/validation, this package owns the
// business logic, matching drilldown.PRParams/BuildPRsResponse's own
// division of labor (cmd/query-api/internal/drilldown/prs.go).
type SearchParams struct {
	// Query is the raw, untrimmed q query param (main.py:1052's
	// `q: str = ""`).
	Query string
	// Limit is the route's own `_bounded_limit_param(limit, 50)` result
	// (main.py:1061). search_people_response then reapplies the
	// IDENTICAL bound itself (`_bounded_limit(limit, _MAX_SEARCH_LIMIT)`,
	// services/people.py:411, _MAX_SEARCH_LIMIT=50) -- both call sites
	// bound with the same max (50), so the second application is a no-op
	// given the first already ran. This port applies the bound ONCE, in
	// boundedSearchLimit below, rather than reproducing the redundant
	// double-clamp: the net result for every input is identical either
	// way.
	Limit int
	// Now stands in for utc_today() (dev_health_ops/utils/datetime.py:
	// 32-34), injected by the route file (time.Now().UTC()) so this
	// package's own tests can pin it -- matching
	// drilldown.PRParams.StartDay/EndDay's own "route computes, package
	// consumes" division for a time-dependent value.
	Now time.Time
}

// SearchResult ports PersonSearchResult (api/models/schemas.py:361-374):
// PersonSummaryPerson's person_id/display_name/identities plus active.
type SearchResult struct {
	PersonID    string           `json:"person_id"`
	DisplayName string           `json:"display_name"`
	Identities  []PersonIdentity `json:"identities"`
	Active      bool             `json:"active"`
}

// boundedSearchLimit ports _bounded_limit(limit, _MAX_SEARCH_LIMIT)
// (services/people.py:267-271) specialized to this package's constant
// max_limit (search_people_response's only call site,
// services/people.py:411, always passes _MAX_SEARCH_LIMIT): an
// absent/non-positive limit falls back to 50 (== maxSearchLimit here, so
// Python's `min(50, max_limit)` collapses to maxSearchLimit), otherwise
// the limit is clamped to [1, maxSearchLimit].
func boundedSearchLimit(limit int) int {
	if limit <= 0 {
		return maxSearchLimit
	}
	if limit > maxSearchLimit {
		return maxSearchLimit
	}
	return limit
}

// searchPeopleQuery is the Go port of people/people_search.sql, queried
// by search_people (api/queries/people.py:23-32).
//
// DEDUP: Python query modules are not correct exemplars for
// ReplacingMergeTree reads. user_metrics_daily is
// ReplacingMergeTree(computed_at) with sorting key (org_id, repo_id,
// author_email, day) (migration 096,
// daily_family_tables_replacing_merge_tree.py) -- the reference SQL reads
// it with no FINAL/argMax dedup at all, only work_item_user_metrics_daily
// (also ReplacingMergeTree(computed_at) since migration 055) already
// carries FINAL. This port reads BOTH tables FINAL, matching
// cmd/query-api/internal/quadrant/identity.go's resolvePersonIdentity,
// which reads the SAME two tables for the SAME reason (this package's own
// class ruling is one dedup shape for every ReplacingMergeTree read, not a
// per-query exception). Concretely: without FINAL, two unmerged physical
// copies of the same (org_id, repo_id, author_email, day) key could carry
// DIFFERENT identity_id values (identity_id defaults to author_email but
// is not itself part of the sorting key, so a later write correcting it
// for an existing key is possible) -- a raw scan could then surface BOTH
// the stale and the corrected identity as separate search results for the
// same person, where FINAL always resolves to the one the latest write
// intended. This is a declared Python-plane defect, not a data-semantics
// choice.
//
// ORG SCOPE sits INSIDE each UNION branch's own WHERE, at the same
// nesting depth as its FINAL source (pinned by sqlshape_test.go) -- never
// a filter applied only after a cross-tenant merge/scan.
const searchPeopleQuery = `
WITH identities AS (
    SELECT
        identity_id AS identity,
        max(day) AS last_seen
    FROM user_metrics_daily FINAL
    WHERE identity_id != ''
      AND org_id = {org_id:String}
    GROUP BY identity_id

    UNION ALL

    SELECT
        user_identity AS identity,
        max(day) AS last_seen
    FROM work_item_user_metrics_daily FINAL
    WHERE user_identity != ''
      AND org_id = {org_id:String}
    GROUP BY user_identity
)
SELECT
    identity AS identity_id,
    max(last_seen) AS last_seen
FROM identities
WHERE identity != ''
  AND lower(identity) LIKE {query:String}
GROUP BY identity
ORDER BY last_seen DESC
LIMIT {limit:UInt64}
%s
`

// BuildSearchResponse is the Go port of search_people_response
// (api/services/people.py:398-449): resolve the identity-alias map, run
// search_people, canonicalize each raw identity through the reverse-alias
// map, and compute active from the 90-day recency window. Auth
// (current_user.org_id), the outer try/except -> 503 fallback, and
// _reject_comparative_params are the CALLER's job (route file), matching
// drilldown.BuildPRsResponse's own division -- this function returns a
// plain Go error for any failure, never an HTTP status.
func BuildSearchResponse(ctx context.Context, reader *Reader, orgID string, params SearchParams) ([]SearchResult, error) {
	// `if not trimmed: return []` (services/people.py:405-406) -- short
	// circuits BEFORE the alias file is loaded or ClickHouse is touched,
	// same order Python's own early return keeps.
	trimmed := strings.TrimSpace(params.Query)
	if trimmed == "" {
		return []SearchResult{}, nil
	}
	if reader == nil {
		return nil, ErrUnavailable
	}

	aliases := loadIdentityAliases()
	reverseAliases := buildReverseAliasMap(aliases)
	limit := boundedSearchLimit(params.Limit)

	query := fmt.Sprintf(searchPeopleQuery, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "query", Value: "%" + strings.ToLower(trimmed) + "%"},
		{Name: "limit", Value: limit},
	}

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("search people: %w", err)
	}
	defer rows.Close()

	today := params.Now
	results := make([]SearchResult, 0)
	for rows.Next() {
		var identityID string
		var lastSeen time.Time
		if err := rows.Scan(&identityID, &lastSeen); err != nil {
			return nil, fmt.Errorf("scan person search row: %w", err)
		}

		// `identity = str(row.get("identity_id") or "").strip(); if not
		// identity: continue` (services/people.py:421-423).
		identity := strings.TrimSpace(identityID)
		if identity == "" {
			continue
		}

		canonical, ok := reverseAliases[normalizeAlias(identity)]
		if !ok {
			canonical = identity
		}
		aliasList := append([]string(nil), aliases[canonical]...)
		inList := false
		for _, a := range aliasList {
			if a == identity {
				inList = true
				break
			}
		}
		if !inList && identity != canonical {
			aliasList = append(aliasList, identity)
		}

		// `active = True; if last_seen: ... active = (today -
		// seen_day).days <= 90` (services/people.py:432-438). last_seen
		// is a ClickHouse Date (day-granularity), so truncating both
		// sides to a UTC day boundary before subtracting reproduces
		// Python's date-only (not datetime) subtraction exactly.
		active := true
		if !lastSeen.IsZero() {
			seenDay := lastSeen.Truncate(24 * time.Hour)
			todayDay := today.Truncate(24 * time.Hour)
			daysSince := int(todayDay.Sub(seenDay).Hours() / 24)
			active = daysSince <= 90
		}

		results = append(results, SearchResult{
			PersonID:    quadrant.PersonIDForIdentity(canonical),
			DisplayName: displayNameForIdentity(canonical),
			Identities:  identitiesForPerson(canonical, aliasList),
			Active:      active,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate person search rows: %w", err)
	}
	return results, nil
}
