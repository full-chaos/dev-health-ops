// Package repoinventory is an organization's repository inventory: the
// canonical "owner/name" slugs in the ClickHouse repos table. It is the one
// definition of that list. query-api's catalog serves it as the REPO
// dimension (internal/queryapi/analytics, the list web reads as an org's Ask
// Dev repository scope), and go-api's ACR access read
// (internal/apiservice/acr) serves it as repository_scope. Both run this
// package's Statement and Canonical, so the two answers cannot drift.
package repoinventory

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Limit is the most repositories an inventory may hold. A larger inventory is
// refused (ErrOverLimit), never truncated, so a scope reader never sees a
// partial repository list.
const Limit = 100

// QueryLimit is the row limit the inventory read binds as limit_n: one more
// than Limit, so an inventory above Limit is seen and refused.
const QueryLimit = Limit + 1

// ErrOverLimit is an inventory with more than Limit repositories.
var ErrOverLimit = errors.New("repoinventory: repository inventory exceeds the supported scope limit")

// part matches one half of an "owner/name" repository slug.
var part = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?$`)

// Statement is the inventory read: the lowercased, trimmed repo of every
// repos row of the organization, kept only when it is a well-formed slug,
// counted per slug and ordered by slug. It binds {org_id:String} and
// {limit_n:UInt32} (QueryLimit); settings is the trailing SETTINGS clause.
func Statement(settings string) string {
	return fmt.Sprintf(`
SELECT
    canonical_repo AS value,
    count() AS count
FROM (
    SELECT lowerUTF8(trimBoth(repo)) AS canonical_repo
    FROM repos FINAL
    WHERE org_id = {org_id:String}
)
WHERE match(
    canonical_repo,
    '^[a-z0-9]([a-z0-9._-]{0,98}[a-z0-9])?/[a-z0-9]([a-z0-9._-]{0,98}[a-z0-9])?$'
)
GROUP BY canonical_repo
ORDER BY value
LIMIT {limit_n:UInt32}
%s
`, settings)
}

// Row is one row of Statement: a slug and its row count.
type Row struct {
	Value string
	Count uint64
}

// Item is one repository of the inventory and the number of repos rows that
// name it.
type Item struct {
	Slug  string
	Count uint64
}

// Canonical trims and lowercases each value, keeps only well-formed
// "owner/name" slugs, merges duplicates by summing their counts, refuses an
// inventory above Limit (ErrOverLimit) and returns the slugs sorted. An empty
// inventory is a non-nil empty slice.
func Canonical(rows []Row) ([]Item, error) {
	counts := map[string]uint64{}
	for _, row := range rows {
		parts := strings.Split(strings.ToLower(strings.TrimSpace(row.Value)), "/")
		if len(parts) != 2 || !part.MatchString(parts[0]) || !part.MatchString(parts[1]) {
			continue
		}
		slug := parts[0] + "/" + parts[1]
		counts[slug] += row.Count
	}
	if len(counts) > Limit {
		return nil, fmt.Errorf("%w: %d repositories", ErrOverLimit, len(counts))
	}
	slugs := make([]string, 0, len(counts))
	for slug := range counts {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)
	items := make([]Item, 0, len(slugs))
	for _, slug := range slugs {
		items = append(items, Item{Slug: slug, Count: counts[slug]})
	}
	return items, nil
}
