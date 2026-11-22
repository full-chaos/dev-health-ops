// Package repoinventory is an organization's repository inventory: the
// canonical repository slugs in the ClickHouse repos table, in the Auth
// Control Plane's repository key grammar (internal/auth/repokey) without its
// "*" token, so a GitLab path in nested groups is a repository too. It is the one
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

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey"
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

// SlugPattern is repokey.SchemaPattern without its "*" arm: the inventory
// never holds the organization-wide token as a repository. It has no quote and
// no backslash, so it is one ClickHouse string literal as written.
var SlugPattern = slugPattern()

var slugRegexp = regexp.MustCompile(SlugPattern)

func slugPattern() string {
	const starArm = `|\*)$`
	if !strings.HasSuffix(repokey.SchemaPattern, starArm) {
		panic("repoinventory: repokey.SchemaPattern does not end with its \"*\" arm")
	}
	return strings.TrimSuffix(repokey.SchemaPattern, starArm) + `)$`
}

// validSlug is the grammar Statement and ExistsStatement apply in SQL.
func validSlug(value string) bool {
	return len(value) <= repokey.MaxBytes && slugRegexp.MatchString(value)
}

// Statement is the inventory read: the lowercased, trimmed repo of every
// repos row of the organization, kept only when it is a well-formed slug,
// counted per slug and ordered by slug. A slug is 2-20 segments in at most
// repokey.MaxBytes bytes (SlugPattern). It binds {org_id:String} and
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
WHERE length(canonical_repo) <= %d
  AND match(
    canonical_repo,
    '%s'
  )
GROUP BY canonical_repo
ORDER BY value
LIMIT {limit_n:UInt32}
%s
`, repokey.MaxBytes, SlugPattern, settings)
}

// ExistsStatement is the existence probe: one row when the organization has at
// least one repos row whose lowercased, trimmed repo is a well-formed slug (the
// rows Statement counts), else none. LIMIT 1: it reads no list and holds no
// Limit, so an organization of any size answers. It binds {org_id:String};
// settings is the trailing SETTINGS clause.
func ExistsStatement(settings string) string {
	return fmt.Sprintf(`
SELECT 1 AS present
FROM repos FINAL
WHERE org_id = {org_id:String}
  AND length(lowerUTF8(trimBoth(repo))) <= %d
  AND match(
    lowerUTF8(trimBoth(repo)),
    '%s'
  )
LIMIT 1
%s
`, repokey.MaxBytes, SlugPattern, settings)
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

// Canonical trims and lowercases each value, keeps only well-formed slugs (the
// grammar of Statement), merges duplicates by summing their counts, refuses an
// inventory above Limit (ErrOverLimit) and returns the slugs sorted. An empty
// inventory is a non-nil empty slice.
func Canonical(rows []Row) ([]Item, error) {
	counts := map[string]uint64{}
	for _, row := range rows {
		value := strings.ToLower(strings.TrimSpace(row.Value))
		if !validSlug(value) {
			continue
		}
		counts[value] += row.Count
	}
	if len(counts) > Limit {
		return nil, fmt.Errorf("%w: %d repositories", ErrOverLimit, len(counts))
	}
	if len(counts) == 0 {
		return []Item{}, nil
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
