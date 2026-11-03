// Package projectinventory is an organization's project inventory: the
// (provider, id) pair of every row of the ClickHouse projects table for the
// organization. It is the one definition of that list. go-api's ACR access
// read (internal/apiservice/acr) serves it as project_scope for an
// organization with no repository inventory (CHAOS-12224; acr derives the
// canonical project.v2 identity from each pair, design CHAOS-12107 v3 F6).
//
// The pairs are the projects row's own provider and id, as stored: no
// lowercasing, no trimming, no Ops encoding. Ids are compared exactly and
// case-sensitively downstream, so a transformed id would name a different
// project. A row with an empty provider or id is not a project and is left
// out (acr refuses the whole access read on an empty pair).
package projectinventory

import (
	"errors"
	"fmt"
	"sort"
)

// Limit is the most projects an inventory may hold. A larger inventory is
// refused (ErrOverLimit), never truncated, so a scope reader never sees a
// partial project list. It matches repoinventory.Limit.
const Limit = 100

// QueryLimit is the row limit the inventory read binds as limit_n: one more
// than Limit, so an inventory above Limit is seen and refused.
const QueryLimit = Limit + 1

// ErrOverLimit is an inventory with more than Limit projects.
var ErrOverLimit = errors.New("projectinventory: project inventory exceeds the supported scope limit")

// Statement is the inventory read: the distinct (provider, id) of every
// projects row of the organization with a non-empty provider and id, ordered
// by provider then id. It binds {org_id:String} and {limit_n:UInt32}
// (QueryLimit); settings is the trailing SETTINGS clause.
func Statement(settings string) string {
	return fmt.Sprintf(`
SELECT DISTINCT
    provider,
    id
FROM projects FINAL
WHERE org_id = {org_id:String}
  AND provider != ''
  AND id != ''
ORDER BY provider, id
LIMIT {limit_n:UInt32}
%s
`, settings)
}

// Item is one project of the inventory: the projects row's provider and id.
type Item struct {
	Provider string
	ID       string
}

// Canonical drops a pair with an empty provider or id, merges duplicates,
// refuses an inventory above Limit (ErrOverLimit) and returns the pairs
// sorted by provider then id. An empty inventory is a non-nil empty slice.
// It never changes a provider or an id.
func Canonical(rows []Item) ([]Item, error) {
	seen := map[Item]bool{}
	out := make([]Item, 0, len(rows))
	for _, row := range rows {
		if row.Provider == "" || row.ID == "" || seen[row] {
			continue
		}
		seen[row] = true
		out = append(out, row)
	}
	if len(out) > Limit {
		return nil, ErrOverLimit
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Provider != out[j].Provider {
			return out[i].Provider < out[j].Provider
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}
