package teamscope

import (
	"context"
	"fmt"
	"slices"
	"strings"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// Binding names of the repository ceiling.
const (
	BindingCeilingIDs  = "ceiling_ids"
	BindingCeilingKeys = "ceiling_keys"
)

// Ceiling is the repositories one request may read (CHAOS-13090). The zero value reads every repository. A restricted
// ceiling with no id reads none.
type Ceiling struct {
	restricted bool
	ids        []string
}

// NoCeiling reads every repository. It is the explicit form for a read that is not narrowed; a census lists the files
// that may call it.
func NoCeiling() Ceiling { return Ceiling{} }

// Restricted reports a repository ceiling.
func (c Ceiling) Restricted() bool { return c.restricted }

// IDs is a copy of the repository ids the ceiling allows.
func (c Ceiling) IDs() []string { return slices.Clone(c.ids) }

// RepoTerm is the " AND ..." fragment that holds column to the ceiling: none when unrestricted, " AND 1 = 0" when the
// ceiling allows no repository, else a membership test over the bound ids.
func (c Ceiling) RepoTerm(column string) (string, []dhclickhouse.Binding) {
	switch {
	case !c.restricted:
		return "", nil
	case len(c.ids) == 0:
		return " AND 1 = 0", nil
	}
	return " AND " + c.membership(column), c.bindings()
}

func (c Ceiling) membership(column string) string {
	return column + " IN {" + BindingCeilingIDs + ":Array(String)}"
}

func (c Ceiling) bindings() []dhclickhouse.Binding {
	return []dhclickhouse.Binding{{Name: BindingCeilingIDs, Value: c.IDs()}}
}

// matcherStatement resolves allowlist keys (a lower-case path, no provider) to repository ids: a key names a
// repository by its current name or by its identity path (the name a rename left behind). One statement per request,
// the organization and the keys bound.
const matcherStatement = `SELECT path, groupUniqArray(toString(id)) AS ids
FROM (
    SELECT lower(repo) AS path, id FROM repos FINAL WHERE org_id = {org_id:String}
    UNION ALL
    SELECT lower(identity_path) AS path, id FROM repos FINAL WHERE org_id = {org_id:String} AND identity_path != ''
)
WHERE path IN {` + BindingCeilingKeys + `:Array(String)}
GROUP BY path`

// ResolveCeiling is the ceiling of a decision scope. Legacy and Organization read every repository, with no
// statement. No scope and an empty allowlist read none, with no statement. An allowlist runs ONE matcher statement;
// a key resolves iff it names exactly one repository id: no repository, or two or more (the same path on two
// providers, a name taken again after a rename), matches nothing (acp Q2).
func ResolveCeiling(ctx context.Context, q RowQuerier, scope pepgate.Scope) (Ceiling, error) {
	switch scope.Kind() {
	case pepgate.KindLegacy, pepgate.KindOrganization:
		return NoCeiling(), nil
	case pepgate.KindAllowlist:
	default:
		return Ceiling{restricted: true}, nil
	}
	org, _ := scope.OrgID()
	var keys []string
	for _, key := range scope.RepoKeys() {
		if key = strings.ToLower(strings.TrimSpace(key)); key != "" {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	if len(keys) == 0 {
		return Ceiling{restricted: true}, nil
	}
	rows, err := q.Query(ctx, matcherStatement, []dhclickhouse.Binding{
		{Name: "org_id", Value: org},
		{Name: BindingCeilingKeys, Value: keys},
	})
	if err != nil {
		return Ceiling{}, fmt.Errorf("resolve repository ceiling: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var path string
		var matched []string
		if err := rows.Scan(&path, &matched); err != nil {
			return Ceiling{}, fmt.Errorf("scan repository ceiling row: %w", err)
		}
		slices.Sort(matched)
		if matched = slices.Compact(matched); len(matched) == 1 {
			ids = append(ids, matched[0])
		}
	}
	if err := rows.Err(); err != nil {
		return Ceiling{}, fmt.Errorf("iterate repository ceiling rows: %w", err)
	}
	slices.Sort(ids)
	return Ceiling{restricted: true, ids: slices.Compact(ids)}, nil
}
