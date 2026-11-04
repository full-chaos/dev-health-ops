// Package cells is the one table of repository keys every ops key validator is
// held to: the grant store, the decision point, the request decoder, the
// provisioning vocabulary and the published schemas. It is test support and
// imports nothing.
package cells

import "strings"

// Cell is one repository key and whether the canonical lowercase form accepts it.
type Cell struct {
	Name  string
	Key   string
	Valid bool
}

// Table is the cell table of the nested repository key (2-20 segments, at most
// 255 bytes). The literal "*" is a key too; it is not in this table.
func Table() []Cell {
	segment := func(n int) string { return strings.Repeat("a", n) }
	return []Cell{
		{"two segments", "acme/web", true},
		{"three segments", "acme/grp/web", true},
		{"twenty segments", strings.Repeat("g/", 19) + "x", true},
		{"255 bytes", segment(100) + "/" + segment(100) + "/" + segment(53), true},
		{"100-character segments", segment(100) + "/" + segment(100), true},
		{"dot-leading last segment", "acme/grp/.github", true},
		{"dots and dashes inside", "acme/g.r_p-1/we.b_2-x", true},
		{"21 segments", strings.Repeat("g/", 20) + "x", false},
		{"256 bytes", segment(100) + "/" + segment(100) + "/" + segment(54), false},
		{"uppercase", "Acme/Grp/Web", false},
		{"dot segment", "acme/./web", false},
		{"dot-dot segment", "acme/../web", false},
		{"dot-dot last", "acme/grp/..", false},
		{"dot last", "acme/grp/.", false},
		{"leading dot group", "acme/.grp/web", false},
		{"leading dot owner", ".acme/web", false},
		{"group ends dot", "acme/grp./web", false},
		{"group starts hyphen", "acme/-grp/web", false},
		{"repo starts hyphen", "acme/-web", false},
		{"repo ends underscore", "acme/web_", false},
		{"101-character group", "acme/" + segment(101) + "/web", false},
		{"101-character last", "acme/" + segment(101), false},
		{"one segment", "acme", false},
		{"empty", "", false},
		{"empty segment", "acme//web", false},
		{"trailing slash", "acme/grp/", false},
		{"leading slash", "/acme/web", false},
		{"owner wildcard", "acme/*", false},
		{"group wildcard", "acme/*/web", false},
		{"space", "acme/g rp/web", false},
		{"newline", "acme/web\n", false},
		{"non-ASCII letter", "acme/wéb", false},
		{"kelvin sign", "acme/Keep", false},
	}
}
