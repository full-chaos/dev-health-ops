package docs_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
)

const resourceGrantsPage = "docs/reference/auth/resource-grants.md"

// resourceGrantsGuard holds the repository grants page to the grant package: the role table to grant.Roles() and the
// repository key pattern to grant.KeyPattern().
func resourceGrantsGuard(d docTree) []string {
	var out []string
	page, ok := readOrNote(&out, d, resourceGrantsPage)
	if !ok {
		return out
	}

	roles, found := sectionOf(page, "## Roles")
	if !found {
		out = append(out, resourceGrantsPage+": no ## Roles section")
	} else {
		var got []string
		for _, r := range mdTableRows(roles) {
			if len(r) != 1 {
				out = append(out, fmt.Sprintf("roles table: a row has %d cells, want 1", len(r)))
				continue
			}
			got = append(got, r[0])
		}
		out = append(out, diffSets("roles table", got, grant.Roles())...)
	}

	key, found := sectionOf(page, "## Repository key")
	if !found {
		return append(out, resourceGrantsPage+": no ## Repository key section")
	}
	var patterns []string
	for _, l := range fencedBlocks(key) {
		if l = strings.TrimSpace(l); l != "" {
			patterns = append(patterns, l)
		}
	}
	if len(patterns) != 1 || patterns[0] != grant.KeyPattern() {
		out = append(out, fmt.Sprintf("key pattern block %q, grant.KeyPattern %q", patterns, grant.KeyPattern()))
	}
	return out
}

func TestResourceGrantsPageMatchesTheGrantPackage(t *testing.T) {
	edit := func(old, repl string) func(*testing.T, docTree) docTree {
		return func(t *testing.T, d docTree) docTree { return d.edit(t, resourceGrantsPage, old, repl) }
	}
	assertGuard(t, resourceGrantsGuard, []plantCase{
		{"a role row is dropped", edit("| `admin` |\n", ""), "missing row"},
		{"a role the package refuses is added", edit("| `viewer` |\n", "| `viewer` |\n| `owner` |\n"), "not in the source"},
		{"the key pattern drifts", edit("{0,99}/[a-z0-9]", "{0,98}/[a-z0-9]"), "key pattern block"},
		{"the key pattern block is gone", edit("```text", "~~~text"), "key pattern block"},
		{"the page is gone", func(t *testing.T, d docTree) docTree { return d.removed(resourceGrantsPage) }, resourceGrantsPage},
	})
}
