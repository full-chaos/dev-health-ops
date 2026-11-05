package venueoracle

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

// A golden writes down what it does not compare. Every leaf a recording
// replaces by a placeholder (a token projected to its claims, a Volatile
// header, a generated id or run time the Scrub blanked) is a leaf the golden
// no longer holds by value. The header's "blanked" lists them by pattern, with
// how many were blanked; a replay that blanks a leaf of a pattern the recording
// did not fails.

// blankedLeaf is one leaf a projection replaced.
type blankedLeaf struct {
	pattern string
	raw     string
}

var indexSegment = regexp.MustCompile(`\[\d+\]`)

// leafDiffs lists the leaves of before that after replaced. kind says where the
// text sits ("body", "header x-request-id", "rows mail"). JSON texts of one
// shape are compared leaf by leaf and a leaf is named by its path without
// array indexes; any other text is one leaf, "<kind> text".
func leafDiffs(kind, before, after string) []blankedLeaf {
	if before == after {
		return nil
	}
	var left, right any
	if decodeJSON(before, &left) && decodeJSON(after, &right) {
		var out []blankedLeaf
		if walkLeaves(left, right, "$", kind, &out) {
			return out
		}
	}
	return []blankedLeaf{{pattern: kind + " text", raw: before}}
}

func decodeJSON(text string, into *any) bool {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if decoder.Decode(into) != nil {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

// walkLeaves reports false when the two values differ in shape.
func walkLeaves(a, b any, path, kind string, out *[]blankedLeaf) bool {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			yv, ok := y[k]
			if !ok || !walkLeaves(x[k], yv, path+"."+k, kind, out) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !walkLeaves(x[i], y[i], path+"[]", kind, out) {
				return false
			}
		}
		return true
	case string:
		y, ok := b.(string)
		if !ok {
			return false
		}
		if x != y {
			*out = append(*out, blankedLeaf{pattern: kind + " " + indexSegment.ReplaceAllString(path, "[]"), raw: x})
		}
		return true
	default:
		return fmt.Sprintf("%T:%v", a, a) == fmt.Sprintf("%T:%v", b, b)
	}
}

// noteBlanks records what the projection of one text replaced. stageA is the
// text after the token projection (and the Volatile and Allow rules, which are
// not Scrub), stageB after the spec's Scrub. It reports a pattern the
// recording did not blank.
func (g *Golden) noteBlanks(request, kind, raw, stageA, stageB string) error {
	for _, leaf := range leafDiffs(kind, raw, stageA) {
		if err := g.noteBlank(request, leaf); err != nil {
			return err
		}
	}
	for _, leaf := range leafDiffs(kind, stageA, stageB) {
		if err := g.noteBlank(request, leaf); err != nil {
			return err
		}
	}
	return nil
}

func (g *Golden) noteBlank(request string, leaf blankedLeaf) error {
	if g.loaded.Header.Blanked == nil {
		return nil // recorded before a golden listed what it blanks
	}
	if _, listed := g.loaded.Header.Blanked[leaf.pattern]; listed {
		return nil
	}
	if _, declared := g.spec.GoBlanked[leaf.pattern]; declared {
		if g.goBlankedSeen == nil {
			g.goBlankedSeen = map[string]bool{}
		}
		g.goBlankedSeen[leaf.pattern] = true
		return nil
	}
	return fmt.Errorf("golden %s: the Go plane's %q is blanked on replay (request %q), and the recording blanked no leaf of that pattern: a path that stopped being compared is a path the golden must list (or the test's GoldenSpec.GoBlanked, with its reason)", g.spec.Path, leaf.pattern, request)
}

// goBlankedErr refuses a GoldenSpec.GoBlanked that is not one exact pattern per entry, with a reason, that the
// recording did not list, on a golden whose header lists what it blanks.
func (g *Golden) goBlankedErr() error {
	if len(g.spec.GoBlanked) == 0 {
		return nil
	}
	if g.loaded.Header.Blanked == nil {
		return fmt.Errorf("golden %s lists no blanked leaves, so nothing is checked against them: GoldenSpec.GoBlanked declares nothing on it", g.spec.Path)
	}
	patterns := make([]string, 0, len(g.spec.GoBlanked))
	for pattern := range g.spec.GoBlanked {
		patterns = append(patterns, pattern)
	}
	sort.Strings(patterns)
	for _, pattern := range patterns {
		switch {
		case strings.TrimSpace(g.spec.GoBlanked[pattern]) == "":
			return fmt.Errorf("golden %s: GoldenSpec.GoBlanked %q has no reason: a path that stops being compared says why its value is not stable", g.spec.Path, pattern)
		case strings.ContainsAny(pattern, "*?"):
			return fmt.Errorf("golden %s: GoldenSpec.GoBlanked %q is a glob: one exact path each", g.spec.Path, pattern)
		}
		if _, listed := g.loaded.Header.Blanked[pattern]; listed {
			return fmt.Errorf("golden %s: GoldenSpec.GoBlanked %q: the recording's header already lists it", g.spec.Path, pattern)
		}
	}
	return nil
}

// unusedGoBlanked is an error naming each GoldenSpec.GoBlanked pattern that blanked no leaf: a declaration that
// covers nothing is stale.
func (g *Golden) unusedGoBlanked() error {
	var unused []string
	for pattern := range g.spec.GoBlanked {
		if !g.goBlankedSeen[pattern] {
			unused = append(unused, pattern)
		}
	}
	if len(unused) == 0 {
		return nil
	}
	sort.Strings(unused)
	return fmt.Errorf("golden %s: GoldenSpec.GoBlanked declares %s, but no Go answer blanked a leaf of it: drop the declaration", g.spec.Path, strings.Join(unused, ", "))
}
