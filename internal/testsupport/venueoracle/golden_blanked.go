package venueoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
)

// A golden writes down what it does not compare. Every leaf a recording
// replaces by a placeholder (a token projected to its claims, a Volatile
// header, a generated id or run time the Scrub blanked) is a leaf the golden
// no longer holds by value. The header's "blanked" lists them by pattern, with
// how many were blanked; a replay that blanks a leaf of a pattern the recording
// did not fails; and a leaf a Scrub blanked whose raw value is the same in two
// recordings is deterministic, so the two-run check refuses it (the raw digests
// go to a sidecar, never a raw value).

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

// scrubEntry is the digest of the raw value of one leaf a Scrub blanked, by
// request, pattern and order within them.
type scrubEntry struct {
	key    string
	digest string
}

// noteBlanks records what the projection of one text replaced. stageA is the
// text after the token projection (and the Volatile and Allow rules, which are
// not Scrub), stageB after the spec's Scrub. Recording, it counts the patterns
// and keeps the digest of each raw value a Scrub blanked. Replaying, it
// reports a pattern the recording did not blank.
func (g *Golden) noteBlanks(request, kind, raw, stageA, stageB string) error {
	for _, leaf := range leafDiffs(kind, raw, stageA) {
		if err := g.noteBlank(request, leaf, false); err != nil {
			return err
		}
	}
	for _, leaf := range leafDiffs(kind, stageA, stageB) {
		if err := g.noteBlank(request, leaf, true); err != nil {
			return err
		}
	}
	return nil
}

func (g *Golden) noteBlank(request string, leaf blankedLeaf, scrubbed bool) error {
	if g.recording {
		if g.blankCounts == nil {
			g.blankCounts = map[string]int{}
			g.blankOrdinals = map[string]int{}
		}
		g.blankCounts[leaf.pattern]++
		if scrubbed {
			key := request + "|" + leaf.pattern
			g.blankOrdinals[key]++
			sum := sha256.Sum256([]byte(leaf.raw))
			g.scrubEntries = append(g.scrubEntries, scrubEntry{key: fmt.Sprintf("%s|%d", key, g.blankOrdinals[key]), digest: hex.EncodeToString(sum[:])})
		}
		return nil
	}
	if g.loaded.Header.Blanked == nil {
		return nil // recorded before a golden listed what it blanks
	}
	if _, listed := g.loaded.Header.Blanked[leaf.pattern]; !listed {
		return fmt.Errorf("golden %s: the Go plane's %q is blanked on replay (request %q), and the recording blanked no leaf of that pattern: a path that stopped being compared is a path the golden must list; regenerate: %s", g.spec.Path, leaf.pattern, request, g.spec.Recipe)
	}
	return nil
}

// blankedHeader is what the candidate's header lists.
func (g *Golden) blankedHeader() map[string]int {
	out := make(map[string]int, len(g.blankCounts))
	for pattern, count := range g.blankCounts {
		out[pattern] = count
	}
	return out
}

// scrubSidecarSuffix is appended to a candidate's path for the file of raw
// digests the two-run check reads.
const scrubSidecarSuffix = ".raw"

// writeScrubSidecar writes the digests of the raw values the Scrub blanked.
// Digests only: no raw value is ever written.
func (g *Golden) writeScrubSidecar(candidate string) error {
	entries := map[string]string{}
	for _, entry := range g.scrubEntries {
		entries[entry.key] = entry.digest
	}
	raw, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(candidate+scrubSidecarSuffix, append(raw, '\n'), 0o644)
}
