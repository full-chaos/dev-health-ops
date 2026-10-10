package server

import (
	"encoding/json"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
	"sort"
	"strings"
	"testing"
)

// CHAOS-9111: the frozen Python bodies hold a percent of 0.0 where a window has no
// stored value; Go serves null there (a percent has no meaning against a value
// nobody measured). The venue oracles compare Go with the frozen Python bodies, so
// they restore the 0.0 in the Go body, ONLY where the Go body's own flags say a
// window holds no data, and only in the three percent keys of the wire. Everything
// else stays byte-for-byte: a null percent beside two true flags (or beside no
// flags), a number, any other key all still differ from Python.
//
// The edit is made on the text (a null literal becomes 0.0 at its own offset), so
// the order and formatting of every other byte is the writer's, which the Home
// ledger compares as text.

// restoredZero is the text each key held in the frozen Python bodies: REST writes a float
// (0.0), GraphQL an integral float as 0. The operating review's percent is not here: its
// no-data week is the existing CHAOS-8525 mask (graphql_edge_no_data_status_mask_test.go).
var restoredZero = map[string]string{"delta_pct": "0.0", "deltaPct": "0"}

var percentKeys = map[string]bool{"delta_pct": true, "deltaPct": true}

type jsonNode struct {
	start, end int
	object     bool
	fields     map[string]*jsonNode
	order      []string
	list       []*jsonNode
	scalar     string // raw text of a scalar
	key        string // the key that holds this node in its object
}

func parseJSONNode(text string) (*jsonNode, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	node, err := readJSONNode(dec, text)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func readJSONNode(dec *json.Decoder, text string) (*jsonNode, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	end := int(dec.InputOffset())
	switch v := tok.(type) {
	case json.Delim:
		if v == '{' {
			n := &jsonNode{object: true, fields: map[string]*jsonNode{}, start: end - 1}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key := kt.(string)
				child, err := readJSONNode(dec, text)
				if err != nil {
					return nil, err
				}
				n.fields[key] = child
				n.order = append(n.order, key)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			n.end = int(dec.InputOffset())
			return n, nil
		}
		n := &jsonNode{start: end - 1}
		for dec.More() {
			child, err := readJSONNode(dec, text)
			if err != nil {
				return nil, err
			}
			n.list = append(n.list, child)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		n.end = int(dec.InputOffset())
		return n, nil
	default:

		// the scalar's own text: from the end of the previous token to end
		startAt := end
		for startAt > 0 && !strings.ContainsRune(",:[{ \n\t", rune(text[startAt-1])) {
			startAt--
		}
		return &jsonNode{start: startAt, end: end, scalar: text[startAt:end]}, nil
	}
}

func boolField(n *jsonNode, names ...string) (value, ok bool) {
	for _, name := range names {
		if f, found := n.fields[name]; found && (f.scalar == "true" || f.scalar == "false") {
			return f.scalar == "true", true
		}
	}
	return false, false
}

// restoreZeroPercentWhereAWindowHasNoData returns the Go body with each null percent
// restored to 0.0 where its own flags say a window holds no data: the object holding
// the percent (or, for the operating review, the metric object above it) says
// has_data / hasData false, or the object says has_prior_data / hasPriorData false.
// It also reports how many it restored.
func restoreZeroPercentWhereAWindowHasNoData(body string) (string, int, error) {
	root, err := parseJSONNode(body)
	if err != nil {
		return "", 0, fmt.Errorf("not JSON: %w", err)
	}
	var edits []*jsonNode
	var walk func(n *jsonNode, parent *jsonNode)
	walk = func(n *jsonNode, parent *jsonNode) {
		if n.object {
			for _, key := range n.order {
				child := n.fields[key]
				child.key = key
				if percentKeys[key] && child.scalar == "null" {
					hasData, hasDataOK := boolField(n, "has_data", "hasData")
					if sp, ok := n.fields["spark"]; ok && !hasDataOK && sp.start+2 <= len(body) && body[sp.start:sp.end] == "[]" {
						// A document without the flags (the frozen Python documents have none): an
						// empty spark is the same fact. A window with data has at least one
						// non-null day in its series; none = no data in the current window.
						hasData, hasDataOK = false, true
					}
					hasPrior, hasPriorOK := boolField(n, "has_prior_data", "hasPriorData")
					parentHasData, parentOK := false, false
					if parent != nil {
						parentHasData, parentOK = boolField(parent, "has_data", "hasData")
					}
					if (hasDataOK && !hasData) || (hasPriorOK && !hasPrior) || (parentOK && !parentHasData) {
						edits = append(edits, child)
					}
				}
				walk(child, n)
			}
			return
		}
		for _, child := range n.list {
			walk(child, parent)
		}
	}
	walk(root, nil)
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	out := body
	for _, e := range edits {
		if out[e.start:e.end] != "null" {
			return "", 0, fmt.Errorf("offset %d is %q, not null", e.start, out[e.start:e.end])
		}
		out = out[:e.start] + restoredZero[e.key] + out[e.end:]
	}
	return out, len(edits), nil
}

func TestRestoreZeroPercentWhereAWindowHasNoDataIsNarrow(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
		count          int
	}{
		{"no current data", `{"delta_pct":null,"has_data":false,"has_prior_data":true}`, `{"delta_pct":0.0,"has_data":false,"has_prior_data":true}`, 1},
		{"no prior data", `{"delta_pct":null,"has_data":true,"has_prior_data":false}`, `{"delta_pct":0.0,"has_data":true,"has_prior_data":false}`, 1},
		{"both true: a null percent is NOT restored", `{"delta_pct":null,"has_data":true,"has_prior_data":true}`, `{"delta_pct":null,"has_data":true,"has_prior_data":true}`, 0},
		{"no flags: NOT restored", `{"delta_pct":null}`, `{"delta_pct":null}`, 0},
		{"a number is untouched", `{"delta_pct":7.5,"has_data":false,"has_prior_data":false}`, `{"delta_pct":7.5,"has_data":false,"has_prior_data":false}`, 0},
		{"another key is untouched", `{"value":null,"has_data":false}`, `{"value":null,"has_data":false}`, 0},
		{"each row by its own flags", `{"deltas":[{"deltaPct":null,"hasData":false,"hasPriorData":true},{"deltaPct":null,"hasData":true,"hasPriorData":true}]}`, `{"deltas":[{"deltaPct":0,"hasData":false,"hasPriorData":true},{"deltaPct":null,"hasData":true,"hasPriorData":true}]}`, 1},
		{"no flags, an empty spark: no data in the window", `{"delta_pct":null,"spark":[]}`, `{"delta_pct":0.0,"spark":[]}`, 1},
		{"no flags, a spark point: NOT restored", `{"delta_pct":null,"spark":[{"ts":"x","value":1}]}`, `{"delta_pct":null,"spark":[{"ts":"x","value":1}]}`, 0},
		{"spacing and escapes are untouched", "{\"a\":\"x\\u0026y\",\"delta_pct\":null, \"has_data\":false}", "{\"a\":\"x\\u0026y\",\"delta_pct\":0.0, \"has_data\":false}", 1},
	} {
		got, count, err := restoreZeroPercentWhereAWindowHasNoData(tc.in)
		if err != nil || got != tc.want || count != tc.count {
			t.Errorf("%s: got %s (%d, %v), want %s (%d)", tc.name, got, count, err, tc.want, tc.count)
		}
	}
	if _, _, err := restoreZeroPercentWhereAWindowHasNoData("{"); err == nil {
		t.Error("a body that is not JSON must fail")
	}
}

// venueGoBody is the Go body of a venue comparison with the null percents of a window
// without data restored to the 0.0 the frozen Python bodies hold. A body that is not
// JSON (an error text) is returned as it is.
func venueGoBody(body string) string {
	out, _, err := restoreZeroPercentWhereAWindowHasNoData(body)
	if err != nil {
		return body
	}
	return out
}

// nullPercentNormalize is the Normalize step of the GraphQL edge oracle's parity
// comparison (the Python bodies hold no null percent: it changes nothing there).
func nullPercentNormalize(_ venueoracle.Request, body string) string { return venueGoBody(body) }

func TestRestoreZeroPercentUsesTheKeysOwnFormat(t *testing.T) {
	got, _, err := restoreZeroPercentWhereAWindowHasNoData(`{"deltaPct":null,"spark":[],"x":{"delta_pct":null,"has_data":false}}`)
	if err != nil || got != `{"deltaPct":0,"spark":[],"x":{"delta_pct":0.0,"has_data":false}}` {
		t.Fatalf("got %s, %v", got, err)
	}
	// the operating review's percent is the CHAOS-8525 mask's, not this helper's
	if got, n, _ := restoreZeroPercentWhereAWindowHasNoData(`{"metrics":[{"hasData":false,"delta":{"percent":null,"hasPriorData":false}}]}`); n != 0 || strings.Contains(got, "percent\":0") {
		t.Fatalf("a percent of the operating review was restored: %s", got)
	}
}
