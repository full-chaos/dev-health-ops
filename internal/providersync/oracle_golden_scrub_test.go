package providersync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// The Python producers stamp a few fields from the wall clock or from uuid4.
// Such a value differs on every run, so it is no evidence and it would make a
// golden differ from one recording to the next. A golden stores a typed
// placeholder there instead: the leaf keeps its place and its type tag, and its
// value is oraclePerRunValue. Go never compares these fields with Python: each
// is one of the pair's own excluded_fields, or the sink column the comparison
// skips by name.

// oraclePerRunValue is the value a golden stores in a per-run leaf.
const oraclePerRunValue = "per-run value"

// oraclePerRunFields names, by pair, the row fields the Python producer makes
// anew on every run. Each must be one of that pair's excluded_fields
// (perRunFieldsErr): a placeholder is never compared. A per-run field that is
// missing here fails the recording, which runs the producer twice.
var oraclePerRunFields = map[string][]string{
	"github/work-items/ai-attribution": {"ingested_at", "record_id"},
	"github/work-items/dependency":     {"last_synced"},
	"github/work-items/interaction":    {"last_synced"},
	"github/work-items/reopen":         {"last_synced"},
	"github/work-items/sprint":         {"last_synced"},
	"gitlab/security/dependency":       {"created_at"},
	"gitlab/work-items/ai-attribution": {"ingested_at", "record_id"},
	"linear/work-items/ai-attribution": {"ingested_at", "record_id"},
	"linear/work-items/dependency":     {"last_synced"},
	"linear/work-items/interaction":    {"last_synced"},
	"linear/work-items/reopen":         {"last_synced"},
	"linear/work-items/sprint":         {"last_synced"},
}

// pairPerRunScrub is the GoldenSpec.Scrub of a pair golden: nil for a pair with
// no per-run field, else the function that puts the placeholder in every
// cases[i].row.<field>.v of the answer. Text that is not such an answer is
// returned as it is.
func pairPerRunScrub(pairID string) func(text string) string {
	fields := oraclePerRunFields[pairID]
	if len(fields) == 0 {
		return nil
	}
	perRun := map[string]bool{}
	for _, field := range fields {
		perRun[field] = true
	}
	return func(text string) string {
		scrubbed, err := replaceJSONStringValues(text, oraclePerRunValue, func(path []string) bool {
			return len(path) == 5 && path[0] == "cases" && path[2] == "row" && perRun[path[3]] && path[4] == "v"
		})
		if err != nil {
			return text
		}
		return scrubbed
	}
}

// perRunFieldsErr is an error when a per-run field of pairID is compared (it is
// not one of the answer's excluded_fields), is in no row of the answer (a stale
// entry), or holds anything but a typed leaf with the placeholder.
func perRunFieldsErr(pairID string, output []byte) error {
	fields := oraclePerRunFields[pairID]
	if len(fields) == 0 {
		return nil
	}
	var answer struct {
		Cases []struct {
			ID  string                     `json:"id"`
			Row map[string]json.RawMessage `json:"row"`
		} `json:"cases"`
		ExcludedFields map[string]string `json:"excluded_fields"`
	}
	if err := json.Unmarshal(output, &answer); err != nil {
		return fmt.Errorf("pair %q: decode the answer: %w", pairID, err)
	}
	for _, field := range fields {
		if _, excluded := answer.ExcludedFields[field]; !excluded {
			return fmt.Errorf("pair %q stores a placeholder for the per-run field %q, but the pair compares that field (it is not in excluded_fields): a placeholder is no evidence", pairID, field)
		}
		present := false
		for _, entry := range answer.Cases {
			raw, has := entry.Row[field]
			if !has || string(bytes.TrimSpace(raw)) == "null" {
				continue
			}
			present = true
			var leaf struct {
				Tag   string  `json:"t"`
				Value *string `json:"v"`
			}
			// A value that is not a typed leaf leaves leaf empty.
			_ = json.Unmarshal(raw, &leaf)
			if leaf.Tag == "" || leaf.Value == nil || *leaf.Value != oraclePerRunValue {
				return fmt.Errorf("pair %q case %q: the per-run field %q holds %s, want a typed leaf with the value %q", pairID, entry.ID, field, raw, oraclePerRunValue)
			}
		}
		if !present {
			return fmt.Errorf("pair %q: the per-run field %q is in no row of the answer: a stale entry of oraclePerRunFields", pairID, field)
		}
	}
	return nil
}

// sinkPerRunColumn reports whether the Python sink stamps column of caseID from
// the wall clock inside the writer: last_synced on the work-item and the
// transition destinations.
func sinkPerRunColumn(caseID, column string) bool {
	return column == "last_synced" && (strings.HasPrefix(caseID, "work_items") || caseID == "work_item_transitions")
}

// scriptPerRunScrub is the GoldenSpec.Scrub of a script oracle's golden: the
// sink scrub for the work-item sink, nil for an oracle with no per-run value.
func scriptPerRunScrub(oracleName string) func(text string) string {
	if oracleName == "work-item-sink" {
		return sinkPerRunScrub
	}
	return nil
}

// perRunSinkValueErr is an error unless the frozen Python value of a per-run
// sink column is a datetime leaf that holds the placeholder, and the Go value
// is a datetime.
func perRunSinkValueErr(pythonValue json.RawMessage, goRendered string) error {
	var leaf struct {
		Tag   string  `json:"t"`
		Value *string `json:"v"`
	}
	// A value that is not a typed leaf leaves leaf empty.
	_ = json.Unmarshal(pythonValue, &leaf)
	if leaf.Tag != "datetime" || leaf.Value == nil || *leaf.Value != oraclePerRunValue {
		return fmt.Errorf("the frozen Python value is %s, want a datetime leaf with the value %q", pythonValue, oraclePerRunValue)
	}
	if !strings.HasPrefix(goRendered, "datetime:") {
		return fmt.Errorf("expected a datetime on the Go side, go=%s", goRendered)
	}
	return nil
}

// sinkPerRunScrub is the GoldenSpec.Scrub of a work-item sink golden: it puts
// the placeholder in every value of a per-run column (sinkPerRunColumn). Text
// that is not a sink answer is returned as it is.
func sinkPerRunScrub(text string) string {
	var answer struct {
		Cases []struct {
			ID          string   `json:"id"`
			ColumnNames []string `json:"column_names"`
		} `json:"cases"`
	}
	if err := json.Unmarshal([]byte(text), &answer); err != nil {
		return text
	}
	scrubbed, err := replaceJSONStringValues(text, oraclePerRunValue, func(path []string) bool {
		if len(path) != 6 || path[0] != "cases" || path[2] != "rows" || path[5] != "v" {
			return false
		}
		caseIndex, caseErr := strconv.Atoi(path[1])
		column, columnErr := strconv.Atoi(path[4])
		if caseErr != nil || columnErr != nil || caseIndex >= len(answer.Cases) || column >= len(answer.Cases[caseIndex].ColumnNames) {
			return false
		}
		return sinkPerRunColumn(answer.Cases[caseIndex].ID, answer.Cases[caseIndex].ColumnNames[column])
	})
	if err != nil {
		return text
	}
	return scrubbed
}

// replaceJSONStringValues returns the JSON document text with every string
// value whose path match accepts replaced by replacement. A path is the object
// keys and the array indexes (in decimal) from the root to the value. Every
// other byte of text is kept: the answer stays the producer's own text.
func replaceJSONStringValues(text, replacement string, match func(path []string) bool) (string, error) {
	quoted, err := json.Marshal(replacement)
	if err != nil {
		return "", err
	}
	type frame struct {
		object    bool
		key       string
		expectKey bool
		index     int
	}
	var stack []frame
	path := func() []string {
		steps := make([]string, len(stack))
		for position, level := range stack {
			if level.object {
				steps[position] = level.key
			} else {
				steps[position] = strconv.Itoa(level.index)
			}
		}
		return steps
	}
	valueDone := func() {
		if len(stack) == 0 {
			return
		}
		top := &stack[len(stack)-1]
		if top.object {
			top.expectKey = true
		} else {
			top.index++
		}
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var out strings.Builder
	copied := 0
	for {
		before := int(decoder.InputOffset())
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		after := int(decoder.InputOffset())
		switch value := token.(type) {
		case json.Delim:
			if value == '{' || value == '[' {
				stack = append(stack, frame{object: value == '{', expectKey: true})
				continue
			}
			stack = stack[:len(stack)-1]
			valueDone()
		case string:
			if len(stack) > 0 && stack[len(stack)-1].object && stack[len(stack)-1].expectKey {
				stack[len(stack)-1].key, stack[len(stack)-1].expectKey = value, false
				continue
			}
			if match(path()) {
				start := before + strings.IndexByte(text[before:after], '"')
				out.WriteString(text[copied:start])
				out.Write(quoted)
				copied = after
			}
			valueDone()
		default:
			valueDone()
		}
	}
	if len(stack) != 0 {
		return "", errors.New("the JSON document ends inside a value")
	}
	out.WriteString(text[copied:])
	return out.String(), nil
}

// sortedPerRunPairs is the pairs of oraclePerRunFields in order.
func sortedPerRunPairs() []string {
	pairs := make([]string, 0, len(oraclePerRunFields))
	for pair := range oraclePerRunFields {
		pairs = append(pairs, pair)
	}
	sort.Strings(pairs)
	return pairs
}
