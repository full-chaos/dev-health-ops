package providersync

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestReplaceJSONStringValuesKeepsEveryOtherByte pins the splice the scrubs
// are built on: only the matched string values change, and spacing, escapes,
// numbers and key order stay the producer's.
func TestReplaceJSONStringValuesKeepsEveryOtherByte(t *testing.T) {
	text := `{"a": {"v": "one", "n": 1.50, "s": "q\"\u00e9"},  "list": [ "x", {"v": "two"}, ["y", "z"] ], "v": "top"}`
	got, err := replaceJSONStringValues(text, "P", func(path []string) bool {
		return strings.Join(path, "/") == "a/v" || strings.Join(path, "/") == "list/1/v" || strings.Join(path, "/") == "list/2/1"
	})
	want := `{"a": {"v": "P", "n": 1.50, "s": "q\"\u00e9"},  "list": [ "x", {"v": "P"}, ["y", "P"] ], "v": "top"}`
	if err != nil || got != want {
		t.Fatalf("got  %s (%v)\nwant %s", got, err, want)
	}
	again, err := replaceJSONStringValues(got, "P", func(path []string) bool { return path[len(path)-1] == "v" && len(path) > 1 })
	if err != nil || again != got {
		t.Fatalf("a second pass changed the text: %s (%v)", again, err)
	}
	unchanged, err := replaceJSONStringValues(text, "P", func([]string) bool { return false })
	if err != nil || unchanged != text {
		t.Fatalf("no match changed the text: %s (%v)", unchanged, err)
	}
	// A key is never a value, and a value that is not a string is never replaced.
	keys, err := replaceJSONStringValues(`{"v": 1, "k": {"v": null}}`, "P", func([]string) bool { return true })
	if err != nil || keys != `{"v": 1, "k": {"v": null}}` {
		t.Fatalf("a key or a non-string value was replaced: %s (%v)", keys, err)
	}
	for name, malformed := range map[string]string{"cut short": `{"a": ["x"`, "not JSON": `Traceback (most recent call last)`} {
		if _, err := replaceJSONStringValues(malformed, "P", func([]string) bool { return true }); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

const perRunPairAnswer = `{"cases": [{"id": "one", "row": {"key": {"t": "str", "v": "last_synced"}, "last_synced": {"t": "datetime", "v": "2026-10-01T03:58:04.786071Z"}, "nested": {"last_synced": {"t": "str", "v": "kept"}}, "gone": null}, "meta": {"last_synced": {"t": "str", "v": "kept"}}}, ` +
	`{"id": "two", "row": {"last_synced": null, "key": {"t": "str", "v": "b"}}}], "excluded_fields": {"last_synced": "the constructor stamps wall-clock now"}, "shadow": [{"row": {"last_synced": {"t": "str", "v": "kept"}}}]}`

// TestThePairScrubStoresAPlaceholderInThePerRunFieldsOnly pins the pair scrub:
// the value of a per-run field of a row becomes the placeholder and keeps its
// type tag; a field of the same name anywhere else, the reason text and every
// other value stay; a second pass changes nothing.
func TestThePairScrubStoresAPlaceholderInThePerRunFieldsOnly(t *testing.T) {
	scrub := pairPerRunScrub("github/work-items/dependency")
	if scrub == nil {
		t.Fatal("the pair has no scrub")
	}
	got := scrub(perRunPairAnswer)
	want := strings.Replace(perRunPairAnswer, `"2026-10-01T03:58:04.786071Z"`, `"`+oraclePerRunValue+`"`, 1)
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if again := scrub(got); again != got {
		t.Fatalf("a second pass changed the answer: %s", again)
	}
	if text := "Traceback (most recent call last)"; scrub(text) != text {
		t.Error("text that is not an answer was changed")
	}
	// Only the value of the leaf itself is a per-run value: a string deeper
	// under it, and a case that is no object, stay.
	for _, text := range []string{
		`{"cases": [{"id": "one", "row": {"last_synced": {"t": "list", "v": ["kept"]}}}], "excluded_fields": {"last_synced": "r"}}`,
		`{"cases": ["last_synced"], "excluded_fields": {}}`,
	} {
		if got := scrub(text); got != text {
			t.Errorf("scrub(%s) = %s", text, got)
		}
	}
	if pairPerRunScrub("github/prs/row") != nil {
		t.Error("a pair with no per-run field has a scrub")
	}
	if err := perRunFieldsErr("github/work-items/dependency", []byte(got)); err != nil {
		t.Errorf("the scrubbed answer was refused: %v", err)
	}
}

// TestAPerRunFieldMustBeExcludedPresentAndAPlaceholder pins perRunFieldsErr on
// each of its refusals.
func TestAPerRunFieldMustBeExcludedPresentAndAPlaceholder(t *testing.T) {
	const pair = "github/work-items/dependency"
	leaf := `{"t": "datetime", "v": "` + oraclePerRunValue + `"}`
	answer := func(row, excluded string) []byte {
		return []byte(`{"cases": [{"id": "one", "row": {` + row + `}}], "excluded_fields": {` + excluded + `}}`)
	}
	if err := perRunFieldsErr(pair, answer(`"last_synced": `+leaf, `"last_synced": "wall clock"`)); err != nil {
		t.Fatalf("a placeholder in an excluded field was refused: %v", err)
	}
	if err := perRunFieldsErr("github/prs/row", answer(`"state": {"t": "str", "v": "open"}`, ``)); err != nil {
		t.Fatalf("a pair with no per-run field was refused: %v", err)
	}
	refused := map[string][]byte{
		"a per-run field that is compared":      answer(`"last_synced": `+leaf, ``),
		"a per-run field in no row":             answer(`"state": {"t": "str", "v": "open"}`, `"last_synced": "wall clock"`),
		"a per-run field that is only null":     answer(`"last_synced": null`, `"last_synced": "wall clock"`),
		"a per-run field with the clock value":  answer(`"last_synced": {"t": "datetime", "v": "2026-10-01T03:58:04Z"}`, `"last_synced": "wall clock"`),
		"a per-run field with no value":         answer(`"last_synced": {"t": "datetime"}`, `"last_synced": "wall clock"`),
		"a per-run field without a type tag":    answer(`"last_synced": {"v": "`+oraclePerRunValue+`"}`, `"last_synced": "wall clock"`),
		"a per-run field that is a bare string": answer(`"last_synced": "`+oraclePerRunValue+`"`, `"last_synced": "wall clock"`),
		"an answer that is not JSON":            []byte("Traceback"),
	}
	for name, output := range refused {
		if err := perRunFieldsErr(pair, output); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestEveryPerRunPairHasAGolden keeps oraclePerRunFields honest: each pair in
// it is a pair this package freezes.
func TestEveryPerRunPairHasAGolden(t *testing.T) {
	for _, pair := range sortedPerRunPairs() {
		prefix := strings.ReplaceAll(pair, "/", "_") + "."
		found := false
		for name := range oraclePairGoldenPins {
			if strings.HasPrefix(name, prefix) {
				found = true
			}
		}
		if !found {
			t.Errorf("oraclePerRunFields names the pair %q, which has no golden (looked for %s*)", pair, prefix)
		}
	}
}

const perRunSinkAnswer = `{"cases": [{"id": "work_items_single", "table": "work_items", "column_names": ["id", "last_synced", "title"], "shadow": [[null, {"t": "str", "v": "kept"}]], "rows": [[{"t": "str", "v": "a"}, {"t": "datetime", "v": "2026-10-01T04:11:02.577380Z"}, {"t": "str", "v": "last_synced"}], [{"t": "str", "v": "b"}, {"t": "datetime", "v": "2026-10-01T04:11:02.577549Z"}, null]]}, ` +
	`{"id": "sprints", "table": "sprints", "column_names": ["id", "last_synced"], "rows": [[{"t": "str", "v": "s"}, {"t": "datetime", "v": "2026-08-01T00:00:00Z"}]]}], ` +
	`"other": {"0": {"rows": [[null, {"t": "str", "v": "kept"}]]}}}`

// TestTheSinkScrubStoresAPlaceholderInThePerRunColumnsOnly pins the sink scrub:
// last_synced of a work-item destination becomes the placeholder in every row;
// last_synced of another destination, which comes from the row, stays.
func TestTheSinkScrubStoresAPlaceholderInThePerRunColumnsOnly(t *testing.T) {
	got := sinkPerRunScrub(perRunSinkAnswer)
	want := strings.NewReplacer(`"2026-10-01T04:11:02.577380Z"`, `"`+oraclePerRunValue+`"`, `"2026-10-01T04:11:02.577549Z"`, `"`+oraclePerRunValue+`"`).Replace(perRunSinkAnswer)
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if !strings.Contains(got, `"2026-08-01T00:00:00Z"`) {
		t.Error("last_synced of a destination that takes it from the row was replaced")
	}
	if again := sinkPerRunScrub(got); again != got {
		t.Fatalf("a second pass changed the answer: %s", again)
	}
	if text := "Traceback (most recent call last)"; sinkPerRunScrub(text) != text {
		t.Error("text that is not an answer was changed")
	}
	if scriptPerRunScrub("work-item-sink") == nil || scriptPerRunScrub("repo-listing") != nil {
		t.Error("the sink scrub is not the scrub of the work-item sink only")
	}
	for caseID, want := range map[string]bool{"work_items_single": true, "work_item_transitions": true, "work_item_dependencies": false, "sprints": false} {
		if got := sinkPerRunColumn(caseID, "last_synced"); got != want {
			t.Errorf("sinkPerRunColumn(%q, last_synced) = %t, want %t", caseID, got, want)
		}
	}
	if sinkPerRunColumn("work_items_single", "updated_at") {
		t.Error("a column that is not last_synced is a per-run column")
	}
}

// TestAPerRunSinkValueIsADatetimePlaceholder pins what the sink comparison
// still checks of a per-run column.
func TestAPerRunSinkValueIsADatetimePlaceholder(t *testing.T) {
	placeholder := json.RawMessage(`{"t": "datetime", "v": "` + oraclePerRunValue + `"}`)
	if err := perRunSinkValueErr(placeholder, "datetime:2026-08-01T00:00:00Z"); err != nil {
		t.Fatalf("a datetime placeholder was refused: %v", err)
	}
	refused := map[string][2]string{
		"a clock value":            {`{"t": "datetime", "v": "2026-10-01T04:11:02Z"}`, "datetime:2026-08-01T00:00:00Z"},
		"a placeholder of a str":   {`{"t": "str", "v": "` + oraclePerRunValue + `"}`, "datetime:2026-08-01T00:00:00Z"},
		"a null":                   {`null`, "datetime:2026-08-01T00:00:00Z"},
		"a datetime with no value": {`{"t": "datetime"}`, "datetime:2026-08-01T00:00:00Z"},
		"a Go value of a str":      {string(placeholder), "str:2026-08-01T00:00:00Z"},
		"a Go value that is null":  {string(placeholder), "null"},
	}
	for name, values := range refused {
		if err := perRunSinkValueErr(json.RawMessage(values[0]), values[1]); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}
