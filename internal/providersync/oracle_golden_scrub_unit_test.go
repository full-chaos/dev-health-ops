package providersync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReplaceJSONStringValuesKeepsEveryOtherByte pins the splice the scrubs
// are built on: only the matched string values change, and spacing, escapes,
// numbers and key order stay the producer's.
func TestReplaceJSONStringValuesKeepsEveryOtherByte(t *testing.T) {
	text := `{"a": {"v": "one", "n": 1.50, "s": "q\"\u00e9"},  "list": [ "x", {"v": "two"}, ["y", "z"] ], "v": "top"}`
	got, err := replaceJSONStringValues(text, "P", func(path []string, _ string) bool {
		return strings.Join(path, "/") == "a/v" || strings.Join(path, "/") == "list/1/v" || strings.Join(path, "/") == "list/2/1"
	})
	want := `{"a": {"v": "P", "n": 1.50, "s": "q\"\u00e9"},  "list": [ "x", {"v": "P"}, ["y", "P"] ], "v": "top"}`
	if err != nil || got != want {
		t.Fatalf("got  %s (%v)\nwant %s", got, err, want)
	}
	again, err := replaceJSONStringValues(got, "P", func(path []string, _ string) bool { return path[len(path)-1] == "v" && len(path) > 1 })
	if err != nil || again != got {
		t.Fatalf("a second pass changed the text: %s (%v)", again, err)
	}
	unchanged, err := replaceJSONStringValues(text, "P", func([]string, string) bool { return false })
	if err != nil || unchanged != text {
		t.Fatalf("no match changed the text: %s (%v)", unchanged, err)
	}
	// A key is never a value, and a value that is not a string is never replaced.
	keys, err := replaceJSONStringValues(`{"v": 1, "k": {"v": null}}`, "P", func([]string, string) bool { return true })
	if err != nil || keys != `{"v": 1, "k": {"v": null}}` {
		t.Fatalf("a key or a non-string value was replaced: %s (%v)", keys, err)
	}
	// match gets each value with its path.
	var seen []string
	if _, err := replaceJSONStringValues(`{"a": ["x", {"b": "y"}], "c": 1}`, "P", func(path []string, value string) bool {
		seen = append(seen, strings.Join(path, "/")+"="+value)
		return false
	}); err != nil || strings.Join(seen, " ") != "a/0=x a/1/b=y" {
		t.Fatalf("values seen = %q (%v)", seen, err)
	}
	for name, malformed := range map[string]string{"cut short": `{"a": ["x"`, "not JSON": `Traceback (most recent call last)`} {
		if _, err := replaceJSONStringValues(malformed, "P", func([]string, string) bool { return true }); err == nil {
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
}

// TestAPairAnswerAndThePerRunListAreHeldToEachOther pins pairPerRunErr in both
// directions: every row holds the placeholder in every declared field, which
// the pair excludes from the comparison; and no placeholder sits anywhere the
// list does not declare.
func TestAPairAnswerAndThePerRunListAreHeldToEachOther(t *testing.T) {
	const pair = "github/work-items/dependency"
	leaf := `{"t": "datetime", "v": "` + oraclePerRunValue + `"}`
	answer := func(rows []string, excluded string) []byte {
		cases := make([]string, len(rows))
		for index, row := range rows {
			cases[index] = `{"id": "case-` + string(rune('a'+index)) + `", "row": {` + row + `}}`
		}
		return []byte(`{"cases": [` + strings.Join(cases, ", ") + `], "excluded_fields": {` + excluded + `}}`)
	}
	excluded := `"last_synced": "wall clock"`
	kept := `"key": {"t": "str", "v": "a"}`
	if err := pairPerRunErr(pair, answer([]string{`"last_synced": ` + leaf + `, ` + kept, `"last_synced": ` + leaf}, excluded)); err != nil {
		t.Fatalf("a placeholder in the declared field of every row was refused: %v", err)
	}
	if err := pairPerRunErr("github/prs/row", answer([]string{`"state": {"t": "str", "v": "open"}`}, ``)); err != nil {
		t.Fatalf("a pair with no per-run field and no placeholder was refused: %v", err)
	}
	// A pair with no per-run field is held to nothing but "no placeholder":
	// its answer need not have the shape of a row answer.
	if err := pairPerRunErr("github/prs/row", []byte(`[1, "two"]`)); err != nil {
		t.Fatalf("an answer of another shape, of a pair with no per-run field, was refused: %v", err)
	}
	refused := map[string]struct {
		pair   string
		output []byte
		names  string
	}{
		"a per-run field that is compared":                   {pair, answer([]string{`"last_synced": ` + leaf}, ``), "compares that field"},
		"a row without the per-run field":                    {pair, answer([]string{`"last_synced": ` + leaf, kept}, excluded), `case "case-b"`},
		"a per-run field that is null in a row":              {pair, answer([]string{`"last_synced": ` + leaf, `"last_synced": null`}, excluded), `case "case-b"`},
		"a per-run field with the clock value":               {pair, answer([]string{`"last_synced": {"t": "datetime", "v": "2026-10-01T03:58:04Z"}`}, excluded), "2026-10-01T03:58:04Z"},
		"a per-run field with no value":                      {pair, answer([]string{`"last_synced": {"t": "datetime"}`}, excluded), "want a typed leaf"},
		"a per-run field without a type tag":                 {pair, answer([]string{`"last_synced": {"v": "` + oraclePerRunValue + `"}`}, excluded), "want a typed leaf"},
		"a per-run field that is a bare string":              {pair, answer([]string{`"last_synced": "` + oraclePerRunValue + `"`}, excluded), "declares no per-run value"},
		"an answer with no case":                             {pair, answer(nil, excluded), "holds no case"},
		"an answer that is not JSON":                         {pair, []byte("Traceback"), "read the answer"},
		"a placeholder in a field the list lacks":            {pair, answer([]string{`"last_synced": ` + leaf + `, "created_at": ` + leaf}, excluded), "cases.0.row.created_at.v"},
		"a placeholder in a pair the list lacks":             {"github/prs/row", answer([]string{`"last_synced": ` + leaf}, excluded), "cases.0.row.last_synced.v"},
		"a placeholder below the declared field":             {pair, answer([]string{`"last_synced": ` + leaf + `, "nested": {"last_synced": ` + leaf + `}`}, excluded), "cases.0.row.nested.last_synced.v"},
		"a placeholder outside the rows":                     {pair, answer([]string{`"last_synced": ` + leaf}, excluded+`, "note": "`+oraclePerRunValue+`"`), "excluded_fields.note"},
		"a placeholder as the type tag of the per-run field": {pair, answer([]string{`"last_synced": {"t": "` + oraclePerRunValue + `", "v": "` + oraclePerRunValue + `"}`}, excluded), "cases.0.row.last_synced.t"},
		"a placeholder beside the row":                       {pair, []byte(`{"cases": [{"id": "a", "row": {"last_synced": ` + leaf + `}, "meta": {"last_synced": ` + leaf + `}}], "excluded_fields": {` + excluded + `}}`), "cases.0.meta.last_synced.v"},
		"a placeholder outside the cases":                    {pair, []byte(`{"cases": [{"id": "a", "row": {"last_synced": ` + leaf + `}}], "excluded_fields": {` + excluded + `}, "shadow": [{"row": {"last_synced": ` + leaf + `}}]}`), "shadow.0.row.last_synced.v"},
		"a placeholder deeper than the leaf value":           {pair, answer([]string{`"last_synced": {"t": "list", "v": ["` + oraclePerRunValue + `"]}`}, excluded), "cases.0.row.last_synced.v.0"},
	}
	for name, c := range refused {
		err := pairPerRunErr(c.pair, c.output)
		if err == nil {
			t.Errorf("%s was accepted", name)
		} else if !strings.Contains(err.Error(), c.names) {
			t.Errorf("%s: the refusal does not name %q: %v", name, c.names, err)
		}
	}
}

// TestEveryStoredGoldenAndThePerRunDeclarationAgree reads every golden this
// package pins, whether or not its test runs, and holds its answers and the
// per-run declaration to each other: a placeholder only where a per-run value
// is declared, and a placeholder in every declared place. A failure names the
// golden file.
func TestEveryStoredGoldenAndThePerRunDeclarationAgree(t *testing.T) {
	sets := []struct {
		dir   string
		pins  map[string]string
		check func(requestName string, answer []byte) error
	}{
		{oraclePairGoldenDir, oraclePairGoldenPins, pairPerRunErr},
		{scriptOracleGoldenDir, oracleScriptGoldenPins, scriptPerRunErr},
	}
	goldens, placeholders := 0, 0
	for _, set := range sets {
		for name := range set.pins {
			raw, err := os.ReadFile(filepath.Join(set.dir, name))
			if err != nil {
				t.Errorf("golden %s: %v", name, err)
				continue
			}
			var file struct {
				Requests []struct {
					Name string `json:"name"`
					Body string `json:"body"`
				} `json:"requests"`
			}
			if err := json.Unmarshal(raw, &file); err != nil || len(file.Requests) == 0 {
				t.Errorf("golden %s: no request can be read (%v)", name, err)
				continue
			}
			goldens++
			for _, request := range file.Requests {
				answer := oraclePairAnswerText(t, request.Body)
				placeholders += strings.Count(answer, `"`+oraclePerRunValue+`"`)
				if err := set.check(request.Name, []byte(answer)); err != nil {
					t.Errorf("golden %s: %v", name, err)
				}
			}
		}
	}
	if goldens == 0 || placeholders == 0 {
		t.Fatalf("%d goldens read, %d placeholders in them: the check measured nothing", goldens, placeholders)
	}
	t.Logf("%d goldens, %d placeholders, each in a declared per-run place", goldens, placeholders)
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

// TestASinkAnswerAndThePerRunColumnsAreHeldToEachOther pins sinkPerRunErr and
// scriptPerRunErr in both directions.
func TestASinkAnswerAndThePerRunColumnsAreHeldToEachOther(t *testing.T) {
	leaf := `{"t": "datetime", "v": "` + oraclePerRunValue + `"}`
	answer := func(caseID, columns, rows string) []byte {
		return []byte(`{"cases": [{"id": "` + caseID + `", "table": "t", "column_names": [` + columns + `], "rows": [` + rows + `]}]}`)
	}
	id := `{"t": "str", "v": "a"}`
	good := answer("work_items_single", `"id", "last_synced"`, `[`+id+`, `+leaf+`], [`+id+`, `+leaf+`]`)
	if err := scriptPerRunErr("work-item-sink", good); err != nil {
		t.Fatalf("a placeholder in every value of the per-run column was refused: %v", err)
	}
	if err := scriptPerRunErr("work-item-sink", answer("sprints", `"id", "last_synced"`, `[`+id+`, {"t": "datetime", "v": "2026-08-01T00:00:00Z"}]`)); err != nil {
		t.Fatalf("a destination that takes last_synced from the row was refused: %v", err)
	}
	if err := scriptPerRunErr("repo-listing", []byte(`[{"name": "a"}]`)); err != nil {
		t.Fatalf("an oracle with no per-run value and no placeholder was refused: %v", err)
	}
	if err := scriptPerRunErr("json-dumps-evidence", []byte("plain text, not JSON")); err != nil {
		t.Fatalf("a text answer with no placeholder was refused: %v", err)
	}
	refused := map[string]struct {
		oracle string
		output []byte
		names  string
	}{
		"a clock value in the per-run column":                    {"work-item-sink", answer("work_items_single", `"id", "last_synced"`, `[`+id+`, `+leaf+`], [`+id+`, {"t": "datetime", "v": "2026-10-01T04:11:02Z"}]`), "row 1"},
		"a null in the per-run column":                           {"work-item-sink", answer("work_item_transitions", `"id", "last_synced"`, `[`+id+`, null]`), "row 0"},
		"a row too short for the per-run column":                 {"work-item-sink", answer("work_items_single", `"id", "last_synced"`, `[`+id+`]`), "no value"},
		"a placeholder in another column":                        {"work-item-sink", answer("work_items_single", `"id", "last_synced"`, `[`+leaf+`, `+leaf+`]`), "cases.0.rows.0.0.v"},
		"a placeholder in another destination":                   {"work-item-sink", answer("sprints", `"id", "last_synced"`, `[`+id+`, `+leaf+`]`), "cases.0.rows.0.1.v"},
		"a placeholder outside the rows":                         {"work-item-sink", answer("work_items_single", `"id", "`+oraclePerRunValue+`"`, `[`+id+`, `+id+`]`), "cases.0.column_names.1"},
		"a placeholder beside the rows":                          {"work-item-sink", []byte(`{"cases": [{"id": "work_items_single", "column_names": ["id", "last_synced"], "rows": [[` + id + `, ` + leaf + `]], "shadow": [[` + id + `, ` + leaf + `]]}]}`), "cases.0.shadow.0.1.v"},
		"a placeholder outside the cases":                        {"work-item-sink", []byte(`{"cases": [{"id": "work_items_single", "column_names": ["id", "last_synced"], "rows": [[` + id + `, ` + leaf + `]]}], "other": {"0": {"rows": [[` + id + `, ` + leaf + `]]}}}`), "other.0.rows.0.1.v"},
		"a placeholder as the type tag of the per-run column":    {"work-item-sink", answer("work_items_single", `"id", "last_synced"`, `[`+id+`, {"t": "`+oraclePerRunValue+`", "v": "`+oraclePerRunValue+`"}]`), "cases.0.rows.0.1.t"},
		"a bare placeholder in the per-run column":               {"work-item-sink", answer("work_items_single", `"id", "last_synced"`, `[`+id+`, "`+oraclePerRunValue+`"]`), "at cases.0.rows.0.1, and"},
		"a placeholder deeper than the leaf value of the column": {"work-item-sink", answer("work_items_single", `"id", "last_synced"`, `[`+id+`, {"t": "list", "v": ["`+oraclePerRunValue+`"]}]`), "cases.0.rows.0.1.v.0"},
		"a sink answer that is not JSON":                         {"work-item-sink", []byte("Traceback"), "decode the answer"},
		"a placeholder in an oracle that declares none":          {"repo-listing", []byte(`[{"name": "` + oraclePerRunValue + `"}]`), "declares no per-run value"},
		"a placeholder in a text answer":                         {"json-dumps-evidence", []byte("text with " + oraclePerRunValue), "declares no per-run value"},
	}
	for name, c := range refused {
		err := scriptPerRunErr(c.oracle, c.output)
		if err == nil {
			t.Errorf("%s was accepted", name)
		} else if !strings.Contains(err.Error(), c.names) {
			t.Errorf("%s: the refusal does not name %q: %v", name, c.names, err)
		}
	}
}
