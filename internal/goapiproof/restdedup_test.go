package goapiproof

import "testing"

func TestInjectRESTDedupKeys_JoinsCompositeKey(t *testing.T) {
	data := map[string]any{
		"items": []any{
			map[string]any{"repo_id": "r1", "number": 7},
		},
	}
	InjectRESTDedupKeys(data, "items", []string{"repo_id", "number"})
	items := data["items"].([]any)
	element := items[0].(map[string]any)
	got, ok := element[RESTDedupKeyField]
	if !ok {
		t.Fatal("no synthetic dedup key was injected")
	}
	want := "r1" + restDedupKeySeparator + "7"
	if got != want {
		t.Fatalf("dedup key = %q, want %q", got, want)
	}
}

func TestInjectRESTDedupKeys_SkipsElementMissingAField(t *testing.T) {
	data := map[string]any{
		"items": []any{
			map[string]any{"repo_id": "r1"}, // no "number"
		},
	}
	InjectRESTDedupKeys(data, "items", []string{"repo_id", "number"})
	element := data["items"].([]any)[0].(map[string]any)
	if _, ok := element[RESTDedupKeyField]; ok {
		t.Fatal("injected a key for an element missing a declared field")
	}
}

func TestInjectRESTDedupKeys_SkipsNonObjectElement(t *testing.T) {
	data := map[string]any{"items": []any{"not-an-object"}}
	// Must not panic.
	InjectRESTDedupKeys(data, "items", []string{"repo_id"})
}

func TestInjectRESTDedupKeys_NoopWithoutListPathOrKeyFields(t *testing.T) {
	data := map[string]any{"items": []any{map[string]any{"repo_id": "r1"}}}
	InjectRESTDedupKeys(data, "", []string{"repo_id"})
	InjectRESTDedupKeys(data, "items", nil)
	element := data["items"].([]any)[0].(map[string]any)
	if _, ok := element[RESTDedupKeyField]; ok {
		t.Fatal("injected a key despite an empty ListPath/KeyFields")
	}
}

// End-to-end: a baseline holding a content-identical duplicate physical
// row for one PR (the same ReplacingMergeTree-without-FINAL mechanism
// drilldownPRsParity declares) is admitted once InjectRESTDedupKeys gives
// WorkGraphEdgeDedupShape a composite identity to key on -- proving the
// injection makes that GraphQL-shaped shape type usable for a REST list
// with no single id field.
func TestInjectRESTDedupKeys_MakesWorkGraphEdgeDedupShapeAdmitDuplicateRow(t *testing.T) {
	prA := `{"repo_id":"r1","number":1,"title":"a"}`
	prADup := prA
	prB := `{"repo_id":"r1","number":2,"title":"b"}`
	prC := `{"repo_id":"r1","number":3,"title":"c"}`

	baseline := restSnapshotFromJSON(t, `{"items":[`+prA+`,`+prADup+`,`+prB+`]}`)
	candidate := restSnapshotFromJSON(t, `{"items":[`+prA+`,`+prB+`,`+prC+`]}`)

	InjectRESTDedupKeys(baseline.Data, "items", []string{"repo_id", "number"})
	InjectRESTDedupKeys(candidate.Data, "items", []string{"repo_id", "number"})

	opts := Options{BaselineDefects: []BaselineDefect{{
		Ticket: "CHAOS-5803", Reason: "test fixture",
		Paths: []string{"data.items"}, Intermittent: true, IntermittentReason: "test fixture",
		WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{EdgesListPath: "data.items", IDField: RESTDedupKeyField},
	}}}

	result := Compare(baseline, candidate, opts)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-5803"}) {
		t.Fatalf("matched = %v, want [CHAOS-5803]", result.BaselineDefectsMatched)
	}
}

// A shared (repo_id, number) whose CONTENT genuinely disagrees is not the
// duplicate-row mechanism, and the injected composite key must not hide
// it -- the same discipline TestWorkGraphEdgeDedupShape_
// DuplicateGroupThatDisagreesIsNotAdmitted pins for a single-field id.
func TestInjectRESTDedupKeys_DoesNotAdmitAGenuineFieldRegression(t *testing.T) {
	prA := `{"repo_id":"r1","number":1,"title":"a"}`
	prARegressed := `{"repo_id":"r1","number":1,"title":"REGRESSED"}`
	prB := `{"repo_id":"r1","number":2,"title":"b"}`

	baseline := restSnapshotFromJSON(t, `{"items":[`+prA+`,`+prB+`]}`)
	candidate := restSnapshotFromJSON(t, `{"items":[`+prARegressed+`,`+prB+`]}`)

	InjectRESTDedupKeys(baseline.Data, "items", []string{"repo_id", "number"})
	InjectRESTDedupKeys(candidate.Data, "items", []string{"repo_id", "number"})

	opts := Options{BaselineDefects: []BaselineDefect{{
		Ticket: "CHAOS-5803", Reason: "test fixture",
		Paths: []string{"data.items"}, Intermittent: true, IntermittentReason: "test fixture",
		WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{EdgesListPath: "data.items", IDField: RESTDedupKeyField},
	}}}

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0: a genuine title regression on a shared id must not be admitted by the dedup shape; findings %+v", result.Findings)
	}
}
