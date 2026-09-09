package workgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// --- CHAOS-3969: resolveWorkUnitTeamAttributions -------------------------

// TestResolveWorkUnitTeamAttributions_QueryShape is a fake-client SQL-shape
// regression guard (same convention as membership_test.go's
// TestBatchResolveMembership_QueriesAPairBoundMatch): it locks that the
// query reuses this package's shared run-scope protocol
// (membershipRunSubquery/legacyNodeMaxJoin/runScopePredicate) rather than a
// re-derived copy, joins work_item_team_attributions FINAL with the
// latest-computed-at filter, and applies the work-unit-id/team-id filters
// only when the corresponding argument is non-empty.
func TestResolveWorkUnitTeamAttributions_QueryShape(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{}}}}

	teamID := "team-1"
	_, err := resolveWorkUnitTeamAttributions(context.Background(), client, "org1", []string{"wu-1", "wu-2"}, &teamID, 5000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(client.statements) != 1 {
		t.Fatalf("got %d Query calls, want 1", len(client.statements))
	}
	stmt := client.statements[0]

	for _, want := range []string{
		"INNER JOIN (" + latestCompleteRunSubquery + ") AS latest_run ON 1 = 1",
		legacyNodeMaxJoin,
		runScopePredicate,
		"work_item_team_attributions FINAL",
		"AND m.work_unit_id IN {work_unit_ids:Array(String)}",
		"WHERE team_id = {team_id:String}",
		"ifNull(team_id, '') AS team_id",
		"ifNull(team_name, '') AS team_name",
		teamAttributionSourceRankSQL,
	} {
		if !strings.Contains(stmt, want) {
			t.Fatalf("query is missing expected fragment %q; got:\n%s", want, stmt)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(stmt), "WITH ") {
		t.Fatalf("query must not start with WITH (dev-health-go rejects non-SELECT-first statements): %s", stmt)
	}

	if v, ok := bindingValue(client.bindings[0], "org_id"); !ok || v != "org1" {
		t.Fatalf("org_id binding = %v, %v", v, ok)
	}
	if v, ok := bindingValue(client.bindings[0], "work_unit_ids"); !ok {
		t.Fatalf("missing work_unit_ids binding")
	} else if ids, ok := v.([]string); !ok || len(ids) != 2 {
		t.Fatalf("work_unit_ids binding = %v", v)
	}
	if v, ok := bindingValue(client.bindings[0], "team_id"); !ok || v != "team-1" {
		t.Fatalf("team_id binding = %v, %v", v, ok)
	}
	if v, ok := bindingValue(client.bindings[0], "limit"); !ok || v != uint64(5000) {
		t.Fatalf("limit binding = %v, %v", v, ok)
	}
}

// TestResolveWorkUnitTeamAttributions_NoFiltersOmitsClauses is the
// complementary case: empty workUnitIDs / nil teamID must not render
// either optional clause or bind either optional parameter, matching
// team_attribution.py's own `if work_unit_ids: ...` / `if team_id: ...`
// guards.
func TestResolveWorkUnitTeamAttributions_NoFiltersOmitsClauses(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{}}}}

	_, err := resolveWorkUnitTeamAttributions(context.Background(), client, "org1", nil, nil, 5000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	stmt := client.statements[0]
	if strings.Contains(stmt, "work_unit_ids") {
		t.Fatalf("query should not reference work_unit_ids when none given: %s", stmt)
	}
	if strings.Contains(stmt, "WHERE team_id") {
		t.Fatalf("query should not filter on team_id when none given: %s", stmt)
	}
	if _, ok := bindingValue(client.bindings[0], "work_unit_ids"); ok {
		t.Fatalf("should not bind work_unit_ids")
	}
	if _, ok := bindingValue(client.bindings[0], "team_id"); ok {
		t.Fatalf("should not bind team_id")
	}
}

// TestResolveWorkUnitTeamAttributions_ShapesRows pins
// rowToWorkUnitTeamAttribution's field mapping end to end through a fake
// row, including the evidence string's exact wording (team_attribution.py's
// f-string) and empty-string-as-NULL team_id/team_name handling (the
// ifNull(..., ”) sidestep -- see resolveWorkUnitTeamAttributions' doc
// comment).
func TestResolveWorkUnitTeamAttributions_ShapesRows(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"wu-1", "team-42", "Payments", "native_team", "high", uint64(3)},
		{"wu-2", "", "", "unassigned", "none", uint64(1)},
	}}}}

	got, err := resolveWorkUnitTeamAttributions(context.Background(), client, "org1", nil, nil, 5000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}

	r0 := got[0]
	if r0.WorkUnitID != "wu-1" || r0.TeamID == nil || *r0.TeamID != "team-42" || r0.TeamName == nil || *r0.TeamName != "Payments" {
		t.Fatalf("row 0 ids/names: %+v", r0)
	}
	if r0.Source != "NATIVE_TEAM" || r0.Confidence != "HIGH" || !r0.IsPrimary || r0.MemberCount != 3 {
		t.Fatalf("row 0 enums/count: %+v", r0)
	}
	wantEvidence := "3 member work item(s) attributed to Payments via native_team"
	if r0.Evidence != wantEvidence {
		t.Fatalf("row 0 evidence = %q, want %q", r0.Evidence, wantEvidence)
	}

	r1 := got[1]
	if r1.TeamID != nil || r1.TeamName != nil {
		t.Fatalf("row 1 (empty-string team columns) should map to nil *string: %+v", r1)
	}
	if r1.Source != "UNASSIGNED" || r1.Confidence != "NONE" {
		t.Fatalf("row 1 enums: %+v", r1)
	}
	wantEvidence1 := "1 member work item(s) attributed to no team via unassigned"
	if r1.Evidence != wantEvidence1 {
		t.Fatalf("row 1 evidence = %q, want %q", r1.Evidence, wantEvidence1)
	}
}

// TestMapTeamAttributionSource_UnrecognizedFallsBackToUnassigned mirrors
// team_attribution.py's _map_source fallback contract: an unrecognized
// string maps to the floor value instead of erroring -- deliberately NOT
// mapNodeType's validate-and-error contract elsewhere in this package.
func TestMapTeamAttributionSource_UnrecognizedFallsBackToUnassigned(t *testing.T) {
	if got := mapTeamAttributionSource("some_future_source"); got != "UNASSIGNED" {
		t.Fatalf("mapTeamAttributionSource(unrecognized) = %q, want UNASSIGNED", got)
	}
	if got := mapTeamAttributionSource(""); got != "UNASSIGNED" {
		t.Fatalf("mapTeamAttributionSource(\"\") = %q, want UNASSIGNED", got)
	}
}

// TestMapTeamAttributionConfidence_UnrecognizedFallsBackToNone mirrors
// team_attribution.py's _map_confidence fallback contract.
func TestMapTeamAttributionConfidence_UnrecognizedFallsBackToNone(t *testing.T) {
	if got := mapTeamAttributionConfidence("some_future_confidence"); got != "NONE" {
		t.Fatalf("mapTeamAttributionConfidence(unrecognized) = %q, want NONE", got)
	}
	if got := mapTeamAttributionConfidence(""); got != "NONE" {
		t.Fatalf("mapTeamAttributionConfidence(\"\") = %q, want NONE", got)
	}
}

// TestResolveWorkUnitTeamAttributions_TruncationSignalFiresAtLimit is the
// fake-client half of CHAOS-3969's truncation-signal proof: when the
// result comes back with EXACTLY `limit` rows,
// recordWorkUnitTeamAttributionsTruncation must fire, with the org id and
// limit it was called with. The real-engine half (does this actually
// happen when a real query truncates a real >limit seed) lives in
// teamattribution_integration_test.go -- a fake client can only prove this
// function's own "len(results) == limit" branch is wired, not that a real
// truncated ClickHouse read reaches it.
func TestResolveWorkUnitTeamAttributions_TruncationSignalFiresAtLimit(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"wu-1", "", "", "unassigned", "none", uint64(1)},
		{"wu-2", "", "", "unassigned", "none", uint64(1)},
	}}}}

	var recorded []struct {
		orgID string
		limit int
	}
	previous := recordWorkUnitTeamAttributionsTruncation
	recordWorkUnitTeamAttributionsTruncation = func(_ context.Context, orgID string, limit int) {
		recorded = append(recorded, struct {
			orgID string
			limit int
		}{orgID, limit})
	}
	t.Cleanup(func() { recordWorkUnitTeamAttributionsTruncation = previous })

	got, err := resolveWorkUnitTeamAttributions(context.Background(), client, "org1", nil, nil, 2)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	if len(recorded) != 1 || recorded[0].orgID != "org1" || recorded[0].limit != 2 {
		t.Fatalf("truncation signal not recorded correctly: %+v", recorded)
	}
}

// TestResolveWorkUnitTeamAttributions_NoTruncationSignalBelowLimit is the
// negative case: fewer rows than limit must never fire the signal.
func TestResolveWorkUnitTeamAttributions_NoTruncationSignalBelowLimit(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{
		{"wu-1", "", "", "unassigned", "none", uint64(1)},
	}}}}

	var calls int
	previous := recordWorkUnitTeamAttributionsTruncation
	recordWorkUnitTeamAttributionsTruncation = func(context.Context, string, int) { calls++ }
	t.Cleanup(func() { recordWorkUnitTeamAttributionsTruncation = previous })

	if _, err := resolveWorkUnitTeamAttributions(context.Background(), client, "org1", nil, nil, 5000); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 0 {
		t.Fatalf("truncation signal fired when result was below limit: %d calls", calls)
	}
}

// TestDefaultRecordWorkUnitTeamAttributionsTruncation_LogsAndIncrementsCounter
// is CHAOS-3969 round 2's proof (team-lead review: a WARN log alone is not
// a sufficient truncation signal, add a counter too): this is the layer
// the resolveWorkUnitTeamAttributions-level tests above cannot reach --
// they swap recordWorkUnitTeamAttributionsTruncation WHOLESALE, which
// proves the call site's "len(results) == limit" wiring but never
// exercises defaultRecordWorkUnitTeamAttributionsTruncation's own body
// (same "injection seam masks the layer behind it" shape
// analytics/investmentmembershiptelemetry_test.go's
// TestDefaultRecordStaleInvestmentMembershipScope_RecordsToRealMeter doc
// comment names). This test calls the default implementation directly,
// swaps ONLY incrementWorkUnitTeamAttributionsTruncationCounter (a spy,
// not the real meter -- the real meter is proven separately, against a
// REAL truncating ClickHouse read, by
// teamattribution_integration_test.go's
// TestResolveWorkUnitTeamAttributions_TruncationSignalRealEngine), and
// asserts BOTH the log line and the counter-increment seam fire from the
// SAME call -- the log line's wording/fields are unchanged by this round.
func TestDefaultRecordWorkUnitTeamAttributionsTruncation_LogsAndIncrementsCounter(t *testing.T) {
	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	var counterCalls int
	previous := incrementWorkUnitTeamAttributionsTruncationCounter
	incrementWorkUnitTeamAttributionsTruncationCounter = func(context.Context) { counterCalls++ }
	t.Cleanup(func() { incrementWorkUnitTeamAttributionsTruncationCounter = previous })

	defaultRecordWorkUnitTeamAttributionsTruncation(context.Background(), "org1", 5000)

	if counterCalls != 1 {
		t.Fatalf("counter increment seam called %d times, want 1", counterCalls)
	}

	var record map[string]any
	line := strings.TrimSpace(logBuf.String())
	if line == "" {
		t.Fatalf("no log line emitted")
	}
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		t.Fatalf("log line is not valid JSON: %s: %v", line, err)
	}
	if record["msg"] != "query_api.workgraph.work_unit_team_attributions.truncated" {
		t.Fatalf("log msg = %v, want unchanged truncation message", record["msg"])
	}
	if record["org_id"] != "org1" || record["limit"] != float64(5000) {
		t.Fatalf("log fields = %+v, want org_id=org1 limit=5000", record)
	}
}
