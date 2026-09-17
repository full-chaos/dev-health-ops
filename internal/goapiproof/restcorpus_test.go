package goapiproof

import "testing"

func TestRESTCorpusIsValid(t *testing.T) {
	if err := ValidateRESTCorpus(); err != nil {
		t.Fatalf("ValidateRESTCorpus: %v", err)
	}
}

func TestSpecForREST_RefusesAnUncoveredOperation(t *testing.T) {
	if _, err := SpecForREST("REST:GET:/api/v1/does-not-exist"); err == nil {
		t.Fatal("want an error for an uncovered operation, got nil")
	}
}

func TestSpecForREST_ReturnsTheCommittedSpec(t *testing.T) {
	spec, err := SpecForREST("REST:GET:/api/v1/quadrant")
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}
	if spec.Method != "GET" || spec.Path != "/api/v1/quadrant" {
		t.Fatalf("spec = %+v, want method GET path /api/v1/quadrant", spec)
	}
	if len(spec.Requests) == 0 {
		t.Fatal("spec declares zero requests")
	}
}

func TestKnownRESTOperations_IsSortedAndNonEmpty(t *testing.T) {
	ops := KnownRESTOperations()
	if len(ops) == 0 {
		t.Fatal("KnownRESTOperations is empty")
	}
	for i := 1; i < len(ops); i++ {
		if ops[i-1] >= ops[i] {
			t.Fatalf("KnownRESTOperations not sorted: %v", ops)
		}
	}
}

func TestKnownRESTPaths_CollapsesTheSharedDrilldownPath(t *testing.T) {
	// GET and POST /api/v1/drilldown/prs are two operations, one path --
	// restendpoints.go's own established path-level matching convention.
	paths := KnownRESTPaths()
	count := 0
	for _, p := range paths {
		if p == "/api/v1/drilldown/prs" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("KnownRESTPaths lists /api/v1/drilldown/prs %d times, want 1: %v", count, paths)
	}
}

func TestAssertRESTPathCoverage_AgreesWithItself(t *testing.T) {
	if err := AssertRESTPathCoverage(KnownRESTPaths()); err != nil {
		t.Fatalf("AssertRESTPathCoverage(KnownRESTPaths()): %v", err)
	}
}

func TestAssertRESTPathCoverage_RefusesAnUncoveredMountedPath(t *testing.T) {
	mounted := append(append([]string(nil), KnownRESTPaths()...), "/api/v1/does-not-exist")
	if err := AssertRESTPathCoverage(mounted); err == nil {
		t.Fatal("want an error when the mux mounts a path with no corpus entry")
	}
}

func TestAssertRESTPathCoverage_RefusesAStaleCorpusEntry(t *testing.T) {
	mounted := KnownRESTPaths()[1:] // drop one path the corpus still covers
	if err := AssertRESTPathCoverage(mounted); err == nil {
		t.Fatal("want an error when the corpus covers a path the mux no longer mounts")
	}
}

func TestRESTRequest_StatusDivergenceIsDeclaredOnlyWhereGenuine(t *testing.T) {
	// A regression test for the corpus's own data, not the validator: the
	// shared Pydantic-shaped validator (pydantic_metric_filter.go,
	// pydantic_validation_error.go) gives every route in this table the
	// SAME status on both planes for the overwhelming majority of
	// declared requests -- a status divergence is not a casual thing to
	// add to this corpus. This test pins the exact set accepted so far,
	// by operation and request name, so a future entry that declares one
	// is a deliberate, reviewed addition to this allowlist, not a silent
	// regression this test stopped checking.
	knownStatusDivergences := map[string]map[string]bool{
		"REST:GET:/api/v1/quadrant": {
			// baseline's wip_throughput read answers 503 Data unavailable
			// on both scopes; query-api answers 200 with real data on
			// both.
			"wip_throughput_org":           true,
			"wip_throughput_person_scoped": true,
		},
	}
	for operation, spec := range restEndpointSpecs {
		for _, req := range spec.Requests {
			if req.WantCandidateStatus == req.WantBaselineStatus {
				continue
			}
			if knownStatusDivergences[operation][req.Name] {
				continue
			}
			t.Errorf("unexpected status-divergent entry %s/%s (%d/%d) -- add it to knownStatusDivergences if the corpus gains a new one deliberately", operation, req.Name, req.WantCandidateStatus, req.WantBaselineStatus)
		}
	}
}

// TestEveryRESTBaselineDefectIsIntermittent pins a real production risk: a
// live request whose result happens to carry no data under a declared
// citation's Paths (an empty items list, or a window whose only rows leave
// every cited field null) makes that citation cover NOTHING for this
// comparison -- and by BaselineDefect's own stale rule, a non-Intermittent
// entry that covers nothing FAILS THE WHOLE RUN as a tool error
// (resultVacuityErrors in cmd/go-api-rest-prove/main.go), even though
// nothing is actually wrong. Every REST citation in this table is keyed
// off live org data this table has no control over, so every one of them
// must be Intermittent, with no exception -- pinned here rather than left
// to be rediscovered per entry, the way drilldownPRsParity's own datetime
// entry and drilldownIssuesParity's entry were both found missing it.
func TestEveryRESTBaselineDefectIsIntermittent(t *testing.T) {
	for operation, spec := range restEndpointSpecs {
		for _, req := range spec.Requests {
			for _, defect := range req.Parity.BaselineDefects {
				if !defect.Intermittent {
					t.Errorf("%s/%s: BaselineDefect %s is not Intermittent -- a live request that happens to return no data under its Paths would fail the whole run as a tool error, not report a clean comparison", operation, req.Name, defect.Ticket)
				}
			}
		}
	}
}

// TestDrilldownPRsParity_EmptyItemsIsAStructuralRefusalNotStale covers the
// FULLY-EMPTY case (a narrow or quiet window that genuinely returns zero
// PRs on both planes): PRsResponse's only field is "items", so an empty
// list leaves ZERO non-null leaves anywhere under the body --
// vacuousEmptyLegs' own trigger (compare.go) -- and Compare refuses
// outright rather than reporting anything as stale. This is the SAME
// class of protection Intermittent exists for, reached through a
// different, already-existing mechanism; both are pinned so neither one's
// absence goes unnoticed.
func TestDrilldownPRsParity_EmptyItemsIsAStructuralRefusalNotStale(t *testing.T) {
	empty := Snapshot{Data: map[string]any{"items": []any{}}, DataPresent: true}
	result := Compare(empty, empty, drilldownPRsParity)
	if result.StructuralRefusal != RefusalVacuousEmptyLegs {
		t.Fatalf("StructuralRefusal = %q, want %q", result.StructuralRefusal, RefusalVacuousEmptyLegs)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("a structural refusal must never also report stale: %v", result.StaleBaselineDefects)
	}
}

// TestDrilldownPRsParityDatetimeCitation_NonVacuousMatchIsIdleNotStale is
// the concrete, corpus-level regression the Intermittent fix exists for:
// a NON-vacuous response (real content: repo_id/number/created_at) where
// baseline and candidate genuinely agree records drilldownPRsParity's two
// entries as idle, never stale -- a stale verdict here is exactly the
// tool-failure-on-a-clean-run bug being fixed. (A live comparison's
// created_at would normally itself diverge, per this citation's own
// Reason -- this synthetic body holds it identical on both sides
// specifically to isolate "nothing differs anywhere" from "the two legs
// disagree about the timestamp", the same technique
// intermittent_baseline_defect_test.go's own synthetic bodies use.)
func TestDrilldownPRsParityDatetimeCitation_NonVacuousMatchIsIdleNotStale(t *testing.T) {
	body := `{"items":[{"repo_id":"r1","number":1,"created_at":"2024-01-01T00:00:00Z","merged_at":null,"first_review_at":null}]}`
	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, drilldownPRsParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("identical non-vacuous bodies must match, got %s (structural refusal %q)", result.TerminalState, result.StructuralRefusal)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("a genuine, non-vacuous match must never go stale: %v", result.StaleBaselineDefects)
	}
	if !equalStrings(result.IdleIntermittentBaselineDefects, []string{"CHAOS-5803", "CHAOS-5803"}) {
		t.Fatalf("idle = %v, want both drilldownPRsParity entries idle", result.IdleIntermittentBaselineDefects)
	}
}

// TestDrilldownIssuesParity_EmptyItemsIsAStructuralRefusalNotStale is
// drilldownIssuesParity's own version of the fully-empty case.
func TestDrilldownIssuesParity_EmptyItemsIsAStructuralRefusalNotStale(t *testing.T) {
	empty := Snapshot{Data: map[string]any{"items": []any{}}, DataPresent: true}
	result := Compare(empty, empty, drilldownIssuesParity)
	if result.StructuralRefusal != RefusalVacuousEmptyLegs {
		t.Fatalf("StructuralRefusal = %q, want %q", result.StructuralRefusal, RefusalVacuousEmptyLegs)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("a structural refusal must never also report stale: %v", result.StaleBaselineDefects)
	}
}

// TestDrilldownIssuesParityDatetimeCitation_NonVacuousMatchIsIdleNotStale
// is drilldownIssuesParity's own version of the non-vacuous regression --
// unlike PRItem's non-nullable created_at, IssueItem's started_at/
// completed_at are BOTH Nullable, so an issue that has neither started nor
// completed yet is a realistic, live response shape with real content
// (work_item_id/provider/status) and nothing under either cited path,
// on either plane.
func TestDrilldownIssuesParityDatetimeCitation_NonVacuousMatchIsIdleNotStale(t *testing.T) {
	body := `{"items":[{"work_item_id":"w1","provider":"github","status":"open","team_id":null,"cycle_time_hours":null,"lead_time_hours":null,"started_at":null,"completed_at":null}]}`
	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, drilldownIssuesParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("identical non-vacuous bodies must match, got %s (structural refusal %q)", result.TerminalState, result.StructuralRefusal)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("a genuine, non-vacuous match must never go stale: %v", result.StaleBaselineDefects)
	}
	if !equalStrings(result.IdleIntermittentBaselineDefects, []string{"CHAOS-5808"}) {
		t.Fatalf("idle = %v, want [CHAOS-5808]", result.IdleIntermittentBaselineDefects)
	}
}

// TestFlameCorpus_HasIDBoundLiveEntriesForPRAndIssue pins the corpus-level
// contract that lets flame's 200 path actually be exercised: flame's spec
// must carry exactly one live (200/200) entry per producible entity_type,
// each id-bound to the producer restcorpus.go's own doc comment names --
// pr_id (from GET /api/v1/drilldown/prs) for "pr", work_item_id (from GET
// /api/v1/drilldown/issues) for "issue" -- and the "pr" entry must carry
// flamePRIDBoundParity's own declared BaselineDefect. A silent
// regression that drops either entry, or that binds the wrong producer,
// fails here rather than only being noticed the next time an operator
// runs go-api-rest-prove live.
func TestFlameCorpus_HasIDBoundLiveEntriesForPRAndIssue(t *testing.T) {
	spec, err := SpecForREST("REST:GET:/api/v1/flame")
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}

	var pr, issue *RESTRequest
	for i := range spec.Requests {
		switch spec.Requests[i].Name {
		case "pr_entity_id_bound_200":
			pr = &spec.Requests[i]
		case "issue_entity_id_bound_200":
			issue = &spec.Requests[i]
		}
	}
	if pr == nil {
		t.Fatal("flame corpus has no pr_entity_id_bound_200 entry")
	}
	if issue == nil {
		t.Fatal("flame corpus has no issue_entity_id_bound_200 entry")
	}

	for _, tc := range []struct {
		name     string
		req      *RESTRequest
		producer string
	}{
		{"pr", pr, "pr_id"},
		{"issue", issue, "work_item_id"},
	} {
		if tc.req.WantCandidateStatus != 200 || tc.req.WantBaselineStatus != 200 {
			t.Errorf("%s entry status = (%d, %d), want (200, 200)", tc.name, tc.req.WantCandidateStatus, tc.req.WantBaselineStatus)
		}
		if len(tc.req.IDBindings) != 1 || tc.req.IDBindings[0].Producer != tc.producer || tc.req.IDBindings[0].QueryParam != "entity_id" {
			t.Errorf("%s entry IDBindings = %+v, want one binding on entity_id to producer %q", tc.name, tc.req.IDBindings, tc.producer)
		}
	}

	if len(pr.Parity.BaselineDefects) != 1 || pr.Parity.BaselineDefects[0].Ticket != "CHAOS-5803" || !pr.Parity.BaselineDefects[0].Intermittent {
		t.Errorf("pr entry BaselineDefects = %+v, want exactly one Intermittent CHAOS-5803 entry", pr.Parity.BaselineDefects)
	}

	drillPRs, err := SpecForREST("REST:GET:/api/v1/drilldown/prs")
	if err != nil {
		t.Fatalf("SpecForREST(drilldown/prs): %v", err)
	}
	drillIssues, err := SpecForREST("REST:GET:/api/v1/drilldown/issues")
	if err != nil {
		t.Fatalf("SpecForREST(drilldown/issues): %v", err)
	}
	if !producesID(drillPRs, "default_window", "pr_id") {
		t.Error("GET /api/v1/drilldown/prs' default_window entry no longer Produces pr_id")
	}
	if !producesID(drillIssues, "default_window", "work_item_id") {
		t.Error("GET /api/v1/drilldown/issues' default_window entry no longer Produces work_item_id")
	}
}

func producesID(spec RESTEndpointSpec, requestName, producerName string) bool {
	for _, req := range spec.Requests {
		if req.Name != requestName {
			continue
		}
		for _, p := range req.Produces {
			if p.Name == producerName {
				return true
			}
		}
	}
	return false
}

// TestFlamePRIDBoundParity_NonVacuousMatchIsIdleNotStale is
// flamePRIDBoundParity's own version of
// TestDrilldownPRsParityDatetimeCitation_NonVacuousMatchIsIdleNotStale: a
// non-vacuous, identical flame "pr" body on both legs must record its
// declared citation as idle, never stale -- the same regression class
// pinned for every other declared Parity in this table.
func TestFlamePRIDBoundParity_NonVacuousMatchIsIdleNotStale(t *testing.T) {
	body := `{"entity":{"repo_id":"r1","number":42,"title":"t","state":"open"},"timeline":{"start":"2024-01-01T00:00:00Z","end":"2024-01-02T00:00:00Z"},"frames":[{"id":"pr:r1:42","parent_id":null,"label":"PR lifecycle","start":"2024-01-01T00:00:00Z","end":"2024-01-02T00:00:00Z","state":"active","category":"planned"}]}`
	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, flamePRIDBoundParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("identical non-vacuous bodies must match, got %s (structural refusal %q)", result.TerminalState, result.StructuralRefusal)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("a genuine, non-vacuous match must never go stale: %v", result.StaleBaselineDefects)
	}
	if !equalStrings(result.IdleIntermittentBaselineDefects, []string{"CHAOS-5803"}) {
		t.Fatalf("idle = %v, want [CHAOS-5803]", result.IdleIntermittentBaselineDefects)
	}
}
