package syncdispatchruntime

import "testing"

// legacyCommitPredicate is the r2 head's commitCheckSQL WHERE clause, written
// out in Go with SQL NULL semantics (a NULL comparison is not true; the route
// was an inner join, so a missing route row matched nothing).
func legacyCommitPredicate(s commitSnapshot, generation, attempt int64) bool {
	if !s.found || s.attempts != attempt || s.routeTransport == nil || *s.routeTransport != "river" ||
		s.routeGeneration == nil || *s.routeGeneration != generation || s.routePaused == nil || *s.routePaused {
		return false
	}
	dispatched := s.status == "dispatched" && s.dispatchedTransport != nil && *s.dispatchedTransport == "river" &&
		s.dispatchedGeneration != nil && *s.dispatchedGeneration == generation
	return dispatched || s.status == "pending" && s.unclaimed
}

func stringRef(value string) *string { return &value }
func int64Ref(value int64) *int64    { return &value }
func boolRef(value bool) *bool       { return &value }

// TestDecideCommitEqualsTheLegacyCommitPredicate pins decideCommit's decision
// to the SQL predicate it replaced, over every combination of the clauses it
// reads, and requires a named reason exactly on a refusal.
func TestDecideCommitEqualsTheLegacyCommitPredicate(t *testing.T) {
	const generation, attempt = 3, 2
	reasons := map[string]bool{
		"outbox_row_missing": true, "delivery_attempt_mismatch": true, "route_row_missing": true,
		"route_other_transport": true, "route_paused": true, "route_generation_mismatch": true,
		"outbox_other_transport": true, "outbox_not_dispatched": true,
	}
	transports := []*string{nil, stringRef("river"), stringRef("celery")}
	generations := []*int64{nil, int64Ref(generation), int64Ref(generation + 1)}
	pauses := []*bool{nil, boolRef(false), boolRef(true)}
	cells, commits := 0, 0
	for _, found := range []bool{false, true} {
		for _, status := range []string{"dispatched", "pending", "failed"} {
			for _, dispatchedTransport := range transports {
				for _, dispatchedGeneration := range generations {
					for _, attempts := range []int64{attempt, attempt + 1} {
						for _, unclaimed := range []bool{true, false} {
							for _, routeTransport := range transports {
								for _, routeGeneration := range generations {
									for _, routePaused := range pauses {
										snapshot := commitSnapshot{found, status, dispatchedTransport, dispatchedGeneration, attempts,
											unclaimed, routeTransport, routeGeneration, routePaused}
										if !found {
											snapshot = commitSnapshot{}
										}
										commit, reason := decideCommit(snapshot, generation, attempt)
										cells++
										if commit {
											commits++
										}
										if want := legacyCommitPredicate(snapshot, generation, attempt); commit != want {
											t.Fatalf("decideCommit(%+v) commit=%t, legacy predicate %t", snapshot, commit, want)
										}
										if commit != (reason == "") || !commit && !reasons[reason] {
											t.Fatalf("decideCommit(%+v) = %t/%q: a refusal needs a named reason, a commit none", snapshot, commit, reason)
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("%d cells, %d commit", cells, commits)
	if commits == 0 {
		t.Fatal("no cell committed: the table does not reach the commit arms")
	}
}

// TestDecideCommitReasons pins one cell per F1 arm and per refusal reason.
func TestDecideCommitReasons(t *testing.T) {
	const generation, attempt = 3, 2
	dispatched := commitSnapshot{found: true, status: "dispatched", dispatchedTransport: stringRef("river"),
		dispatchedGeneration: int64Ref(generation), attempts: attempt, unclaimed: true,
		routeTransport: stringRef("river"), routeGeneration: int64Ref(generation), routePaused: boolRef(false)}
	with := func(edit func(*commitSnapshot)) commitSnapshot {
		snapshot := dispatched
		edit(&snapshot)
		return snapshot
	}
	wake := func(s *commitSnapshot) { s.status, s.dispatchedTransport, s.dispatchedGeneration = "pending", nil, nil }
	cases := []struct {
		name       string
		snapshot   commitSnapshot
		wantCommit bool
		wantReason string
	}{
		{"dispatched_to_the_route", dispatched, true, ""},
		{"same_attempt_wake", with(wake), true, ""},
		{"row_missing", commitSnapshot{}, false, "outbox_row_missing"},
		{"attempt_moved_on", with(func(s *commitSnapshot) { s.attempts = attempt + 1 }), false, "delivery_attempt_mismatch"},
		{"wake_then_claim", with(func(s *commitSnapshot) { wake(s); s.unclaimed = false; s.attempts = attempt + 1 }), false, "delivery_attempt_mismatch"},
		{"claim_and_paused_route", with(func(s *commitSnapshot) { wake(s); s.attempts = attempt + 1; s.routePaused = boolRef(true) }), false, "delivery_attempt_mismatch"},
		{"route_missing", with(func(s *commitSnapshot) { s.routeTransport, s.routeGeneration, s.routePaused = nil, nil, nil }), false, "route_row_missing"},
		{"route_other_transport", with(func(s *commitSnapshot) { s.routeTransport = stringRef("celery") }), false, "route_other_transport"},
		{"route_paused", with(func(s *commitSnapshot) { wake(s); s.routePaused = boolRef(true) }), false, "route_paused"},
		{"wake_then_route_moved", with(func(s *commitSnapshot) { wake(s); s.routeGeneration = int64Ref(generation + 2) }), false, "route_generation_mismatch"},
		{"dispatched_other_transport", with(func(s *commitSnapshot) { s.dispatchedTransport = nil }), false, "outbox_other_transport"},
		{"dispatched_other_generation", with(func(s *commitSnapshot) { s.dispatchedGeneration = int64Ref(generation - 1) }), false, "route_generation_mismatch"},
		{"pending_claimed_same_attempt", with(func(s *commitSnapshot) { wake(s); s.unclaimed = false }), false, "outbox_not_dispatched"},
		{"other_status", with(func(s *commitSnapshot) { s.status = "failed" }), false, "outbox_not_dispatched"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			commit, reason := decideCommit(c.snapshot, generation, attempt)
			if commit != c.wantCommit || reason != c.wantReason {
				t.Fatalf("decideCommit = %t/%q, want %t/%q", commit, reason, c.wantCommit, c.wantReason)
			}
		})
	}
}
