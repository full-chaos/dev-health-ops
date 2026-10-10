package providersync

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

// The proof of an absence in the one snapshot rule. A kind whose scope and
// walk are proven closes an open fact the run does not hold only when its
// AbsenceProof says the fact is gone; a later duplicate of a fact the run does
// hold is not an absence.
func TestPlanSnapshotClosesAnAbsentFactOnlyOnAProofOfItsAbsence(t *testing.T) {
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	later, now := first.Add(24*time.Hour), first.Add(48*time.Hour)
	fact := func(project string, validFrom time.Time) OwnershipSnapshotRow {
		return OwnershipSnapshotRow{TeamID: "T", ProjectID: testPID(project), Source: "native", ValidFrom: validFrom}
	}
	fresh := []OwnershipSnapshotRow{fact("held", now)}
	open := []OwnershipSnapshotRow{
		fact("held", later), // 0: a later duplicate of a held fact
		fact("held", first), // 1: the held fact
		fact("gone", first), // 2
		fact("lost", first), // 3: the listing lost it; the provider says it holds
		fact("late", first), // 4: past the budget
		fact("fail", first), // 5: the provider's answer failed
	}
	answers := map[string]SnapshotAbsence{
		"gone": SnapshotAbsenceProven, "lost": SnapshotFactStillHeld, "late": SnapshotAbsenceOverBudget, "fail": SnapshotAbsenceNotProven,
	}
	var asked []string
	answer := func(row OwnershipSnapshotRow) SnapshotAbsence {
		asked = append(asked, row.ProjectID.String())
		return answers[row.ProjectID.String()]
	}
	kind := testEveryRowKind(EmptyIsAnAnswer)
	closedOf := func(plan SnapshotPlan) []int {
		out := []int{}
		for _, retraction := range plan.Retract {
			out = append(out, retraction.Open)
		}
		return out
	}
	for _, test := range []struct {
		name      string
		absence   AbsenceProof[OwnershipSnapshotRow]
		closed    []int
		statement string
		// held, notProven, overBudget are the counts of the kind's outcome.
		held, notProven, overBudget int
		asked                       []string
	}{
		{"no proof stated", AbsenceProof[OwnershipSnapshotRow]{}, []int{0}, AbsenceProofNotStated, 0, 4, 0, nil},
		{"a walk with no name", AbsenceByWalk[OwnershipSnapshotRow](" "), []int{0}, AbsenceProofNotStated, 0, 4, 0, nil},
		{"the cursor walk", AbsenceByWalk[OwnershipSnapshotRow](AbsenceWalkByCursor), []int{0, 2, 3, 4, 5}, "cursor_walk", 0, 0, 0, nil},
		{"every listing was one response", AbsenceByListing(testOneResponse[OwnershipSnapshotRow], answer),
			[]int{0, 2, 3, 4, 5}, AbsenceByListingStatement, 0, 0, 0, nil},
		{"a listing of more than one response, with the provider's answers", AbsenceByListing(testPaged[OwnershipSnapshotRow], answer),
			[]int{0, 2}, AbsenceByListingStatement, 1, 1, 1, []string{"gone", "lost", "late", "fail"}},
		{"a listing of more than one response, with no answer", AbsenceByListing[OwnershipSnapshotRow](nil, nil),
			[]int{0}, AbsenceByListingStatement, 0, 4, 0, nil},
		{"one listing of one response, one of more", AbsenceByListing(func(row OwnershipSnapshotRow) []ListWalk {
			if row.ProjectID.String() == "lost" || row.ProjectID.String() == "fail" {
				return testOneResponse(row)
			}
			return testPaged(row)
		}, answer), []int{0, 2, 3, 5}, AbsenceByListingStatement, 0, 0, 1, []string{"gone", "late"}},
		// The held set is the union of its walks: two walks of one response
		// and ONE walk of two responses prove nothing by themselves.
		{"three walks feed the held set and one of them took two responses", AbsenceByListing(func(OwnershipSnapshotRow) []ListWalk {
			return []ListWalk{{Name: "first", Responses: 1}, {Name: "second", Responses: 2}, {Name: "third", Responses: 1}}
		}, answer), []int{0, 2}, AbsenceByListingStatement, 1, 1, 1, []string{"gone", "lost", "late", "fail"}},
		{"three walks feed the held set and each took one response", AbsenceByListing(func(OwnershipSnapshotRow) []ListWalk {
			return []ListWalk{{Name: "first", Responses: 1}, {Name: "second", Responses: 1}, {Name: "third", Responses: 1}}
		}, answer), []int{0, 2, 3, 4, 5}, AbsenceByListingStatement, 0, 0, 0, nil},
		{"a walk that was not read", AbsenceByListing(func(OwnershipSnapshotRow) []ListWalk {
			return []ListWalk{{Name: "first", Responses: 1}, {Name: "second", Responses: 0}}
		}, nil), []int{0}, AbsenceByListingStatement, 0, 4, 0, nil},
		{"no walk is named", AbsenceByListing(func(OwnershipSnapshotRow) []ListWalk { return nil }, nil),
			[]int{0}, AbsenceByListingStatement, 0, 4, 0, nil},
		{"a walk with no name", AbsenceByListing(func(OwnershipSnapshotRow) []ListWalk {
			return []ListWalk{{Name: " ", Responses: 1}}
		}, nil), []int{0}, AbsenceByListingStatement, 0, 4, 0, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			asked = nil
			plan := PlanOwnershipSnapshot(fresh, open, now, kind.Snapshot(testSoleScope(), testProof(true), test.absence))
			outcome := plan.Kinds[0]
			if got := closedOf(plan); !reflect.DeepEqual(got, test.closed) {
				t.Errorf("the plan closes the open rows %v, want %v", got, test.closed)
			}
			if outcome.Closed != len(test.closed) || outcome.StillHeld != test.held || outcome.AbsenceNotProven != test.notProven ||
				outcome.AbsenceOverBudget != test.overBudget || outcome.AbsenceProof != test.statement || len(outcome.Abandoned) != 0 {
				t.Errorf("the outcome is %+v, want closed %d, still held %d, not proven %d, over budget %d, proof %q, not abandoned",
					outcome, len(test.closed), test.held, test.notProven, test.overBudget, test.statement)
			}
			// The order of the questions changes with the run: compare as sets.
			sort.Strings(asked)
			wantAsked := append([]string(nil), test.asked...)
			sort.Strings(wantAsked)
			if !reflect.DeepEqual(asked, wantAsked) {
				t.Errorf("the provider was asked for %v, want %v: only a fact the run does not hold, of a listing of more than one response", asked, wantAsked)
			}
			// A fact the run holds keeps its first valid_from, whatever the proof.
			if !plan.ValidFrom[0].Equal(first) {
				t.Errorf("the held fact is written with valid_from %s, want its first one %s", plan.ValidFrom[0], first)
			}
		})
	}

	// A kind that may not close (its walk did not reach its end) asks the
	// provider for nothing.
	asked = nil
	plan := PlanOwnershipSnapshot(fresh, open, now, kind.Snapshot(testSoleScope(), testProof(false),
		AbsenceByListing(testPaged[OwnershipSnapshotRow], answer)))
	if len(plan.Retract) != 0 || len(asked) != 0 {
		t.Errorf("a kind whose walk did not end closes %d row(s) and asked the provider for %v, want none", len(plan.Retract), asked)
	}
}

// The direct answers of one run: each candidate is asked once, inside the
// budget, and only "gone" and "still held" are answers.
func TestAbsenceLookupsAskEachCandidateOnceInsideTheBudget(t *testing.T) {
	row := func(project string) OwnershipSnapshotRow {
		return OwnershipSnapshotRow{TeamID: "T", ProjectID: testPID(project), Source: "native"}
	}
	calls := map[string]int{}
	lookups := NewAbsenceLookups(context.Background(), ownershipSnapshotKey, func(_ context.Context, row OwnershipSnapshotRow) SnapshotAbsence {
		calls[row.ProjectID.String()]++
		switch row.ProjectID.String() {
		case "gone":
			return SnapshotAbsenceProven
		case "held":
			return SnapshotFactStillHeld
		case "claims-budget":
			return SnapshotAbsenceOverBudget
		case "unknown-value":
			return SnapshotAbsence(99)
		}
		return SnapshotAbsenceNotProven
	})
	for project, want := range map[string]SnapshotAbsence{
		"gone": SnapshotAbsenceProven, "held": SnapshotFactStillHeld, "failed": SnapshotAbsenceNotProven,
		// The budget is the run's, not the prover's: a prover cannot claim it.
		"claims-budget": SnapshotAbsenceNotProven, "unknown-value": SnapshotAbsenceNotProven,
	} {
		for attempt := 0; attempt < 2; attempt++ {
			if got := lookups.Answer(row(project)); got != want {
				t.Errorf("the answer for %q is %d, want %d", project, got, want)
			}
		}
		if calls[project] != 1 {
			t.Errorf("the provider was asked %d time(s) for %q, want once", calls[project], project)
		}
	}
	// The rest of the budget, then one more candidate.
	for index := len(calls); index < AbsenceLookupBudget; index++ {
		if got := lookups.Answer(row(fmt.Sprintf("more-%03d", index))); got != SnapshotAbsenceNotProven {
			t.Fatalf("candidate %d inside the budget got %d, want an asked answer", index, got)
		}
	}
	if got := lookups.Answer(row("one-too-many")); got != SnapshotAbsenceOverBudget || calls["one-too-many"] != 0 {
		t.Errorf("the candidate past the budget of %d got %d after %d request(s), want over budget and no request",
			AbsenceLookupBudget, got, calls["one-too-many"])
	}
	// An answer given inside the budget stays known after the budget ended.
	if got := lookups.Answer(row("gone")); got != SnapshotAbsenceProven || calls["gone"] != 1 {
		t.Errorf("an answer given before the budget ended is %d after %d request(s), want the first answer and no new request", got, calls["gone"])
	}
	if len(calls) != AbsenceLookupBudget {
		t.Errorf("the run made requests for %d fact(s), want exactly the budget of %d", len(calls), AbsenceLookupBudget)
	}

	// No prover: nothing is proven and nothing panics.
	var none *AbsenceLookups[OwnershipSnapshotRow]
	if got := none.Answer(row("x")); got != SnapshotAbsenceNotProven {
		t.Errorf("a nil lookups answers %d, want not proven", got)
	}
	if got := NewOwnershipAbsenceLookups(context.Background(), nil).Answer(row("x")); got != SnapshotAbsenceNotProven {
		t.Errorf("lookups with no prover answer %d, want not proven", got)
	}
}

// The gate: a team whose listing was one response proves its absences by the
// listing; a team whose listing took more than one response needs the answer.
func TestTheGateTakesAOneResponseListingAsTheProofAndAsksForTheOthers(t *testing.T) {
	at := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	row := func(team, project string) OwnershipSnapshotRow {
		return OwnershipSnapshotRow{TeamID: team, ProjectID: testPID(project), Source: gitlabTeamCatalogSource, ValidFrom: at.Add(-time.Hour)}
	}
	// gl:uncounted is a listed team with no count of responses: its listing is
	// not taken as one response.
	open := []OwnershipSnapshotRow{row("gl:one", "one/a"), row("gl:paged", "paged/a"), row("gl:paged", "paged/b"), row("gl:uncounted", "uncounted/a")}
	decision := decideOwnershipClose(context.Background(), staticScopeCensus{}, ownershipCloseRequest{
		ref: TeamCatalogReference{OrgID: "org-1", IntegrationID: "integration-a"}, provider: gitlabTeamCatalogProvider,
		listed: []string{"gl:one", "gl:paged", "gl:uncounted"}, responses: map[string]int{"gl:one": 1, "gl:paged": 2},
	})
	var asked []string
	plan := PlanOwnershipSnapshot(nil, open, at, decision.snapshot(GitLabGroupProjectGrantKind, gitlabGroupProjectsWalk, func(row OwnershipSnapshotRow) SnapshotAbsence {
		asked = append(asked, row.ProjectID.String())
		if row.ProjectID.String() == "paged/a" {
			return SnapshotAbsenceProven
		}
		return SnapshotFactStillHeld
	}))
	closed := []int{}
	for _, retraction := range plan.Retract {
		closed = append(closed, retraction.Open)
	}
	sort.Strings(asked)
	if !reflect.DeepEqual(closed, []int{0, 1}) || !reflect.DeepEqual(asked, []string{"paged/a", "paged/b", "uncounted/a"}) || plan.Kinds[0].StillHeld != 2 {
		t.Errorf("the plan closes %v after asking for %v with %d still held; want rows 0 and 1, the facts of the paged and the uncounted team asked, 2 still held",
			closed, asked, plan.Kinds[0].StillHeld)
	}
	// With no answer, the paged team closes nothing and the other team closes.
	plan = PlanOwnershipSnapshot(nil, open, at, decision.snapshot(GitLabGroupProjectGrantKind, gitlabGroupProjectsWalk, nil))
	if len(plan.Retract) != 1 || plan.Retract[0].Open != 0 || plan.Kinds[0].AbsenceNotProven != 3 {
		t.Errorf("with no answer the plan closes %v and counts %d not proven; want row 0 only and 3", plan.Retract, plan.Kinds[0].AbsenceNotProven)
	}
	legs := SnapshotAbsenceLegs(plan)
	if len(legs) != 1 || legs[0].Reason != OwnershipAbsenceNotProven || legs[0].Leg != ownershipAbsenceLeg {
		t.Errorf("the legs of the plan are %+v, want one %s leg", legs, OwnershipAbsenceNotProven)
	}
}

// The budget of direct answers must not go to the same candidates at every
// run. 120 open facts are absent. For 100 of them the provider's answer always
// fails; for 20 it says "gone". In a fixed order the 100 could take the whole
// budget at every run and the 20 would never be asked. The rule asks in an
// order that changes with the run, so over a few runs every one of the 20 is
// asked and closed.
func TestTheBudgetOfDirectAnswersDoesNotGoToTheSameCandidatesAtEveryRun(t *testing.T) {
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	const failing, answering = AbsenceLookupBudget, 20
	open := make([]OwnershipSnapshotRow, 0, failing+answering)
	// The failing ones come FIRST in the table's order.
	for index := 0; index < failing; index++ {
		open = append(open, OwnershipSnapshotRow{TeamID: "T", ProjectID: testPID(fmt.Sprintf("a-fails-%03d", index)), Source: "native", ValidFrom: first})
	}
	for index := 0; index < answering; index++ {
		open = append(open, OwnershipSnapshotRow{TeamID: "T", ProjectID: testPID(fmt.Sprintf("z-gone-%03d", index)), Source: "native", ValidFrom: first})
	}
	kind := testEveryRowKind(EmptyIsAnAnswer)
	closed := map[string]bool{}
	orders := map[string]bool{}
	for run := 1; run <= 8; run++ {
		at := first.Add(time.Duration(run) * time.Hour)
		var stillOpen []OwnershipSnapshotRow
		for _, row := range open {
			if !closed[row.ProjectID.String()] {
				stillOpen = append(stillOpen, row)
			}
		}
		var asked []string
		lookups := NewAbsenceLookups(context.Background(), ownershipSnapshotKey, func(_ context.Context, row OwnershipSnapshotRow) SnapshotAbsence {
			asked = append(asked, row.ProjectID.String())
			if row.ProjectID.String()[0] == 'z' {
				return SnapshotAbsenceProven
			}
			return SnapshotAbsenceNotProven
		})
		plan := PlanOwnershipSnapshot(nil, stillOpen, at, kind.Snapshot(testSoleScope(), testProof(true),
			AbsenceByListing(testPaged[OwnershipSnapshotRow], lookups.Answer)))
		if len(asked) != AbsenceLookupBudget {
			t.Fatalf("run %d asked the provider %d time(s), want the budget of %d: the case is not set", run, len(asked), AbsenceLookupBudget)
		}
		for _, retraction := range plan.Retract {
			closed[stillOpen[retraction.Open].ProjectID.String()] = true
		}
		if outcome := plan.Kinds[0]; outcome.Closed+outcome.AbsenceNotProven+outcome.AbsenceOverBudget != len(stillOpen) {
			t.Fatalf("run %d: the outcome %+v does not account for the %d open rows", run, outcome, len(stillOpen))
		}
		orders[fmt.Sprint(asked[:5])] = true
		// The rows of the plan stay in the order of the open rows.
		for index := 1; index < len(plan.Retract); index++ {
			if plan.Retract[index-1].Open >= plan.Retract[index].Open {
				t.Fatalf("run %d: the retractions are not in the order of the open rows", run)
			}
		}
	}
	if len(closed) != answering {
		t.Errorf("after 8 runs %d of the %d facts the provider calls gone are closed: the budget went to the candidates that never answer", len(closed), answering)
	}
	if len(orders) < 2 {
		t.Errorf("every run asked the same first candidates: the order does not change with the run")
	}
	// The same run time asks in the same order: a plan can be made again.
	rank := snapshotAskRank("a fact", first)
	if rank != snapshotAskRank("a fact", first) || rank == snapshotAskRank("a fact", first.Add(time.Hour)) {
		t.Errorf("the order of a run is not a function of its time alone, or two run times give one order")
	}
}
