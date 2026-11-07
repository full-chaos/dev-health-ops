package shadowvenue

import (
	"strings"
	"testing"
	"time"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// controlOn is the embedded table with control ctrl moved onto target's route (ext - and destructive no, as a control
// row is written).
func controlOn(t *testing.T, ctrl string, target Row) []Row {
	t.Helper()
	var out []string
	for _, line := range strings.Split(string(DefaultRows), "\n") {
		if f := strings.Split(line, "\t"); len(f) == 10 && f[0] == ctrl {
			f[1], f[2], f[8] = target.Method, target.Route, target.Route
			line = strings.Join(f, "\t")
		}
		out = append(out, line)
	}
	return mustRows(t, strings.Join(out, "\n"))
}

// classed are the embedded table's external and destructive rows.
func classed(t *testing.T) []Row {
	t.Helper()
	var out []Row
	for _, row := range mustRows(t, string(DefaultRows)) {
		if !row.Control() && (row.Ext != none || row.Destructive) {
			out = append(out, row)
		}
	}
	return out
}

// The reviewer's form: C1 or C2 moved onto an external or destructive route is refused at plan (CheckRows and the
// library MakePlan) and voids both judges, for all 44 combinations; a control stays on a plain route.
func TestControlRowsStayOnPlainRoutes(t *testing.T) {
	targets := classed(t)
	if len(targets) != 22 {
		t.Fatalf("%d external or destructive rows, want 22", len(targets))
	}
	venue, frozen, _, _ := testPoolVenue(t)
	combos := 0
	for _, ctrl := range []string{"C1", "C2"} {
		for _, target := range targets {
			combos++
			table := controlOn(t, ctrl, target)
			name := ctrl + " on " + target.Key()
			if err := CheckRows(table, authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "control "+ctrl) && !strings.Contains(err.Error(), "row "+ctrl) {
				t.Errorf("%s: CheckRows %v", name, err)
			}
			if _, err := MakePlan(table, venue, PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, K: 3, Destructive: true, AllowExternal: StubServices}); err == nil || !strings.Contains(err.Error(), "class") {
				t.Errorf("%s: MakePlan %v", name, err)
			}
			slots, lines := controlsOK()
			ev, _ := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
			if r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Rows: table}}); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "class") {
				t.Errorf("%s: judge void %v %v", name, r.Void, r.Reasons)
			}
			run := newEnfRun()
			run.in.Outbound = Outbound{Rows: table}
			if r := run.judge(); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "class") {
				t.Errorf("%s: judge-enforced void %v", name, r.Void)
			}
		}
	}
	if combos != 44 {
		t.Fatalf("%d combinations, want 44", combos)
	}
}

// A control that also carries an external route's class (its ext column set to the route's service, so the class
// compare holds) is still refused at plan and voids both judges: a control stays on a plain route.
func TestAControlCarryingAnExternalClassIsRefused(t *testing.T) {
	venue, frozen, _, _ := testPoolVenue(t)
	n := 0
	for _, target := range classed(t) {
		if target.Destructive {
			continue
		}
		for _, ctrl := range []string{"C1", "C2"} {
			n++
			table := controlOn(t, ctrl, target)
			for i := range table {
				if table[i].ID == ctrl {
					table[i].Ext = target.Ext
				}
			}
			name := ctrl + " as " + target.Key()
			if err := CheckRows(table, authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "control "+ctrl) {
				t.Errorf("%s: CheckRows %v", name, err)
			}
			if _, err := MakePlan(table, venue, PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, K: 3, AllowExternal: StubServices}); err == nil || !strings.Contains(err.Error(), "control "+ctrl) {
				t.Errorf("%s: MakePlan %v", name, err)
			}
			slots, lines := controlsOK()
			ev, _ := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
			if r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Rows: table}}); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "control "+ctrl) {
				t.Errorf("%s: judge void %v", name, r.Void)
			}
			run := newEnfRun()
			run.in.Outbound = Outbound{Rows: table}
			if r := run.judge(); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "control "+ctrl) {
				t.Errorf("%s: judge-enforced void %v", name, r.Void)
			}
		}
	}
	if n != 34 {
		t.Fatalf("%d combinations, want 34 (17 external routes x 2 controls)", n)
	}
}

// A destructive-flag mismatch on rows 1, 6, 9, 10 and 13 voids the judge and judge-enforced.
func TestJudgesVoidADestructiveFlagMismatch(t *testing.T) {
	for _, id := range []string{"1", "6", "9", "10", "13"} {
		var out []string
		for _, line := range strings.Split(string(DefaultRows), "\n") {
			if f := strings.Split(line, "\t"); len(f) == 10 && f[0] == id {
				f[4] = "no"
				line = strings.Join(f, "\t")
			}
			out = append(out, line)
		}
		table := mustRows(t, strings.Join(out, "\n"))
		slots, lines := controlsOK()
		ev, _ := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
		if r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Rows: table}}); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "row "+id+":") {
			t.Errorf("row %s destructive=no: judge void %v %v", id, r.Void, r.Reasons)
		}
		run := newEnfRun()
		run.in.Outbound = Outbound{Rows: table}
		if r := run.judge(); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "row "+id+":") {
			t.Errorf("row %s destructive=no: judge-enforced void %v", id, r.Void)
		}
	}
}

// The clean table passes all three: CheckRows and MakePlan take the embedded table, the judge takes it with the
// control slots, and judge-enforced takes the table of its own slots.
func TestTheCleanTablePassesPlanAndBothJudges(t *testing.T) {
	table := mustRows(t, string(DefaultRows))
	if err := CheckRows(table, authv1.GoAPIEndpointProfiles); err != nil {
		t.Fatal(err)
	}
	venue, frozen, _, _ := testPoolVenue(t)
	if _, err := MakePlan(table, venue, PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, K: 3, OnlyRows: []string{"58"}}); err != nil {
		t.Fatalf("MakePlan: %v", err)
	}
	slots, lines := controlsOK()
	ev, _ := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Rows: table}}); r.Void || !r.Pass {
		t.Fatalf("judge: void %v pass %v %v", r.Void, r.Pass, r.Reasons)
	}
	run := newEnfRun()
	run.in.Outbound = Outbound{Rows: tableOf(run.in.Slots, nil)}
	if r := run.judge(); r.Void || !r.Pass {
		t.Fatalf("judge-enforced: void %v pass %v %v", r.Void, r.Pass, r.Reasons)
	}
}

// A table that is clean by itself but whose row id names another route than the slot sent (row 22 on a plain route,
// the slot on the Stripe route) voids both judges: each slot's class is its route's.
func TestJudgesVoidASlotWhoseTableRowNamesAnotherRoute(t *testing.T) {
	slots, ev := stripeSlots(t)
	table := tableOf(slots, map[string]string{"22": "stripe"})
	for i := range table {
		if table[i].ID == "22" {
			table[i].Method, table[i].Route, table[i].Ext = "GET", "/api/v1/admin/teams", none
		}
	}
	if _, err := CheckRowClasses(table); err != nil {
		t.Fatalf("the table is not clean by itself: %v", err)
	}
	r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: true, Rows: table, Egress: egressReads("before", "between"), Seed: noSeed}})
	if !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "the row table gives the class") {
		t.Fatalf("judge void %v: %v", r.Void, r.Reasons)
	}
	run := newEnfRun()
	table = tableOf(run.in.Slots, nil)
	for i := range table {
		if table[i].ID == "3" {
			table[i].Method, table[i].Route = "POST", "/api/v1/billing/plans/pull-stripe"
			table[i].Ext = "stripe"
		}
	}
	run.in.Outbound = Outbound{Rows: table}
	if rep := run.judge(); !rep.Void || !strings.Contains(strings.Join(rep.Reasons, "\n"), "the row table gives the class") {
		t.Fatalf("judge-enforced void %v: %v", rep.Void, rep.Reasons)
	}
}

// The run's table and its slot records agree with EACH OTHER (row 22 on the Stripe route, its table row ext -) but
// disagree with the pinned table: both judges void. The truth is the pinned class, never the run's own table.
func TestJudgesVoidATableThatAgreesWithItsSlotsButNotThePinnedClasses(t *testing.T) {
	slots, ev := stripeSlots(t)
	table := tableOf(slots, map[string]string{"22": none})
	if r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: true, Rows: table, Egress: egressReads("before", "between"), Seed: noSeed}}); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "row 22: POST /api/v1/billing/plans/pull-stripe has the class ext stripe") {
		t.Fatalf("judge void %v: %v", r.Void, r.Reasons)
	}
	run := newEnfRun()
	seq := len(run.in.Slots) + 1
	for _, caller := range callerOrder {
		for k := 1; k <= 3; k++ {
			start := enfT0.Add(time.Duration(seq-1) * 3 * time.Second)
			run.in.Slots = append(run.in.Slots, Slot{Seq: seq, Row: "22", Caller: caller, Principal: caller, Expect: "match", K: k, Method: "POST",
				Route: "/api/v1/billing/plans/pull-stripe", Path: "/api/v1/billing/plans/pull-stripe", Start: start, End: start.Add(100 * time.Millisecond), Status: 403})
			run.in.Yard = append(run.in.Yard, YardSlot{Row: "22", Caller: caller, K: k, Key: "POST /api/v1/billing/plans/pull-stripe", Status: 403, Verdict: VerdictMatch})
			seq++
		}
	}
	run.in.Namespaces["POST /api/v1/billing/plans/pull-stripe"] = "billing"
	run.in.Outbound = Outbound{Rows: tableOf(run.in.Slots, map[string]string{"22": none})}
	if rep := run.judge(); !rep.Void || !strings.Contains(strings.Join(rep.Reasons, "\n"), "row 22: POST /api/v1/billing/plans/pull-stripe has the class ext stripe") {
		t.Fatalf("judge-enforced void %v: %v", rep.Void, rep.Reasons)
	}
}
