package shadowvenue

import (
	"strings"
	"testing"
	"time"
)

const (
	orgDeleteID = "3b1c2e6b-67fc-420d-a346-8028f5ff587c"
)

// with13 adds row 13 (DELETE org) to a strict run: member and admin are refused (403 in both runs); admin2 stands for
// the role the venue answers 503 (ClickHouse unset), here a second caller of the row so the fixture needs no membership
// snapshot. The refused callers keep MATCH_ENFORCED.
func with13(r *enfRun) []Slot {
	var sent []Slot
	add := func(caller string, status int) {
		for k := 1; k <= 3; k++ {
			start := enfT0.Add(time.Duration(len(r.in.Slots)) * 3 * time.Second)
			s := Slot{Start: start, End: start.Add(100 * time.Millisecond), Seq: len(r.in.Slots) + 1, Row: "13", Caller: caller, Principal: caller + "-A",
				Expect: "match", K: k, Method: "DELETE", Route: "/api/v1/admin/orgs/{org_id}", Path: "/api/v1/admin/orgs/" + orgDeleteID, Status: status}
			s = claimsOf([]Slot{s})[0]
			s.BodyNormSHA = bodyA
			r.in.Slots = append(r.in.Slots, s)
			r.in.Yard = append(r.in.Yard, YardSlot{Seq: len(r.in.Yard) + 1, Row: "13", Caller: caller, K: k, Key: orgDeleteKey, Status: status, BodyNorm: bodyA, Verdict: VerdictMatch})
			if caller == "admin" {
				sent = append(sent, s)
			}
		}
	}
	add("member", 403)
	add("admin", 503)
	r.in.Namespaces[orgDeleteKey] = "admin"
	r.in.Outbound = Outbound{Rows: tableOf(r.in.Slots, nil)}
	return sent
}

func refusalText(at time.Time, orgID, reason string) string {
	return logLine(at, "admin: org deletion refused", "org_id", orgID, "reason", reason, "dry_run", "false", "error", "ClickHouse URI not configured")
}

// Row 13 (acp, window 4): the venue answers 503 because ClickHouse is not configured, the same in both runs. The 5xx is
// EQUAL_PAST_GUARD when the go-api log has the line "admin: org deletion refused" with reason dsn_unset for the org of
// the slot's path inside the slot's window; the refused callers keep their equal 403.
func TestEnforcedOrgDeleteDsnUnsetIsEqualPastGuard(t *testing.T) {
	run := strictEnfRun()
	sent := with13(run)
	var lines []string
	for _, s := range sent {
		lines = append(lines, refusalText(s.Start.Add(30*time.Millisecond), orgDeleteID, "dsn_unset"))
	}
	run.in.Log = logOf(t, lines...)
	rep := run.judge()
	if rep.Void || !rep.Pass || rep.VerdictCounts()[VerdictEqualPastGuard] != 3 {
		t.Fatalf("void %v pass %v %v %v", rep.Void, rep.Pass, rep.VerdictCounts(), rep.Reasons)
	}
	for _, sv := range rep.Slots {
		if sv.Row == "13" && sv.Caller == "member" && sv.Verdict != VerdictMatchEnforced {
			t.Fatalf("the refused member slot %d is %s, want MATCH_ENFORCED", sv.Seq, sv.Verdict)
		}
	}
	cases := map[string]func(*enfRun, []Slot){
		"no go-api log":                func(r *enfRun, _ []Slot) { r.in.Log = nil },
		"a refusal for another reason": func(r *enfRun, s []Slot) { r.in.Log = refusalsOf(t, s, orgDeleteID, "open_failed") },
		"a refusal of another org": func(r *enfRun, s []Slot) {
			r.in.Log = refusalsOf(t, s, "11111111-2222-4333-8444-555555555555", "dsn_unset")
		},
		"a refusal outside the window": func(r *enfRun, s []Slot) {
			r.in.Log = logOf(t, refusalText(enfT0.Add(48*time.Hour), orgDeleteID, "dsn_unset"))
		},
		"two refusals in one window":    func(r *enfRun, s []Slot) { r.in.Log = refusalsTwiceOf(t, s) },
		"the refusal only for one slot": func(r *enfRun, s []Slot) { r.in.Log = refusalsOf(t, s[:1], orgDeleteID, "dsn_unset") },
	}
	for name, plant := range cases {
		t.Run(name, func(t *testing.T) {
			r := strictEnfRun()
			s := with13(r)
			plant(r, s)
			rep := r.judge()
			if rep.Pass || rep.VerdictCounts()[VerdictEqualPastGuard] > 0 && name != "the refusal only for one slot" {
				t.Fatalf("pass %v %v", rep.Pass, rep.VerdictCounts())
			}
			if name == "the refusal only for one slot" && (rep.VerdictCounts()[VerdictEqualPastGuard] != 1 || rep.VerdictCounts()[VerdictVoidParity] != 2) {
				t.Fatalf("%v", rep.VerdictCounts())
			}
		})
	}
	t.Run("a different body", func(t *testing.T) {
		r := strictEnfRun()
		s := with13(r)
		r.in.Log = refusalsOf(t, s, orgDeleteID, "dsn_unset")
		for i := range r.in.Slots {
			if r.in.Slots[i].Row == "13" && r.in.Slots[i].Caller == "admin" && r.in.Slots[i].K == 3 {
				r.in.Slots[i].BodyNormSHA = bodyB
			}
		}
		rep := r.judge()
		if rep.Pass || rep.VerdictCounts()[VerdictMismatchBody] != 1 || rep.VerdictCounts()[VerdictEqualPastGuard] != 2 {
			t.Fatalf("%v", rep.VerdictCounts())
		}
	})
	t.Run("another route with a refusal line is not helped", func(t *testing.T) {
		r := fiveXXRun(500)
		var lines []string
		for i := range r.in.Slots {
			if s := &r.in.Slots[i]; s.Row == "1" && s.Caller == "admin" {
				s.Path = "/api/v1/admin/other/" + orgDeleteID
				lines = append(lines, refusalText(s.Start.Add(30*time.Millisecond), orgDeleteID, "dsn_unset"))
			}
		}
		r.in.Log = logOf(t, lines...)
		if rep := r.judge(); rep.VerdictCounts()[VerdictEqualPastGuard] != 0 || rep.VerdictCounts()[VerdictVoidParity] != 3 {
			t.Fatalf("%v", rep.VerdictCounts())
		}
	})
}

func refusalsOf(t *testing.T, slots []Slot, orgID, reason string) *Evidence {
	var lines []string
	for _, s := range slots {
		lines = append(lines, refusalText(s.Start.Add(30*time.Millisecond), orgID, reason))
	}
	return logOf(t, lines...)
}

func refusalsTwiceOf(t *testing.T, slots []Slot) *Evidence {
	var lines []string
	for _, s := range slots {
		lines = append(lines, refusalText(s.Start.Add(30*time.Millisecond), orgDeleteID, "dsn_unset"), refusalText(s.Start.Add(40*time.Millisecond), orgDeleteID, "dsn_unset"))
	}
	return logOf(t, lines...)
}

// The schedule (acp, window 4): the two runs send the same slots in the same order, paired position by position. The
// schedule sha is the sha of the sent slots' row, caller, k and route key in order; a differing order voids the
// enforced run.
func TestScheduleSHAIsOrderSensitiveAndIgnoresThePath(t *testing.T) {
	r := newEnfRun()
	a := ScheduleSHA12(r.in.Slots)
	if len(a) != 12 {
		t.Fatalf("sha %q", a)
	}
	other := append([]Slot{}, r.in.Slots...)
	for i := range other {
		other[i].Path = "/elsewhere/" + itoa(i)
		other[i].Status = 500
		other[i].Start = other[i].Start.Add(time.Hour)
	}
	if ScheduleSHA12(other) != a {
		t.Fatal("the schedule sha changed with the path, status or time")
	}
	other[6], other[9] = other[9], other[6]
	if ScheduleSHA12(other) == a {
		t.Fatal("the schedule sha ignores the order")
	}
}

func TestEnforcedVoidsWhenTheYardstickScheduleDiffers(t *testing.T) {
	run := newEnfRun()
	if rep := run.judge(); rep.Void {
		t.Fatalf("a matching schedule voided: %v", rep.Reasons)
	}
	run = newEnfRun()
	run.in.Yard[6], run.in.Yard[9] = run.in.Yard[9], run.in.Yard[6]
	rep := run.judge()
	if !rep.Void || !strings.Contains(strings.Join(rep.Reasons, "\n"), "schedule") {
		t.Fatalf("void %v %v", rep.Void, rep.Reasons)
	}
	run = newEnfRun()
	run.in.Slots = append(run.in.Slots[:6], run.in.Slots[9:]...)
	if rep := run.judge(); !rep.Void || !strings.Contains(strings.Join(rep.Reasons, "\n"), "schedule") {
		t.Fatalf("a run missing cells did not void on the schedule: %v", rep.Reasons)
	}
}

// Row 40 (acp, window 4): the stub answered 401 to every request and the handler relayed it, so a 401 with a stub hit is
// not proof of a call before the guard, and the "0 reached the stub" line must not hide the slots that did reach it.
func TestOutboundTextNamesTheRelayedRefusalAndCountsTheHits(t *testing.T) {
	slots := []Slot{{Seq: 1, Row: "40", Caller: "admin", K: 1, Method: "GET", Route: "/api/v1/admin/integrations/pagerduty/services", Start: enfT0, End: enfT0.Add(100 * time.Millisecond), Status: 401}}
	hits := []StubHit{{At: enfT0.Add(20 * time.Millisecond), Service: "pagerduty", Method: "GET", Host: "api.pagerduty.com", Path: "/services"}}
	check := checkOutbound(slots, Outbound{Logged: true, Hits: hits, Rows: tableOf(slots, map[string]string{"40": "pagerduty"})}, nil, 2*time.Second, nil)
	text := strings.Join(check.fails, "\n")
	if strings.Contains(text, "outside calls (a call before the guard)") {
		t.Fatalf("the old label stands: %s", text)
	}
	if !strings.Contains(text, "relayed") || !strings.Contains(text, "answered 401") {
		t.Fatalf("no relayed-refusal text: %s", text)
	}
	if !strings.Contains(text, "1 answered 401/403 with a stub hit") {
		t.Fatalf("the row line does not count the refused slots that reached the stub: %s", text)
	}
}
