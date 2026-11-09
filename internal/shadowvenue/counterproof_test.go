package shadowvenue

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func decisions(ns, outcome string, n string) string {
	return `dev_health_authz_enforcement_decisions_total{namespace="` + ns + `",outcome="` + outcome + `",service="go-api"} ` + n
}

func httpLine(s Slot, offset time.Duration) string {
	raw, _ := json.Marshal(map[string]any{"time": s.Start.Add(offset).Format(time.RFC3339Nano), "level": "INFO", "msg": MsgHTTPRequest,
		"method": s.Method, "route": s.Route, "status": s.Status, "request_id": "req-" + itoa(s.Seq), "peer": "10.0.0.1", "listener": "public"})
	return string(raw)
}

func httpExtra(at time.Time, method, route string, status int) string {
	raw, _ := json.Marshal(map[string]any{"time": at.Format(time.RFC3339Nano), "level": "INFO", "msg": MsgHTTPRequest, "method": method, "route": route, "status": status, "request_id": "req-extra", "peer": "10.0.0.1"})
	return string(raw)
}

// j1Run is fiveXXRun(503) held to form B: row 1 admin answers 503 in both runs, its yardstick cells are MATCH with a
// decision id, the guard's 403 answers have their own digest, the 4b counters equal the judged slots (teams allow 3,
// deny 6; integrations deny 3) and the go-api log has one access line per sent slot.
func j1Run(t *testing.T, extra ...string) *enfRun {
	t.Helper()
	r := fiveXXRun(503)
	r.in.J1 = true
	for i := range r.in.Slots {
		if r.in.Slots[i].Status == 403 {
			r.in.Slots[i].BodyNormSHA = bodyB
		}
	}
	for i := range r.in.Yard {
		if r.in.Yard[i].Status == 403 {
			r.in.Yard[i].BodyNorm = bodyB
		}
	}
	r.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", unjudged(noOrgKey, "no_org", "5"), unjudged(seamKey, "served_org", "2"))
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"),
		decisions("teams", "allow", "3"), decisions("teams", "deny", "6"), decisions("integrations", "deny", "3"))
	var lines []string
	for _, s := range r.in.Slots {
		if !s.Gap() {
			lines = append(lines, httpLine(s, 20*time.Millisecond))
		}
	}
	r.in.Log = logOf(t, append(lines, extra...)...)
	return r
}

func TestJ1CounterProofMakesAnEqualFiveXXPastTheGuard(t *testing.T) {
	rep := j1Run(t).judge()
	if rep.Void || !rep.Pass || rep.VerdictCounts()[VerdictEqualPastGuard] != 3 || rep.VerdictCounts()[VerdictVoidParity] != 0 {
		t.Fatalf("void %v pass %v %v %v", rep.Void, rep.Pass, rep.VerdictCounts(), rep.Reasons)
	}
	text := strings.Join(rep.Notes, "\n")
	for _, want := range []string{"counter teams: allow delta 3 vs 3 allowed slots, deny delta 6 vs 6 denied slots", "exact", "idle sweep: ", "guard deny body digests (1): " + bodyB} {
		if !strings.Contains(text, want) {
			t.Fatalf("the record lacks %q:\n%s", want, text)
		}
	}
	off := j1Run(t)
	off.in.J1 = false
	if rep := off.judge(); rep.VerdictCounts()[VerdictVoidParity] != 3 {
		t.Fatalf("without form B the three cells stay VOID_PARITY: %v", rep.VerdictCounts())
	}
}

// The mutants of D6894 (4): every one is refused, the cells stay VOID_PARITY and the run does not pass.
func TestJ1MutantsAreRefused(t *testing.T) {
	cases := map[string]func(*enfRun){
		"no counter delta": func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"))
		},
		"the allow delta is one short": func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"),
				decisions("teams", "allow", "2"), decisions("teams", "deny", "6"), decisions("integrations", "deny", "3"))
		},
		"the deny delta is one more": func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"),
				decisions("teams", "allow", "3"), decisions("teams", "deny", "7"), decisions("integrations", "deny", "3"))
		},
		"an error_deny was counted": func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"),
				decisions("teams", "allow", "3"), decisions("teams", "deny", "6"), decisions("teams", "error_deny", "1"), decisions("integrations", "deny", "3"))
		},
		"a decision of a namespace with no slot": func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"),
				decisions("teams", "allow", "3"), decisions("teams", "deny", "6"), decisions("integrations", "deny", "3"), decisions("settings", "allow", "1"))
		},
		"the guard's deny digest equals the allow cells' answer": func(r *enfRun) {
			for i := range r.in.Slots {
				if r.in.Slots[i].Status == 403 {
					r.in.Slots[i].BodyNormSHA = bodyA
				}
			}
			for i := range r.in.Yard {
				if r.in.Yard[i].Status == 403 {
					r.in.Yard[i].BodyNorm = bodyA
				}
			}
		},
		"the yardstick cell has no decision id": func(r *enfRun) {
			for i := range r.in.Yard {
				if r.in.Yard[i].Row == "1" && r.in.Yard[i].Caller == "admin" {
					r.in.Yard[i].Verdict = VerdictNoPDP
				}
			}
		},
		"the yardstick cell is a skip": func(r *enfRun) {
			for i := range r.in.Yard {
				if r.in.Yard[i].Row == "1" && r.in.Yard[i].Caller == "admin" {
					r.in.Yard[i].Verdict = VerdictSkipped
				}
			}
		},
		"an unattributed extra request": func(r *enfRun) {
			r.in.Log = j1Log(t, r, httpExtra(enfT0.Add(-time.Minute), "POST", "/api/v1/auth/login", 200))
		},
		"a request of a counted route with no slot": func(r *enfRun) {
			r.in.Log = j1Log(t, r, httpExtra(enfT0.Add(time.Hour), "GET", "/api/v1/admin/teams", 200))
		},
		"no access lines in the log": func(r *enfRun) {
			r.in.Log = logOf(t, result(r.in.Slots[0], 0, "match", "dec_X"+strings.Repeat("A", 20)))
		},
		"no go-api log": func(r *enfRun) { r.in.Log = nil },
	}
	for name, plant := range cases {
		t.Run(name, func(t *testing.T) {
			r := j1Run(t)
			plant(r)
			rep := r.judge()
			if rep.Pass {
				t.Fatalf("the mutant passes: %v %v", rep.VerdictCounts(), rep.Reasons)
			}
			// A decision with no slot fails the run on its own; a skip is a different verdict; the others leave the three
			// cells without proof.
			switch {
			case name == "a decision of a namespace with no slot":
			case name == "the yardstick cell is a skip":
				if rep.VerdictCounts()[VerdictEqualPastGuard] != 0 {
					t.Fatalf("%v", rep.VerdictCounts())
				}
			case rep.VerdictCounts()[VerdictEqualPastGuard] != 0 || rep.VerdictCounts()[VerdictVoidParity] != 3:
				t.Fatalf("the three cells must stay VOID_PARITY: %v %v", rep.VerdictCounts(), rep.Reasons)
			}
		})
	}
}

func j1Log(t *testing.T, r *enfRun, extra ...string) *Evidence {
	t.Helper()
	var lines []string
	for _, s := range r.in.Slots {
		if !s.Gap() {
			lines = append(lines, httpLine(s, 20*time.Millisecond))
		}
	}
	return logOf(t, append(lines, extra...)...)
}

// The drive's own sign-in is the one request with no slot: attributed (file:line) and outside the counted namespaces it
// changes nothing; an attributed request of a counted namespace is counted by the proof, so a counter that does not
// include it is no proof.
func TestJ1AttributedExtraRequests(t *testing.T) {
	r := j1Run(t)
	r.in.Log = j1Log(t, r, httpExtra(enfT0.Add(-time.Minute), "POST", "/api/v1/auth/login", 200))
	r.in.Attributed = []AttributedRequest{{Method: "POST", Route: "/api/v1/auth/login", Source: "internal/shadowvenue/drive.go:1"}}
	rep := r.judge()
	if rep.Void || !rep.Pass || rep.VerdictCounts()[VerdictEqualPastGuard] != 3 {
		t.Fatalf("void %v pass %v %v %v", rep.Void, rep.Pass, rep.VerdictCounts(), rep.Reasons)
	}
	if !strings.Contains(strings.Join(rep.Notes, "\n"), "1 attributed extra, 0 unattributed") {
		t.Fatalf("%v", rep.Notes)
	}
	counted := j1Run(t)
	counted.in.Log = j1Log(t, counted, httpExtra(enfT0.Add(time.Hour), "GET", "/api/v1/admin/teams", 200))
	counted.in.Attributed = []AttributedRequest{{Method: "GET", Route: "/api/v1/admin/teams", Source: "x.go:1"}}
	if rep := counted.judge(); rep.Pass || rep.VerdictCounts()[VerdictEqualPastGuard] != 0 {
		t.Fatalf("a counted-namespace extra the counter does not include is no proof: %v %v", rep.Pass, rep.VerdictCounts())
	}
	counted.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"),
		decisions("teams", "allow", "4"), decisions("teams", "deny", "6"), decisions("integrations", "deny", "3"))
	if rep := counted.judge(); !rep.Pass || rep.VerdictCounts()[VerdictEqualPastGuard] != 3 {
		t.Fatalf("a counter that includes the attributed extra stands: %v %v %v", rep.Pass, rep.VerdictCounts(), rep.Reasons)
	}
	bad := j1Run(t)
	bad.in.Attributed = []AttributedRequest{{Method: "POST", Route: "/api/v1/auth/login", Source: "x.go:1"}}
	if rep := bad.judge(); !rep.Pass {
		t.Fatalf("an attribution nobody used is harmless: %v", rep.Reasons)
	}
}

func TestParseDecisionDeltasAndAttributed(t *testing.T) {
	before := scrape(decisions("teams", "allow", "1"))
	after := scrape(decisions("teams", "allow", "4"), decisions("admin", "deny", "2"), `dev_health_authz_enforcement_decisions_total{namespace="x",outcome="allow",service="other"} 9`)
	got, err := ParseDecisionDeltas(before, after)
	if err != nil || got["teams"]["allow"] != 3 || got["admin"]["deny"] != 2 || got["x"] != nil {
		t.Fatalf("%v %v", got, err)
	}
	if _, err := ParseDecisionDeltas(after, before); err == nil {
		t.Fatal("a counter that went down is refused")
	}
	if _, err := ParseDecisionDeltas(scrape(`dev_health_authz_enforcement_decisions_total{namespace=teams,outcome="allow",service="go-api"} 1`), after); err == nil {
		t.Fatal("an unquoted label is refused")
	}
	if _, err := ParseDecisionDeltas(scrape(decisions("teams", "allow", "1.5")), after); err == nil {
		t.Fatal("a fractional value is refused")
	}
	if a, err := ParseAttributed([]byte("# sign-in\nPOST\t/api/v1/auth/login\tdrive.go:1\treq-1\n")); err != nil || len(a) != 1 || a[0].RequestID != "req-1" {
		t.Fatalf("%v %v", a, err)
	}
	if _, err := ParseAttributed([]byte("POST\t/x\n")); err == nil {
		t.Fatal("a line without a source is refused")
	}
}
