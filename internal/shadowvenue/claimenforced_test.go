package shadowvenue

import (
	"strings"
	"testing"
	"time"
)

const (
	bodyA = "1111111111111111111111111111111111111111111111111111111111111111"
	bodyB = "2222222222222222222222222222222222222222222222222222222222222222"
)

// strictEnfRun is newEnfRun with the claim read-back and body digests of a healthy pair, and the yardstick's digests.
func strictEnfRun() *enfRun {
	r := newEnfRun()
	r.in.Strict = true
	r.in.Grace = 2200 * time.Millisecond
	r.in.Slots = claimsOf(r.in.Slots)
	for i := range r.in.Slots {
		if !r.in.Slots[i].Gap() {
			r.in.Slots[i].BodyNormSHA = bodyA
		}
	}
	for i := range r.in.Yard {
		r.in.Yard[i].BodyNorm = bodyA
	}
	return r
}

func logOf(t *testing.T, lines ...string) *Evidence {
	t.Helper()
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return &ev
}

func TestEnforcedBodyParityPassesEqualBodies(t *testing.T) {
	r := strictEnfRun().judge()
	if r.Void || !r.Pass {
		t.Fatalf("void %v pass %v %v", r.Void, r.Pass, r.Reasons)
	}
}

func TestEnforcedBodyDefectsAreRed(t *testing.T) {
	cases := map[string]struct {
		plant func(*enfRun)
		void  bool
		want  string
	}{
		"a body differs on a status-equal cell": {func(r *enfRun) {
			for i := range r.in.Slots {
				if r.in.Slots[i].Row == "1" && r.in.Slots[i].Caller == "member" && r.in.Slots[i].K == 2 {
					r.in.Slots[i].BodyNormSHA = bodyB
				}
			}
		}, false, VerdictMismatchBody},
		"a slot without a body digest":           {func(r *enfRun) { r.in.Slots[6].BodyNormSHA = "" }, true, "body digest"},
		"a yardstick slot without a body digest": {func(r *enfRun) { r.in.Yard[6].BodyNorm = "" }, true, "body digest"},
		"a slot without a claim read-back":       {func(r *enfRun) { r.in.Slots[6].ClaimSub = "" }, true, "claim"},
		"a wrong claim org":                      {func(r *enfRun) { r.in.Slots[6].ClaimOrg = claimOrgE }, false, VerdictMismatchCaller},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			run := strictEnfRun()
			c.plant(run)
			report := run.judge()
			text := strings.ToLower(strings.Join(report.Reasons, "\n"))
			if c.void {
				if !report.Void || report.Exit() != 3 || !strings.Contains(text, c.want) {
					t.Fatalf("want void with %q: void %v exit %d %v", c.want, report.Void, report.Exit(), report.Reasons)
				}
				return
			}
			if report.Void || report.Pass || report.VerdictCounts()[c.want] == 0 {
				t.Fatalf("want a failed run with %s: void %v pass %v %v", c.want, report.Void, report.Pass, report.VerdictCounts())
			}
		})
	}
}

// A 5xx cell that is equal in both runs is EQUAL_PAST_GUARD only when the go-api log shows the decision point allowed
// the request; a PDP-unavailable 503 is never equal.
func fiveXXRun(status int) *enfRun {
	r := strictEnfRun()
	for i := range r.in.Slots {
		if r.in.Slots[i].Row == "1" && r.in.Slots[i].Caller == "admin" {
			r.in.Slots[i].Status = status
		}
	}
	for i := range r.in.Yard {
		if r.in.Yard[i].Row == "1" && r.in.Yard[i].Caller == "admin" {
			r.in.Yard[i].Status = status
		}
	}
	return r
}

func adminLines(t *testing.T, r *enfRun, outcome, decision string) *Evidence {
	var lines []string
	for _, s := range r.in.Slots {
		if s.Row == "1" && s.Caller == "admin" {
			lines = append(lines, result(s, 200*time.Millisecond, outcome, decision))
		}
	}
	return logOf(t, lines...)
}

func TestEnforcedFiveXXIsEqualPastTheGuardOnlyWithTheAllowProof(t *testing.T) {
	run := fiveXXRun(500)
	run.in.Log = adminLines(t, run, "match", "dec_ALLOW"+strings.Repeat("A", 20))
	r := run.judge()
	if r.Void || !r.Pass || r.VerdictCounts()[VerdictEqualPastGuard] != 3 {
		t.Fatalf("void %v pass %v counts %v %v", r.Void, r.Pass, r.VerdictCounts(), r.Reasons)
	}
	for name, plant := range map[string]func(*enfRun){
		"no go-api log given":           func(r *enfRun) { r.in.Log = nil },
		"the shadow skipped the slot":   func(r *enfRun) { r.in.Log = adminLines(t, r, "skipped", "") },
		"a match line without decision": func(r *enfRun) { r.in.Log = adminLines(t, r, "match", "") },
		"no result line in the window": func(r *enfRun) {
			r.in.Log = logOf(t, logLine(enfT0.Add(time.Hour), MsgResult, "route", "GET /x", "caller", "admin", "result", "match", "decision_id", "dec_x"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			run := fiveXXRun(500)
			plant(run)
			rep := run.judge()
			if rep.Pass || rep.VerdictCounts()[VerdictVoidParity] != 3 || rep.VerdictCounts()[VerdictEqualPastGuard] != 0 {
				t.Fatalf("want VOID_PARITY x3: pass %v %v", rep.Pass, rep.VerdictCounts())
			}
		})
	}
	t.Run("an unjudged count of another reason on the route", func(t *testing.T) {
		run := fiveXXRun(500)
		run.in.Log = adminLines(t, run, "match", "dec_ALLOW"+strings.Repeat("A", 20))
		run.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged(teamsKey, "impersonation", "1"))
		rep := run.judge()
		if rep.VerdictCounts()[VerdictVoidParity] != 3 || rep.VerdictCounts()[VerdictEqualPastGuard] != 0 {
			t.Fatalf("an impersonation unjudged count on the route did not void the parity: %v", rep.VerdictCounts())
		}
	})
	for name, outcome := range map[string]string{"an error line": "error", "a dropped line": "dropped"} {
		t.Run(name, func(t *testing.T) {
			run := fiveXXRun(503)
			run.in.Log = adminLines(t, run, outcome, "")
			rep := run.judge()
			if rep.Pass || rep.VerdictCounts()[VerdictPDPUnavailable] != 3 {
				t.Fatalf("a PDP-unavailable 503 was equal: pass %v %v", rep.Pass, rep.VerdictCounts())
			}
		})
	}
}

func TestEnforcedFourXXNeedsNoLogProof(t *testing.T) {
	run := fiveXXRun(404)
	if rep := run.judge(); rep.Void || !rep.Pass || rep.VerdictCounts()[VerdictEqualPastGuard] != 0 {
		t.Fatalf("void %v pass %v %v %v", rep.Void, rep.Pass, rep.VerdictCounts(), rep.Reasons)
	}
}

// A 5xx cell that the yardstick skipped (the no-org superuser) passes on the counter, with no log proof.
func TestEnforcedFiveXXOfASkippedCellNeedsNoLogProof(t *testing.T) {
	run := strictEnfRun()
	for i := range run.in.Slots {
		if run.in.Slots[i].Row == "3" {
			run.in.Slots[i].Status = 502
		}
	}
	for i := range run.in.Yard {
		if run.in.Yard[i].Row == "3" {
			run.in.Yard[i].Status = 502
		}
	}
	rep := run.judge()
	if rep.Void || !rep.Pass || rep.VerdictCounts()[VerdictDecidedNoOrg] != 3 || rep.VerdictCounts()[VerdictVoidParity] != 0 {
		t.Fatalf("void %v pass %v %v %v", rep.Void, rep.Pass, rep.VerdictCounts(), rep.Reasons)
	}
}

func TestEnforcedAllowProofIsOneResultLineOfTheSlotsRouteAndCallerKind(t *testing.T) {
	for name, plant := range map[string]func(*enfRun, *[]string){
		"two result lines in the window": func(r *enfRun, lines *[]string) {
			for _, s := range r.in.Slots {
				if s.Row == "1" && s.Caller == "admin" && s.K == 1 {
					*lines = append(*lines, result(s, 300*time.Millisecond, "match", "dec_TWO"+strings.Repeat("B", 20)))
				}
			}
		},
		"a line of another route in the window": func(r *enfRun, lines *[]string) {
			*lines = (*lines)[1:]
			for _, s := range r.in.Slots {
				if s.Row == "1" && s.Caller == "admin" && s.K == 1 {
					other := s
					other.Route = "/api/v1/admin/integrations"
					*lines = append(*lines, result(other, 200*time.Millisecond, "match", "dec_ROUTE"+strings.Repeat("C", 18)))
				}
			}
		},
		"a line of another caller kind in the window": func(r *enfRun, lines *[]string) {
			*lines = (*lines)[1:]
			for _, s := range r.in.Slots {
				if s.Row == "1" && s.Caller == "admin" && s.K == 1 {
					other := s
					other.Caller = "member"
					*lines = append(*lines, result(other, 200*time.Millisecond, "match", "dec_KIND"+strings.Repeat("D", 19)))
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			run := fiveXXRun(500)
			var lines []string
			for _, s := range run.in.Slots {
				if s.Row == "1" && s.Caller == "admin" {
					lines = append(lines, result(s, 200*time.Millisecond, "match", "dec_ALLOW"+strings.Repeat("A", 20)))
				}
			}
			plant(run, &lines)
			run.in.Log = logOf(t, lines...)
			rep := run.judge()
			if rep.VerdictCounts()[VerdictVoidParity] == 0 {
				t.Fatalf("an allow proof was taken from a wrong line: %v", rep.VerdictCounts())
			}
		})
	}
}

func TestParseYardstickRefusesTheV1Header(t *testing.T) {
	if _, err := ParseYardstick([]byte(verdictsHeaderV1 + "\n1\tA\tmember\t1\tGET /a\t200\tMATCH\n")); err == nil || !strings.Contains(err.Error(), "v1") {
		t.Fatalf("a v1 yardstick was read: %v", err)
	}
}
