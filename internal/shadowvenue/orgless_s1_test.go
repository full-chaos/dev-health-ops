package shadowvenue

import (
	"strings"
	"testing"
	"time"
)

// CHAOS-10872 S1: on a go-api build that decides a token with no organization, the superuser's no-org slots of an
// organization row are no longer unjudged no_org. An AdminOrg row refuses them with no question, counted unjudged
// org_required; any other organization row asks the decision point, which counts a decision in the row's namespace and
// no unjudged reason. The judge accepts either build, per run, and never a mix.

// decidedNoOrg is the verdict of a no-org slot an S1 build decided (VerdictDecidedNoOrg).
const decidedNoOrg = "DECIDED_NOORG"

// s1Run is newEnfRun on an S1 build: the no-org route (billing) counts no unjudged reason and its namespace counts three
// more decisions; the seam route counts nothing either (it is decided too).
func s1Run(decided string) *enfRun {
	r := newEnfRun()
	r.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", unjudged(seamKey, "served_org", "2"), decisions("billing", "allow", "10"), decisions("billing", "deny", "1"))
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(seamKey, "served_org", "2"), decisions("billing", "allow", decided), decisions("billing", "deny", "1"))
	return r
}

func TestEnforcedS1DecidedNoOrgSlotsPassOnTheirNamespaceDecisions(t *testing.T) {
	report := s1Run("13").judge()
	if report.Void || !report.Pass || report.VerdictCounts()[decidedNoOrg] != 3 || report.VerdictCounts()[VerdictUnevidencedNoOrg] != 0 {
		t.Fatalf("void %v pass %v counts %v %v", report.Void, report.Pass, report.VerdictCounts(), report.Reasons)
	}
}

// A measurement that did not happen must fail: a no-org route with no unjudged count passes only when its namespace
// counted at least that many decisions.
func TestEnforcedS1DecidedNoOrgSlotsFailWithoutTheDecisions(t *testing.T) {
	for name, decided := range map[string]string{"two decisions for three slots": "12", "no decision": "10"} {
		t.Run(name, func(t *testing.T) {
			report := s1Run(decided).judge()
			if report.Pass || report.VerdictCounts()[VerdictUnevidencedNoOrg] != 3 || report.VerdictCounts()[decidedNoOrg] != 0 {
				t.Fatalf("pass %v counts %v", report.Pass, report.VerdictCounts())
			}
		})
	}
	r := s1Run("13")
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(seamKey, "served_org", "2"))
	if report := r.judge(); report.Pass || report.VerdictCounts()[decidedNoOrg] != 0 {
		t.Fatalf("no decision series: pass %v counts %v", report.Pass, report.VerdictCounts())
	}
}

// An AdminOrg row's refusal is counted per route: exactly one org_required per slot, and no other reason.
func TestEnforcedS1AdminOrgRefusalPassesOnOrgRequired(t *testing.T) {
	r := newEnfRun()
	r.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", unjudged(noOrgKey, "org_required", "4"))
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "org_required", "7"))
	if report := r.judge(); report.Void || !report.Pass || report.VerdictCounts()[decidedNoOrg] != 3 {
		t.Fatalf("void %v pass %v counts %v %v", report.Void, report.Pass, report.VerdictCounts(), report.Reasons)
	}
	for name, after := range map[string][]byte{
		"one short":             live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "org_required", "6")),
		"one over":              live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "org_required", "8")),
		"another reason grew":   live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "org_required", "7"), unjudged(noOrgKey, "hs256", "1")),
		"no_org grew with it":   live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "org_required", "7"), unjudged(noOrgKey, "no_org", "1")),
		"delegated_orgless too": live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "org_required", "7"), unjudged(noOrgKey, "delegated_orgless", "1")),
	} {
		t.Run(name, func(t *testing.T) {
			run := newEnfRun()
			run.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", unjudged(noOrgKey, "org_required", "4"))
			run.in.MetricsAfter = after
			if report := run.judge(); report.Pass || report.VerdictCounts()[decidedNoOrg] != 0 {
				t.Fatalf("pass %v counts %v", report.Pass, report.VerdictCounts())
			}
		})
	}
}

// A run is one build: a no-org route judged the legacy way (no_org) beside one judged the S1 way fails the run.
func TestEnforcedLegacyAndS1NoOrgRoutesInOneRunFail(t *testing.T) {
	const otherKey = "GET /api/v1/admin/settings/{category}"
	r := newEnfRun()
	for k := 1; k <= 3; k++ {
		start := enfT0.Add(time.Duration(len(r.in.Slots)) * 3 * time.Second)
		r.in.Yard = append(r.in.Yard, YardSlot{Seq: len(r.in.Yard) + 1, Row: "4", Caller: "superuser", K: k, Key: otherKey, Status: 403, Verdict: VerdictSkipped})
		r.in.Slots = append(r.in.Slots, Slot{Start: start, End: start.Add(100 * time.Millisecond), Seq: len(r.in.Slots) + 1, Row: "4", Caller: "superuser",
			Principal: "superuser-A", Expect: "match", K: k, Method: "GET", Route: "/api/v1/admin/settings/{category}", Path: "/api/v1/admin/settings/general", Status: 403})
	}
	r.in.Namespaces[otherKey] = "settings"
	r.in.Outbound = Outbound{Rows: tableOf(r.in.Slots, nil)}
	r.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", unjudged(noOrgKey, "no_org", "5"), unjudged(seamKey, "served_org", "2"), unjudged(otherKey, "org_required", "1"))
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"), unjudged(otherKey, "org_required", "4"))
	report := r.judge()
	if report.Pass || !strings.Contains(strings.Join(report.Reasons, "\n"), "one build") {
		t.Fatalf("a mixed run passed or gave no reason: pass %v %v %v", report.Pass, report.VerdictCounts(), report.Reasons)
	}
}

// org_required is a no-org superuser's count like no_org: it does not take the allow proof from the route's other 5xx
// cells.
func TestEnforcedOrgRequiredOnTheRouteKeepsTheAllowProof(t *testing.T) {
	run := fiveXXRun(500)
	run.in.Log = adminLines(t, run, "match", "dec_ALLOW"+strings.Repeat("A", 20))
	run.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"), unjudged(teamsKey, "org_required", "1"))
	rep := run.judge()
	if rep.VerdictCounts()[VerdictEqualPastGuard] != 3 || rep.VerdictCounts()[VerdictVoidParity] != 0 {
		t.Fatalf("an org_required count voided the parity: %v", rep.VerdictCounts())
	}
}
func TestUnjudgedTotalsCountOrgRequiredApart(t *testing.T) {
	before := scrape(unjudged("GET /a", "org_required", "1"))
	after := scrape(unjudged("GET /a", "org_required", "4"), unjudged("GET /a", "no_org", "2"), unjudged("GET /b", "delegated_orgless", "1"))
	totals, err := UnjudgedTotals(before, after)
	if err != nil || totals.NoOrg != 2 || totals.ServedOrg != 0 || totals.OrgRequired != 3 || totals.Other != 1 {
		t.Fatalf("totals %+v err %v, want no_org 2, served_org 0, org_required 3, other 1", totals, err)
	}
}

// The verdict the cells above read is the judge's, and it passes.
func TestDecidedNoOrgIsTheJudgesVerdict(t *testing.T) {
	if decidedNoOrg != VerdictDecidedNoOrg || !(SlotVerdict{Verdict: VerdictDecidedNoOrg}).Pass() {
		t.Fatalf("decidedNoOrg %q, VerdictDecidedNoOrg %q", decidedNoOrg, VerdictDecidedNoOrg)
	}
}

// A decided slot may be a refusal (a caller the decision point denies admin.orgs): deny decisions count as evidence too.
func TestEnforcedS1DecidedNoOrgSlotsCountDenyDecisions(t *testing.T) {
	r := newEnfRun()
	r.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", unjudged(seamKey, "served_org", "2"), decisions("billing", "allow", "10"), decisions("billing", "deny", "1"))
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(seamKey, "served_org", "2"), decisions("billing", "allow", "10"), decisions("billing", "deny", "4"))
	if report := r.judge(); !report.Pass || report.VerdictCounts()[decidedNoOrg] != 3 {
		t.Fatalf("pass %v counts %v %v", report.Pass, report.VerdictCounts(), report.Reasons)
	}
}

// A decided route passes on no unjudged growth only: an unjudged count of any reason beside the namespace decisions
// fails the slots, however many decisions there are.
func TestEnforcedS1DecidedNoOrgSlotsFailOnAnyUnjudgedGrowth(t *testing.T) {
	for _, reason := range []string{"hs256", "no_org", "org_required", "delegated_orgless"} {
		t.Run(reason, func(t *testing.T) {
			r := s1Run("13")
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(seamKey, "served_org", "2"), unjudged(noOrgKey, reason, "1"),
				decisions("billing", "allow", "13"), decisions("billing", "deny", "1"))
			if report := r.judge(); report.Pass || report.VerdictCounts()[decidedNoOrg] != 0 {
				t.Fatalf("pass %v counts %v", report.Pass, report.VerdictCounts())
			}
		})
	}
}

// Only the venue's org-less caller, the superuser, is decided this way: a skip of any other caller keeps failing on an
// S1 build, however the counters read.
func TestEnforcedS1DecidedNoOrgIsTheSuperusersOnly(t *testing.T) {
	r := s1Run("13")
	for i := range r.in.Yard {
		if r.in.Yard[i].Row == "1" && r.in.Yard[i].Caller == "admin" {
			r.in.Yard[i].Verdict = VerdictSkipped
		}
	}
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(seamKey, "served_org", "2"), decisions("billing", "allow", "13"), decisions("billing", "deny", "1"),
		decisions("teams", "allow", "3"))
	if report := r.judge(); report.Pass || report.VerdictCounts()[decidedNoOrg] != 3 || report.VerdictCounts()[VerdictUnevidencedNoOrg] != 3 {
		t.Fatalf("pass %v counts %v", report.Pass, report.VerdictCounts())
	}
}
