package shadowvenue

import (
	"strings"
	"testing"
)

// CHAOS-10872 S1: on a go-api build that decides a token with no organization, the superuser's no-org slots of an
// organization row are no longer unjudged no_org. An AdminOrg row refuses them with no question, counted unjudged
// org_required; any other organization row asks the decision point, which counts a decision in the row's namespace and
// no unjudged reason. Since CHAOS-13673 the judge judges only such builds: a no_org series voids the run.

// decidedNoOrg is the verdict of a no-org slot an S1 build decided (VerdictDecidedNoOrg).
const decidedNoOrg = "DECIDED_NOORG"

// s1Run is newEnfRun on an S1 build: the no-org route (billing) counts no unjudged reason and its namespace counts three
// more decisions; the seam route counts nothing either (it is decided too, CHAOS-10872 S2).
func s1Run(decided string) *enfRun {
	r := newEnfRun()
	r.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", decisions("billing", "allow", "10"), decisions("billing", "deny", "1"))
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", decisions("billing", "allow", decided), decisions("billing", "deny", "1"))
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
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0")
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
		"another reason grew":   live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "org_required", "7"), unjudged(noOrgKey, "impersonation", "1")),
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

// CHAOS-13673: a no-org route counted the legacy way (no_org) is a build before CHAOS-13596. The judge no longer judges
// it, so the run is void, whatever the other routes count.
func TestEnforcedALegacyNoOrgCountVoidsTheRun(t *testing.T) {
	r := s1Run("13")
	r.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", unjudged(noOrgKey, "no_org", "5"), decisions("billing", "allow", "10"), decisions("billing", "deny", "1"))
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), decisions("billing", "allow", "13"), decisions("billing", "deny", "1"))
	report := r.judge()
	if !report.Void || report.Exit() != 3 || !strings.Contains(strings.Join(report.Reasons, "\n"), "no_org") {
		t.Fatalf("a legacy no_org count was judged: void %v exit %d %v", report.Void, report.Exit(), report.Reasons)
	}
}

// org_required is a no-org superuser's count: it does not take the allow proof from the route's other 5xx
// cells.
func TestEnforcedOrgRequiredOnTheRouteKeepsTheAllowProof(t *testing.T) {
	run := fiveXXRun(500)
	run.in.Log = adminLines(t, run, "match", "dec_ALLOW"+strings.Repeat("A", 20))
	run.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged(teamsKey, "org_required", "1"))
	rep := run.judge()
	if rep.VerdictCounts()[VerdictEqualPastGuard] != 3 || rep.VerdictCounts()[VerdictVoidParity] != 0 {
		t.Fatalf("an org_required count voided the parity: %v", rep.VerdictCounts())
	}
}
func TestUnjudgedTotalsCountOrgRequiredApart(t *testing.T) {
	before := scrape(unjudged("GET /a", "org_required", "1"))
	after := scrape(unjudged("GET /a", "org_required", "4"), unjudged("GET /a", "impersonation", "2"), unjudged("GET /b", "delegated_orgless", "1"))
	totals, err := UnjudgedTotals(before, after)
	if err != nil || totals.OrgRequired != 3 || totals.Other != 3 {
		t.Fatalf("totals %+v err %v, want org_required 3, other 3", totals, err)
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
	r.in.MetricsBefore = live(allEnforced, 1000, "f50883b", "0", decisions("billing", "allow", "10"), decisions("billing", "deny", "1"))
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", decisions("billing", "allow", "10"), decisions("billing", "deny", "4"))
	if report := r.judge(); !report.Pass || report.VerdictCounts()[decidedNoOrg] != 3 {
		t.Fatalf("pass %v counts %v %v", report.Pass, report.VerdictCounts(), report.Reasons)
	}
}

// A decided route passes on no unjudged growth only: an unjudged count of any reason beside the namespace decisions
// fails the slots, however many decisions there are.
func TestEnforcedS1DecidedNoOrgSlotsFailOnAnyUnjudgedGrowth(t *testing.T) {
	for _, reason := range []string{"impersonation", "org_required", "delegated_orgless"} {
		t.Run(reason, func(t *testing.T) {
			r := s1Run("13")
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, reason, "1"),
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
	r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", decisions("billing", "allow", "13"), decisions("billing", "deny", "1"),
		decisions("teams", "allow", "3"))
	if report := r.judge(); report.Pass || report.VerdictCounts()[decidedNoOrg] != 3 || report.VerdictCounts()[VerdictUnevidencedNoOrg] != 3 {
		t.Fatalf("pass %v counts %v", report.Pass, report.VerdictCounts())
	}
}
