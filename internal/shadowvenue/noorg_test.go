package shadowvenue

import (
	"strings"
	"testing"
	"time"
)

const unjudgedHelp = "# HELP dev_health_authz_enforcement_unjudged_total x\n# TYPE dev_health_authz_enforcement_unjudged_total counter\n"

func scrape(lines ...string) []byte { return []byte(unjudgedHelp + strings.Join(lines, "\n") + "\n") }

func unjudged(key, reason string, n string) string {
	return `dev_health_authz_enforcement_unjudged_total{reason="` + reason + `",route="` + key + `"} ` + n
}

func TestParseUnjudgedDeltaCountsOnlyNoOrgByRouteAndRefusesADecreasingCounter(t *testing.T) {
	const k = "GET /api/v1/admin/teams"
	before := scrape(unjudged(k, "no_org", "4"), unjudged(k, "served_org", "9"), unjudged("GET /api/v1/billing/invoices", "no_org", "1"))
	after := scrape(unjudged(k, "no_org", "7"), unjudged(k, "served_org", "20"), unjudged("GET /api/v1/billing/invoices", "no_org", "1"), unjudged("POST /api/v1/x", "no_org", "3"))
	got, err := ParseUnjudgedDelta(before, after)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]int{k: 3, "GET /api/v1/billing/invoices": 0, "POST /api/v1/x": 3} {
		if n, ok := got.Delta(key); !ok || n != want {
			t.Errorf("%s: delta %d (found %v), want %d", key, n, ok, want)
		}
	}
	if _, ok := got.Delta("GET /nowhere"); ok {
		t.Error("a route with no series reported a delta")
	}
	if _, err := ParseUnjudgedDelta(after, before); err == nil {
		t.Error("a counter that went down (a restart) was accepted")
	}
	if _, err := ParseUnjudgedDelta(scrape(unjudged(k, "no_org", "7")), scrape(unjudged(k, "no_org", "4"))); err == nil {
		t.Error("a counter that went down on a route in both scrapes was accepted")
	}
	if _, err := ParseUnjudgedDelta(scrape(unjudged(k, "no_org", "7")), scrape()); err == nil {
		t.Error("a series that vanished between the scrapes was accepted")
	}
	if _, err := ParseUnjudgedDelta(scrape(), after); err != nil {
		t.Errorf("an empty before scrape: %v", err)
	}
	if d, err := ParseUnjudgedDelta([]byte("dev_health_runtime_uptime_seconds 5\n"), after); err != nil || d[k] != 7 {
		t.Errorf("a before scrape with no unjudged series is a scrape of zero (the counter is created at its first increment): %v %v", d, err)
	}
}

// noOrgRun judges n superuser skips on key (plus the controls) with the counter delta given.
func noOrgRun(t *testing.T, caller, key string, n int, evidence NoOrgEvidence) Report {
	t.Helper()
	slots, lines := controlsOK()
	method, route, _ := strings.Cut(key, " ")
	var cell []Slot
	for i := 0; i < n; i++ {
		s := slotAt(7+i, "23", caller, i+1)
		s.Method, s.Route, s.Path = method, route, route
		cell = append(cell, s)
		lines = append(lines, result(s, 300*time.Millisecond, "skipped", ""))
	}
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return Judge(append(slots, cell...), ev, nil, JudgeOptions{K: n, Grace: 2200 * time.Millisecond, NoOrg: evidence})
}

type fixedDelta map[string]int

func (f fixedDelta) Delta(key string) (int, bool) { n, ok := f[key]; return n, ok }

// SKIPPED_NOORG passes only for the superuser, only on skips the enforcement counter counted as no_org on that very
// route, and only when the counter delta equals the number of those skips.
func TestJudgePassesANoOrgSkipOnlyWhenTheCounterDeltaEqualsTheSlots(t *testing.T) {
	const key = "GET /api/v1/billing/invoices"
	verdicts := func(r Report) string {
		var out []string
		for _, sv := range r.Slots {
			if sv.Row == "23" {
				out = append(out, sv.Verdict)
			}
		}
		return strings.Join(out, " ")
	}
	if got, want := verdicts(noOrgRun(t, "superuser", key, 3, fixedDelta{key: 3})), strings.Join([]string{VerdictSkippedNoOrg, VerdictSkippedNoOrg, VerdictSkippedNoOrg}, " "); got != want {
		t.Errorf("delta equal: %s, want %s", got, want)
	}
	for name, ev := range map[string]NoOrgEvidence{
		"delta one short": fixedDelta{key: 2}, "delta one over": fixedDelta{key: 4}, "delta zero": fixedDelta{key: 0},
		"other route only": fixedDelta{"GET /api/v1/billing/invoices/{invoice_id}": 3}, "no evidence": nil,
	} {
		for _, v := range strings.Fields(verdicts(noOrgRun(t, "superuser", key, 3, ev))) {
			if v != VerdictSkipped {
				t.Errorf("%s: %s, want SKIPPED (fail)", name, v)
			}
		}
	}
	for _, caller := range []string{"member", "admin"} {
		for _, v := range strings.Fields(verdicts(noOrgRun(t, caller, key, 3, fixedDelta{key: 3}))) {
			if v != VerdictSkipped {
				t.Errorf("%s skip passed on a no_org counter: %s", caller, v)
			}
		}
	}
	if counts := noOrgRun(t, "superuser", key, 3, fixedDelta{key: 3}).VerdictCounts(); counts[VerdictSkippedNoOrg] != 3 || counts[VerdictMatch] != 0 {
		t.Errorf("counts %v", counts)
	}
}

// A seam row keeps its own verdict even when the counter also names it.
func TestNoOrgEvidenceDoesNotRelabelASeamSkip(t *testing.T) {
	const key = "GET /api/v1/admin/orgs/{org_id}/members"
	r := noOrgRun(t, "superuser", key, 3, fixedDelta{key: 3})
	for _, sv := range r.Slots {
		if sv.Row == "23" && sv.Verdict != VerdictSkippedSeam {
			t.Errorf("seam skip became %s", sv.Verdict)
		}
	}
}

// The record shows the org_required count on its own line: every other reason is summed apart.
func TestUnjudgedTotalsKeepOrgRequiredApart(t *testing.T) {
	before := scrape(unjudged("GET /a", "org_required", "1"), unjudged("GET /b", "impersonation", "0"))
	after := scrape(unjudged("GET /a", "org_required", "6"), unjudged("GET /b", "org_required", "2"), unjudged("GET /b", "impersonation", "3"), unjudged("GET /b", "delegated_orgless", "1"))
	sums, err := UnjudgedTotals(before, after)
	if err != nil || sums.OrgRequired != 7 || sums.Other != 4 {
		t.Fatalf("sums %+v err %v, want org_required 7, other 4", sums, err)
	}
}
