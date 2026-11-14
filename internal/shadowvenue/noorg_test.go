package shadowvenue

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

const unjudgedHelp = "# HELP dev_health_authz_enforcement_unjudged_total x\n# TYPE dev_health_authz_enforcement_unjudged_total counter\n"

func scrape(lines ...string) []byte { return []byte(unjudgedHelp + strings.Join(lines, "\n") + "\n") }

func unjudged(key, reason string, n string) string {
	return `dev_health_authz_enforcement_unjudged_total{reason="` + reason + `",route="` + key + `"} ` + n
}

func TestParseUnjudgedReasonsCountsByRouteAndReasonAndRefusesADecreasingCounter(t *testing.T) {
	const k = "GET /api/v1/admin/teams"
	before := scrape(unjudged(k, "org_required", "4"), unjudged("GET /api/v1/billing/invoices", "impersonation", "1"))
	after := scrape(unjudged(k, "org_required", "7"), unjudged("GET /api/v1/billing/invoices", "impersonation", "1"), unjudged("POST /api/v1/x", "delegated_orgless", "3"))
	got, err := ParseUnjudgedReasons(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if got[k]["org_required"] != 3 || got["GET /api/v1/billing/invoices"]["impersonation"] != 0 || got["POST /api/v1/x"]["delegated_orgless"] != 3 {
		t.Errorf("deltas %v", got)
	}
	if _, err := ParseUnjudgedReasons(after, before); err == nil {
		t.Error("a counter that went down (a restart) was accepted")
	}
	if _, err := ParseUnjudgedReasons(scrape(unjudged(k, "org_required", "7")), scrape()); err == nil {
		t.Error("a series that vanished between the scrapes was accepted")
	}
	if d, err := ParseUnjudgedReasons([]byte("dev_health_runtime_uptime_seconds 5\n"), after); err != nil || d[k]["org_required"] != 7 {
		t.Errorf("a before scrape with no unjudged series is a scrape of zero (the counter is created at its first increment): %v %v", d, err)
	}
}

// skipRun judges n skips of caller on key (plus the controls).
func skipRun(t *testing.T, caller, key string, n int) Report {
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
	return Judge(append(slots, cell...), ev, nil, JudgeOptions{K: n, Grace: 2200 * time.Millisecond})
}

// CHAOS-13675: the shadow decides a token with no organization as the enforcer does, so a skip is no longer the
// superuser's no-organization evidence. A skip of any caller, on a no-organization route or on a former seam row,
// stays SKIPPED and fails the run: nothing relabels it.
func TestJudgeFailsEverySkipAndRelabelsNone(t *testing.T) {
	for _, key := range []string{
		"GET /api/v1/billing/invoices", "GET /api/v1/admin/teams",
		"GET /api/v1/admin/orgs/{org_id}/members", "POST /api/v1/admin/orgs/{org_id}/members",
		"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}", "PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}",
		"GET /api/v1/billing/entitlements/{org_id}", "GET /api/v1/licensing/entitlements/{org_id}",
	} {
		for _, caller := range []string{"superuser", "admin", "member"} {
			r := skipRun(t, caller, key, 3)
			for _, sv := range r.Slots {
				if sv.Row == "23" && (sv.Verdict != VerdictSkipped || sv.Pass()) {
					t.Errorf("%s %s: slot %d is %s (pass %v), want SKIPPED, failing", key, caller, sv.Seq, sv.Verdict, sv.Pass())
				}
			}
			if r.Pass || r.VerdictCounts()[VerdictSkipped] != 3 {
				t.Errorf("%s %s: pass %v counts %v, want 3 SKIPPED and no pass", key, caller, r.Pass, r.VerdictCounts())
			}
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

// CHAOS-13675 (acp Q-A): decided_no_question passes only on a slot whose legacy answer is the enforcer's unasked
// refusal of a token with no organization on an AdminOrg row, status and body bytes; it is counted on its own line,
// never as a match. Any other legacy answer fails.
func TestJudgeHoldsDecidedNoQuestionToTheRefusalBytes(t *testing.T) {
	status, body := policy.EnforcedOrglessRefusal()
	sum := sha256.Sum256(body)
	refusal := hex.EncodeToString(sum[:])
	other := sha256.Sum256([]byte(`{"detail":"Admin access required"}`))
	judge := func(status int, bodySHA string) SlotVerdict {
		slot := slotAt(1, "2", "superuser", 1)
		slot.Status, slot.BodySHA = status, bodySHA
		ev, err := ParseLog(strings.NewReader(result(slot, 300*time.Millisecond, "decided_no_question", "") + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		r := Judge([]Slot{slot}, ev, nil, JudgeOptions{K: 1, Grace: 2200 * time.Millisecond})
		if r.VerdictCounts()[VerdictMatch] != 0 {
			t.Fatalf("decided_no_question folded into MATCH: %v", r.VerdictCounts())
		}
		return r.Slots[0]
	}
	if sv := judge(status, refusal); sv.Verdict != VerdictDecidedNoQuestion || !sv.Pass() {
		t.Errorf("the legacy refusal: %s (pass %v), want DECIDED_NO_QUESTION passing", sv.Verdict, sv.Pass())
	}
	for name, c := range map[string]struct {
		status int
		sha    string
	}{
		"another refusal body": {status, hex.EncodeToString(other[:])},
		"served":               {200, refusal},
		"another status":       {401, refusal},
		"no body digest":       {status, ""},
	} {
		if sv := judge(c.status, c.sha); sv.Verdict != VerdictDecidedNoQuestionDiffers || sv.Pass() {
			t.Errorf("%s: %s (pass %v), want DECIDED_NO_QUESTION_DIFFERS failing", name, sv.Verdict, sv.Pass())
		}
	}
	if status != 403 || !strings.Contains(string(body), "Organization context required") {
		t.Errorf("the enforcer's org-less AdminOrg refusal is %d %q, want the legacy 403 Organization context required", status, body)
	}
}
