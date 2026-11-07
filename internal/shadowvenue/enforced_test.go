package shadowvenue

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

var enfT0 = time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)

var allEnforced = []string{"admin", "billing", "integrations", "license", "members", "settings", "teams"}

// live builds a go-api /metrics scrape: the enforcement gauge for every catalog namespace (1 for those enabled), the
// config gauge, the build info, the uptime and the given unjudged series.
func live(enabled []string, uptime int, version, invalid string, series ...string) []byte {
	var lines []string
	for _, ns := range []string{"acr", "admin", "billing", "integrations", "license", "members", "settings", "teams"} {
		v := "0"
		if slicesContains(enabled, ns) {
			v = "1"
		}
		lines = append(lines, `dev_health_authz_enforcement_enabled{namespace="`+ns+`",service="go-api"} `+v)
	}
	lines = append(lines, `dev_health_authz_enforcement_config_invalid{service="go-api"} `+invalid,
		`dev_health_runtime_info{service="dev-health-api",version="`+version+`"} 1`, "dev_health_runtime_uptime_seconds "+itoa(uptime))
	return scrape(append(lines, series...)...)
}

func slicesContains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func itoa(n int) string { return strconv.Itoa(n) }

const (
	enfDigest = "d41d8cd98f00b204e9800998ecf8427e1111111111111111111111111111aaaa"
	teamsKey  = "GET /api/v1/admin/teams"
	seamKey   = "GET /api/v1/admin/orgs/{org_id}/members"
	noOrgKey  = "GET /api/v1/billing/invoices"
)

// enfRun is one paired set: the yardstick slots with their verdicts and the run-3 slots, three per cell.
type enfRun struct {
	yard  []YardSlot
	slots []Slot
	in    EnforcedInput
}

func newEnfRun() *enfRun {
	r := &enfRun{}
	add := func(row, caller string, key string, legacy, enforced int, yardVerdict string, control bool) {
		method, route, _ := strings.Cut(key, " ")
		for k := 1; k <= 3; k++ {
			expect := "match"
			if control {
				expect = "mismatch"
			}
			r.yard = append(r.yard, YardSlot{Row: row, Caller: caller, K: k, Key: key, Status: legacy, Verdict: yardVerdict})
			start := enfT0.Add(time.Duration(len(r.slots)) * 3 * time.Second)
			r.slots = append(r.slots, Slot{Start: start, End: start.Add(100 * time.Millisecond), Seq: len(r.slots) + 1, Row: row, Caller: caller, Principal: caller + "-A", Expect: expect, K: k, Method: method, Route: route, Path: route, Status: enforced})
		}
	}
	add("C1", "superuser", teamsKey, 200, 403, VerdictMismatch, true)
	add("C2", "superuser", "GET /api/v1/admin/integrations", 200, 403, VerdictMismatch, true)
	add("1", "member", teamsKey, 403, 403, VerdictMatch, false)
	add("1", "admin", teamsKey, 200, 200, VerdictMatch, false)
	add("2", "superuser", seamKey, 200, 200, VerdictSkippedSeam, false)
	add("3", "superuser", noOrgKey, 403, 403, VerdictSkipped, false)
	r.in = EnforcedInput{
		Slots: r.slots, Yard: r.yard, K: 3,
		Namespaces: map[string]string{teamsKey: "teams", "GET /api/v1/admin/integrations": "integrations", seamKey: "members", noOrgKey: "billing"},
		YardDigest: enfDigest, YardDigestAfter: enfDigest, DigestBefore: enfDigest, DigestAfter: enfDigest,
		MetricsBefore: live(allEnforced, 1000, "f50883b", "0", unjudged(noOrgKey, "no_org", "5"), unjudged(seamKey, "served_org", "2")),
		MetricsAfter:  live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5")),
	}
	return r
}

func (r *enfRun) judge() Report { return JudgeEnforced(r.in) }

func TestEnforcedPassesWhenEveryCellEqualsTheYardstickAndTheEvidenceHolds(t *testing.T) {
	r := newEnfRun().judge()
	if r.Void || !r.Pass || r.Exit() != 0 {
		t.Fatalf("void %v pass %v exit %d: %v", r.Void, r.Pass, r.Exit(), r.Reasons)
	}
	counts := r.VerdictCounts()
	if counts[VerdictMatchEnforced] != 6 || counts[VerdictSkippedNoOrg] != 3 || counts[VerdictSkippedSeam] != 3 || counts[VerdictEnforcedDeny] != 6 || counts[VerdictMatch] != 0 {
		t.Fatalf("counts %v", counts)
	}
}

// Each planted defect turns the run red the way the amendment says: a void run (exit 3) or a failed run (exit 1).
func TestEnforcedEachPlantedDefectIsRed(t *testing.T) {
	cases := map[string]struct {
		plant func(*enfRun)
		void  bool
		want  string
	}{
		"no enforcement gauge in the scrapes": {func(r *enfRun) {
			r.in.MetricsBefore = scrape(unjudged(noOrgKey, "no_org", "5"))
			r.in.MetricsAfter = scrape(unjudged(noOrgKey, "no_org", "8"))
		}, true, "read back"},
		"billing not enforced before": {func(r *enfRun) {
			r.in.MetricsBefore = live([]string{"admin", "integrations", "license", "members", "settings", "teams"}, 1000, "f50883b", "0", unjudged(noOrgKey, "no_org", "5"))
		}, true, "billing"},
		"billing not enforced after": {func(r *enfRun) {
			r.in.MetricsAfter = live([]string{"admin", "integrations", "license", "members", "settings", "teams"}, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"))
		}, true, "billing"},
		"no namespace enforced": {func(r *enfRun) {
			r.in.MetricsBefore = live(nil, 1000, "f50883b", "0", unjudged(noOrgKey, "no_org", "5"))
		}, true, "enforced"},
		"enforcement config invalid": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "1", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"))
		}, true, "invalid"},
		"build differs": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "aaaaaaa", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"))
		}, true, "build"},
		"uptime went down (restart between the scrapes)": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 40, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"))
		}, true, "restarted"},
		"uptime grew less than the drive (restart in the run)": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1020, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"))
		}, true, "restarted"},
		"yardstick digest none":           {func(r *enfRun) { r.in.YardDigest = "" }, true, "missing"},
		"yardstick digest differs":        {func(r *enfRun) { r.in.YardDigest = strings.Repeat("c", 64) }, true, "differs"},
		"yardstick after digest differs":  {func(r *enfRun) { r.in.YardDigestAfter = strings.Repeat("d", 64) }, true, "differs"},
		"yardstick after digest none":     {func(r *enfRun) { r.in.YardDigestAfter = "" }, true, "missing"},
		"digest before differs":           {func(r *enfRun) { r.in.DigestBefore = strings.Repeat("a", 64) }, true, "digest"},
		"digest after differs":            {func(r *enfRun) { r.in.DigestAfter = strings.Repeat("b", 64) }, true, "digest"},
		"control not enforced":            {func(r *enfRun) { r.in.Slots[0].Status = 200 }, true, "control"},
		"one control cell only":           {func(r *enfRun) { r.in.Slots = r.in.Slots[:3] }, true, "control"},
		"enforced stricter":               {func(r *enfRun) { r.in.Slots[9].Status = 403; r.in.Slots[10].Status = 403; r.in.Slots[11].Status = 403 }, false, VerdictStricter},
		"enforced wider":                  {func(r *enfRun) { r.in.Slots[6].Status = 200 }, false, VerdictWider},
		"enforced 404 against legacy 403": {func(r *enfRun) { r.in.Slots[6].Status = 404 }, false, VerdictStatusDiffers},
		"enforced 503":                    {func(r *enfRun) { r.in.Slots[6].Status = 503 }, false, VerdictStatusDiffers},
		"no yardstick slot":               {func(r *enfRun) { r.in.Yard = r.in.Yard[:len(r.in.Yard)-1] }, false, VerdictMissing},
		"no_org delta one short": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "7"), unjudged(seamKey, "served_org", "5"))
		}, false, VerdictUnevidencedNoOrg},
		"no_org delta one over": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "9"), unjudged(seamKey, "served_org", "5"))
		}, false, VerdictUnevidencedNoOrg},
		"other reason on the no_org route": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(noOrgKey, "served_org", "1"), unjudged(seamKey, "served_org", "5"))
		}, false, VerdictUnevidencedNoOrg},
		"other reason on the seam route": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "8"), unjudged(seamKey, "served_org", "5"), unjudged(seamKey, "no_org", "1"))
		}, false, VerdictUnevidencedSeam},
		"counter went down": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "4"), unjudged(seamKey, "served_org", "5"))
		}, true, "counter"},
		"scrape unreadable": {func(r *enfRun) { r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0")[:20] }, true, "scrape"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			run := newEnfRun()
			c.plant(run)
			report := run.judge()
			text := strings.Join(report.Reasons, "\n")
			if c.void {
				if !report.Void || report.Exit() != 3 || !strings.Contains(strings.ToLower(text), strings.ToLower(c.want)) {
					t.Fatalf("void %v exit %d want void with %q: %v", report.Void, report.Exit(), c.want, report.Reasons)
				}
				return
			}
			if report.Void || report.Pass || report.Exit() != 1 || report.VerdictCounts()[c.want] == 0 {
				t.Fatalf("void %v pass %v exit %d counts %v want a failed run with %s", report.Void, report.Pass, report.Exit(), report.VerdictCounts(), c.want)
			}
		})
	}
}

func TestYardstickRoundTripsThroughTheVerdictsFile(t *testing.T) {
	run := newEnfRun()
	report := JudgeEnforced(run.in)
	var b strings.Builder
	if err := report.WriteVerdicts(&b); err != nil {
		t.Fatal(err)
	}
	yard, err := ParseYardstick([]byte(b.String()))
	if err != nil || len(yard) != len(run.slots) {
		t.Fatalf("%d yardstick slots, want %d: %v", len(yard), len(run.slots), err)
	}
	for _, bad := range []string{"", "seq\trow\n", verdictsHeader + "\n1\tA\tmember\tx\tGET /a\t200\tMATCH\n", verdictsHeader + "\n"} {
		if _, err := ParseYardstick([]byte(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
