package shadowvenue

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
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
			r.yard = append(r.yard, YardSlot{Seq: len(r.yard) + 1, Row: row, Caller: caller, K: k, Key: key, Status: legacy, Verdict: yardVerdict})
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
		MetricsBefore: enfBefore(allEnforced, 1000, "f50883b", "0"),
		MetricsAfter:  enfAfter(allEnforced, 1100, "f50883b", "0"),
	}
	r.in.Outbound = Outbound{Rows: tableOf(r.slots, nil)}
	return r
}

func (r *enfRun) judge() Report { return JudgeEnforced(r.in) }

// enfBefore and enfAfter are the scrapes of a go-api build after CHAOS-13596: no unjudged series (it emits only the
// reasons of policy.EnforceUnjudgedReasons, and the run gives it none). The no-org route's namespace (billing) counts
// three more denies, one per decided no-org superuser slot (DECIDED_NOORG), and the seam route's namespace (members)
// three more allows: a request that serves another organization is decided on the token organization (CHAOS-10872 S2).
func enfBefore(enabled []string, uptime int, version, invalid string, series ...string) []byte {
	return live(enabled, uptime, version, invalid, append([]string{decisions("billing", "allow", "10"), decisions("billing", "deny", "1"),
		decisions("members", "allow", "20")}, series...)...)
}

func enfAfter(enabled []string, uptime int, version, invalid string, series ...string) []byte {
	return live(enabled, uptime, version, invalid, append([]string{decisions("billing", "allow", "10"), decisions("billing", "deny", "4"),
		decisions("members", "allow", "23")}, series...)...)
}

// A go-api that has not yet counted an unjudged request has no series of the counter in the before scrape: the delta is
// the whole after series (here one org_required count on a route the run did not send), and the run is judged, not voided.
func TestEnforcedAcceptsABeforeScrapeWithNoUnjudgedSeries(t *testing.T) {
	r := newEnfRun()
	r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged("GET /api/v1/admin/orgs", "org_required", "1"))
	if report := r.judge(); report.Void || !report.Pass {
		t.Fatalf("void %v pass %v: %v", report.Void, report.Pass, report.Reasons)
	}
}

// The absent unjudged family counts as zero only in a scrape proven complete: the same scrape holds the uptime and the
// enforcement gauge of every namespace a row needs, and ends with its newline.
func TestEnforcedAbsentUnjudgedFamilyIsZeroOnlyInACompleteScrape(t *testing.T) {
	complete := func(uptime int) []byte { return live(allEnforced, uptime, "f50883b", "0") }
	cases := map[string]struct {
		plant func(*enfRun)
		void  bool
		want  string
	}{
		"absent before, present after: the delta is the after series": {func(r *enfRun) {
			r.in.MetricsBefore = complete(1000)
		}, false, VerdictDecidedNoOrg},
		"absent in both: no decision is counted, the skip fails": {func(r *enfRun) {
			r.in.MetricsBefore = complete(1000)
			r.in.MetricsAfter = complete(1100)
		}, false, VerdictUnevidencedNoOrg},
		"absent family and no uptime gauge in the before scrape": {func(r *enfRun) {
			r.in.MetricsBefore = []byte(strings.Join(strings.Split(string(complete(1000)), "\n")[:len(allEnforced)+3], "\n") + "\n")
		}, true, "read back"},
		"truncated before scrape": {func(r *enfRun) {
			r.in.MetricsBefore = complete(1000)[:len(complete(1000))-12]
		}, true, "read back"},
		"before scrape cut inside a later line after every needed gauge": {func(r *enfRun) {
			r.in.MetricsBefore = append(complete(1000), []byte(`devhealth_api_auth_proxy_requests_total{route="GET /a`)...)
		}, true, "read back"},
		"truncated after scrape": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0")[:200]
		}, true, "read back"},
		"after scrape misses the gauge of one needed namespace": {func(r *enfRun) {
			r.in.MetricsAfter = []byte(strings.ReplaceAll(string(enfAfter(allEnforced, 1100, "f50883b", "0")), `dev_health_authz_enforcement_enabled{namespace="billing",service="go-api"} 1`+"\n", ""))
		}, true, "billing"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			run := newEnfRun()
			c.plant(run)
			report := run.judge()
			text := strings.ToLower(strings.Join(report.Reasons, "\n"))
			if c.void {
				if !report.Void || report.Exit() != 3 || !strings.Contains(text, strings.ToLower(c.want)) {
					t.Fatalf("want void with %q: void %v exit %d %v", c.want, report.Void, report.Exit(), report.Reasons)
				}
				return
			}
			if report.Void || report.VerdictCounts()[c.want] == 0 {
				t.Fatalf("want a judged run with %s: void %v counts %v %v", c.want, report.Void, report.VerdictCounts(), report.Reasons)
			}
			if c.want == VerdictUnevidencedNoOrg && report.Pass {
				t.Fatal("a skip with no counter evidence passed")
			}
		})
	}
}

func TestEnforcedPassesWhenEveryCellEqualsTheYardstickAndTheEvidenceHolds(t *testing.T) {
	r := newEnfRun().judge()
	if r.Void || !r.Pass || r.Exit() != 0 {
		t.Fatalf("void %v pass %v exit %d: %v", r.Void, r.Pass, r.Exit(), r.Reasons)
	}
	counts := r.VerdictCounts()
	if counts[VerdictMatchEnforced] != 9 || counts[VerdictDecidedNoOrg] != 3 || counts[VerdictEnforcedDeny] != 6 || counts[VerdictMatch] != 0 {
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
			r.in.MetricsBefore = scrape(decisions("billing", "deny", "1"))
			r.in.MetricsAfter = scrape(decisions("billing", "deny", "4"))
		}, true, "read back"},
		"billing not enforced before": {func(r *enfRun) {
			r.in.MetricsBefore = enfBefore([]string{"admin", "integrations", "license", "members", "settings", "teams"}, 1000, "f50883b", "0")
		}, true, "billing"},
		"billing not enforced after": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter([]string{"admin", "integrations", "license", "members", "settings", "teams"}, 1100, "f50883b", "0")
		}, true, "billing"},
		"no namespace enforced": {func(r *enfRun) {
			r.in.MetricsBefore = enfBefore(nil, 1000, "f50883b", "0")
		}, true, "enforced"},
		"enforcement config invalid": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "1")
		}, true, "invalid"},
		"build differs": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "aaaaaaa", "0")
		}, true, "build"},
		"uptime went down (restart between the scrapes)": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 40, "f50883b", "0")
		}, true, "went from"},
		"uptime went down by a few seconds (counters reset)": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 997, "f50883b", "0")
		}, true, "went from"},
		"uptime grew less than the drive (restart in the run)": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 1020, "f50883b", "0")
		}, true, "grew"},
		"yardstick digest none":                    {func(r *enfRun) { r.in.YardDigest = "" }, true, "missing"},
		"yardstick digest differs":                 {func(r *enfRun) { r.in.YardDigest = strings.Repeat("c", 64) }, true, "differs"},
		"yardstick after digest differs":           {func(r *enfRun) { r.in.YardDigestAfter = strings.Repeat("d", 64) }, true, "differs"},
		"yardstick after digest none":              {func(r *enfRun) { r.in.YardDigestAfter = "" }, true, "missing"},
		"digest before differs":                    {func(r *enfRun) { r.in.DigestBefore = strings.Repeat("a", 64) }, true, "digest"},
		"digest after differs":                     {func(r *enfRun) { r.in.DigestAfter = strings.Repeat("b", 64) }, true, "digest"},
		"control not enforced":                     {func(r *enfRun) { r.in.Slots[0].Status = 200 }, true, "control"},
		"one control cell only":                    {func(r *enfRun) { r.in.Slots = r.in.Slots[:3] }, true, "control"},
		"enforced stricter":                        {func(r *enfRun) { r.in.Slots[9].Status = 403; r.in.Slots[10].Status = 403; r.in.Slots[11].Status = 403 }, false, VerdictStricter},
		"enforced wider":                           {func(r *enfRun) { r.in.Slots[6].Status = 200 }, false, VerdictWider},
		"enforced 404 against legacy 403":          {func(r *enfRun) { r.in.Slots[6].Status = 404 }, false, VerdictStatusDiffers},
		"enforced 503":                             {func(r *enfRun) { r.in.Slots[6].Status = 503 }, false, VerdictStatusDiffers},
		"no yardstick slot (the schedules differ)": {func(r *enfRun) { r.in.Yard = r.in.Yard[:len(r.in.Yard)-1] }, true, "schedule"},
		"decisions one short of the no-org slots": {func(r *enfRun) {
			r.in.MetricsAfter = live(allEnforced, 1100, "f50883b", "0", decisions("billing", "allow", "10"), decisions("billing", "deny", "3"))
		}, false, VerdictUnevidencedNoOrg},
		"another unjudged reason on the no-org route": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "impersonation", "1"))
		}, false, VerdictUnevidencedNoOrg},
		"a seam answer differs": {func(r *enfRun) { r.in.Slots[12].Status = 403 }, false, VerdictStricter},
		"counter went down": {func(r *enfRun) {
			r.in.MetricsBefore = enfBefore(allEnforced, 1000, "f50883b", "0", unjudged(teamsKey, "impersonation", "5"))
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged(teamsKey, "impersonation", "4"))
		}, true, "counter"},
		// CHAOS-13673: a reason outside go-api's closed set (policy.EnforceUnjudgedReasons) is a build before CHAOS-13596,
		// which this judge does not judge: the run is void, never judged without the legacy half.
		"a served_org series": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged(seamKey, "served_org", "1"))
		}, true, "outside go-api's closed set"},
		"a served_org series that did not grow": {func(r *enfRun) {
			r.in.MetricsBefore = enfBefore(allEnforced, 1000, "f50883b", "0", unjudged(seamKey, "served_org", "2"))
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged(seamKey, "served_org", "2"))
		}, true, "outside go-api's closed set"},
		"a no_org series": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged(noOrgKey, "no_org", "3"))
		}, true, "outside go-api's closed set"},
		"an hs256 series": {func(r *enfRun) {
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged(teamsKey, "hs256", "1"))
		}, true, "outside go-api's closed set"},
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

// CHAOS-13673: the judge's VOID set is the complement of go-api's closed unjudged reason set, read from the production
// policy package (policy.EnforceUnjudgedReasons), with no second list: a series of a reason in the set is judged, a
// series of any other reason (the retired hs256, no_org and served_org, or a reason no build emits) voids the run.
func TestEnforcedVoidSetIsTheComplementOfGoAPIsClosedReasonSet(t *testing.T) {
	closed := policy.EnforceUnjudgedReasons()
	if len(closed) == 0 {
		t.Fatal("go-api's closed reason set is empty")
	}
	probes := append(slices.Clone(closed), "hs256", "no_org", "served_org", "a_reason_no_build_emits")
	for _, reason := range probes {
		t.Run(reason, func(t *testing.T) {
			r := newEnfRun()
			r.in.MetricsAfter = enfAfter(allEnforced, 1100, "f50883b", "0", unjudged("GET /api/v1/admin/orgs", reason, "1"))
			report := r.judge()
			voided := report.Void && strings.Contains(strings.Join(report.Reasons, "\n"), "outside go-api's closed set")
			if want := !slices.Contains(closed, reason); voided != want {
				t.Fatalf("reason %q: voided %v, want %v (closed set %v): %v", reason, voided, want, closed, report.Reasons)
			}
		})
	}
}
