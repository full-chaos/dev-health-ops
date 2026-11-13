package shadowvenue

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
)

// Verdicts of an enforced run (the venue go-api has enforcement on for the namespaces under test). The yardstick is the
// legacy status of the same slot in a paired run with enforcement off, on the same image and the same data.
const (
	VerdictMatchEnforced    = "MATCH_ENFORCED"
	VerdictEnforcedDeny     = "ENFORCED_DENY"
	VerdictStatusDiffers    = "MISMATCH_STATUS"
	VerdictUnevidencedNoOrg = "UNEVIDENCED_NOORG"
	VerdictControlNotDenied = "CONTROL_NOT_ENFORCED"
	// VerdictMismatchCaller: the slot's token is not the one the plan wanted (sub or org claim).
	VerdictMismatchCaller = "MISMATCH_CALLER"
	// VerdictMismatchBody: the status is equal but the normalized answer differs between the two runs.
	VerdictMismatchBody = "MISMATCH_BODY"
	// VerdictEqualPastGuard: an equal 5xx answer with equal bodies, and the go-api log shows the decision point allowed
	// the request, so the 5xx is the handler's after the guard.
	VerdictEqualPastGuard = "EQUAL_PAST_GUARD"
	// VerdictVoidParity: an equal 5xx answer with no log proof that it is past the guard; the cell is no evidence.
	VerdictVoidParity = "VOID_PARITY"
	// VerdictPDPUnavailable: the go-api log shows the decision point gave no verdict for the request.
	VerdictPDPUnavailable = "PDP_UNAVAILABLE"
)

// YardSlot is a slot of the yardstick run: its cell, its legacy HTTP status and the verdict that run gave it.
type YardSlot struct {
	Row, Caller string
	// Seq is the slot seq in the yardstick run's own slots file (the join key of its side file).
	Seq    int
	K      int
	Key    string
	Status int
	// BodyNorm is the normalized body digest of the yardstick slot.
	BodyNorm string
	Verdict  string
}

// EnforcedInput is everything an enforced judgement reads.
type EnforcedInput struct {
	Slots []Slot
	Yard  []YardSlot
	K     int
	// Namespaces maps each row key to its action namespace, so the namespaces the run needs are known. The enforcement
	// switch is read back from the running go-api: the gauge dev_health_authz_enforcement_enabled in both scrapes.
	Namespaces map[string]string
	// The data digests, all four equal: before the yardstick run, after it, before this run, after it. A write that a
	// run made and did not restore shows as a difference.
	YardDigest, YardDigestAfter, DigestBefore, DigestAfter string
	// go-api /metrics scrapes before and after the run, for the unjudged counter.
	MetricsBefore, MetricsAfter []byte
	// Grace is the time after a slot's answer in which its outside call may still land.
	Grace time.Duration
	// Outbound is this run's stub evidence; YardOutbound the yardstick run's outbound file (WriteOutbound), given when
	// YardOutboundGiven. Each enforced slot must make as many outside calls as its yardstick slot.
	Outbound          Outbound
	YardOutbound      []OutboundSlot
	YardOutboundGiven bool
	// Strict holds the run to claim read-back, body digests and body parity (the command always sets it); Membership
	// is the two snapshots of the superuser-O pairs; Log is the go-api log of the run, the proof that an equal 5xx cell is
	// past the guard (nil = none given).
	Strict     bool
	Membership Membership
	Log        *Evidence
	// Leaves, when set, replaces the body_norm_sha comparison with the per-leaf parity after the frozen volatile set.
	Leaves *LeafParity
	// J1 turns on the counter proof of form B (D6894): an equal 5xx cell whose yardstick cell is a MATCH with a decision id
	// is past the guard when its namespace's decision counter equals the judged slots exactly and the idle sweep holds.
	// Attributed lists the requests of the run that are no slot (the drive's sign-in).
	J1         bool
	Attributed []AttributedRequest
	// Process names the go-api process each scrape came from (nil = none given: no counter proof stands).
	Process *ProcessFacts
}

// liveEnforcement is what one /metrics scrape of the running go-api says: the namespaces whose enforcement gauge is 1,
// whether the enforcement config was invalid, the build version and the process uptime. ok is false when the scrape
// carries no enforcement gauge or no uptime, or does not end with its newline (a cut or failed scrape): only a scrape
// that is ok may stand for an absent counter family as zero.
type liveEnforcement struct {
	enabled       map[string]bool
	configInvalid bool
	version       string
	uptime        float64
	ok            bool
}

func readLive(scrape []byte) liveEnforcement {
	out := liveEnforcement{enabled: map[string]bool{}}
	sawGauge, sawUptime := false, false
	for _, line := range strings.Split(string(scrape), "\n") {
		switch {
		case strings.HasPrefix(line, "dev_health_authz_enforcement_enabled{"):
			labels, value, _ := strings.Cut(strings.TrimPrefix(line, "dev_health_authz_enforcement_enabled{"), "} ")
			if !strings.Contains(labels, `service="go-api"`) {
				continue
			}
			sawGauge = true
			if ns := labelValue(labels, "namespace"); ns != "" && strings.TrimSpace(value) == "1" {
				out.enabled[ns] = true
			}
		case strings.HasPrefix(line, "dev_health_authz_enforcement_config_invalid{"):
			if _, value, ok := strings.Cut(line, "} "); ok && strings.Contains(line, `service="go-api"`) && strings.TrimSpace(value) != "0" {
				out.configInvalid = true
			}
		case strings.HasPrefix(line, "dev_health_runtime_info{"):
			out.version = labelValue(line, "version")
		case strings.HasPrefix(line, "dev_health_runtime_uptime_seconds "):
			if v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, "dev_health_runtime_uptime_seconds ")), 64); err == nil {
				out.uptime, sawUptime = v, true
			}
		}
	}
	out.ok = sawGauge && sawUptime && strings.HasSuffix(string(scrape), "\n")
	return out
}

// labelValue returns the value of label name in a metric line, "" when absent.
func labelValue(line, name string) string {
	_, rest, ok := strings.Cut(line, name+`="`)
	if !ok {
		return ""
	}
	value, _, _ := strings.Cut(rest, `"`)
	return value
}

// driveSpan is the first start and the last end of the slots that were sent.
func driveSpan(slots []Slot) (first, last time.Time) {
	for _, s := range slots {
		if s.Gap() {
			continue
		}
		if first.IsZero() || s.Start.Before(first) {
			first = s.Start
		}
		if s.End.After(last) {
			last = s.End
		}
	}
	return first, last
}

// JudgeEnforced judges a run with enforcement on against the yardstick. Void (exit 3): the enforcement switch not read
// back from the running go-api or missing a namespace the rows need, a go-api restart or build change between the scrapes, a data digest that differs, an unreadable counter, an unjudged
// series with a reason outside go-api's closed set (policy.EnforceUnjudgedReasons: a go-api build before CHAOS-13596,
// which this judge does not judge), fewer than two controls, a control the enforcement did not deny. Per slot, every
// caller: the enforced status must equal the yardstick's legacy status; a difference is MISMATCH_STRICTER or
// MISMATCH_WIDER by direction (403 = deny), else MISMATCH_STATUS. The superuser's skips of the yardstick (a token with no
// organization) are decided by the enforcement (CHAOS-10872 S1): a route passes when unjudged{reason="org_required"} grew
// by exactly the number of slots (an AdminOrg row) and no other reason grew, or when no reason grew and the route's
// namespace counted at least as many allow or deny decisions as the run has such slots (DECIDED_NOORG).
func JudgeEnforced(in EnforcedInput) Report {
	var report Report
	fail := func(format string, args ...any) {
		report.Reasons = append(report.Reasons, fmt.Sprintf(format, args...))
	}
	void := func(format string, args ...any) {
		report.Void = true
		fail("VOID: "+format, args...)
	}
	if in.Outbound.Rows == nil {
		void("no row table was given (Outbound.Rows): every run is judged against its table, held to the pinned classes")
	}
	byRow, classErr := CheckRowClasses(in.Outbound.Rows)
	if classErr != nil {
		void("%v", classErr)
	}
	before, after := readLive(in.MetricsBefore), readLive(in.MetricsAfter)
	readable := before.ok && after.ok
	if !readable {
		void("the enforcement switch was not read back from the running go-api: a scrape has no dev_health_authz_enforcement_enabled gauge or no uptime")
	}
	if readable && (before.configInvalid || after.configInvalid) {
		void("the go-api reports its enforcement config invalid")
	}
	if readable && (before.version == "" || before.version != after.version) {
		void("the go-api build differs between the scrapes (%q, %q): another process ran", before.version, after.version)
	}
	if readable {
		first, last := driveSpan(in.Slots)
		switch {
		case after.uptime < before.uptime:
			void("the go-api uptime went from %.0fs to %.0fs between the scrapes: the process restarted, its counters are not comparable", before.uptime, after.uptime)
		case after.uptime-before.uptime < last.Sub(first).Seconds()-5:
			void("the go-api uptime grew %.0fs over a drive of %.0fs: the process restarted during the run", after.uptime-before.uptime, last.Sub(first).Seconds())
		}
	}
	var need []string
	for _, s := range in.Slots {
		if ns := in.Namespaces[s.Key()]; ns != "" && !s.Gap() && !slices.Contains(need, ns) {
			need = append(need, ns)
		}
	}
	slices.Sort(need)
	for _, ns := range need {
		if readable && !(before.enabled[ns] && after.enabled[ns]) {
			void("namespace %s is not enforced on the running go-api in both scrapes, but a row of it runs", ns)
		}
	}
	digests := []string{in.YardDigest, in.YardDigestAfter, in.DigestBefore, in.DigestAfter}
	if slices.Contains(digests, "") {
		void("a data digest is missing (yardstick before %q, yardstick after %q, before %q, after %q)", in.YardDigest, in.YardDigestAfter, in.DigestBefore, in.DigestAfter)
	} else if slices.Min(digests) != slices.Max(digests) {
		void("the data digest differs (yardstick before %.12s, yardstick after %.12s, before %.12s, after %.12s): the seeded state is not the yardstick's, or a run left a change", in.YardDigest, in.YardDigestAfter, in.DigestBefore, in.DigestAfter)
	}
	if sent, yard := ScheduleSHA12(in.Slots), yardScheduleSHA12(in.Yard); sent != yard {
		void("the schedule differs: the run's sent slots (sha12 %s) are not the yardstick's (sha12 %s) in the same order, so the cells cannot be paired position by position", sent, yard)
	}
	reasons, err := ParseUnjudgedReasons(in.MetricsBefore, in.MetricsAfter)
	if err != nil {
		void("the unjudged counter is unreadable: %v", err)
	}
	if err == nil {
		if outside := unjudgedOutsideClosedSet(reasons); len(outside) > 0 {
			void("the unjudged counter has series with reasons outside go-api's closed set %v: %s; a go-api build before CHAOS-13596, which this judge does not judge",
				policy.EnforceUnjudgedReasons(), strings.Join(outside, ", "))
		}
	}

	type cell struct {
		row, caller string
		k           int
	}
	yard := map[cell]YardSlot{}
	for _, y := range in.Yard {
		yard[cell{y.Row, y.Caller, y.K}] = y
	}
	// A route whose decision point was not asked for another reason (a legacy HS256 caller, an impersonation read) has
	// no allow proof in the log.
	unjudgedRoutes := map[string]bool{}
	for route, byReason := range reasons {
		for reason, n := range byReason {
			if n > 0 && reason != "org_required" {
				unjudgedRoutes[route] = true
			}
		}
	}
	bodyMissing, leafMissing := 0, 0
	for _, slot := range in.Slots {
		sv := SlotVerdict{Slot: slot}
		y, found := yard[cell{slot.Row, slot.Caller, slot.K}]
		switch {
		case slot.Gap():
			sv.Verdict = VerdictGap
		case !found:
			sv.Verdict = VerdictMissing
		case slot.Expect == expectMismatch:
			sv.Verdict = VerdictControlNotDenied
			if slot.Status == 403 && y.Status != 403 {
				sv.Verdict = VerdictEnforcedDeny
			}
		case slot.Status == y.Status:
			sv.Verdict = VerdictMatchEnforced
			if y.Verdict == VerdictSkipped {
				sv.Verdict = VerdictUnevidencedNoOrg
			}
			if in.Strict {
				differs := slot.BodyNormSHA != y.BodyNorm
				if in.Leaves != nil {
					var missing bool
					if differs, missing = in.Leaves.differs(slot, y); missing {
						leafMissing++
						differs = false
					}
				}
				if y.BodyNorm == "" || slot.BodyNormSHA == "" {
					bodyMissing++
				} else {
					sv.Verdict = bodyVerdict(in, slot, y, sv.Verdict, unjudgedRoutes[slot.Key()], differs)
				}
			}
		case slot.Status == 403:
			sv.Verdict = VerdictStricter
		case y.Status == 403 && slot.Status < 400:
			sv.Verdict = VerdictWider
		default:
			sv.Verdict = VerdictStatusDiffers
		}
		report.Slots = append(report.Slots, sv)
	}
	if leafMissing > 0 {
		void("%d sent cells have no line in a side file: the leaves of the answers cannot be compared", leafMissing)
	}
	if bodyMissing > 0 {
		void("%d cells have no body digest in the slots or in the yardstick: the answers cannot be compared", bodyMissing)
	}
	applyStrict(&report, in.Strict, in.Membership, void)
	// The superuser's skips of the yardstick are decided by the enforcement (S1): they pass on the counter, per route.
	if err == nil {
		perKey := map[string][]int{}
		for i, sv := range report.Slots {
			if sv.Verdict == VerdictUnevidencedNoOrg {
				perKey[sv.Key()] = append(perKey[sv.Key()], i)
			}
		}
		decided, decidedErr := ParseDecisionDeltas(in.MetricsBefore, in.MetricsAfter)
		decidedByNS := map[string][]int{}
		for key, indexes := range perKey {
			residual := orglessResidual(reasons[key], len(indexes))
			// Only the org-less caller of the venue, the superuser, is decided this way; any other caller's skip fails.
			for _, i := range indexes {
				if report.Slots[i].Caller != "superuser" {
					residual = residualNone
				}
			}
			switch residual {
			case residualOrgRequired:
				for _, i := range indexes {
					report.Slots[i].Verdict = VerdictDecidedNoOrg
				}
			case residualDecided:
				ns := in.Namespaces[key]
				decidedByNS[ns] = append(decidedByNS[ns], indexes...)
			}
		}
		for ns, indexes := range decidedByNS {
			if decidedErr != nil || ns == "" || decided[ns]["allow"]+decided[ns]["deny"] < len(indexes) {
				continue
			}
			for _, i := range indexes {
				report.Slots[i].Verdict = VerdictDecidedNoOrg
			}
		}
	}
	if in.J1 {
		applyCounterProof(&report, in, func(row, caller string, k int) string { return yard[cell{row, caller, k}].Verdict }, unjudgedRoutes)
	}
	outbound := judgeOutboundEnforced(&report, in, byRow)
	for _, reason := range outbound.voids {
		void("%s", reason)
	}
	report.Reasons = append(report.Reasons, outbound.fails...)
	for _, sv := range report.Slots {
		if sv.Expect != expectMismatch && !sv.Pass() {
			fail("FAIL: slot %d row %s %s %s: %s", sv.Seq, sv.Row, sv.Caller, sv.Key(), sv.Verdict)
		}
	}
	report.Rows = rowVerdicts(report.Slots, in.K)
	markNoReach(report.Rows, outbound)
	controls := 0
	for _, rv := range report.Rows {
		if strings.HasPrefix(rv.Row, "C") {
			controls++
		}
	}
	if controls < 2 {
		void("%d control cells; a run needs two cells that must be denied by the enforcement while the legacy guard serves them", controls)
	}
	for _, sv := range report.Slots {
		if sv.Expect == expectMismatch && !controlMismatch(sv.Verdict) {
			void("control %s slot %d is %s: the enforcement is not live or the control is not served by the legacy guard", sv.Row, sv.Seq, sv.Verdict)
		}
	}
	for _, rv := range report.Rows {
		if !rv.Pass && !strings.HasPrefix(rv.Row, "C") {
			fail("FAIL: row %s %s: %s", rv.Row, rv.Key, strings.Join(rv.Notes, "; "))
		}
	}
	report.Pass = !report.Void && !slices.ContainsFunc(report.Reasons, func(s string) bool { return strings.HasPrefix(s, "FAIL") })
	return report
}

// applyCounterProof is form B: it lifts the equal 5xx cells that have no log proof to EQUAL_PAST_GUARD, one cell at a
// time, when (1) the yardstick cell is a MATCH (the 4a log has its decision id), (2) the enforced run's answer equals
// the yardstick's in status and body (already so for a VOID_PARITY cell), (3) its namespace's counter proof stands and
// (4) its answer is none of the guard's deny digests. The digests and the counter table go to the record.
func applyCounterProof(report *Report, in EnforcedInput, yardVerdict func(row, caller string, k int) string, unjudgedRoutes map[string]bool) {
	idle := idleSweep(in.Log, in.Slots, in.Attributed, in.Grace)
	report.Notes = append(report.Notes, "judge-enforced: idle sweep: "+idle.note)
	for _, u := range idle.unattributed {
		report.Reasons = append(report.Reasons, "FAIL: unattributed request in the go-api log (unknown traffic): "+u)
	}
	first, _ := driveSpan(in.Slots)
	processNote, process := sameProcess(in.Process, readLive(in.MetricsBefore), readLive(in.MetricsAfter), first)
	report.Notes = append(report.Notes, "judge-enforced: same process: "+processNote)
	if !process {
		report.Reasons = append(report.Reasons, "FAIL: same process not shown: "+processNote)
	}
	proof, notes, fails := counterProof(in, report.Slots, idle, process)
	report.Notes = append(report.Notes, notes...)
	for _, f := range fails {
		report.Reasons = append(report.Reasons, "FAIL: "+f)
	}
	deny := guardDenyDigests(report.Slots)
	report.Notes = append(report.Notes, fmt.Sprintf("judge-enforced: guard deny body digests (%d): %s", len(deny), strings.Join(deny, " ")))
	for i, sv := range report.Slots {
		if sv.Verdict != VerdictVoidParity || sv.Status < 500 || unjudgedRoutes[sv.Key()] {
			continue
		}
		if yardVerdict(sv.Row, sv.Caller, sv.K) == VerdictMatch && proof[in.Namespaces[sv.Key()]] {
			report.Slots[i].Verdict = VerdictEqualPastGuard
		}
	}
	for i, sv := range report.Slots {
		if sv.Verdict == VerdictEqualPastGuard && slices.Contains(deny, sv.BodyNormSHA) {
			report.Slots[i].Verdict = VerdictVoidParity
			report.Reasons = append(report.Reasons, fmt.Sprintf("FAIL: slot %d row %s %s: the answer carries the guard's deny digest %.12s: it is not past the guard", sv.Seq, sv.Row, sv.Caller, sv.BodyNormSHA))
		}
	}
}

// unjudgedOutsideClosedSet lists, sorted, every "route reason" pair of the unjudged counter whose reason go-api can no
// longer emit: its closed set is read from the production policy package (policy.EnforceUnjudgedReasons), never copied.
func unjudgedOutsideClosedSet(reasons map[string]map[string]int) []string {
	closed := policy.EnforceUnjudgedReasons()
	var outside []string
	for route, byReason := range reasons {
		for reason := range byReason {
			if !slices.Contains(closed, reason) {
				outside = append(outside, route+" "+reason)
			}
		}
	}
	slices.Sort(outside)
	return outside
}

// RowNamespaces maps each go-api shadow row key to its action namespace.
func RowNamespaces(profiles []byte) (map[string]string, error) {
	rows, err := policy.ParseShadowRows(profiles, accessrouting.Service)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for key, row := range rows {
		out[key] = row.Namespace
	}
	return out, nil
}

const verdictsHeaderV1 = "seq\trow\tcaller\tk\tkey\tstatus\tverdict"

// verdictsHeader is the v2 header: v1 plus the normalized body digest of the slot.
const verdictsHeader = "seq\trow\tcaller\tk\tkey\tstatus\tbody_norm_sha\tverdict"

// WriteVerdicts writes one line per slot: its cell, its route key, its HTTP status and its verdict. The file of a run
// with enforcement off is the yardstick of an enforced run.
func (r Report) WriteVerdicts(w io.Writer) error {
	var b strings.Builder
	b.WriteString(verdictsHeader + "\n")
	for _, sv := range r.Slots {
		norm := sv.BodyNormSHA
		if norm == "" {
			norm = none
		}
		fmt.Fprintf(&b, "%d\t%s\t%s\t%d\t%s\t%d\t%s\t%s\n", sv.Seq, sv.Row, sv.Caller, sv.K, sv.Key(), sv.Status, norm, sv.Verdict)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// ParseYardstick reads a verdicts file written by WriteVerdicts. A gap slot has no status and is left out.
func ParseYardstick(raw []byte) ([]YardSlot, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) > 0 && lines[0] == verdictsHeaderV1 {
		return nil, errors.New("yardstick: header v1 refused: the file has no body digests (judge the yardstick run again)")
	}
	if len(lines) == 0 || lines[0] != verdictsHeader {
		return nil, errors.New("yardstick: the first line is not the verdicts header")
	}
	var out []YardSlot
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 8 {
			return nil, fmt.Errorf("yardstick: line %d has %d columns, want 8", n+2, len(f))
		}
		k, err1 := strconv.Atoi(f[3])
		status, err2 := strconv.Atoi(f[5])
		if err := errors.Join(err1, err2); err != nil {
			return nil, fmt.Errorf("yardstick: line %d: %w", n+2, err)
		}
		if f[7] == VerdictGap {
			continue
		}
		norm := f[6]
		if norm == none {
			norm = ""
		}
		seq, err := strconv.Atoi(f[0])
		if err != nil {
			return nil, fmt.Errorf("yardstick: line %d: %w", n+2, err)
		}
		out = append(out, YardSlot{Row: f[1], Caller: f[2], Seq: seq, K: k, Key: f[4], Status: status, BodyNorm: norm, Verdict: f[7]})
	}
	if len(out) == 0 {
		return nil, errors.New("yardstick: no slot")
	}
	return out, nil
}

// judgeOutboundEnforced checks the enforced run's stub evidence (checkOutbound), holds the yardstick's own outbound file
// to the same reach rule, and gives a passing slot whose outside calls differ from its yardstick slot's
// MISMATCH_OUTBOUND. Without external rows and without a yardstick outbound file there is nothing to compare.
func judgeOutboundEnforced(report *Report, in EnforcedInput, byRow map[string]rowClass) outboundCheck {
	check := checkOutbound(in.Slots, in.Outbound, byRow, in.Grace, []string{"before", "between", "after"})
	external := map[string]bool{}
	for _, s := range sentSlots(in.Slots) {
		if c, ok := classOf(s.Key()); ok && c.ext != none {
			external[s.Row] = true
		}
	}
	if !in.YardOutboundGiven {
		if len(external) > 0 {
			check.voids = append(check.voids, "external rows were sent but the yardstick's outbound file was not given")
		}
		return check
	}
	type cell struct {
		row, caller string
		k           int
	}
	yard := map[cell]OutboundSlot{}
	for _, y := range in.YardOutbound {
		yard[cell{y.Row, y.Caller, y.K}] = y
	}
	var rows []string
	slotKey := map[string]string{}
	for _, s := range sentSlots(in.Slots) {
		slotKey[s.Row] = s.Key()
	}
	for row := range external {
		rows = append(rows, row)
	}
	slices.Sort(rows)
	for _, row := range rows {
		if yk := keyOfYard(in.YardOutbound, row); yk != "" && yk != slotKey[row] {
			check.voids = append(check.voids, fmt.Sprintf("the yardstick names route %s for row %s, but the run sent it on %s: the files are of different rows", yk, row, slotKey[row]))
			continue
		}
		if reason, ok := NoOutboundRows[slotKey[row]]; ok {
			var callers []string
			var statuses []int
			for _, y := range in.YardOutbound {
				if y.Row != row {
					continue
				}
				callers, statuses = append(callers, y.Caller), append(statuses, y.Status)
				if y.Hits > 0 {
					check.fails = append(check.fails, fmt.Sprintf("FAIL: row %s %s k=%d: %d outside calls in the yardstick, but the row is pinned NO_OUTBOUND (%s)", row, y.Caller, y.K, y.Hits, reason))
				}
			}
			if shown := decisionShown(row, "yardstick", callers, statuses); len(shown) > 0 {
				check.noReach = append(check.noReach, row)
				check.fails = append(check.fails, shown...)
			}
			continue
		}
		allowed, reached, refusedHit := 0, 0, 0
		for _, y := range in.YardOutbound {
			if y.Row != row {
				continue
			}
			if denied(y.Status) {
				if y.Hits > 0 {
					refusedHit++
				}
				continue
			}
			allowed++
			if y.Hits > 0 {
				reached++
			}
		}
		if reached == 0 {
			check.noReach = append(check.noReach, row)
			check.fails = append(check.fails, fmt.Sprintf("FAIL: row %s VOID: in the yardstick %d allowed slots, %d reached the stub (%d answered 401/403 with a stub hit: a relayed refusal or a call before the guard); the row is no evidence", row, allowed, reached, refusedHit))
		}
	}
	for i := range report.Slots {
		sv := &report.Slots[i]
		if sv.Gap() {
			continue
		}
		sv.Outbound = check.hits[sv.Seq]
		y, found := yard[cell{sv.Row, sv.Caller, sv.K}]
		if sv.Pass() && (!found || y.Hits != sv.Outbound) {
			sv.Verdict = VerdictOutboundDiffers
		}
	}
	return check
}

// keyOfYard is the route key of row in the yardstick outbound file, "" when the row is not in it.
func keyOfYard(yard []OutboundSlot, row string) string {
	for _, y := range yard {
		if y.Row == row {
			return y.Key
		}
	}
	return ""
}

// bodyVerdict gives a status-equal cell its body verdict: a differing normalized body is MISMATCH_BODY; an equal 5xx
// is EQUAL_PAST_GUARD only with the log proof that the decision point allowed the request, PDP_UNAVAILABLE when the log
// shows no verdict, else VOID_PARITY (also when the route has an unjudged count for another reason than
// org_required). A cell the yardstick skipped (a token with no organization, the counter decides it) needs no proof here.
func bodyVerdict(in EnforcedInput, slot Slot, y YardSlot, verdict string, unjudged, differs bool) string {
	switch {
	case differs:
		return VerdictMismatchBody
	case slot.Status < 500 || y.Verdict == VerdictSkipped:
		return verdict
	}
	if slot.Key() == orgDeleteKey && orgDeletionRefusedProof(in.Log, slot, in.Grace) {
		if unjudged {
			return VerdictVoidParity
		}
		return VerdictEqualPastGuard
	}
	switch pastGuard(in.Log, slot, in.Grace) {
	case proofAllow:
		if unjudged {
			return VerdictVoidParity
		}
		return VerdictEqualPastGuard
	case proofUnavailable:
		return VerdictPDPUnavailable
	}
	return VerdictVoidParity
}

type guardProof int

const (
	proofNone guardProof = iota
	proofAllow
	proofUnavailable
)

// pastGuard reads the go-api log for slot: exactly one result line of its route and caller kind in its window, a match
// with a decision id, is the proof that the decision point allowed the request; an error or dropped line is no verdict.
func pastGuard(ev *Evidence, slot Slot, grace time.Duration) guardProof {
	if ev == nil {
		return proofNone
	}
	from, to := slot.Start.Add(-windowSkew), slot.End.Add(grace)
	var lines []resultLine
	for _, l := range ev.results {
		if l.route == slot.Key() && l.caller == callerKind(slot.Caller) && !l.at.Before(from) && !l.at.After(to) {
			lines = append(lines, l)
		}
	}
	for _, l := range lines {
		if l.result == "error" || l.result == "dropped" {
			return proofUnavailable
		}
	}
	if len(lines) == 1 && lines[0].result == "match" && lines[0].decision != "" {
		return proofAllow
	}
	return proofNone
}

// orgDeleteKey is row 13, the organization delete; its 5xx in the venue is the ClickHouse refusal that precedes any
// change (orgdeletion_clickhouse.go: refusalDSNUnset), after the guard.
const orgDeleteKey = "DELETE /api/v1/admin/orgs/{org_id}"

// OrgDeletionRefusedMsg is the go-api ERROR line of a refused organization delete; reason dsn_unset is the venue's
// ClickHouse URI left unset on purpose (row 13 stays the designed 503).
const OrgDeletionRefusedMsg = "admin: org deletion refused"

// orgDeletionRefusedProof is the EQUAL_PAST_GUARD proof of row 13: exactly one refusal line with reason dsn_unset for
// the organization named by the last segment of the slot's path, in the slot's window. The line carries the org id and
// no request id, and the slots are sequential with a gap, so the pair (org id, window) is the match.
func orgDeletionRefusedProof(ev *Evidence, slot Slot, grace time.Duration) bool {
	if ev == nil {
		return false
	}
	org := slot.Path[strings.LastIndex(slot.Path, "/")+1:]
	from, to := slot.Start.Add(-windowSkew), slot.End.Add(grace)
	n := 0
	for _, l := range ev.refusals {
		if l.orgID == org && !l.at.Before(from) && !l.at.After(to) {
			if l.reason != "dsn_unset" {
				return false
			}
			n++
		}
	}
	return n == 1
}

// ScheduleSHA12 is the first 12 hex digits of the sha-256 of the sent slots' row, caller, k and route key, in order: the
// schedule of a run. The path (the pool org), the status and the time are not part of it, so the yardstick run and the
// enforced run, which use other throwaway orgs, share one schedule when they send the same cells in the same order.
func ScheduleSHA12(slots []Slot) string {
	h := sha256.New()
	for _, s := range slots {
		if s.Gap() {
			continue
		}
		fmt.Fprintf(h, "%s\t%s\t%d\t%s\n", s.Row, s.Caller, s.K, s.Key())
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

func yardScheduleSHA12(yard []YardSlot) string {
	h := sha256.New()
	for _, y := range yard {
		fmt.Fprintf(h, "%s\t%s\t%d\t%s\n", y.Row, y.Caller, y.K, y.Key)
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}
