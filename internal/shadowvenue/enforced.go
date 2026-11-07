package shadowvenue

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

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
	VerdictUnevidencedSeam  = "UNEVIDENCED_SEAM"
	VerdictControlNotDenied = "CONTROL_NOT_ENFORCED"
)

// YardSlot is a slot of the yardstick run: its cell, its legacy HTTP status and the verdict that run gave it.
type YardSlot struct {
	Row, Caller string
	K           int
	Key         string
	Status      int
	Verdict     string
}

// EnforcedInput is everything an enforced judgement reads.
type EnforcedInput struct {
	Slots []Slot
	Yard  []YardSlot
	K     int
	// EnforceEnv is the readback of the venue go-api environment (an env dump or the one line); Namespaces maps each
	// row key to its action namespace, so the namespaces the run needs are known.
	EnforceEnv string
	Namespaces map[string]string
	// The data digest of the yardstick run, taken before this run, and after it: all three must be equal.
	YardDigest, DigestBefore, DigestAfter string
	// go-api /metrics scrapes before and after the run, for the unjudged counter.
	MetricsBefore, MetricsAfter []byte
}

// enforcedNamespaces reads GO_API_AUTHZ_ENFORCE from env; false when the variable is absent or empty.
func enforcedNamespaces(env string) (map[string]bool, bool) {
	for _, line := range strings.Split(env, "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "GO_API_AUTHZ_ENFORCE="); ok {
			out := map[string]bool{}
			for _, ns := range strings.Split(value, ",") {
				if ns = strings.TrimSpace(ns); ns != "" {
					out[ns] = true
				}
			}
			return out, len(out) > 0
		}
	}
	return nil, false
}

// JudgeEnforced judges a run with enforcement on against the yardstick. Void (exit 3): the enforcement config unread or
// missing a namespace the rows need, a data digest that differs, an unreadable counter, fewer than two controls, a
// control the enforcement did not deny. Per slot, every caller: the enforced status must equal the yardstick's legacy
// status; a difference is MISMATCH_STRICTER or MISMATCH_WIDER by direction (403 = deny), else MISMATCH_STATUS. The
// superuser's skips of the yardstick pass only on the counter: per route, unjudged{reason="no_org"} grew by exactly the
// number of those slots and no other reason grew; a seam row (SKIPPED_SEAM in the yardstick) allows served_org only.
func JudgeEnforced(in EnforcedInput) Report {
	var report Report
	fail := func(format string, args ...any) {
		report.Reasons = append(report.Reasons, fmt.Sprintf(format, args...))
	}
	void := func(format string, args ...any) {
		report.Void = true
		fail("VOID: "+format, args...)
	}
	enforced, readable := enforcedNamespaces(in.EnforceEnv)
	if !readable {
		void("the enforce config GO_API_AUTHZ_ENFORCE was not read back from the venue go-api, or is empty")
	}
	var need []string
	for _, s := range in.Slots {
		if ns := in.Namespaces[s.Key()]; ns != "" && !s.Gap() && !slices.Contains(need, ns) {
			need = append(need, ns)
		}
	}
	slices.Sort(need)
	for _, ns := range need {
		if readable && !enforced[ns] {
			void("namespace %s is not enforced on the venue go-api, but a row of it runs", ns)
		}
	}
	if in.YardDigest == "" || in.DigestBefore == "" || in.DigestAfter == "" {
		void("a data digest is missing (yardstick %q, before %q, after %q)", in.YardDigest, in.DigestBefore, in.DigestAfter)
	} else if in.YardDigest != in.DigestBefore || in.DigestBefore != in.DigestAfter {
		void("the data digest differs (yardstick %.12s, before %.12s, after %.12s): the seeded state is not the yardstick's", in.YardDigest, in.DigestBefore, in.DigestAfter)
	}
	reasons, err := ParseUnjudgedReasons(in.MetricsBefore, in.MetricsAfter)
	if err != nil {
		void("the unjudged counter is unreadable: %v", err)
	}

	type cell struct {
		row, caller string
		k           int
	}
	yard := map[cell]YardSlot{}
	for _, y := range in.Yard {
		yard[cell{y.Row, y.Caller, y.K}] = y
	}
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
			switch y.Verdict {
			case VerdictSkipped:
				sv.Verdict = VerdictUnevidencedNoOrg
			case VerdictSkippedSeam:
				sv.Verdict = VerdictUnevidencedSeam
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
	// The superuser's skips and the seam rows pass on the counter, per route.
	if err == nil {
		perKey := map[string][]int{}
		for i, sv := range report.Slots {
			if sv.Verdict == VerdictUnevidencedNoOrg || sv.Verdict == VerdictUnevidencedSeam {
				perKey[sv.Key()] = append(perKey[sv.Key()], i)
			}
		}
		for key, indexes := range perKey {
			seam := report.Slots[indexes[0]].Verdict == VerdictUnevidencedSeam
			got := reasons[key]
			ok := true
			for reason, n := range got {
				switch {
				case reason == "no_org" && !seam: // counted exactly below
				case reason == "served_org" && seam:
				case n != 0:
					ok = false
				}
			}
			if !seam && got["no_org"] != len(indexes) {
				ok = false
			}
			if ok {
				for _, i := range indexes {
					report.Slots[i].Verdict = VerdictSkippedNoOrg
					if seam {
						report.Slots[i].Verdict = VerdictSkippedSeam
					}
				}
			}
		}
	}
	for _, sv := range report.Slots {
		if sv.Expect != expectMismatch && !sv.Pass() {
			fail("FAIL: slot %d row %s %s %s: %s", sv.Seq, sv.Row, sv.Caller, sv.Key(), sv.Verdict)
		}
	}
	report.Rows = rowVerdicts(report.Slots, in.K)
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

const verdictsHeader = "seq\trow\tcaller\tk\tkey\tstatus\tverdict"

// WriteVerdicts writes one line per slot: its cell, its route key, its HTTP status and its verdict. The file of a run
// with enforcement off is the yardstick of an enforced run.
func (r Report) WriteVerdicts(w io.Writer) error {
	var b strings.Builder
	b.WriteString(verdictsHeader + "\n")
	for _, sv := range r.Slots {
		fmt.Fprintf(&b, "%d\t%s\t%s\t%d\t%s\t%d\t%s\n", sv.Seq, sv.Row, sv.Caller, sv.K, sv.Key(), sv.Status, sv.Verdict)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// ParseYardstick reads a verdicts file written by WriteVerdicts. A gap slot has no status and is left out.
func ParseYardstick(raw []byte) ([]YardSlot, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != verdictsHeader {
		return nil, errors.New("yardstick: the first line is not the verdicts header")
	}
	var out []YardSlot
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 7 {
			return nil, fmt.Errorf("yardstick: line %d has %d columns, want 7", n+2, len(f))
		}
		k, err1 := strconv.Atoi(f[3])
		status, err2 := strconv.Atoi(f[5])
		if err := errors.Join(err1, err2); err != nil {
			return nil, fmt.Errorf("yardstick: line %d: %w", n+2, err)
		}
		if f[6] == VerdictGap {
			continue
		}
		out = append(out, YardSlot{Row: f[1], Caller: f[2], K: k, Key: f[4], Status: status, Verdict: f[6]})
	}
	if len(out) == 0 {
		return nil, errors.New("yardstick: no slot")
	}
	return out, nil
}
