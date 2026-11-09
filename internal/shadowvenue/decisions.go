package shadowvenue

import (
	"bufio"
	"bytes"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// decisionMetric is the enforcement decision counter (internal/api/policy/enforcecounters.go); its labels are
// namespace, service and outcome.
const decisionMetric = "dev_health_authz_enforcement_decisions_total"

// DecisionDeltas is the increase of the decision counter between two scrapes, per namespace and outcome. A scrape with
// no series of the metric has none (the counter is created at its first increment); a series that went down, a label
// that is not quoted, or a value that is not a whole number is refused.
func ParseDecisionDeltas(before, after []byte) (map[string]map[string]int, error) {
	b, err := decisionSeries(before)
	if err != nil {
		return nil, fmt.Errorf("before scrape: %w", err)
	}
	a, err := decisionSeries(after)
	if err != nil {
		return nil, fmt.Errorf("after scrape: %w", err)
	}
	for ns, byOutcome := range b {
		for outcome, was := range byOutcome {
			if now, ok := a[ns][outcome]; !ok || now < was {
				return nil, fmt.Errorf("namespace %s outcome %s: the counter went from %d to %d: the process restarted or the series vanished", ns, outcome, was, a[ns][outcome])
			}
		}
	}
	out := map[string]map[string]int{}
	for ns, byOutcome := range a {
		out[ns] = map[string]int{}
		for outcome, now := range byOutcome {
			out[ns][outcome] = now - b[ns][outcome]
		}
	}
	return out, nil
}

func decisionSeries(scrape []byte) (map[string]map[string]int, error) {
	out := map[string]map[string]int{}
	scanner := bufio.NewScanner(bytes.NewReader(scrape))
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, decisionMetric+"{") {
			continue
		}
		labels, value, ok := strings.Cut(strings.TrimPrefix(line, decisionMetric+"{"), "} ")
		if !ok {
			return nil, fmt.Errorf("unreadable series %q", line)
		}
		ns, outcome, service := "", "", ""
		for _, part := range splitLabels(labels) {
			name, val, ok := strings.Cut(part, "=")
			if !ok || len(val) < 2 || val[0] != '"' || val[len(val)-1] != '"' {
				return nil, fmt.Errorf("unreadable label %q", part)
			}
			switch name {
			case "namespace":
				ns = val[1 : len(val)-1]
			case "outcome":
				outcome = val[1 : len(val)-1]
			case "service":
				service = val[1 : len(val)-1]
			}
		}
		if service != "go-api" {
			continue
		}
		n, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || n < 0 || n != float64(int(n)) {
			return nil, fmt.Errorf("series %s/%s has value %q", ns, outcome, value)
		}
		if ns == "" || outcome == "" {
			return nil, fmt.Errorf("a decision series has no namespace or no outcome label: %q", line)
		}
		if out[ns] == nil {
			out[ns] = map[string]int{}
		}
		out[ns][outcome] += int(n)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// AttributedRequest names a request of the run that is no slot: its method and route, the drive source that makes it
// and, when known, its request id in the go-api log.
type AttributedRequest struct{ Method, Route, Source, RequestID string }

// ParseAttributed reads the attribution file: one request per line, tab-separated: method, route, source file:line and
// an optional request id. A line with fewer than three fields, or an empty field, is refused.
func ParseAttributed(raw []byte) ([]AttributedRequest, error) {
	var out []AttributedRequest
	for i, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) < 3 || len(f) > 4 || f[0] == "" || f[1] == "" || f[2] == "" {
			return nil, fmt.Errorf("attribution line %d: want method, route, source[, request id], tab-separated", i+1)
		}
		a := AttributedRequest{Method: f[0], Route: f[1], Source: f[2]}
		if len(f) == 4 {
			a.RequestID = f[3]
		}
		out = append(out, a)
	}
	return out, nil
}

// idleSweep maps every "http request" line of the enforced run's go-api log to a sent slot, by method, route, status
// and the slot's window; a line left over is extra traffic. The extras must be attributed (the drive's own sign-in);
// one that is not is unknown traffic. extraKeys are the attributed extras' route keys, to be counted by the counter
// proof when their route belongs to a counted namespace.
type idleResult struct {
	slotsMatched, lines int
	extraKeys           []extraRequest
	unattributed        []string
	ok                  bool
	note                string
}

type extraRequest struct {
	key    string
	status int
	source string
}

func idleSweep(ev *Evidence, slots []Slot, attributed []AttributedRequest, grace time.Duration) idleResult {
	var res idleResult
	if ev == nil || len(ev.requests) == 0 {
		res.note = "the go-api log has no http request lines: the idle sweep cannot be derived"
		return res
	}
	res.lines = len(ev.requests)
	used := make([]bool, len(ev.requests))
	for _, s := range slots {
		if s.Gap() {
			continue
		}
		from, to := s.Start.Add(-windowSkew), s.End.Add(grace)
		for i, l := range ev.requests {
			if used[i] || l.method+" "+l.route != s.Key() || l.status != s.Status || l.at.Before(from) || l.at.After(to) {
				continue
			}
			used[i] = true
			res.slotsMatched++
			break
		}
	}
	taken := make([]bool, len(attributed))
	for i, l := range ev.requests {
		if used[i] {
			continue
		}
		matched := false
		for j, a := range attributed {
			if taken[j] || a.Method != l.method || a.Route != l.route || (a.RequestID != "" && a.RequestID != l.requestID) {
				continue
			}
			taken[j], matched = true, true
			res.extraKeys = append(res.extraKeys, extraRequest{key: l.method + " " + l.route, status: l.status, source: a.Source})
			break
		}
		if !matched {
			res.unattributed = append(res.unattributed, fmt.Sprintf("%s %s %d (request %s, %s)", l.method, l.route, l.status, l.requestID, l.at.Format(time.RFC3339Nano)))
		}
	}
	res.ok = len(res.unattributed) == 0
	res.note = fmt.Sprintf("%d http request lines: %d are sent slots, %d attributed extra, %d unattributed", res.lines, res.slotsMatched, len(res.extraKeys), len(res.unattributed))
	return res
}

type nsCount struct{ allow, deny int }

// counterProof is the exact-match proof of form B (D6894): per namespace, the increase of the decision counter
// between the scrapes equals the allowed and denied slots the judge counts for it (a slot answered 403 is denied, any
// other judged slot is allowed; the superuser's no-org skips and the seam cells are unjudged and not counted), plus
// the attributed extra requests of that namespace, and no error_deny or impersonation_refused counted. The proof stands
// only when the idle sweep holds. It returns the namespaces whose proof stands, one note per namespace and the reasons
// of the ones that do not.
func counterProof(in EnforcedInput, slots []SlotVerdict, idle idleResult) (proof map[string]bool, notes, fails []string) {
	proof = map[string]bool{}
	deltas, err := ParseDecisionDeltas(in.MetricsBefore, in.MetricsAfter)
	if err != nil {
		return proof, nil, []string{fmt.Sprintf("the decision counter is unreadable: %v", err)}
	}
	want := map[string]nsCount{}
	for _, sv := range slots {
		ns := in.Namespaces[sv.Key()]
		if sv.Gap() || ns == "" {
			continue
		}
		switch sv.Verdict {
		case VerdictSkippedNoOrg, VerdictUnevidencedNoOrg, VerdictEvidencedNoOrg, VerdictSkippedSeam, VerdictUnevidencedSeam, VerdictMissing:
			continue
		}
		c := want[ns]
		if sv.Status == 403 {
			c.deny++
		} else {
			c.allow++
		}
		want[ns] = c
	}
	for _, e := range idle.extraKeys {
		if ns := in.Namespaces[e.key]; ns != "" {
			c := want[ns]
			if e.status == 403 {
				c.deny++
			} else {
				c.allow++
			}
			want[ns] = c
		}
	}
	names := map[string]bool{}
	for ns := range want {
		names[ns] = true
	}
	for ns := range deltas {
		names[ns] = true
	}
	sorted := make([]string, 0, len(names))
	for ns := range names {
		sorted = append(sorted, ns)
	}
	sort.Strings(sorted)
	for _, ns := range sorted {
		got, w := deltas[ns], want[ns]
		extra := got["error_deny"] + got["impersonation_refused"]
		for outcome := range got {
			if !slices.Contains([]string{"allow", "deny", "error_deny", "impersonation_refused"}, outcome) {
				extra++
			}
		}
		ok := got["allow"] == w.allow && got["deny"] == w.deny && extra == 0 && idle.ok
		notes = append(notes, fmt.Sprintf("judge-enforced: counter %s: allow delta %d vs %d allowed slots, deny delta %d vs %d denied slots, other outcomes %d: %s", ns, got["allow"], w.allow, got["deny"], w.deny, extra, map[bool]string{true: "exact", false: "NO PROOF"}[ok]))
		if ok {
			proof[ns] = true
		} else {
			fails = append(fails, fmt.Sprintf("namespace %s has no counter proof: allow %d/%d, deny %d/%d, other outcomes %d, idle sweep %v", ns, got["allow"], w.allow, got["deny"], w.deny, extra, idle.ok))
		}
	}
	return proof, notes, fails
}

// guardDenyDigests are the distinct normalized body digests of the sent slots the guard denied (403): the answer the
// guard gives. A cell called past the guard whose answer carries one of them is not past it.
func guardDenyDigests(slots []SlotVerdict) []string {
	seen := map[string]bool{}
	for _, sv := range slots {
		if !sv.Gap() && sv.Status == 403 && sv.BodyNormSHA != "" {
			seen[sv.BodyNormSHA] = true
		}
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}
