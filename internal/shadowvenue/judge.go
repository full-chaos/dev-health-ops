package shadowvenue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"
)

// The go-api shadow log lines the judge reads (internal/api/policy/shadow.go).
const (
	MsgResult      = "api shadow pdp: result"
	MsgMismatch    = "api shadow pdp: mismatch with the legacy guard"
	MsgServedOther = "api shadow pdp: the request names an organization that is not the token organization"
	// MsgHTTPRequest is the access line of every request the listener served (internal/auth/httpapi/accesslog.go).
	MsgHTTPRequest = "http request"
)

// Slot verdicts. Only MATCH, MATCH_SERVED_OTHER (given to a cross-org cell only), MISMATCH_INTENTIONAL and MISMATCH_RULED
// pass.
const (
	VerdictMatch                = "MATCH"
	VerdictMatchServedOther     = "MATCH_SERVED_OTHER"
	VerdictNoPDP                = "NO_PDP"
	VerdictMismatch             = "MISMATCH"
	VerdictMismatchIntentional  = "MISMATCH_INTENTIONAL"
	VerdictMismatchRuled        = "MISMATCH_RULED"
	VerdictMismatchNoTuple      = "MISMATCH_NO_TUPLE"
	VerdictError                = "ERROR"
	VerdictDropped              = "DROPPED"
	VerdictSkipped              = "SKIPPED"
	VerdictImpersonationRefused = "IMPERSONATION_REFUSED"
	VerdictMissing              = "MISSING"
	VerdictDuplicate            = "DUPLICATE"
	VerdictGap                  = "GAP"
	VerdictKShort               = "K_SHORT"
	VerdictLegacyDisagrees      = "CONTROL_LEGACY_DISAGREES"
	VerdictSkippedSeam          = "SKIPPED_SEAM"
	VerdictSkippedNoOrg         = "SKIPPED_NOORG"
	// VerdictDecidedNoOrg: a no-org superuser slot that a go-api build which decides a token with no organization
	// (CHAOS-10872 S1) answered as the yardstick did, with the counter evidence of that build (JudgeEnforced).
	VerdictDecidedNoOrg = "DECIDED_NOORG"
	VerdictStricter     = "MISMATCH_STRICTER"
	VerdictWider        = "MISMATCH_WIDER"
)

var resultVerdicts = map[string]string{
	"error": VerdictError, "dropped": VerdictDropped, "skipped": VerdictSkipped, "impersonation_refused": VerdictImpersonationRefused,
}

// windowSkew allows for the driver stamping its start a little after go-api's own clock reads the request.
const windowSkew = 50 * time.Millisecond

type resultLine struct {
	at                              time.Time
	route, caller, result, decision string
	used                            bool
}

// requestLine is a go-api "http request" access line: the request the listener served.
type requestLine struct {
	at              time.Time
	method, route   string
	status          int
	requestID, peer string
}

// refusalLine is a go-api "admin: org deletion refused" line: the organization and the refusal reason.
type refusalLine struct {
	at            time.Time
	orgID, reason string
}

type warnLine struct {
	at            time.Time
	route, caller string
	decision      string
	tuple         Tuple
}

// Tuple is a mismatch as the WARN line names it.
type Tuple struct{ Class, Legacy, Shadow, Reason string }

// Evidence is the shadow lines of one go-api log.
type Evidence struct {
	results     []resultLine
	requests    []requestLine
	refusals    []refusalLine
	mismatches  []warnLine
	servedOther []warnLine
}

// ParseLog reads go-api's JSON log. Lines that are not JSON, or JSON of another message, are skipped; a shadow line
// that lacks a field or has a value outside its closed set is refused, as the evidence would be unreadable.
func ParseLog(r io.Reader) (Evidence, error) {
	var ev Evidence
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	n := 0
	for scanner.Scan() {
		n++
		var rec map[string]any
		if json.Unmarshal(scanner.Bytes(), &rec) != nil {
			for _, msg := range []string{MsgResult, MsgMismatch, MsgServedOther, OrgDeletionRefusedMsg, MsgHTTPRequest} {
				if strings.Contains(scanner.Text(), `"`+msg+`"`) {
					return Evidence{}, fmt.Errorf("log line %d: a %q line that is not JSON: the evidence is unreadable", n, msg)
				}
			}
			continue
		}
		msg, _ := rec["msg"].(string)
		if msg == OrgDeletionRefusedMsg {
			stamp, _ := rec["time"].(string)
			at, err := time.Parse(time.RFC3339Nano, stamp)
			orgID, _ := rec["org_id"].(string)
			reason, _ := rec["reason"].(string)
			if err != nil || orgID == "" || reason == "" {
				return Evidence{}, fmt.Errorf("log line %d: a %q line without a valid time, org_id or reason", n, msg)
			}
			ev.refusals = append(ev.refusals, refusalLine{at: at, orgID: orgID, reason: reason})
			continue
		}
		if msg == MsgHTTPRequest {
			stamp, _ := rec["time"].(string)
			at, err := time.Parse(time.RFC3339Nano, stamp)
			method, _ := rec["method"].(string)
			route, _ := rec["route"].(string)
			status, okStatus := rec["status"].(float64)
			id, _ := rec["request_id"].(string)
			peer, _ := rec["peer"].(string)
			if err != nil || method == "" || route == "" || !okStatus {
				return Evidence{}, fmt.Errorf("log line %d: a %q line without a valid time, method, route or status", n, msg)
			}
			ev.requests = append(ev.requests, requestLine{at: at, method: method, route: route, status: int(status), requestID: id, peer: peer})
			continue
		}
		if msg != MsgResult && msg != MsgMismatch && msg != MsgServedOther {
			continue
		}
		field := func(name string) (string, bool) {
			v, ok := rec[name].(string)
			return v, ok && v != ""
		}
		stamp, _ := field("time")
		at, err := time.Parse(time.RFC3339Nano, stamp)
		route, okRoute := field("route")
		caller, okCaller := field("caller")
		if err != nil || !okRoute || !okCaller || !slices.Contains(callerOrder, caller) {
			return Evidence{}, fmt.Errorf("log line %d: a %q line without a valid time, route or caller", n, msg)
		}
		decision, _ := field("decision_id")
		if _, present := rec["decision_id"]; present && decision == "" {
			return Evidence{}, fmt.Errorf("log line %d: decision_id is not a non-empty string", n)
		}
		switch msg {
		case MsgResult:
			result, _ := field("result")
			if result != "match" && result != "mismatch" && resultVerdicts[result] == "" {
				return Evidence{}, fmt.Errorf("log line %d: result %q", n, result)
			}
			ev.results = append(ev.results, resultLine{at: at, route: route, caller: caller, result: result, decision: decision})
		default:
			class, ok1 := field("class")
			legacy, ok2 := field("legacy")
			shadow, ok3 := field("shadow")
			reason, ok4 := field("reason_code")
			if msg == MsgServedOther {
				reason, ok4 = field("reason")
			}
			if !ok1 || !ok2 || !ok3 || !ok4 {
				return Evidence{}, fmt.Errorf("log line %d: a mismatch line without its class, legacy, shadow and reason", n)
			}
			w := warnLine{at: at, route: route, caller: caller, decision: decision, tuple: Tuple{Class: class, Legacy: legacy, Shadow: shadow, Reason: reason}}
			if msg == MsgMismatch {
				if decision == "" {
					return Evidence{}, fmt.Errorf("log line %d: a decision mismatch line without decision_id", n)
				}
				ev.mismatches = append(ev.mismatches, w)
			} else {
				ev.servedOther = append(ev.servedOther, w)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return Evidence{}, err
	}
	return ev, nil
}

// Ruling is a mismatch tuple pm has given a passing verdict (intentional or legacy_bug_kept).
type Ruling struct{ Route, Caller, Class, Legacy, Shadow, Reason, Verdict string }

var passingRulings = []string{"intentional", "legacy_bug_kept"}

const rulingsHeader = "route\tcaller\tclass\tlegacy\tshadow\treason\tverdict"

// ParseRulings reads a rulings file.
func ParseRulings(raw []byte) ([]Ruling, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != rulingsHeader {
		return nil, fmt.Errorf("rulings: the first line is not %q", rulingsHeader)
	}
	var out []Ruling
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 7 || !slices.Contains(passingRulings, f[6]) {
			return nil, fmt.Errorf("rulings: line %d: want 7 columns and a verdict of %v", n+2, passingRulings)
		}
		out = append(out, Ruling{Route: f[0], Caller: f[1], Class: f[2], Legacy: f[3], Shadow: f[4], Reason: f[5], Verdict: f[6]})
	}
	return out, nil
}

// JudgeOptions: K slots per cell; Grace after a slot's answer in which its result line may still land.
type JudgeOptions struct {
	K     int
	Grace time.Duration
	// NoOrg is the enforcement counter evidence for the superuser's no-organization skips; nil gives none.
	NoOrg NoOrgEvidence
	// Outbound is the venue stub's evidence of outside calls (checkOutbound).
	Outbound Outbound
	// Strict holds every sent slot to its claim read-back and body digest (a missing one voids the run) and the
	// superuser-O slots to the membership snapshots. The command always sets it; a run without it judges as before.
	Strict     bool
	Membership Membership
}

// SlotVerdict is one slot and what its evidence says.
type SlotVerdict struct {
	Slot
	Verdict, DecisionID, Ruling string
	Tuple                       Tuple
	// Outbound is the number of outside calls the venue stub saw in the slot's window.
	Outbound int
}

// Pass reports whether the slot's evidence passes it.
func (s SlotVerdict) Pass() bool {
	switch s.Verdict {
	case VerdictMatch, VerdictMatchServedOther, VerdictMismatchIntentional, VerdictMismatchRuled, VerdictSkippedSeam, VerdictSkippedNoOrg, VerdictDecidedNoOrg, VerdictMatchEnforced, VerdictEqualPastGuard:
		return true
	}
	return false
}

// seamSkipRows are the rows on which the superuser, whose token has no organization, is answered by the served-org seam
// (CHAOS-13209, #5862): the request names an organization, the seam keeps the legacy answer and asks the decision
// point nothing, so the shadow's result is "skipped" and enforcement equals legacy by construction. The result line
// carries no reason, so the pass is pinned to these rows and to the superuser; any other skip fails the run.
var seamSkipRows = []string{
	"GET /api/v1/admin/orgs/{org_id}/members", "POST /api/v1/admin/orgs/{org_id}/members",
	"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}", "PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}",
	"GET /api/v1/billing/entitlements/{org_id}", "GET /api/v1/licensing/entitlements/{org_id}",
}

// RowVerdict is one row (or control) and its cells, by caller kind.
type RowVerdict struct {
	Row, Key string
	Cells    map[string]string
	Pass     bool
	Notes    []string
}

// Report is a judged run.
type Report struct {
	Slots   []SlotVerdict
	Rows    []RowVerdict
	Void    bool
	Pass    bool
	Reasons []string
	// Notes are informational lines of the record (the counter table, the guard deny digests, the idle sweep); they
	// never decide a verdict.
	Notes []string
}

// VerdictCounts counts the slots by verdict; a seam skip is its own count, never a match.
func (r Report) VerdictCounts() map[string]int {
	out := map[string]int{}
	for _, sv := range r.Slots {
		out[sv.Verdict]++
	}
	return out
}

// Exit is 0 when every row passes, 1 when a row fails, 3 when the run is void.
func (r Report) Exit() int {
	switch {
	case r.Void:
		return 3
	case !r.Pass:
		return 1
	}
	return 0
}

// Judge gives each slot its one result line: the line of the slot's route and caller in [start, end+grace]. No
// line, two lines, a line outside every slot, overlapping windows or a short cell fail the run; a control cell that
// does not mismatch, fewer than two control cells, or a log with no result line void it.
func Judge(slots []Slot, ev Evidence, rulings []Ruling, o JudgeOptions) Report {
	var report Report
	fail := func(format string, args ...any) {
		report.Reasons = append(report.Reasons, fmt.Sprintf(format, args...))
	}
	void := func(format string, args ...any) {
		report.Void = true
		fail("VOID: "+format, args...)
	}
	if o.Outbound.Rows == nil {
		void("no row table was given (Outbound.Rows): every run is judged against its table, held to the pinned classes")
	}
	byRow, classErr := CheckRowClasses(o.Outbound.Rows)
	if classErr != nil {
		void("%v", classErr)
	}
	if len(ev.results) == 0 {
		void("the log has no result line %q: wrong log, an image without the line, or the shadow is off", MsgResult)
	}
	results := slices.Clone(ev.results)
	sent := sentSlots(slots)
	for i := 1; i < len(sent); i++ {
		if sent[i].Start.Before(sent[i-1].End.Add(o.Grace + windowSkew)) {
			fail("FAIL: the windows of slots %d and %d overlap: the gap is shorter than grace %s", sent[i-1].Seq, sent[i].Seq, o.Grace)
		}
	}
	for _, slot := range slots {
		sv := SlotVerdict{Slot: slot}
		if slot.Gap() {
			sv.Verdict = VerdictGap
		} else {
			judgeSlot(&sv, results, ev, rulings, o.Grace)
			controlLegacyHolds(&sv)
		}
		report.Slots = append(report.Slots, sv)
	}
	applyStrict(&report, o.Strict, o.Membership, void)
	relabelNoOrgSkips(report.Slots, o.NoOrg)
	outbound := checkOutbound(slots, o.Outbound, byRow, o.Grace, []string{"before", "between"})
	for i := range report.Slots {
		report.Slots[i].Outbound = outbound.hits[report.Slots[i].Seq]
	}
	for _, reason := range outbound.voids {
		void("%s", reason)
	}
	report.Reasons = append(report.Reasons, outbound.fails...)
	for _, sv := range report.Slots {
		if sv.Expect != expectMismatch && !sv.Pass() {
			fail("FAIL: slot %d row %s %s %s: %s%s", sv.Seq, sv.Row, sv.Caller, sv.Key(), sv.Verdict, hint(sv))
		}
	}
	if len(sent) > 0 {
		from, to := sent[0].Start.Add(-windowSkew), sent[len(sent)-1].End.Add(o.Grace)
		for _, line := range results {
			if !line.used && !line.at.Before(from) && !line.at.After(to) {
				fail("FAIL: stray result line at %s (%s, %s, %s) in the run window: another client on the venue, or a window is wrong",
					line.at.Format(time.RFC3339Nano), line.route, line.caller, line.result)
			}
		}
	}
	report.Rows = rowVerdicts(report.Slots, o.K)
	markNoReach(report.Rows, outbound)
	controls := 0
	for _, rv := range report.Rows {
		if strings.HasPrefix(rv.Row, "C") {
			controls++
		}
	}
	if controls < 2 {
		void("%d control cells; a run needs two cells that must mismatch", controls)
	}
	for _, sv := range report.Slots {
		if sv.Expect == expectMismatch && !controlMismatch(sv.Verdict) {
			void("control %s slot %d is %s, not a mismatch with its tuple: the readout cannot be trusted", sv.Row, sv.Seq, sv.Verdict)
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

// controlLegacyHolds ties a control's mismatch to the legacy guard's real answer: the WARN line names the legacy
// verdict the shadow was told, and the HTTP status is what the guard answered (403 = deny). A control whose line and
// status disagree proves nothing about the readout, so it no longer counts as a mismatch.
func controlLegacyHolds(sv *SlotVerdict) {
	if sv.Expect != expectMismatch || !controlMismatch(sv.Verdict) {
		return
	}
	if denied := sv.Status == 403; denied != (sv.Tuple.Legacy == "deny") {
		sv.Verdict = VerdictLegacyDisagrees
	}
}

// intentionalMustEqualLegacy holds an intentional mismatch on a row to the product rule that the answer under enforce
// equals legacy: the shadow's answer must be what the guard really answered (HTTP 403 = deny). A served-org refusal
// of a request the guard also refused is equal; one the guard served is stricter, and a shadow allow of a refused
// request is wider. Both fail the row.
func intentionalMustEqualLegacy(sv *SlotVerdict) {
	legacyDenied, shadowDenied := sv.Status == 403, sv.Tuple.Shadow == "deny"
	switch {
	case legacyDenied == shadowDenied:
	case shadowDenied:
		sv.Verdict = VerdictStricter
	default:
		sv.Verdict = VerdictWider
	}
}

func sentSlots(slots []Slot) []Slot {
	var out []Slot
	for _, s := range slots {
		if !s.Gap() {
			out = append(out, s)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

func judgeSlot(sv *SlotVerdict, results []resultLine, ev Evidence, rulings []Ruling, grace time.Duration) {
	from, to := sv.Start.Add(-windowSkew), sv.End.Add(grace)
	var hits []int
	for i, line := range results {
		if line.route == sv.Key() && line.caller == callerKind(sv.Caller) && !line.at.Before(from) && !line.at.After(to) {
			hits = append(hits, i)
		}
	}
	for _, i := range hits {
		results[i].used = true
	}
	switch len(hits) {
	case 0:
		sv.Verdict = VerdictMissing
		return
	case 1:
	default:
		sv.Verdict = VerdictDuplicate
		return
	}
	line := results[hits[0]]
	sv.DecisionID = line.decision
	switch line.result {
	case "match":
		sv.Verdict = VerdictMatch
		if line.decision == "" {
			sv.Verdict = VerdictNoPDP
			if sv.CrossOrg {
				sv.Verdict = VerdictMatchServedOther
			}
		}
	case "mismatch":
		tuple, ok := mismatchTuple(line, ev, from, to)
		if !ok {
			sv.Verdict = VerdictMismatchNoTuple
			return
		}
		sv.Tuple = tuple
		sv.Verdict = VerdictMismatch
		if tuple.Class == "intentional" {
			sv.Verdict = VerdictMismatchIntentional
			if sv.Expect != expectMismatch {
				intentionalMustEqualLegacy(sv)
			}
			return
		}
		for _, r := range rulings {
			if r.Route == sv.Key() && r.Caller == sv.Caller && (Tuple{r.Class, r.Legacy, r.Shadow, r.Reason}) == tuple {
				sv.Verdict, sv.Ruling = VerdictMismatchRuled, r.Verdict
				return
			}
		}
	default:
		sv.Verdict = resultVerdicts[line.result]
		if line.result == "skipped" && sv.Caller == "superuser" && slices.Contains(seamSkipRows, sv.Key()) {
			sv.Verdict = VerdictSkippedSeam
		}
	}
}

// mismatchTuple finds the WARN line of a mismatch: by decision id when the decision point answered, else the
// served-org line of the same route and caller in the slot window.
func mismatchTuple(line resultLine, ev Evidence, from, to time.Time) (Tuple, bool) {
	var found []Tuple
	if line.decision != "" {
		for _, w := range ev.mismatches {
			if w.decision == line.decision && w.route == line.route && w.caller == line.caller {
				found = append(found, w.tuple)
			}
		}
	} else {
		for _, w := range ev.servedOther {
			if w.route == line.route && w.caller == line.caller && !w.at.Before(from) && !w.at.After(to) {
				found = append(found, w.tuple)
			}
		}
	}
	if len(found) != 1 {
		return Tuple{}, false
	}
	return found[0], true
}

func hint(sv SlotVerdict) string {
	switch sv.Verdict {
	case VerdictMissing:
		return fmt.Sprintf(" (no result line in the slot window; http %d%s)", sv.Status, statusHint(sv.Status))
	case VerdictMismatch, VerdictMismatchNoTuple:
		return fmt.Sprintf(" (class %s legacy %s shadow %s reason %s decision %s)", sv.Tuple.Class, sv.Tuple.Legacy, sv.Tuple.Shadow, sv.Tuple.Reason, sv.DecisionID)
	case VerdictGap:
		return " (" + sv.Note + ")"
	}
	return ""
}

func statusHint(status int) string {
	switch status {
	case 0:
		return ": no answer"
	case 401:
		return ": not authenticated, the guard never observed"
	case 413, 422:
		return ": the body was refused before the guard"
	case 429:
		return ": rate limited"
	}
	return ""
}

func rowVerdicts(slots []SlotVerdict, k int) []RowVerdict {
	var out []RowVerdict
	index := map[string]int{}
	type cellKey struct{ row, caller string }
	counts := map[cellKey]map[string]int{}
	order := map[cellKey]bool{}
	var cells []cellKey
	for _, sv := range slots {
		if _, ok := index[sv.Row]; !ok {
			index[sv.Row] = len(out)
			out = append(out, RowVerdict{Row: sv.Row, Key: sv.Key(), Cells: map[string]string{}, Pass: true})
		}
		key := cellKey{sv.Row, sv.Caller}
		if !order[key] {
			order[key] = true
			cells = append(cells, key)
			counts[key] = map[string]int{}
		}
		counts[key][sv.Verdict]++
		rv := &out[index[sv.Row]]
		if sv.Expect == expectMismatch && !controlMismatch(sv.Verdict) || sv.Expect != expectMismatch && !sv.Pass() {
			rv.Pass = false
		}
	}
	for _, key := range cells {
		rv := &out[index[key.row]]
		var parts []string
		total := 0
		for verdict, n := range counts[key] {
			parts = append(parts, fmt.Sprintf("%s %d/%d", verdict, n, k))
			total += n
		}
		slices.Sort(parts)
		rv.Cells[key.caller] = strings.Join(parts, ",")
		if counts[key][VerdictGap] == 0 && total != k {
			rv.Pass = false
			rv.Cells[key.caller] += fmt.Sprintf(",%s %d/%d", VerdictKShort, total, k)
		}
		if strings.Contains(rv.Cells[key.caller], VerdictGap) || !cellPasses(counts[key], key.row) {
			rv.Notes = append(rv.Notes, key.caller+": "+rv.Cells[key.caller])
		}
	}
	for i := range out {
		if !out[i].Pass && len(out[i].Notes) == 0 {
			out[i].Notes = append(out[i].Notes, "a cell failed")
		}
	}
	return out
}

func cellPasses(counts map[string]int, row string) bool {
	for verdict := range counts {
		switch verdict {
		case VerdictMatch, VerdictMatchServedOther, VerdictMismatchIntentional, VerdictMismatchRuled, VerdictSkippedSeam, VerdictSkippedNoOrg, VerdictMatchEnforced:
		default:
			if !(strings.HasPrefix(row, "C") && controlMismatch(verdict)) {
				return false
			}
		}
	}
	return true
}

// WriteTable writes the per-row match table: one line per row and control, a cell per caller kind.
func (r Report) WriteTable(w io.Writer) error {
	var b strings.Builder
	b.WriteString("row\tkey\t" + strings.Join(cellCallers, "\t") + "\tverdict\tnotes\n")
	for _, rv := range r.Rows {
		verdict := "PASS"
		switch {
		case strings.HasPrefix(rv.Row, "C") && rv.Pass:
			verdict = "CONTROL_OK"
		case strings.HasPrefix(rv.Row, "C"):
			verdict = "CONTROL_BROKEN"
		case !rv.Pass:
			verdict = "FAIL"
		}
		fields := []string{rv.Row, rv.Key}
		for _, caller := range cellCallers {
			cell := rv.Cells[caller]
			if cell == "" {
				cell = none
			}
			fields = append(fields, cell)
		}
		notes := strings.Join(rv.Notes, "; ")
		if notes == "" {
			notes = none
		}
		fields = append(fields, verdict, notes)
		b.WriteString(strings.Join(fields, "\t") + "\n")
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// controlMismatch: the verdicts a control slot must have, a mismatch with its tuple.
func controlMismatch(verdict string) bool {
	return verdict == VerdictEnforcedDeny || verdict == VerdictMismatch || verdict == VerdictMismatchIntentional || verdict == VerdictMismatchRuled
}
