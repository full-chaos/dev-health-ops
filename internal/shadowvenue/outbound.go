package shadowvenue

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
)

// StubServices are the outside services the venue stub (cmd/venue-extstub) stands in for; a row's ext names one.
var StubServices = []string{"stripe", "pagerduty", "github", "mail"}

// VerdictOutboundDiffers: the enforced slot made a different number of outside calls than the yardstick slot.
const VerdictOutboundDiffers = "MISMATCH_OUTBOUND"

// NoOutboundRows are the external rows that make no outside call on any path (acp, ADDENDUM-13267-1 Q-A2 and the word
// on rows 42/44/45/46/82), by row key, with the code-argued reason. They pass on status equality and on 0 stub hits in
// every slot of both runs; one hit fails the row. So that such a row is no empty evidence, in each run at least one
// caller must get past the decision in every slot (an answer other than 401/403) and each caller must get one class of
// answer in all its slots; else the row is VOID. The reason goes into the row table.
var NoOutboundRows = map[string]string{
	"POST /api/v1/admin/integrations/pagerduty/authorize":                              "builds the PagerDuty authorize URL only (internal/apiservice/admin/pagerduty_authorize.go:67-99)",
	"POST /api/v1/admin/integrations/github/install-url":                               "builds the GitHub App install URL only (internal/api/githubapp/routes.go:110-135)",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings":                       "DB only (internal/apiservice/admin/pagerduty_bindings.go:228-299)",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate": "DB only (internal/apiservice/admin/pagerduty_bindings.go:396-477)",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke":   "DB only (internal/apiservice/admin/pagerduty_bindings.go:479-515)",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate":   "DB only (internal/apiservice/admin/pagerduty_bindings.go:301-394)",
	"POST /api/v1/admin/integrations/pagerduty/preflight":                              "reads the credential and its scopes from the DB only (internal/apiservice/admin/pagerduty.go:274-375)",
}

// decisionShown holds a NO_OUTBOUND row to evidence that the decision point ran: statuses are the (caller, status) of
// the row's slots in one run. Each caller must get one class of answer (refused 401/403, or past the decision) in all
// its slots, and at least one caller must get past the decision; each problem is a failure line.
func decisionShown(row, run string, callers []string, statuses []int) []string {
	var fails []string
	refused, past := map[string]int{}, map[string]int{}
	var order []string
	for i, caller := range callers {
		if refused[caller]+past[caller] == 0 {
			order = append(order, caller)
		}
		if denied(statuses[i]) {
			refused[caller]++
		} else {
			past[caller]++
		}
	}
	through := 0
	for _, caller := range order {
		switch {
		case refused[caller] > 0 && past[caller] > 0:
			fails = append(fails, fmt.Sprintf("FAIL: row %s %s: %s is refused in %d slots and served in %d: one class of answer per caller", row, run, caller, refused[caller], past[caller]))
		case past[caller] > 0:
			through++
		}
	}
	if through == 0 {
		fails = append(fails, fmt.Sprintf("FAIL: row %s VOID: in the %s no caller got past the decision; a NO_OUTBOUND row is no evidence then", row, run))
	}
	return fails
}

// StubHit is one request the venue stub served. The stub logs no header, body or query.
type StubHit struct {
	At                          time.Time
	Service, Method, Host, Path string
}

// ParseStubLog reads the stub's JSON lines: each line holds exactly the string fields t, service, method, host and
// path. Any other line refuses the log, as the evidence would be unreadable.
func ParseStubLog(raw []byte) ([]StubHit, error) {
	var hits []StubHit
	for n, line := range strings.Split(string(raw), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			T       *string `json:"t"`
			Service *string `json:"service"`
			Method  *string `json:"method"`
			Host    *string `json:"host"`
			Path    *string `json:"path"`
		}
		dec := json.NewDecoder(strings.NewReader(line))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rec); err != nil || dec.More() {
			return nil, fmt.Errorf("stub log line %d: not one object of the fields t, service, method, host, path", n+1)
		}
		fields := []*string{rec.T, rec.Service, rec.Method, rec.Host, rec.Path}
		if slices.ContainsFunc(fields, func(f *string) bool { return f == nil || *f == "" }) {
			return nil, fmt.Errorf("stub log line %d: a field is missing or empty", n+1)
		}
		at, err := time.Parse(time.RFC3339Nano, *rec.T)
		if err != nil {
			return nil, fmt.Errorf("stub log line %d: t is not an RFC 3339 time", n+1)
		}
		hits = append(hits, StubHit{At: at, Service: *rec.Service, Method: *rec.Method, Host: *rec.Host, Path: *rec.Path})
	}
	return hits, nil
}

// Outbound is a run's stub evidence. Logged reports whether the stub log was given (an empty log is a log); Rows is the
// row table the drive used (nil when no table is given): the judges hold it to the pinned classes (CheckRowClasses) before
// anything else and take each slot's class from its route; Egress are the run's egress records (ParseEgress), in read
// order.
type Outbound struct {
	Logged bool
	Hits   []StubHit
	Rows   []Row
	Egress []EgressRead
	// Seed is the stub's seed-mode log (acp ADDENDUM-13267-2 Q-B1): no line of it may fall in the drive window.
	Seed SeedLog
}

// CheckRowKeys refuses slots whose row id names another route in rows than the slot's: the ext map would not be the
// drive's.
func CheckRowKeys(slots []Slot, rows []Row) error {
	keys := map[string]string{}
	for _, row := range rows {
		keys[row.ID] = row.Key()
	}
	for _, s := range slots {
		if keys[s.Row] != s.Key() {
			return fmt.Errorf("slot %d: row %s is %q in the row table, the slot sent %q", s.Seq, s.Row, keys[s.Row], s.Key())
		}
	}
	return nil
}

func denied(status int) bool { return status == 401 || status == 403 }

// outboundCheck is the stub evidence of one run, judged against its sent slots.
type outboundCheck struct {
	hits         map[int]int
	voids, fails []string
	// noReach are the external rows no allowed slot of which reached the stub; pinned the NO_OUTBOUND rows that ran.
	noReach, pinned []string
}

// checkOutbound gives each stub hit to the sent slot whose window [start-skew, end+grace] holds it. Void: an external
// row sent with no stub log or without the egress records egressWant names (checkEgress), a hit in no window, a hit of
// a service the stub does not serve. Fail: a hit of a service other than the slot row's ext, a denied slot (401/403)
// with a hit, an external row none of whose allowed slots (not 401/403) reached the stub, or a NO_OUTBOUND row with a
// hit.
func checkOutbound(slots []Slot, ob Outbound, byRow map[string]rowClass, grace time.Duration, egressWant []string) outboundCheck {
	out := outboundCheck{hits: map[int]int{}}
	sent := sentSlots(slots)
	external := map[string]bool{}
	keyOf := map[string]string{}
	extOf := map[string]string{}
	for _, s := range sent {
		c, ok := classOf(s.Key())
		switch {
		case !ok:
			out.voids = append(out.voids, fmt.Sprintf("slot %d row %s: route %s has no class in the embedded row table", s.Seq, s.Row, s.Key()))
			continue
		case ob.Rows != nil && byRow[s.Row] != c:
			out.voids = append(out.voids, fmt.Sprintf("slot %d row %s: the row table gives the class %+v, but the class of %s is %+v", s.Seq, s.Row, byRow[s.Row], s.Key(), c))
		}
		extOf[s.Row] = c.ext
		if c.ext != none {
			external[s.Row] = true
			keyOf[s.Row] = s.Key()
		}
	}
	if len(external) > 0 {
		out.voids = append(out.voids, checkEgress(ob.Egress, egressWant)...)
		out.voids = append(out.voids, checkSeedLog(slots, ob.Seed, grace)...)
	}
	if !ob.Logged {
		if len(external) > 0 {
			out.voids = append(out.voids, "external rows were sent without a stub log: an outside call cannot be seen")
		}
		return out
	}
	byService := map[int]map[string]int{}
	for _, hit := range ob.Hits {
		if !slices.Contains(StubServices, hit.Service) {
			out.voids = append(out.voids, fmt.Sprintf("stub hit at %s names service %q, which the stub does not serve", hit.At.Format(time.RFC3339Nano), hit.Service))
			continue
		}
		owner := -1
		for i, s := range sent {
			if !hit.At.Before(s.Start.Add(-windowSkew)) && !hit.At.After(s.End.Add(grace)) {
				owner = i
				break
			}
		}
		if owner < 0 {
			out.voids = append(out.voids, fmt.Sprintf("stray stub hit at %s (%s %s %s%s) in no slot window: another client, or a call the run cannot own",
				hit.At.Format(time.RFC3339Nano), hit.Service, hit.Method, hit.Host, hit.Path))
			continue
		}
		s := sent[owner]
		out.hits[s.Seq]++
		if byService[s.Seq] == nil {
			byService[s.Seq] = map[string]int{}
		}
		byService[s.Seq][hit.Service]++
	}
	for _, s := range sent {
		for service := range byService[s.Seq] {
			if service != extOf[s.Row] {
				out.fails = append(out.fails, fmt.Sprintf("FAIL: slot %d row %s %s: a %s call, but the row's ext is %q", s.Seq, s.Row, s.Caller, service, extOf[s.Row]))
			}
		}
		if denied(s.Status) && out.hits[s.Seq] > 0 {
			out.fails = append(out.fails, fmt.Sprintf("FAIL: slot %d row %s %s: answered %d but made %d outside calls (a call before the guard, or the stub's own %d relayed by the handler: the stub log has no answer status, so the two cannot be told apart; the row's stub must answer 2xx)", s.Seq, s.Row, s.Caller, s.Status, out.hits[s.Seq], s.Status))
		}
	}
	var rows []string
	for row := range external {
		rows = append(rows, row)
	}
	slices.Sort(rows)
	for _, row := range rows {
		if reason, ok := NoOutboundRows[keyOf[row]]; ok {
			out.pinned = append(out.pinned, row)
			var callers []string
			var statuses []int
			for _, s := range sent {
				if s.Row != row {
					continue
				}
				callers, statuses = append(callers, s.Caller), append(statuses, s.Status)
				if out.hits[s.Seq] > 0 {
					out.fails = append(out.fails, fmt.Sprintf("FAIL: slot %d row %s %s: %d outside calls, but the row is pinned NO_OUTBOUND (%s)", s.Seq, row, s.Caller, out.hits[s.Seq], reason))
				}
			}
			if shown := decisionShown(row, "run", callers, statuses); len(shown) > 0 {
				out.noReach = append(out.noReach, row)
				out.fails = append(out.fails, shown...)
			}
			continue
		}
		allowed, reached, refusedHit := 0, 0, 0
		for _, s := range sent {
			if s.Row != row {
				continue
			}
			if denied(s.Status) {
				if out.hits[s.Seq] > 0 {
					refusedHit++
				}
				continue
			}
			allowed++
			if out.hits[s.Seq] > 0 {
				reached++
			}
		}
		if reached == 0 {
			out.noReach = append(out.noReach, row)
			out.fails = append(out.fails, fmt.Sprintf("FAIL: row %s VOID: %d allowed slots, %d reached the %s stub (%d answered 401/403 with a stub hit: a relayed refusal or a call before the guard); the row is no evidence", row, allowed, reached, extOf[row], refusedHit))
		}
	}
	return out
}

// markNoReach fails the rows of check.noReach in rows and notes the reason of each NO_OUTBOUND row.
func markNoReach(rows []RowVerdict, check outboundCheck) {
	for i := range rows {
		if slices.Contains(check.noReach, rows[i].Row) {
			rows[i].Pass = false
			rows[i].Notes = append(rows[i].Notes, "no allowed caller reached the stub")
		}
		if slices.Contains(check.pinned, rows[i].Row) {
			rows[i].Notes = append(rows[i].Notes, "NO_OUTBOUND: "+NoOutboundRows[rows[i].Key])
		}
	}
}

const outboundHeader = "seq\trow\tcaller\tk\tkey\tstatus\thits"

// OutboundSlot is one sent slot of a run and the outside calls the stub saw in its window.
type OutboundSlot struct {
	Row, Caller string
	K           int
	Key         string
	Status      int
	Hits        int
}

// WriteOutbound writes one line per sent slot: its cell, route key, HTTP status and stub hits.
func (r Report) WriteOutbound(w io.Writer) error {
	var b strings.Builder
	b.WriteString(outboundHeader + "\n")
	for _, sv := range r.Slots {
		if sv.Gap() {
			continue
		}
		fmt.Fprintf(&b, "%d\t%s\t%s\t%d\t%s\t%d\t%d\n", sv.Seq, sv.Row, sv.Caller, sv.K, sv.Key(), sv.Status, sv.Outbound)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// ParseOutbound reads a file WriteOutbound wrote.
func ParseOutbound(raw []byte) ([]OutboundSlot, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != outboundHeader {
		return nil, errors.New("outbound: the first line is not the outbound header")
	}
	var out []OutboundSlot
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 7 {
			return nil, fmt.Errorf("outbound: line %d has %d columns, want 7", n+2, len(f))
		}
		k, err1 := strconv.Atoi(f[3])
		status, err2 := strconv.Atoi(f[5])
		hits, err3 := strconv.Atoi(f[6])
		if err := errors.Join(err1, err2, err3); err != nil || hits < 0 {
			return nil, fmt.Errorf("outbound: line %d: bad number", n+2)
		}
		out = append(out, OutboundSlot{Row: f[1], Caller: f[2], K: k, Key: f[4], Status: status, Hits: hits})
	}
	return out, nil
}
