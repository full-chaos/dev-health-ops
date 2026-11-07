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

// Outbound is a run's stub evidence. Logged reports whether the stub log was given (an empty log is a log); Ext maps
// each row id to its ext column.
type Outbound struct {
	Logged bool
	Hits   []StubHit
	Ext    map[string]string
}

// RowExt maps each row id of rows to its ext.
func RowExt(rows []Row) map[string]string {
	out := map[string]string{}
	for _, row := range rows {
		out[row.ID] = row.Ext
	}
	return out
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
	// noReach are the external rows no allowed slot of which reached the stub.
	noReach []string
}

// checkOutbound gives each stub hit to the sent slot whose window [start-skew, end+grace] holds it. Void: an external
// row sent with no stub log, a hit in no window, a hit of a service the stub does not serve. Fail: a hit of a service
// other than the slot row's ext, a denied slot (401/403) with a hit, an external row none of whose allowed slots (not
// 401/403, at least one) reached the stub, or one with no allowed slot at all.
func checkOutbound(slots []Slot, ob Outbound, grace time.Duration) outboundCheck {
	out := outboundCheck{hits: map[int]int{}}
	sent := sentSlots(slots)
	external := map[string]bool{}
	for _, s := range sent {
		if ext := ob.Ext[s.Row]; ext != "" && ext != none {
			external[s.Row] = true
		}
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
			if service != ob.Ext[s.Row] {
				out.fails = append(out.fails, fmt.Sprintf("FAIL: slot %d row %s %s: a %s call, but the row's ext is %q", s.Seq, s.Row, s.Caller, service, ob.Ext[s.Row]))
			}
		}
		if denied(s.Status) && out.hits[s.Seq] > 0 {
			out.fails = append(out.fails, fmt.Sprintf("FAIL: slot %d row %s %s: answered %d but made %d outside calls (a call before the guard)", s.Seq, s.Row, s.Caller, s.Status, out.hits[s.Seq]))
		}
	}
	var rows []string
	for row := range external {
		rows = append(rows, row)
	}
	slices.Sort(rows)
	for _, row := range rows {
		allowed, reached := 0, 0
		for _, s := range sent {
			if s.Row != row || denied(s.Status) {
				continue
			}
			allowed++
			if out.hits[s.Seq] > 0 {
				reached++
			}
		}
		if reached == 0 {
			out.noReach = append(out.noReach, row)
			out.fails = append(out.fails, fmt.Sprintf("FAIL: row %s VOID: %d allowed slots, %d reached the %s stub; the row is no evidence", row, allowed, reached, ob.Ext[row]))
		}
	}
	return out
}

// markNoReach fails the rows of check.noReach in rows.
func markNoReach(rows []RowVerdict, check outboundCheck) {
	for i := range rows {
		if slices.Contains(check.noReach, rows[i].Row) {
			rows[i].Pass = false
			rows[i].Notes = append(rows[i].Notes, "no allowed caller reached the stub")
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
