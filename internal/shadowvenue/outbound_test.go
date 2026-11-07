package shadowvenue

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

const stripeKey = "POST /api/v1/billing/subscriptions/cancel"

func hitLine(at time.Time, service, host string) string {
	return `{"t":"` + at.Format(time.RFC3339Nano) + `","service":"` + service + `","method":"POST","host":"` + host + `","path":"/v1/subscriptions/sub_x"}`
}

// stripeRun is controls (from controlsOK) plus row 27 (ext stripe) for member (denied), admin and superuser (allowed),
// with the shadow lines that make every cell a MATCH, and one stub hit in each allowed slot.
func stripeRun(t *testing.T) ([]Slot, []string, []string) {
	t.Helper()
	slots, lines := controlsOK()
	var hits []string
	seq := len(slots) + 1
	for _, caller := range callerOrder {
		for k := 1; k <= 3; k++ {
			s := slotAt(seq, "27", caller, k)
			s.Method, s.Route, s.Path = "POST", "/api/v1/billing/subscriptions/cancel", "/api/v1/billing/subscriptions/cancel"
			if caller == "member" {
				s.Status = 403
			} else {
				hits = append(hits, hitLine(s.Start.Add(40*time.Millisecond), "stripe", "api.stripe.com"))
			}
			slots = append(slots, s)
			lines = append(lines, result(s, 500*time.Millisecond, "match", "dec_"+strings.Repeat(string(rune('a'+seq)), 24)))
			seq++
		}
	}
	return slots, lines, hits
}

func judgeOutbound(t *testing.T, slots []Slot, lines, hits []string, logged bool) Report {
	t.Helper()
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseStubLog([]byte(strings.Join(hits, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	ext := map[string]string{"C1": none, "C2": none, "27": "stripe", "58": none}
	return Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: logged, Hits: parsed, Ext: ext}})
}

func TestJudgeOutboundPassesWhenEveryAllowedSlotReachesTheStub(t *testing.T) {
	slots, lines, hits := stripeRun(t)
	r := judgeOutbound(t, slots, lines, hits, true)
	if r.Void || !r.Pass {
		t.Fatalf("void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
	var out bytes.Buffer
	if err := r.WriteOutbound(&out); err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseOutbound(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, o := range parsed {
		total += o.Hits
		if o.Row == "27" && (o.Caller == "member") != (o.Hits == 0) {
			t.Fatalf("outbound line %+v", o)
		}
	}
	if len(parsed) != 15 || total != 6 {
		t.Fatalf("%d outbound lines, %d hits", len(parsed), total)
	}
}

// Each planted defect: a void run (exit 3) or a failed run (exit 1), with its reason.
func TestJudgeOutboundEachPlantedDefectIsRed(t *testing.T) {
	cases := map[string]struct {
		plant func(slots []Slot, hits []string) ([]Slot, []string, bool)
		void  bool
		want  string
	}{
		"no stub log for an external row": {func(s []Slot, h []string) ([]Slot, []string, bool) { return s, h, false }, true, "without a stub log"},
		"a stray hit in no slot window": {func(s []Slot, h []string) ([]Slot, []string, bool) {
			return s, append(h, hitLine(s[len(s)-1].End.Add(2500*time.Millisecond), "stripe", "api.stripe.com")), true
		}, true, "stray"},
		"a hit of a service the stub does not serve": {func(s []Slot, h []string) ([]Slot, []string, bool) {
			return s, append(h, hitLine(s[10].Start.Add(time.Millisecond), "slack", "slack.com")), true
		}, true, "does not serve"},
		"a denied caller made a call": {func(s []Slot, h []string) ([]Slot, []string, bool) {
			return s, append(h, hitLine(s[6].Start.Add(time.Millisecond), "stripe", "api.stripe.com")), true
		}, false, "before the guard"},
		"a call of another service than the row's": {func(s []Slot, h []string) ([]Slot, []string, bool) {
			return s, append(h, hitLine(s[10].Start.Add(time.Millisecond), "pagerduty", "api.pagerduty.com")), true
		}, false, "row's ext"},
		"no allowed caller reached the stub": {func(s []Slot, h []string) ([]Slot, []string, bool) { return s, nil, true }, false, "reached the stripe stub"},
		"no allowed caller at all": {func(s []Slot, h []string) ([]Slot, []string, bool) {
			for i := range s {
				if s[i].Row == "27" {
					s[i].Status = 403
				}
			}
			return s, nil, true
		}, false, "0 allowed slots"},
	}
	for name, c := range cases {
		slots, lines, hits := stripeRun(t)
		slots, hits, logged := c.plant(slots, hits)
		r := judgeOutbound(t, slots, lines, hits, logged)
		joined := strings.Join(r.Reasons, "\n")
		if r.Void != c.void || r.Pass || !strings.Contains(joined, c.want) {
			t.Errorf("%s: void %v pass %v exit %d, reasons:\n%s", name, r.Void, r.Pass, r.Exit(), joined)
		}
		if strings.HasPrefix(name, "no allowed caller") {
			for _, rv := range r.Rows {
				if rv.Row == "27" && (rv.Pass || !strings.Contains(strings.Join(rv.Notes, ";"), "reached the stub")) {
					t.Errorf("%s: row 27 in the table: pass %v notes %v", name, rv.Pass, rv.Notes)
				}
			}
		}
	}
}

// A hit that lands after the answer but inside the grace (mail sent after the reply) belongs to its slot.
func TestJudgeOutboundGivesALateHitInTheGraceToItsSlot(t *testing.T) {
	slots, lines, hits := stripeRun(t)
	hits[0] = hitLine(slots[9].End.Add(2*time.Second), "stripe", "api.stripe.com")
	if r := judgeOutbound(t, slots, lines, hits, true); r.Void || !r.Pass {
		t.Fatalf("void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
}

// Without external rows and without a stub log nothing changes: the run-3 form.
func TestJudgeOutboundNeedsNoLogWithoutExternalRows(t *testing.T) {
	slots, lines := controlsOK()
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Ext: map[string]string{"C1": none, "C2": none}}})
	if r.Void || !r.Pass {
		t.Fatalf("void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
}

func TestParseStubLogRefusesUnreadableLines(t *testing.T) {
	good := hitLine(t0, "stripe", "api.stripe.com")
	if hits, err := ParseStubLog([]byte(good + "\n\n" + good + "\n")); err != nil || len(hits) != 2 {
		t.Fatalf("%d hits, %v", len(hits), err)
	}
	for name, line := range map[string]string{
		"not json":      "stripe POST /v1/x",
		"extra field":   strings.Replace(good, `"path"`, `"authorization":"Bearer x","path"`, 1),
		"missing field": strings.Replace(good, `,"host":"api.stripe.com"`, "", 1),
		"empty field":   strings.Replace(good, `"service":"stripe"`, `"service":""`, 1),
		"bad time":      strings.Replace(good, t0.Format(time.RFC3339Nano), "yesterday", 1),
		"two objects":   good + good,
	} {
		if _, err := ParseStubLog([]byte(line)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestCheckRowKeysRefusesAnotherRowTable(t *testing.T) {
	rows := mustRows(t, string(DefaultRows))
	s := slotAt(1, "58", "member", 1)
	if err := CheckRowKeys([]Slot{s}, rows); err != nil {
		t.Fatal(err)
	}
	s.Row = "59"
	if err := CheckRowKeys([]Slot{s}, rows); err == nil {
		t.Fatal("a slot of row 59 with row 58's route passed")
	}
}

// The enforced run: every slot must make as many outside calls as its yardstick slot; the yardstick outbound file is
// required when an external row ran, and is itself held to the reach rule.
func TestEnforcedOutboundParity(t *testing.T) {
	build := func() *enfRun {
		r := newEnfRun()
		seq := len(r.in.Slots) + 1
		var hits []StubHit
		for _, caller := range callerOrder {
			for k := 1; k <= 3; k++ {
				start := enfT0.Add(time.Duration(seq-1) * 3 * time.Second)
				status := 200
				if caller == "member" {
					status = 403
				}
				s := Slot{Seq: seq, Row: "27", Caller: caller, Principal: caller, Expect: "match", K: k, Method: "POST",
					Route: "/api/v1/billing/subscriptions/cancel", Path: "/api/v1/billing/subscriptions/cancel", Start: start, End: start.Add(100 * time.Millisecond), Status: status}
				r.in.Slots = append(r.in.Slots, s)
				r.in.Yard = append(r.in.Yard, YardSlot{Row: "27", Caller: caller, K: k, Key: stripeKey, Status: status, Verdict: VerdictMatch})
				n := 0
				if status == 200 {
					n = 1
					hits = append(hits, StubHit{At: start.Add(30 * time.Millisecond), Service: "stripe", Method: "POST", Host: "api.stripe.com", Path: "/v1/x"})
				}
				r.in.YardOutbound = append(r.in.YardOutbound, OutboundSlot{Row: "27", Caller: caller, K: k, Key: stripeKey, Status: status, Hits: n})
				seq++
			}
		}
		for _, s := range r.in.Slots[:len(r.in.Slots)-9] {
			r.in.YardOutbound = append(r.in.YardOutbound, OutboundSlot{Row: s.Row, Caller: s.Caller, K: s.K, Key: s.Key(), Status: s.Status})
		}
		r.in.Namespaces[stripeKey] = "billing"
		r.in.Grace = 2200 * time.Millisecond
		r.in.YardOutboundGiven = true
		ext := map[string]string{"27": "stripe"}
		for _, s := range r.in.Slots {
			if ext[s.Row] == "" {
				ext[s.Row] = none
			}
		}
		r.in.Outbound = Outbound{Logged: true, Hits: hits, Ext: ext}
		return r
	}
	if rep := build().judge(); rep.Void || !rep.Pass {
		t.Fatalf("void %v pass %v: %v", rep.Void, rep.Pass, rep.Reasons)
	}
	cases := map[string]struct {
		plant func(*enfRun)
		void  bool
		want  string
	}{
		"one call fewer than the yardstick": {func(r *enfRun) { r.in.Outbound.Hits = r.in.Outbound.Hits[1:] }, false, VerdictOutboundDiffers},
		"one call more than the yardstick": {func(r *enfRun) {
			h := r.in.Outbound.Hits[0]
			h.At = h.At.Add(time.Millisecond)
			r.in.Outbound.Hits = append(r.in.Outbound.Hits, h)
		}, false, VerdictOutboundDiffers},
		"a yardstick slot missing from its outbound file": {func(r *enfRun) {
			r.in.YardOutbound = r.in.YardOutbound[1:]
		}, false, VerdictOutboundDiffers},
		"no yardstick outbound file": {func(r *enfRun) { r.in.YardOutboundGiven, r.in.YardOutbound = false, nil }, true, "yardstick's outbound file"},
		"no stub log":                {func(r *enfRun) { r.in.Outbound.Logged, r.in.Outbound.Hits = false, nil }, true, "without a stub log"},
		"a failing slot keeps its own verdict": {func(r *enfRun) {
			r.in.Slots[len(r.in.Slots)-6].Status = 500
			r.in.Outbound.Hits = r.in.Outbound.Hits[1:]
		}, false, VerdictStatusDiffers},
		"the yardstick never reached the stub": {func(r *enfRun) {
			for i := range r.in.YardOutbound {
				r.in.YardOutbound[i].Hits = 0
			}
		}, false, "in the yardstick"},
	}
	for name, c := range cases {
		r := build()
		c.plant(r)
		rep := r.judge()
		joined := strings.Join(rep.Reasons, "\n")
		if rep.Void != c.void || rep.Pass || !strings.Contains(joined, c.want) {
			t.Errorf("%s: void %v pass %v, reasons:\n%s", name, rep.Void, rep.Pass, joined)
		}
	}
}
