package shadowvenue

import (
	"bytes"
	"errors"
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

const (
	testImage      = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testStubConfig = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	testGoAPIIP    = "172.30.0.5"
	testRunID      = "13267-4a"
)

// runIDOf is the run id a read of when precedes: before -> 4a, between -> 4b, after -> none.
func runIDOf(when string) string {
	return map[string]string{"before": "13267-4a", "between": "13267-4b", "after": "none"}[when]
}

// noSeed is a given, empty seed-mode log.
var noSeed = SeedLog{Given: true}

// egressText is a passing egress record of the read when.
func egressText(when string) string {
	return "when=" + when + "\nnetwork_internal=true\nnetworks=venue-int\ndefault_route=none\npublic_tcp=refused\npublic_dns=fail\nstub_tcp=ok\n" +
		"hosts_pinned=api.github.com,api.pagerduty.com,api.stripe.com,github.com,identity.pagerduty.com\nimage=" + testImage + "\n" +
		"stub_mode=run\nstub_config=" + testStubConfig + "\nstub_started=2026-11-07T15:00:00Z\ngoapi_ip=" + testGoAPIIP + "\nrun_id=" + runIDOf(when) + "\n"
}

func egressReads(when ...string) []EgressRead {
	var out []EgressRead
	for _, w := range when {
		out = append(out, ParseEgress(w+".txt", []byte(egressText(w))))
	}
	return out
}

func judgeOutbound(t *testing.T, slots []Slot, lines, hits []string, logged bool) Report {
	t.Helper()
	return judgeOutboundEgress(t, slots, lines, hits, logged, egressReads("before", "between"))
}

func judgeOutboundEgress(t *testing.T, slots []Slot, lines, hits []string, logged bool, egress []EgressRead) Report {
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
	return Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: logged, Hits: parsed, Ext: ext, Egress: egress, Seed: noSeed}})
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
		r.in.Outbound = Outbound{Logged: true, Hits: hits, Ext: ext, Egress: egressReads("before", "between", "after"), Seed: noSeed}
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

// Each egress defect voids the run: a record missing, unreadable or failing a fact, a read out of order, and an image
// digest or network that differs between the reads.
func TestJudgeOutboundEgressEachPlantedDefectIsVoid(t *testing.T) {
	slots, lines, hits := stripeRun(t)
	if r := judgeOutboundEgress(t, slots, lines, hits, true, egressReads("before", "between")); r.Void || !r.Pass {
		t.Fatalf("good records: void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
	bad := func(key, value string) []EgressRead {
		text := egressText("between")
		lines := strings.Split(text, "\n")
		for i, l := range lines {
			if strings.HasPrefix(l, key+"=") {
				lines[i] = key + "=" + value
			}
		}
		return []EgressRead{egressReads("before")[0], ParseEgress("between.txt", []byte(strings.Join(lines, "\n")))}
	}
	set2 := func(when, key, value string) EgressRead {
		lines := strings.Split(egressText(when), "\n")
		for i, l := range lines {
			if strings.HasPrefix(l, key+"=") {
				lines[i] = key + "=" + value
			}
		}
		return ParseEgress(when+".txt", []byte(strings.Join(lines, "\n")))
	}
	both := func(key, value string) []EgressRead {
		var out []EgressRead
		for _, when := range []string{"before", "between"} {
			text := egressText(when)
			lines := strings.Split(text, "\n")
			for i, l := range lines {
				if strings.HasPrefix(l, key+"=") {
					lines[i] = key + "=" + value
				}
			}
			out = append(out, ParseEgress(when+".txt", []byte(strings.Join(lines, "\n"))))
		}
		return out
	}
	cases := map[string][]EgressRead{
		"no record":            nil,
		"one record":           egressReads("before"),
		"unreadable":           {egressReads("before")[0], {Name: "between.txt", Err: errors.New("unreadable")}},
		"a key missing":        {egressReads("before")[0], ParseEgress("between.txt", []byte(strings.Replace(egressText("between"), "stub_tcp=ok\n", "", 1)))},
		"an unknown key":       {egressReads("before")[0], ParseEgress("between.txt", []byte(egressText("between")+"proxy=none\n"))},
		"a key twice":          {egressReads("before")[0], ParseEgress("between.txt", []byte(egressText("between")+"stub_tcp=ok\n"))},
		"reads out of order":   egressReads("between", "before"),
		"network not internal": bad("network_internal", "false"),
		"a default route":      bad("default_route", "via 172.18.0.1"),
		"public tcp open":      bad("public_tcp", "open"),
		"public dns resolves":  bad("public_dns", "ok"),
		"stub unreachable":     bad("stub_tcp", "refused"),
		"a host not pinned":    bad("hosts_pinned", "api.stripe.com"),
		"not a digest":         both("image", "latest"),
		"image differs":        bad("image", "sha256:"+strings.Repeat("2", 64)),
		"network differs":      bad("networks", "venue-other"),
		"two networks":         both("networks", "venue-int,bridge"),
		"goapi ip differs":     bad("goapi_ip", "172.30.0.6"),
		"goapi ip public":      both("goapi_ip", "8.8.8.8"),
		"goapi ip no ip":       both("goapi_ip", "go-api"),
		"a key missing in both": {ParseEgress("before.txt", []byte(strings.Replace(egressText("before"), "stub_started=2026-11-07T15:00:00Z\n", "", 1))),
			ParseEgress("between.txt", []byte(strings.Replace(egressText("between"), "stub_started=2026-11-07T15:00:00Z\n", "", 1)))},
		"an empty value in both":         both("stub_started", ""),
		"run id of the next drive twice": {egressReads("before")[0], set2("between", "run_id", "13267-4a")},
		"run id none before the end":     {egressReads("before")[0], set2("between", "run_id", "none")},
		"networks empty": {ParseEgress("before.txt", []byte(strings.Replace(egressText("before"), "networks=venue-int", "networks=", 1))),
			ParseEgress("between.txt", []byte(strings.Replace(egressText("between"), "networks=venue-int", "networks=", 1)))},
	}
	for name, reads := range cases {
		r := judgeOutboundEgress(t, slots, lines, hits, true, reads)
		if !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "egress") {
			t.Errorf("%s: void %v, reasons %v", name, r.Void, r.Reasons)
		}
	}
}

// A NO_OUTBOUND row passes with no hit in any slot and fails on one hit; its reason goes into the row table. In the
// enforced run a yardstick hit on it fails too.
func TestNoOutboundRowPassesWithoutAHitAndFailsOnOne(t *testing.T) {
	const key = "POST /api/v1/admin/integrations/github/install-url"
	run := func(withHit bool) Report {
		slots, lines := controlsOK()
		var hitLines []string
		seq := len(slots) + 1
		for _, caller := range callerOrder {
			for k := 1; k <= 3; k++ {
				s := slotAt(seq, "81", caller, k)
				s.Method, s.Route, s.Path = "POST", "/api/v1/admin/integrations/github/install-url", "/api/v1/admin/integrations/github/install-url"
				if caller == "admin" && k == 2 && withHit {
					hitLines = append(hitLines, hitLine(s.Start.Add(time.Millisecond), "github", "api.github.com"))
				}
				slots = append(slots, s)
				lines = append(lines, result(s, 500*time.Millisecond, "match", "dec_"+strings.Repeat(string(rune('a'+seq)), 24)))
				seq++
			}
		}
		ev, _ := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
		parsed, _ := ParseStubLog([]byte(strings.Join(hitLines, "\n")))
		ext := map[string]string{"C1": none, "C2": none, "81": "github"}
		return Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: true, Hits: parsed, Ext: ext, Egress: egressReads("before", "between"), Seed: noSeed}})
	}
	clean := run(false)
	if clean.Void || !clean.Pass {
		t.Fatalf("no hit: void %v pass %v: %v", clean.Void, clean.Pass, clean.Reasons)
	}
	var notes string
	for _, rv := range clean.Rows {
		if rv.Row == "81" {
			notes = strings.Join(rv.Notes, ";")
		}
	}
	if !strings.Contains(notes, "NO_OUTBOUND: "+NoOutboundRows[key]) {
		t.Fatalf("row 81 notes %q lack the pinned reason", notes)
	}
	if hit := run(true); hit.Void || hit.Pass || !strings.Contains(strings.Join(hit.Reasons, "\n"), "pinned NO_OUTBOUND") {
		t.Fatalf("one hit: void %v pass %v: %v", hit.Void, hit.Pass, hit.Reasons)
	}
	var pinned []string
	for _, row := range mustRows(t, string(DefaultRows)) {
		if _, ok := NoOutboundRows[row.Key()]; ok {
			pinned = append(pinned, row.ID)
		}
	}
	if strings.Join(pinned, ",") != "36,42,44,45,46,81,82" || len(NoOutboundRows) != 7 {
		t.Fatalf("pinned rows %v of %d, want 36,42,44,45,46,81,82 (acp)", pinned, len(NoOutboundRows))
	}
}

// In the enforced run a NO_OUTBOUND row passes on 0 hits in both runs; a hit in the yardstick file, or in the enforced
// stub log, fails it.
func TestEnforcedNoOutboundRowFailsOnAHitInEitherRun(t *testing.T) {
	const key = "POST /api/v1/admin/integrations/github/install-url"
	build := func() *enfRun {
		r := newEnfRun()
		seq := len(r.in.Slots) + 1
		for _, caller := range callerOrder {
			for k := 1; k <= 3; k++ {
				start := enfT0.Add(time.Duration(seq-1) * 3 * time.Second)
				s := Slot{Seq: seq, Row: "81", Caller: caller, Principal: caller, Expect: "match", K: k, Method: "POST",
					Route: "/api/v1/admin/integrations/github/install-url", Path: "/api/v1/admin/integrations/github/install-url", Start: start, End: start.Add(100 * time.Millisecond), Status: 200}
				r.in.Slots = append(r.in.Slots, s)
				r.in.Yard = append(r.in.Yard, YardSlot{Row: "81", Caller: caller, K: k, Key: key, Status: 200, Verdict: VerdictMatch})
				seq++
			}
		}
		for _, s := range r.in.Slots {
			r.in.YardOutbound = append(r.in.YardOutbound, OutboundSlot{Row: s.Row, Caller: s.Caller, K: s.K, Key: s.Key(), Status: s.Status})
		}
		r.in.Namespaces[key] = "integrations"
		r.in.Grace = 2200 * time.Millisecond
		r.in.YardOutboundGiven = true
		ext := map[string]string{"81": "github"}
		for _, s := range r.in.Slots {
			if ext[s.Row] == "" {
				ext[s.Row] = none
			}
		}
		r.in.Outbound = Outbound{Logged: true, Ext: ext, Egress: egressReads("before", "between", "after"), Seed: noSeed}
		return r
	}
	if rep := build().judge(); rep.Void || !rep.Pass {
		t.Fatalf("no hit: void %v pass %v: %v", rep.Void, rep.Pass, rep.Reasons)
	}
	yardHit := build()
	yardHit.in.YardOutbound[len(yardHit.in.YardOutbound)-1].Hits = 1
	if rep := yardHit.judge(); rep.Pass || !strings.Contains(strings.Join(rep.Reasons, "\n"), "in the yardstick, but the row is pinned NO_OUTBOUND") {
		t.Errorf("a yardstick hit: pass %v: %v", rep.Pass, rep.Reasons)
	}
	runHit := build()
	last := runHit.in.Slots[len(runHit.in.Slots)-1]
	runHit.in.Outbound.Hits = []StubHit{{At: last.Start.Add(time.Millisecond), Service: "github", Method: "POST", Host: "api.github.com", Path: "/x"}}
	if rep := runHit.judge(); rep.Pass || !strings.Contains(strings.Join(rep.Reasons, "\n"), "but the row is pinned NO_OUTBOUND") {
		t.Errorf("an enforced hit: pass %v: %v", rep.Pass, rep.Reasons)
	}
}

// A NO_OUTBOUND row is evidence only when the decision point ran: in each run some caller gets past it in every slot and
// no caller gets two classes of answer; else the row fails (VOID), in the judge, in the enforced run and in the
// yardstick file.
func TestNoOutboundRowNeedsACallerPastTheDecision(t *testing.T) {
	const route = "/api/v1/admin/integrations/pagerduty/webhook-bindings"
	slotsWith := func(status func(caller string, k int) int) ([]Slot, []string) {
		slots, lines := controlsOK()
		seq := len(slots) + 1
		for _, caller := range callerOrder {
			for k := 1; k <= 3; k++ {
				s := slotAt(seq, "42", caller, k)
				s.Method, s.Route, s.Path, s.Status = "POST", route, route, status(caller, k)
				slots = append(slots, s)
				lines = append(lines, result(s, 500*time.Millisecond, "match", "dec_"+strings.Repeat(string(rune('a'+seq)), 24)))
				seq++
			}
		}
		return slots, lines
	}
	judgeIt := func(status func(string, int) int) Report {
		slots, lines := slotsWith(status)
		ev, _ := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
		ext := map[string]string{"C1": none, "C2": none, "42": "pagerduty"}
		return Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: true, Ext: ext, Egress: egressReads("before", "between"), Seed: noSeed}})
	}
	legacy := func(caller string, _ int) int {
		if caller == "member" {
			return 403
		}
		return 422
	}
	if r := judgeIt(legacy); r.Void || !r.Pass {
		t.Fatalf("member refused, admin and superuser 422: void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
	cases := map[string]struct {
		status func(string, int) int
		want   string
	}{
		"no caller past the decision": {func(string, int) int { return 403 }, "no caller got past the decision"},
		"a caller with two answers": {func(caller string, k int) int {
			if caller == "admin" && k == 2 {
				return 403
			}
			return legacy(caller, k)
		}, "one class of answer per caller"},
	}
	for name, c := range cases {
		r := judgeIt(c.status)
		if r.Pass || !strings.Contains(strings.Join(r.Reasons, "\n"), c.want) {
			t.Errorf("judge, %s: pass %v: %v", name, r.Pass, r.Reasons)
		}
		for _, rv := range r.Rows {
			if rv.Row == "42" && rv.Pass {
				t.Errorf("judge, %s: row 42 passes in the table", name)
			}
		}
	}
	enforced := func(yardStatus, runStatus func(string, int) int) Report {
		r := newEnfRun()
		seq := len(r.in.Slots) + 1
		for _, caller := range callerOrder {
			for k := 1; k <= 3; k++ {
				start := enfT0.Add(time.Duration(seq-1) * 3 * time.Second)
				r.in.Slots = append(r.in.Slots, Slot{Seq: seq, Row: "42", Caller: caller, Principal: caller, Expect: "match", K: k, Method: "POST",
					Route: route, Path: route, Start: start, End: start.Add(100 * time.Millisecond), Status: runStatus(caller, k)})
				r.in.Yard = append(r.in.Yard, YardSlot{Row: "42", Caller: caller, K: k, Key: "POST " + route, Status: yardStatus(caller, k), Verdict: VerdictMatch})
				seq++
			}
		}
		for i, s := range r.in.Slots {
			status := s.Status
			if s.Row == "42" {
				status = r.in.Yard[i].Status
			}
			r.in.YardOutbound = append(r.in.YardOutbound, OutboundSlot{Row: s.Row, Caller: s.Caller, K: s.K, Key: s.Key(), Status: status})
		}
		r.in.Namespaces["POST "+route] = "integrations"
		r.in.Grace = 2200 * time.Millisecond
		r.in.YardOutboundGiven = true
		ext := map[string]string{"42": "pagerduty"}
		for _, s := range r.in.Slots {
			if ext[s.Row] == "" {
				ext[s.Row] = none
			}
		}
		r.in.Outbound = Outbound{Logged: true, Ext: ext, Egress: egressReads("before", "between", "after"), Seed: noSeed}
		return r.judge()
	}
	if rep := enforced(legacy, legacy); rep.Void || !rep.Pass {
		t.Fatalf("enforced, legacy statuses: void %v pass %v: %v", rep.Void, rep.Pass, rep.Reasons)
	}
	refused := func(string, int) int { return 403 }
	if rep := enforced(refused, refused); rep.Pass || !strings.Contains(strings.Join(rep.Reasons, "\n"), "in the yardstick no caller got past") ||
		!strings.Contains(strings.Join(rep.Reasons, "\n"), "in the run no caller got past") {
		t.Errorf("enforced, every caller refused in both runs: pass %v: %v", rep.Pass, rep.Reasons)
	}
}

// The stub's status in the egress records and its seed-mode log (acp Q-B1): seed mode in any read, a stub config or
// start that differs between reads, a config that is no digest, a missing or unreadable seed log, or a seed line in the
// drive window voids the run; a seed line before the run does not.
func TestJudgeOutboundStubModeAndSeedLog(t *testing.T) {
	slots, lines, hits := stripeRun(t)
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := ParseStubLog([]byte(strings.Join(hits, "\n")))
	ext := map[string]string{"C1": none, "C2": none, "27": "stripe"}
	judgeWith := func(egress []EgressRead, seed SeedLog) Report {
		return Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: true, Hits: parsed, Ext: ext, Egress: egress, Seed: seed}})
	}
	set := func(when, key, value string) EgressRead {
		lines := strings.Split(egressText(when), "\n")
		for i, l := range lines {
			if strings.HasPrefix(l, key+"=") {
				lines[i] = key + "=" + value
			}
		}
		return ParseEgress(when+".txt", []byte(strings.Join(lines, "\n")))
	}
	before := egressReads("before")[0]
	seedLine := func(at time.Time) SeedLog {
		h, err := ParseStubLog([]byte(`{"t":"` + at.Format(time.RFC3339Nano) + `","service":"pagerduty","method":"GET","host":"api.pagerduty.com","path":"/services"}`))
		if err != nil {
			t.Fatal(err)
		}
		return SeedLog{Given: true, Hits: h}
	}
	if r := judgeWith(egressReads("before", "between"), seedLine(slots[0].Start.Add(-time.Hour))); r.Void || !r.Pass {
		t.Fatalf("a seed line an hour before the run: void %v: %v", r.Void, r.Reasons)
	}
	cases := map[string]struct {
		egress []EgressRead
		seed   SeedLog
		want   string
	}{
		"seed mode in a read":      {[]EgressRead{before, set("between", "stub_mode", "seed")}, noSeed, "stub_mode=seed"},
		"stub config differs":      {[]EgressRead{before, set("between", "stub_config", "sha256:"+strings.Repeat("4", 64))}, noSeed, "stub_config"},
		"stub config no digest":    {[]EgressRead{set("before", "stub_config", "frozen"), set("between", "stub_config", "frozen")}, noSeed, "stub_config"},
		"stub restarted":           {[]EgressRead{before, set("between", "stub_started", "2026-11-07T15:30:00Z")}, noSeed, "stub_started"},
		"no seed log":              {egressReads("before", "between"), SeedLog{}, "seed-mode log was not given"},
		"seed log unreadable":      {egressReads("before", "between"), SeedLog{Given: true, Err: errors.New("unreadable")}, "seed-mode log is unreadable"},
		"a seed line in the drive": {egressReads("before", "between"), seedLine(slots[8].Start.Add(time.Second)), "inside the drive window"},
	}
	for name, c := range cases {
		r := judgeWith(c.egress, c.seed)
		if !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), c.want) {
			t.Errorf("%s: void %v, reasons %v", name, r.Void, r.Reasons)
		}
	}
}
