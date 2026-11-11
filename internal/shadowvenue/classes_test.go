package shadowvenue

import (
	"strings"
	"testing"
	"time"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// reclassified is the embedded table with one row's ext or destructive column changed.
func reclassified(t *testing.T, id string, ext, destructive string) []Row {
	t.Helper()
	var out []string
	for _, line := range strings.Split(string(DefaultRows), "\n") {
		f := strings.Split(line, "\t")
		if len(f) == 10 && f[0] == id {
			if ext != "" {
				f[5] = ext
			}
			if destructive != "" {
				f[4] = destructive
				if destructive == "no" {
					f[3] = "A"
				}
			}
			line = strings.Join(f, "\t")
		}
		out = append(out, line)
	}
	return mustRows(t, strings.Join(out, "\n"))
}

// A custom row table can change a row's path or body, never its class: an external row stays external with its own
// service and a destructive row stays destructive (the class is the embedded table's, by route key).
func TestCustomRowTableCannotReclassifyARow(t *testing.T) {
	var external, destructive []string
	for _, row := range mustRows(t, string(DefaultRows)) {
		switch {
		case row.Control():
		case row.Destructive:
			destructive = append(destructive, row.ID)
		case row.Ext != none:
			external = append(external, row.ID)
		}
	}
	if len(external) != 18 || len(destructive) != 6 {
		t.Fatalf("external %v destructive %v", external, destructive)
	}
	for _, id := range external {
		if err := CheckRows(reclassified(t, id, none, ""), authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "class") {
			t.Errorf("row %s with ext - accepted: %v", id, err)
		}
		if err := CheckRows(reclassified(t, id, "mail", ""), authv1.GoAPIEndpointProfiles); id != "1" && (err == nil || !strings.Contains(err.Error(), "class")) {
			t.Errorf("row %s with another service accepted: %v", id, err)
		}
	}
	for _, id := range destructive {
		if err := CheckRows(reclassified(t, id, "", "no"), authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "class") {
			t.Errorf("destructive row %s made plain accepted: %v", id, err)
		}
	}
	if err := CheckRows(reclassified(t, "58", "stripe", ""), authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "class") {
		t.Errorf("a plain row made external accepted: %v", err)
	}
	if err := CheckRows(reclassified(t, "C1", "stripe", ""), authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "class") {
		t.Errorf("a control with an outside service accepted: %v", err)
	}
}

// stripeSlots are three cells of row 22 (an external route) with matching shadow lines, after the controls.
func stripeSlots(t *testing.T) ([]Slot, Evidence) {
	t.Helper()
	slots, lines := controlsOK()
	seq := len(slots) + 1
	for _, caller := range callerOrder {
		for k := 1; k <= 3; k++ {
			s := slotAt(seq, "22", caller, k)
			s.Method, s.Route, s.Path = "POST", "/api/v1/billing/plans/pull-stripe", "/api/v1/billing/plans/pull-stripe"
			slots = append(slots, s)
			lines = append(lines, result(s, 500*time.Millisecond, "match", "dec_"+strings.Repeat(string(rune('a'+seq)), 24)))
			seq++
		}
	}
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return slots, ev
}

// With no row table at all the judge still knows an external route by its class, and a route with no class voids.
func TestJudgeTakesTheClassFromTheRouteWithNoTableAndVoidsAnUnknownRoute(t *testing.T) {
	slots, ev := stripeSlots(t)
	r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond})
	if !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "without a stub log") {
		t.Fatalf("external route, no table, no stub log: void %v: %v", r.Void, r.Reasons)
	}
	slots[len(slots)-1].Method, slots[len(slots)-1].Route = "GET", "/api/v1/admin/nothing"
	r = Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: true, Egress: egressReads("before", "between"), Seed: noSeed}})
	if !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "no class") {
		t.Fatalf("a route with no class: void %v: %v", r.Void, r.Reasons)
	}
}

// The enforced judge, too, takes the class from the route: with no row table an external row still needs the
// yardstick's outbound file.
func TestEnforcedTakesTheClassFromTheRoute(t *testing.T) {
	r := newEnfRun()
	seq := len(r.in.Slots) + 1
	for _, caller := range callerOrder {
		for k := 1; k <= 3; k++ {
			start := enfT0.Add(time.Duration(seq-1) * 3 * time.Second)
			r.in.Slots = append(r.in.Slots, Slot{Seq: seq, Row: "22", Caller: caller, Principal: caller, Expect: "match", K: k, Method: "POST",
				Route: "/api/v1/billing/plans/pull-stripe", Path: "/api/v1/billing/plans/pull-stripe", Start: start, End: start.Add(100 * time.Millisecond), Status: 403})
			r.in.Yard = append(r.in.Yard, YardSlot{Row: "22", Caller: caller, K: k, Key: "POST /api/v1/billing/plans/pull-stripe", Status: 403, Verdict: VerdictMatch})
			seq++
		}
	}
	r.in.Namespaces["POST /api/v1/billing/plans/pull-stripe"] = "billing"
	r.in.Outbound = Outbound{Logged: true, Egress: egressReads("before", "between", "after"), Seed: noSeed}
	rep := r.judge()
	if !rep.Void || !strings.Contains(strings.Join(rep.Reasons, "\n"), "yardstick's outbound file") {
		t.Fatalf("void %v: %v", rep.Void, rep.Reasons)
	}
}

// The judge takes each sent slot's class from its route, not from the row table it is given: a table that names an
// external route as internal voids the run, even with no stub log.
func TestJudgeVoidsARowTableThatReclassifiesARoute(t *testing.T) {
	slots, lines := controlsOK()
	seq := len(slots) + 1
	for _, caller := range callerOrder {
		for k := 1; k <= 3; k++ {
			s := slotAt(seq, "22", caller, k)
			s.Method, s.Route, s.Path = "POST", "/api/v1/billing/plans/pull-stripe", "/api/v1/billing/plans/pull-stripe"
			slots = append(slots, s)
			lines = append(lines, result(s, 500*time.Millisecond, "match", "dec_"+strings.Repeat(string(rune('a'+seq)), 24)))
			seq++
		}
	}
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Rows: tableOf(slots, map[string]string{"22": none})}})
	if !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "class") {
		t.Fatalf("void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
}

// A key given twice is refused even when its first value is blank, and an empty value is refused as it is parsed, for
// every key.
func TestParseEgressRefusesABlankFirstDuplicateOfEveryKey(t *testing.T) {
	good := egressText("before")
	for _, key := range egressKeys {
		read := ParseEgress("before.txt", []byte(key+"=\n"+good))
		if read.Err == nil {
			t.Errorf("%s: a blank first value then a valid one was accepted", key)
		}
		if voids := checkEgress([]EgressRead{read, egressReads("between")[0]}, []string{"before", "between"}); len(voids) == 0 {
			t.Errorf("%s: the run is not void", key)
		}
	}
}

// The reviewer's form: every external row of the table with ext - at once. The judge, given that table and slots of
// every external route, voids the run on each such row, with no stub log, no egress and no seed log.
func TestJudgeVoidsTheTableWithEveryExternalRowMadeInternal(t *testing.T) {
	slots, lines := controlsOK()
	ext := map[string]string{"C1": none, "C2": none}
	seq := len(slots) + 1
	var external []Row
	for _, row := range mustRows(t, string(DefaultRows)) {
		if row.Ext != none && !row.Destructive {
			external = append(external, row)
		}
	}
	for _, row := range external {
		ext[row.ID] = none
		s := slotAt(seq, row.ID, "admin", 1)
		s.Method, s.Route, s.Path = row.Method, row.Route, row.Route
		slots = append(slots, s)
		lines = append(lines, result(s, 500*time.Millisecond, "match", "dec_"+strings.Repeat(string(rune('a'+seq%26)), 20)+itoa(1000+seq)))
		seq++
	}
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	r := Judge(slots, ev, nil, JudgeOptions{K: 1, Grace: 2200 * time.Millisecond, Outbound: Outbound{Rows: tableOf(slots, ext)}})
	joined := strings.Join(r.Reasons, "\n")
	if !r.Void {
		t.Fatalf("the reviewer's table passed the judge: %v", r.Reasons)
	}
	for _, row := range external {
		if !strings.Contains(joined, "row "+row.ID+": "+row.Key()+" has the class ext "+row.Ext) {
			t.Errorf("row %s not voided for its class", row.ID)
		}
	}
}

// The judge takes the service of each hit's slot from the route: a hit of the route's own service is no mismatch.
func TestJudgeComparesHitsToTheRouteService(t *testing.T) {
	slots, ev := stripeSlots(t)
	var hits []StubHit
	for _, s := range slots[6:] {
		hits = append(hits, StubHit{At: s.Start.Add(10 * time.Millisecond), Service: "stripe", Method: "GET", Host: "api.stripe.com", Path: "/v1/products"})
	}
	r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Logged: true, Hits: hits, Rows: tableOf(slots, map[string]string{"22": "stripe"}), Egress: egressReads("before", "between"), Seed: noSeed}})
	if r.Void || !r.Pass {
		t.Fatalf("void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
}

// The strict egress parse, one key each: a blank-first duplicate, a value-first duplicate and an empty value are each
// refused as they are read.
func TestParseEgressStrictForms(t *testing.T) {
	good := egressText("before")
	for _, c := range []struct{ name, text string }{
		{"blank-first_duplicate", "stub_tcp=\n" + good},
		{"value-first_duplicate", good + "stub_tcp=ok\n"},
		{"empty_value", strings.Replace(good, "stub_tcp=ok", "stub_tcp=", 1)},
	} {
		t.Run(c.name, func(t *testing.T) {
			if read := ParseEgress("before.txt", []byte(c.text)); read.Err == nil {
				t.Errorf("%s accepted", c.name)
			}
		})
	}
}
