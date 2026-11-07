package shadowvenue

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

// The enforced judge keys the NO_OUTBOUND exemption on the enforced slot's pinned route, not on the yardstick file: a
// yardstick that names a NO_OUTBOUND route for a row the run sent on an external route voids the run.
func TestEnforcedKeysNoOutboundOnTheSlotsPinnedRoute(t *testing.T) {
	r := newEnfRun()
	seq := len(r.in.Slots) + 1
	for _, caller := range callerOrder {
		for k := 1; k <= 3; k++ {
			start := enfT0.Add(time.Duration(seq-1) * 3 * time.Second)
			r.in.Slots = append(r.in.Slots, Slot{Seq: seq, Row: "81", Caller: caller, Principal: caller, Expect: "match", K: k, Method: "POST",
				Route: "/api/v1/billing/plans/pull-stripe", Path: "/api/v1/billing/plans/pull-stripe", Start: start, End: start.Add(100 * time.Millisecond), Status: 200})
			r.in.Yard = append(r.in.Yard, YardSlot{Row: "81", Caller: caller, K: k, Key: "POST /api/v1/billing/plans/pull-stripe", Status: 200, Verdict: VerdictMatch})
			seq++
		}
	}
	for _, s := range r.in.Slots {
		key := s.Key()
		if s.Row == "81" {
			key = "POST /api/v1/admin/integrations/github/install-url"
		}
		r.in.YardOutbound = append(r.in.YardOutbound, OutboundSlot{Row: s.Row, Caller: s.Caller, K: s.K, Key: key, Status: s.Status})
	}
	r.in.Namespaces["POST /api/v1/billing/plans/pull-stripe"] = "billing"
	r.in.Grace = 2200 * time.Millisecond
	r.in.YardOutboundGiven = true
	r.in.Outbound = Outbound{Logged: true, Rows: tableOf(r.in.Slots, map[string]string{"81": "stripe"}), Egress: egressReads("before", "between", "after"), Seed: noSeed}
	rep := r.judge()
	if !rep.Void || !strings.Contains(strings.Join(rep.Reasons, "\n"), "the yardstick names route POST /api/v1/admin/integrations/github/install-url for row 81") {
		t.Fatalf("void %v: %v", rep.Void, rep.Reasons)
	}
}

// Both judges fail closed with no row table: a library caller that passes none gets a void run, never a silent skip
// of the class checks.
func TestJudgesFailClosedWithNoRowTable(t *testing.T) {
	slots, lines := controlsOK()
	ev, _ := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond}); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "no row table") {
		t.Errorf("judge with no table: void %v %v", r.Void, r.Reasons)
	}
	run := newEnfRun()
	run.in.Outbound = Outbound{}
	if r := run.judge(); !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "no row table") {
		t.Errorf("judge-enforced with no table: void %v %v", r.Void, r.Reasons)
	}
}

// Drive checks a plan it is handed the way MakePlan builds one: a control on an external route, and an external route
// whose service is not allowed, refuse the whole drive before any request; a plain slot is sent.
func TestDriveValidatesAHandBuiltPlan(t *testing.T) {
	api := newFakeAPI(t)
	tokens := map[string]string{"member-A": "m", "superuser": "s"}
	stripe := Slot{Seq: 1, Row: "22", Caller: "superuser", Principal: "superuser", Expect: "match", K: 1, Method: "POST",
		Route: "/api/v1/billing/plans/pull-stripe", Path: "/api/v1/billing/plans/pull-stripe", Body: none}
	control := stripe
	control.Row, control.Expect = "C1", "mismatch"
	for name, c := range map[string]struct {
		slot  Slot
		allow []string
		want  string
	}{
		"a control on an external route":  {control, []string{"stripe"}, "control"},
		"an external route not allowed":   {stripe, nil, "-allow-external stripe"},
		"an external route, other allowed": {stripe, []string{"pagerduty"}, "-allow-external stripe"},
	} {
		d := &Driver{Base: api.srv.URL, Client: api.srv.Client(), Gap: MinGap, AllowExternal: c.allow, Sleep: func(context.Context, time.Duration) error { return nil }}
		if err := d.Drive(context.Background(), Plan{Slots: []Slot{c.slot}}, tokens, io.Discard); err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "before any request") {
			t.Errorf("%s: %v", name, err)
		}
		if len(api.seen) != 0 {
			t.Fatalf("%s: %d requests sent", name, len(api.seen))
		}
	}
	d := &Driver{Base: api.srv.URL, Client: api.srv.Client(), Gap: MinGap, AllowExternal: []string{"stripe"}, Sleep: func(context.Context, time.Duration) error { return nil }}
	if err := d.Drive(context.Background(), Plan{Slots: []Slot{stripe}}, tokens, io.Discard); err != nil || len(api.seen) != 1 {
		t.Fatalf("an allowed external slot: %v, %d requests", err, len(api.seen))
	}
}
