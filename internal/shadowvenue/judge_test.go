package shadowvenue

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 5, 23, 0, 0, 0, time.UTC)

// slotAt is slot seq of a run whose slots start 3 s apart and answer in 100 ms.
func slotAt(seq int, row, caller string, k int) Slot {
	start := t0.Add(time.Duration(seq-1) * 3 * time.Second)
	return Slot{Seq: seq, Row: row, Caller: caller, Principal: caller + "-A", Expect: "match", K: k, Method: "GET",
		Route: "/api/v1/admin/teams", Path: "/api/v1/admin/teams", Start: start, End: start.Add(100 * time.Millisecond), Status: 200}
}

func controlAt(seq int, id string, k int) Slot {
	slot := slotAt(seq, id, "member", k)
	slot.Expect = "mismatch"
	slot.Status = 403
	return slot
}

func logLine(at time.Time, msg string, attrs ...string) string {
	rec := map[string]string{"time": at.Format(time.RFC3339Nano), "level": "INFO", "msg": msg}
	for i := 0; i+1 < len(attrs); i += 2 {
		rec[attrs[i]] = attrs[i+1]
	}
	raw, _ := json.Marshal(rec)
	return string(raw)
}

// result is the result line of slot, written offset after its start.
func result(slot Slot, offset time.Duration, outcome, decision string) string {
	attrs := []string{"route", slot.Method + " " + slot.Route, "namespace", "teams", "caller", slot.Caller, "result", outcome}
	if decision != "" {
		attrs = append(attrs, "decision_id", decision)
	}
	return logLine(slot.Start.Add(offset), MsgResult, attrs...)
}

func mismatchWarn(slot Slot, offset time.Duration, class, decision string) string {
	return logLine(slot.Start.Add(offset), MsgMismatch, "route", slot.Method+" "+slot.Route, "caller", slot.Caller, "class", class,
		"legacy", "deny", "shadow", "allow", "reason_code", "allowed_by_role", "decision_id", decision, "namespace", "teams")
}

func judge(t *testing.T, slots []Slot, lines []string, rulings ...Ruling) Report {
	t.Helper()
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return Judge(slots, ev, rulings, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Rows: tableOf(slots, nil)}})
}

// controlsOK are two control cells of one slot each... K=3, so three slots per control.
func controlsOK() ([]Slot, []string) {
	var slots []Slot
	var lines []string
	for i := 1; i <= 6; i++ {
		id := "C1"
		if i > 3 {
			id = "C2"
		}
		slot := controlAt(i, id, (i-1)%3+1)
		decision := "dec_CTRL" + strings.Repeat(string(rune('A'+i)), 22)
		slots = append(slots, slot)
		lines = append(lines, result(slot, 500*time.Millisecond, "mismatch", decision), mismatchWarn(slot, 500*time.Millisecond, "unresolved", decision))
	}
	return slots, lines
}

func cellOf(seqFrom int, row, caller string) []Slot {
	return []Slot{slotAt(seqFrom, row, caller, 1), slotAt(seqFrom+1, row, caller, 2), slotAt(seqFrom+2, row, caller, 3)}
}

func matches(slots []Slot) []string {
	var lines []string
	for _, slot := range slots {
		lines = append(lines, result(slot, 400*time.Millisecond, "match", "dec_"+strings.Repeat("M", 22)+string(rune('A'+slot.Seq%26))+"AAA"))
	}
	return lines
}

func rowVerdict(t *testing.T, report Report, row string) RowVerdict {
	t.Helper()
	for _, rv := range report.Rows {
		if rv.Row == row {
			return rv
		}
	}
	t.Fatalf("row %s not in report", row)
	return RowVerdict{}
}

func TestJudgePassesARowWhoseEveryCellMatchesWithADecision(t *testing.T) {
	slots, lines := controlsOK()
	cell := cellOf(7, "58", "member")
	slots = append(slots, cell...)
	lines = append(lines, matches(cell)...)
	report := judge(t, slots, lines)
	if report.Void || !report.Pass || report.Exit() != 0 {
		t.Fatalf("void=%v pass=%v exit=%d reasons=%v", report.Void, report.Pass, report.Exit(), report.Reasons)
	}
	if rv := rowVerdict(t, report, "58"); !rv.Pass {
		t.Fatalf("row 58: %+v", rv)
	}
}

func TestJudgeFailsLoudlyOnAMissingResultLine(t *testing.T) {
	slots, lines := controlsOK()
	cell := cellOf(7, "58", "member")
	slots = append(slots, cell...)
	lines = append(lines, matches(cell[:2])...)
	report := judge(t, slots, lines)
	if report.Pass || report.Exit() != 1 || rowVerdict(t, report, "58").Pass {
		t.Fatalf("a missing line passed: %+v", report)
	}
	if got := report.Slots[len(report.Slots)-1].Verdict; got != VerdictMissing {
		t.Fatalf("last slot %s, want %s", got, VerdictMissing)
	}
	if !strings.Contains(strings.Join(report.Reasons, "\n"), "no result line") {
		t.Fatalf("reasons do not name the missing line: %v", report.Reasons)
	}
}

// A line outside the slot window, of another caller or of another route is not the slot's evidence.
func TestJudgeCountsOnlyTheSlotsOwnLine(t *testing.T) {
	for name, mutate := range map[string]func(Slot) string{
		"late":  func(s Slot) string { return result(s, 2500*time.Millisecond, "match", "dec_"+strings.Repeat("L", 26)) },
		"early": func(s Slot) string { return result(s, -200*time.Millisecond, "match", "dec_"+strings.Repeat("E", 26)) },
		"other caller": func(s Slot) string {
			s.Caller = "admin"
			return result(s, 400*time.Millisecond, "match", "dec_"+strings.Repeat("O", 26))
		},
		"other route": func(s Slot) string {
			s.Route = "/api/v1/admin/teams/{team_id}"
			return result(s, 400*time.Millisecond, "match", "dec_"+strings.Repeat("R", 26))
		},
	} {
		slots, lines := controlsOK()
		cell := cellOf(7, "58", "member")
		slots = append(slots, cell...)
		lines = append(lines, matches(cell[:2])...)
		lines = append(lines, mutate(cell[2]))
		report := judge(t, slots, lines)
		if report.Slots[len(report.Slots)-1].Verdict != VerdictMissing || report.Pass {
			t.Errorf("%s: last slot %s pass %v", name, report.Slots[len(report.Slots)-1].Verdict, report.Pass)
		}
	}
}

func TestJudgeRefusesTwoLinesForOneSlot(t *testing.T) {
	slots, lines := controlsOK()
	cell := cellOf(7, "58", "member")
	slots = append(slots, cell...)
	lines = append(lines, matches(cell)...)
	lines = append(lines, result(cell[0], 900*time.Millisecond, "match", "dec_"+strings.Repeat("D", 26)))
	report := judge(t, slots, lines)
	if report.Slots[6].Verdict != VerdictDuplicate || report.Pass {
		t.Fatalf("slot 7 %s pass %v", report.Slots[6].Verdict, report.Pass)
	}
}

// A match with no decision id was decided without the decision point: PDP evidence only for a cross-org cell.
func TestJudgeMatchWithoutDecisionPassesOnlyACrossOrgCell(t *testing.T) {
	for _, crossOrg := range []bool{false, true} {
		slots, lines := controlsOK()
		cell := cellOf(7, "13", "member")
		for i := range cell {
			cell[i].CrossOrg = crossOrg
			lines = append(lines, result(cell[i], 300*time.Millisecond, "match", ""))
		}
		slots = append(slots, cell...)
		report := judge(t, slots, lines)
		if rowVerdict(t, report, "13").Pass != crossOrg {
			t.Fatalf("cross-org %v: row pass %v (%v)", crossOrg, !crossOrg, report.Slots[6].Verdict)
		}
		want := VerdictNoPDP
		if crossOrg {
			want = VerdictMatchServedOther
		}
		if report.Slots[6].Verdict != want {
			t.Fatalf("cross-org %v: %s, want %s", crossOrg, report.Slots[6].Verdict, want)
		}
	}
}

func TestJudgeMismatchFailsUnlessIntentionalOrRuled(t *testing.T) {
	build := func(class string) ([]Slot, []string) {
		slots, lines := controlsOK()
		cell := cellOf(7, "58", "member")
		for i, slot := range cell {
			decision := "dec_MISM" + strings.Repeat(string(rune('A'+i)), 22)
			lines = append(lines, result(slot, 300*time.Millisecond, "mismatch", decision), mismatchWarn(slot, 300*time.Millisecond, class, decision))
		}
		return append(slots, cell...), lines
	}
	slots, lines := build("unresolved")
	if report := judge(t, slots, lines); rowVerdict(t, report, "58").Pass || report.Slots[6].Verdict != VerdictMismatch {
		t.Fatalf("an unruled mismatch passed: %s", report.Slots[6].Verdict)
	}
	ruling := Ruling{Route: "GET /api/v1/admin/teams", Caller: "member", Class: "unresolved", Legacy: "deny", Shadow: "allow", Reason: "allowed_by_role", Verdict: "legacy_bug_kept"}
	if report := judge(t, slots, lines, ruling); !rowVerdict(t, report, "58").Pass || report.Slots[6].Verdict != VerdictMismatchRuled {
		t.Fatalf("a ruled mismatch: %s", report.Slots[6].Verdict)
	}
	for name, mutate := range map[string]func(*Ruling){
		"reason": func(r *Ruling) { r.Reason = "no_membership" },
		"route":  func(r *Ruling) { r.Route = "GET /api/v1/admin/teams/{team_id}" },
		"caller": func(r *Ruling) { r.Caller = "admin" },
	} {
		other := ruling
		mutate(&other)
		if report := judge(t, slots, lines, other); rowVerdict(t, report, "58").Pass {
			t.Fatalf("a ruling of another %s passed the tuple", name)
		}
	}
	slots, lines = build("intentional")
	if report := judge(t, slots, lines); !rowVerdict(t, report, "58").Pass || report.Slots[6].Verdict != VerdictMismatchIntentional {
		t.Fatalf("an intentional mismatch: %s", report.Slots[6].Verdict)
	}
}

// A mismatch result whose WARN line is not in the log has no tuple: nobody can rule it.
func TestJudgeMismatchWithoutItsWarnLineFails(t *testing.T) {
	slots, lines := controlsOK()
	cell := cellOf(7, "58", "member")
	for i, slot := range cell {
		lines = append(lines, result(slot, 300*time.Millisecond, "mismatch", "dec_NOWARN"+strings.Repeat(string(rune('A'+i)), 20)))
	}
	slots = append(slots, cell...)
	report := judge(t, slots, lines)
	if report.Slots[6].Verdict != VerdictMismatchNoTuple || rowVerdict(t, report, "58").Pass {
		t.Fatalf("%s", report.Slots[6].Verdict)
	}
	// Two WARN lines for one decision: the tuple is ambiguous, so no ruling can apply.
	slots, lines = controlsOK()
	cell = cellOf(7, "58", "member")
	for i, slot := range cell {
		decision := "dec_TWICE" + strings.Repeat(string(rune('A'+i)), 21)
		lines = append(lines, result(slot, 300*time.Millisecond, "mismatch", decision), mismatchWarn(slot, 300*time.Millisecond, "unresolved", decision),
			mismatchWarn(slot, 400*time.Millisecond, "intentional", decision))
	}
	report = judge(t, append(slots, cell...), lines)
	if report.Slots[6].Verdict != VerdictMismatchNoTuple {
		t.Fatalf("two WARN lines for one decision: %s", report.Slots[6].Verdict)
	}
}

func TestJudgeFailsEveryNonDecisionResult(t *testing.T) {
	for outcome, want := range map[string]string{"error": VerdictError, "dropped": VerdictDropped, "skipped": VerdictSkipped, "impersonation_refused": VerdictImpersonationRefused} {
		slots, lines := controlsOK()
		cell := cellOf(7, "58", "member")
		for _, slot := range cell {
			lines = append(lines, result(slot, 300*time.Millisecond, outcome, ""))
		}
		report := judge(t, append(slots, cell...), lines)
		if report.Slots[6].Verdict != want || rowVerdict(t, report, "58").Pass {
			t.Errorf("%s: %s", outcome, report.Slots[6].Verdict)
		}
	}
}

// The controls are the readout's positive check: a control that matches, or has no line, voids the run.
func TestJudgeVoidsTheRunWhenAControlDoesNotMismatch(t *testing.T) {
	slots, lines := controlsOK()
	if report := judge(t, slots, lines); report.Void {
		t.Fatalf("controls that mismatch voided the run: %v", report.Reasons)
	}
	matched := append([]string{}, lines...)
	matched[0] = result(slots[0], 500*time.Millisecond, "match", "dec_"+strings.Repeat("Q", 26))
	matched[1] = ""
	if report := judge(t, slots, matched); !report.Void || report.Exit() != 3 {
		t.Fatalf("a control that matched: void %v exit %d", report.Void, report.Exit())
	}
	missing := append([]string{}, lines[2:]...)
	if report := judge(t, slots, missing); !report.Void {
		t.Fatal("a control with no line did not void the run")
	}
	if report := judge(t, slots[:3], lines[:6]); !report.Void || !strings.Contains(strings.Join(report.Reasons, "\n"), "control") {
		t.Fatalf("one control cell only: void %v %v", report.Void, report.Reasons)
	}
	var table strings.Builder
	if err := judge(t, slots, matched).WriteTable(&table); err != nil || !strings.Contains(table.String(), "C1\tGET /api/v1/admin/teams") || !strings.Contains(table.String(), "CONTROL_BROKEN") {
		t.Fatalf("the table does not show the broken control: %v\n%s", err, table.String())
	}
	// An intentional mismatch is a control's mismatch too (the served-org control is one).
	intentional := append([]string{}, lines...)
	intentional[1] = mismatchWarn(slots[0], 500*time.Millisecond, "intentional", "dec_CTRL"+strings.Repeat("B", 22))
	if report := judge(t, slots, intentional); report.Void {
		t.Fatalf("an intentional control mismatch voided the run: %v", report.Reasons)
	}
}

func TestJudgeVoidsALogWithNoResultLine(t *testing.T) {
	slots, _ := controlsOK()
	report := judge(t, slots, []string{logLine(t0, "role posture check"), "not json"})
	if !report.Void || !strings.Contains(strings.Join(report.Reasons, "\n"), "no result line") {
		t.Fatalf("void %v %v", report.Void, report.Reasons)
	}
}

func TestJudgeFailsAShortCellAGapAndAStrayLine(t *testing.T) {
	slots, lines := controlsOK()
	short := cellOf(7, "58", "member")[:2]
	report := judge(t, append(slots, short...), append(lines, matches(short)...))
	if rowVerdict(t, report, "58").Pass || report.Pass {
		t.Fatal("a cell with 2 of 3 slots passed")
	}
	slots, lines = controlsOK()
	gap := Slot{Seq: 7, Row: "22", Caller: "member", Principal: "member-A", Expect: "match", Method: "POST", Route: "/api/v1/billing/plans/pull-stripe", Note: "gap: ext stripe not allowed"}
	report = judge(t, append(slots, gap), lines)
	if rowVerdict(t, report, "22").Pass || report.Slots[6].Verdict != VerdictGap {
		t.Fatalf("a gap: %s", report.Slots[6].Verdict)
	}
	slots, lines = controlsOK()
	cell := cellOf(7, "58", "member")
	stray := cell[0]
	stray.Caller = "admin"
	report = judge(t, append(slots, cell...), append(append(lines, matches(cell)...), result(stray, 300*time.Millisecond, "match", "dec_"+strings.Repeat("S", 26))))
	if report.Pass || !strings.Contains(strings.Join(report.Reasons, "\n"), "stray") {
		t.Fatalf("a stray line passed: %v", report.Reasons)
	}
}

func TestJudgeRefusesOverlappingWindows(t *testing.T) {
	slots, lines := controlsOK()
	cell := cellOf(7, "58", "member")
	cell[1].Start = cell[0].End.Add(time.Second)
	cell[1].End = cell[1].Start.Add(100 * time.Millisecond)
	report := judge(t, append(slots, cell...), append(lines, matches(cell)...))
	if report.Pass || !strings.Contains(strings.Join(report.Reasons, "\n"), "overlap") {
		t.Fatalf("overlapping windows passed: %v", report.Reasons)
	}
}

func TestParseLogRefusesAMalformedEvidenceLine(t *testing.T) {
	for name, line := range map[string]string{
		"no route":               logLine(t0, MsgResult, "caller", "member", "result", "match"),
		"bad result":             logLine(t0, MsgResult, "route", "GET /x", "caller", "member", "result", "maybe"),
		"bad caller":             logLine(t0, MsgResult, "route", "GET /x", "caller", "root", "result", "match"),
		"bad time":               `{"time":"yesterday","msg":"` + MsgResult + `","route":"GET /x","caller":"member","result":"match"}`,
		"warn no tuple":          logLine(t0, MsgMismatch, "route", "GET /x", "caller", "member", "decision_id", "dec_X"),
		"warn no class":          logLine(t0, MsgMismatch, "route", "GET /x", "caller", "member", "decision_id", "dec_X", "legacy", "deny", "shadow", "allow", "reason_code", "r"),
		"warn no legacy":         logLine(t0, MsgMismatch, "route", "GET /x", "caller", "member", "decision_id", "dec_X", "class", "c", "shadow", "allow", "reason_code", "r"),
		"warn no shadow":         logLine(t0, MsgMismatch, "route", "GET /x", "caller", "member", "decision_id", "dec_X", "class", "c", "legacy", "deny", "reason_code", "r"),
		"warn no reason":         logLine(t0, MsgMismatch, "route", "GET /x", "caller", "member", "decision_id", "dec_X", "class", "c", "legacy", "deny", "shadow", "allow"),
		"warn no decision":       logLine(t0, MsgMismatch, "route", "GET /x", "caller", "member", "class", "c", "legacy", "deny", "shadow", "allow", "reason_code", "r"),
		"served-other no reason": logLine(t0, MsgServedOther, "route", "GET /x", "caller", "admin", "class", "intentional", "legacy", "allow", "shadow", "deny"),
		"number caller":          `{"time":"` + t0.Format(time.RFC3339Nano) + `","msg":"` + MsgResult + `","route":"GET /x","caller":5,"result":"match"}`,
	} {
		if _, err := ParseLog(strings.NewReader(line + "\n")); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Lines before the first slot and after the last one are other traffic, not stray evidence of the run.
func TestJudgeIgnoresLinesOutsideTheRunWindow(t *testing.T) {
	slots, lines := controlsOK()
	cell := cellOf(7, "58", "member")
	lines = append(lines, matches(cell)...)
	before, after := cell[0], cell[2]
	before.Caller, after.Caller = "admin", "admin"
	lines = append(lines, result(slotAt(1, "58", "admin", 1), -time.Minute, "match", "dec_"+strings.Repeat("B", 26)),
		result(after, 10*time.Second, "match", "dec_"+strings.Repeat("F", 26)))
	if report := judge(t, append(slots, cell...), lines); !report.Pass {
		t.Fatalf("lines outside the run window failed the run: %v", report.Reasons)
	}
}

// A WARN line joins its result line by decision id, and only one of the same route and caller.
func TestJudgeJoinsTheWarnLineOfTheSameDecisionRouteAndCaller(t *testing.T) {
	slots, lines := controlsOK()
	cell := cellOf(7, "58", "member")
	for i, slot := range cell {
		decision := "dec_JOIN" + strings.Repeat(string(rune('A'+i)), 22)
		other := slot
		other.Route = "/api/v1/admin/teams/{team_id}"
		lines = append(lines, result(slot, 300*time.Millisecond, "mismatch", decision), mismatchWarn(other, 300*time.Millisecond, "intentional", decision))
	}
	report := judge(t, append(slots, cell...), lines)
	if report.Slots[6].Verdict != VerdictMismatchNoTuple {
		t.Fatalf("a WARN line of another route joined: %s", report.Slots[6].Verdict)
	}
}

// A mismatch with no decision id (the request names another org) takes the served-org WARN line in its own window.
func TestJudgeServedOtherMismatchTakesTheWarnInItsWindow(t *testing.T) {
	served := func(slot Slot, offset time.Duration) string {
		return logLine(slot.Start.Add(offset), MsgServedOther, "route", slot.Key(), "caller", slot.Caller, "class", "intentional",
			"legacy", "allow", "shadow", "deny", "reason", "served_org_not_judged_org", "namespace", "members")
	}
	for name, offset := range map[string]time.Duration{"in window": 10 * time.Millisecond, "before": -60 * time.Millisecond, "after": 2500 * time.Millisecond} {
		slots, lines := controlsOK()
		cell := cellOf(7, "2", "admin")
		for i := range cell {
			cell[i].Status = 403
			lines = append(lines, result(cell[i], 10*time.Millisecond, "mismatch", ""), served(cell[i], offset))
		}
		report := judge(t, append(slots, cell...), lines)
		want := VerdictMismatchNoTuple
		if name == "in window" {
			want = VerdictMismatchIntentional
		}
		if got := report.Slots[6].Verdict; got != want {
			t.Errorf("%s: %s, want %s", name, got, want)
		}
	}
}

// C1 (a superuser the legacy guard serves names an organization that is not the token's) in the three states a run can
// show: the planted mismatch (HTTP 200, the line says legacy allow), the old C1 (HTTP 403, the guard refused, yet the
// line says legacy allow: the line and the answer disagree) and a plain match.
func TestControlC1IsGreenOnlyWhenTheLegacyGuardServedIt(t *testing.T) {
	slot := controlAt(1, "C1", 1)
	slot.Caller, slot.Principal = "superuser", "superuser"
	served := func(status int, legacy string) Report {
		slot.Status = status
		lines := []string{
			result(slot, 500*time.Millisecond, "mismatch", ""),
			logLine(slot.Start.Add(500*time.Millisecond), MsgServedOther, "route", slot.Method+" "+slot.Route, "caller", slot.Caller,
				"class", "intentional", "legacy", legacy, "shadow", "deny", "reason", "served_org_not_judged_org"),
		}
		ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		return Judge([]Slot{slot}, ev, nil, JudgeOptions{K: 1, Grace: 2200 * time.Millisecond})
	}
	if v := served(200, "allow").Slots[0].Verdict; !controlMismatch(v) {
		t.Fatalf("green: the guard served it and the line says allow: %s", v)
	}
	if v := served(403, "allow").Slots[0].Verdict; controlMismatch(v) || v != VerdictLegacyDisagrees {
		t.Fatalf("red: the guard refused it but the line says allow: %s", v)
	}
	slot.Status = 403
	ev, _ := ParseLog(strings.NewReader(result(slot, 500*time.Millisecond, "match", "") + "\n"))
	if v := Judge([]Slot{slot}, ev, nil, JudgeOptions{K: 1, Grace: 2200 * time.Millisecond}).Slots[0].Verdict; controlMismatch(v) {
		t.Fatalf("red: a match: %s", v)
	}
}

// A row whose shadow refuses what the legacy guard served is stricter than legacy (product rule: not stricter, not
// wider), even when the class is intentional; the same refusal of a request the guard also refused is equal.
func TestJudgeFailsAnIntentionalRefusalOfARequestLegacyServed(t *testing.T) {
	for status, want := range map[int]string{200: VerdictStricter, 403: VerdictMismatchIntentional} {
		slots, lines := controlsOK()
		cell := cellOf(7, "2", "superuser")
		for i := range cell {
			cell[i].Status = status
			lines = append(lines, result(cell[i], 10*time.Millisecond, "mismatch", ""),
				logLine(cell[i].Start.Add(10*time.Millisecond), MsgServedOther, "route", cell[i].Key(), "caller", "superuser", "class", "intentional",
					"legacy", "allow", "shadow", "deny", "reason", "served_org_not_judged_org", "namespace", "members"))
		}
		report := judge(t, append(slots, cell...), lines)
		if got := report.Slots[6].Verdict; got != want {
			t.Errorf("status %d: %s, want %s", status, got, want)
		}
		if status == 200 && report.Pass {
			t.Error("a stricter row passed the run")
		}
	}
}

// A line that names a shadow evidence message but is not JSON is evidence the judge cannot read: it refuses the log
// instead of skipping the line (a truncated duplicate must not pass a run).
func TestParseLogRefusesATruncatedShadowEvidenceLine(t *testing.T) {
	good := logLine(t0, MsgResult, "route", "GET /x", "caller", "member", "result", "match", "decision_id", "dec_1")
	for _, msg := range []string{MsgResult, MsgMismatch, MsgServedOther} {
		truncated := `{"time":"` + t0.Format(time.RFC3339Nano) + `","level":"INFO","msg":"` + msg + `","route":"GET /x","caller":"mem`
		if _, err := ParseLog(strings.NewReader(good + "\n" + truncated + "\n")); err == nil {
			t.Errorf("a truncated %q line was skipped", msg)
		}
	}
	if _, err := ParseLog(strings.NewReader(good + "\nnot json, and about something else\n")); err != nil {
		t.Errorf("a line that is no evidence was refused: %v", err)
	}
}
