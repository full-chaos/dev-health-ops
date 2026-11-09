package shadowvenue

import (
	"strings"
	"testing"
	"time"
)

var (
	farFuture = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	hex64     = strings.Repeat("ab", 32)
	hex64b    = strings.Repeat("cd", 32)
)

// claimsOf fills the read-back of every sent slot as a healthy run would: the token carries the principal's sub, the
// member and admin the org of the cell, the no-org superuser no org, the superuser-O the org E.
func claimsOf(slots []Slot) []Slot {
	out := append([]Slot{}, slots...)
	for i := range out {
		s := &out[i]
		if s.Gap() {
			continue
		}
		s.WantSub = WirePrincipal(claimUser)
		s.WantOrg = testOrgA
		switch s.Caller {
		case "superuser":
			s.WantOrg = "-"
		case CallerSuperuserOrg:
			s.WantOrg = claimOrgE
		}
		s.ClaimSub, s.ClaimOrg, s.ClaimExp = s.WantSub, s.WantOrg, farFuture
		s.BodyLen, s.BodySHA, s.BodyNormSHA = 2, hex64, hex64
	}
	return out
}

func snapshotOf(at time.Time, rows ...string) *MembershipSnapshot {
	s, err := ParseMembershipSnapshot(snapText(rows...))
	if err != nil {
		panic(err)
	}
	s.TakenAt = at
	return &s
}

// superOrgRun is the controls, a member cell and a superuser-O cell, with the claims of a healthy run.
func superOrgRun() ([]Slot, []string) {
	slots, lines := controlsOK()
	member := cellOf(7, "58", "member")
	super := cellOf(10, "58", CallerSuperuserOrg)
	for _, s := range member {
		lines = append(lines, result(s, 300*time.Millisecond, "match", "dec_M"+strings.Repeat("A", 22)+string(rune('0'+s.K))))
	}
	for _, s := range super {
		line := s
		line.Caller = "superuser"
		lines = append(lines, result(line, 300*time.Millisecond, "match", "dec_S"+strings.Repeat("B", 22)+string(rune('0'+s.K))))
	}
	slots = append(slots, member...)
	slots = append(slots, super...)
	return claimsOf(slots), lines
}

func judgeStrict(t *testing.T, slots []Slot, lines []string, m Membership) Report {
	t.Helper()
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Rows: tableOf(slots, nil)}, Strict: true, Membership: m})
}

func healthyMembership(slots []Slot) Membership {
	first, last := driveSpan(slots)
	row := claimUser + "\t" + claimOrgE + "\t0\t0\t"
	return Membership{
		Before: snapshotOf(first.Add(-2*time.Minute), row+first.Add(-2*time.Minute).Format(time.RFC3339)),
		After:  snapshotOf(last.Add(20*time.Second), row+last.Add(20*time.Second).Format(time.RFC3339)),
	}
}

func TestStrictJudgePassesAHealthyRunAndCountsTheSuperuserOrgCell(t *testing.T) {
	slots, lines := superOrgRun()
	r := judgeStrict(t, slots, lines, healthyMembership(slots))
	if r.Void || !r.Pass {
		t.Fatalf("void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
	if n := r.VerdictCounts()[VerdictMatch]; n != 6 {
		t.Fatalf("counts %v", r.VerdictCounts())
	}
}

// Each planted defect is red the way the plan says: a wrong claim fails the cell, a missing read-back voids the run.
func TestStrictJudgeEachClaimDefectIsRed(t *testing.T) {
	cases := map[string]struct {
		plant func(slots []Slot, m *Membership)
		void  bool
		want  string
	}{
		"claim org differs from the principal's org": {func(s []Slot, _ *Membership) { s[6].ClaimOrg = testOrgB }, false, VerdictMismatchCaller},
		"claim sub is another user":                  {func(s []Slot, _ *Membership) { s[7].ClaimSub = WirePrincipal("11111111-1111-4111-8111-111111111111") }, false, VerdictMismatchCaller},
		"the no-org superuser carries an org":        {func(s []Slot, _ *Membership) { s[9].Caller, s[9].WantOrg, s[9].ClaimOrg = "superuser", "-", testOrgA }, false, VerdictMismatchCaller},
		"the superuser-O token has no org":           {func(s []Slot, _ *Membership) { s[9].ClaimOrg = "-" }, false, VerdictMismatchCaller},
		"a slot whose plan wants no sub":             {func(s []Slot, _ *Membership) { s[6].WantSub = "" }, true, "claim"},
		"a slot without a claim read-back":           {func(s []Slot, _ *Membership) { s[6].ClaimSub = "" }, true, "claim"},
		"a slot without an expiry":                   {func(s []Slot, _ *Membership) { s[6].ClaimExp = time.Time{} }, true, "claim"},
		"a slot without a body digest":               {func(s []Slot, _ *Membership) { s[6].BodyNormSHA = "" }, true, "body digest"},
		"no membership snapshot at all":              {func(_ []Slot, m *Membership) { *m = Membership{} }, true, "snapshot"},
		"no snapshot before the run":                 {func(_ []Slot, m *Membership) { m.Before = nil }, true, "snapshot"},
		"no snapshot after the run":                  {func(_ []Slot, m *Membership) { m.After = nil }, true, "snapshot"},
		"a membership row before the run": {func(_ []Slot, m *Membership) {
			m.Before = snapshotOf(m.Before.TakenAt, claimUser+"\t"+claimOrgE+"\t1\t0\t"+m.Before.TakenAt.Format(time.RFC3339))
		}, true, "membership"},
		"an auth membership row before the run": {func(_ []Slot, m *Membership) {
			m.Before = snapshotOf(m.Before.TakenAt, claimUser+"\t"+claimOrgE+"\t0\t1\t"+m.Before.TakenAt.Format(time.RFC3339))
		}, true, "membership"},
		"a membership row appears after the drive": {func(_ []Slot, m *Membership) {
			m.After = snapshotOf(m.After.TakenAt, claimUser+"\t"+claimOrgE+"\t1\t1\t"+m.After.TakenAt.Format(time.RFC3339))
		}, true, "membership"},
		"the snapshot lacks the user and org": {func(_ []Slot, m *Membership) {
			m.After = snapshotOf(m.After.TakenAt, claimUser+"\t"+bdOrg1+"\t0\t0\t"+m.After.TakenAt.Format(time.RFC3339))
		}, true, "snapshot"},
		"the before snapshot is older than 5 minutes": {func(s []Slot, m *Membership) {
			first, _ := driveSpan(s)
			m.Before = snapshotOf(first.Add(-6*time.Minute), claimUser+"\t"+claimOrgE+"\t0\t0\t"+first.Add(-6*time.Minute).Format(time.RFC3339))
		}, true, "5 minutes"},
		"the before snapshot is taken after the drive began": {func(s []Slot, m *Membership) {
			first, _ := driveSpan(s)
			m.Before = snapshotOf(first.Add(time.Second), claimUser+"\t"+claimOrgE+"\t0\t0\t"+first.Add(time.Second).Format(time.RFC3339))
		}, true, "before"},
		"the after snapshot is taken before the drive ended": {func(s []Slot, m *Membership) {
			_, last := driveSpan(s)
			m.After = snapshotOf(last.Add(-time.Second), claimUser+"\t"+claimOrgE+"\t0\t0\t"+last.Add(-time.Second).Format(time.RFC3339))
		}, true, "after"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			slots, lines := superOrgRun()
			m := healthyMembership(slots)
			c.plant(slots, &m)
			r := judgeStrict(t, slots, lines, m)
			text := strings.ToLower(strings.Join(r.Reasons, "\n"))
			if c.void {
				if !r.Void || r.Exit() != 3 || !strings.Contains(text, c.want) {
					t.Fatalf("want a void run naming %q: void %v exit %d %v", c.want, r.Void, r.Exit(), r.Reasons)
				}
				return
			}
			if r.Void || r.Pass || r.VerdictCounts()[c.want] == 0 {
				t.Fatalf("want a failed run with %s: void %v pass %v %v", c.want, r.Void, r.Pass, r.VerdictCounts())
			}
		})
	}
}

// Without Strict the old runs judge as before: the claim and body checks are the strict form.
func TestJudgeWithoutStrictIgnoresClaimsAndBodies(t *testing.T) {
	slots, lines := controlsOK()
	cell := cellOf(7, "58", "member")
	lines = append(lines, matches(cell)...)
	if r := judge(t, append(slots, cell...), lines); r.Void || !r.Pass {
		t.Fatalf("%v", r.Reasons)
	}
}
