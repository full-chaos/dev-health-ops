package shadowvenue

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const snapshotHeader = "user_id\torg_id\tpublic_rows\tauth_rows\ttaken_at"

// MembershipSnapshot is a read-only SQL read of the membership rows of the superuser-O (user, org) pairs, taken by the
// venue owner: how many public.memberships and auth.memberships rows each pair has, and when.
type MembershipSnapshot struct {
	TakenAt time.Time
	rows    map[string][2]int
}

func pairKey(user, org string) string { return strings.ToLower(user) + "|" + strings.ToLower(org) }

// Counts is the (public, auth) row counts of a pair; false when the snapshot does not list it.
func (s MembershipSnapshot) Counts(user, org string) ([2]int, bool) {
	c, ok := s.rows[pairKey(user, org)]
	return c, ok
}

// Membership is the two snapshots of a run: one taken at most 5 minutes before the drive, one right after it.
type Membership struct{ Before, After *MembershipSnapshot }

// ParseMembershipSnapshot reads a snapshot file: a header, then one line per pair, all with one taken_at.
func ParseMembershipSnapshot(raw []byte) (MembershipSnapshot, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != snapshotHeader {
		return MembershipSnapshot{}, fmt.Errorf("membership snapshot: the first line is not %q", snapshotHeader)
	}
	out := MembershipSnapshot{rows: map[string][2]int{}}
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 5 || !isUUID(f[0]) || !isUUID(f[1]) {
			return MembershipSnapshot{}, fmt.Errorf("membership snapshot: line %d: want user uuid, org uuid, two counts and a time", n+2)
		}
		pub, err1 := strconv.Atoi(f[2])
		auth, err2 := strconv.Atoi(f[3])
		at, err3 := time.Parse(time.RFC3339, f[4])
		if err := errors.Join(err1, err2, err3); err != nil || pub < 0 || auth < 0 {
			return MembershipSnapshot{}, fmt.Errorf("membership snapshot: line %d: counts are not non-negative numbers or the time is not RFC 3339", n+2)
		}
		key := pairKey(f[0], f[1])
		if _, dup := out.rows[key]; dup {
			return MembershipSnapshot{}, fmt.Errorf("membership snapshot: line %d: pair twice", n+2)
		}
		if !out.TakenAt.IsZero() && !at.Equal(out.TakenAt) {
			return MembershipSnapshot{}, fmt.Errorf("membership snapshot: line %d: a second taken_at", n+2)
		}
		out.TakenAt = at.UTC()
		out.rows[key] = [2]int{pub, auth}
	}
	if len(out.rows) == 0 {
		return MembershipSnapshot{}, errors.New("membership snapshot: no row")
	}
	return out, nil
}

// orgOrEmpty gives an org claim in the one form "" for none, whether it comes from the plan ("-") or from a slots file.
func orgOrEmpty(v string) string {
	if v == none {
		return ""
	}
	return v
}

// maxSnapshotAge is how long before the drive the before snapshot may have been taken.
const maxSnapshotAge = 5 * time.Minute

// claimCheck is the outcome of the claim read-back of a run: reasons that void it and the slots whose token is not the
// one the plan wanted.
type claimCheck struct {
	voids []string
	bad   map[int]bool
}

// checkClaims holds every sent slot to its read-back: the token's sub and org claim must be what the plan expects (the
// no-org superuser expects none); a slot with no read-back voids. A superuser-O slot also needs both membership
// snapshots, taken within the window, with no public or auth membership row for its (user, org).
func checkClaims(slots []Slot, m Membership) claimCheck {
	out := claimCheck{bad: map[int]bool{}}
	pairs := map[string][2]string{}
	missing := 0
	for _, s := range slots {
		if s.Gap() {
			continue
		}
		// The no-org superuser wants and carries no org: "-" in memory, "" after ParseSlots. Only the sub and the expiry
		// show that the claims were read back; an org claim that is none on both sides is the proof, not a gap.
		wantOrg, claimOrg := orgOrEmpty(s.WantOrg), orgOrEmpty(s.ClaimOrg)
		if s.ClaimSub == "" || s.ClaimExp.IsZero() || s.WantSub == "" {
			missing++
			continue
		}
		if s.ClaimSub != s.WantSub || claimOrg != wantOrg {
			out.bad[s.Seq] = true
		}
		if s.Caller == CallerSuperuserOrg {
			if user, ok := PrincipalUUID(s.WantSub); ok && wantOrg != "" {
				pairs[pairKey(user, wantOrg)] = [2]string{user, wantOrg}
			}
		}
	}
	if missing > 0 {
		out.voids = append(out.voids, fmt.Sprintf("%d sent slots have no claim read-back (sub, org claim, expiry): the caller of those slots is not shown", missing))
	}
	if len(pairs) == 0 {
		return out
	}
	first, last := driveSpan(slots)
	switch {
	case m.Before == nil || m.After == nil:
		out.voids = append(out.voids, "superuser-O slots ran but a membership snapshot (before or after the drive) was not given")
		return out
	case m.Before.TakenAt.After(first) || first.Sub(m.Before.TakenAt) > maxSnapshotAge:
		out.voids = append(out.voids, fmt.Sprintf("the before membership snapshot (%s) is not within 5 minutes before the drive began (%s)", m.Before.TakenAt.Format(time.RFC3339), first.Format(time.RFC3339)))
	case m.After.TakenAt.Before(last):
		out.voids = append(out.voids, fmt.Sprintf("the after membership snapshot (%s) was taken before the drive ended (%s)", m.After.TakenAt.Format(time.RFC3339), last.Format(time.RFC3339)))
	}
	for _, pair := range pairs {
		for name, snap := range map[string]*MembershipSnapshot{"before": m.Before, "after": m.After} {
			counts, ok := snap.Counts(pair[0], pair[1])
			switch {
			case !ok:
				out.voids = append(out.voids, fmt.Sprintf("the %s membership snapshot does not list the superuser-O pair user %s org %s", name, pair[0], pair[1]))
			case counts != [2]int{0, 0}:
				out.voids = append(out.voids, fmt.Sprintf("the %s membership snapshot shows %d public and %d auth membership rows for user %s in org %s: the superuser-O has a membership", name, counts[0], counts[1], pair[0], pair[1]))
			}
		}
	}
	return out
}

// applyStrict is the strict form of a judgement: every sent slot has a body digest and a claim read-back, a token that
// is not the plan's is MISMATCH_CALLER, and the superuser-O slots meet the membership snapshots.
func applyStrict(report *Report, strict bool, m Membership, void func(string, ...any)) {
	if !strict {
		return
	}
	var slots []Slot
	missing := 0
	for _, sv := range report.Slots {
		slots = append(slots, sv.Slot)
		if !sv.Gap() && sv.BodyNormSHA == "" {
			missing++
		}
	}
	if missing > 0 {
		void("%d sent slots have no body digest: the answers cannot be compared", missing)
	}
	check := checkClaims(slots, m)
	for _, reason := range check.voids {
		void("%s", reason)
	}
	for i := range report.Slots {
		if check.bad[report.Slots[i].Seq] {
			report.Slots[i].Verdict = VerdictMismatchCaller
		}
	}
}
