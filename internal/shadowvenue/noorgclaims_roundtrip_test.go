package shadowvenue

import (
	"testing"
	"time"
)

// The no-org superuser wants and carries no org claim. The drive writes that as "-"; ParseSlots reads "-" as "". The
// claim read-back check must treat a none org on both sides as the proof it is, not as a missing read-back (every
// no-org slot of a real run voided the judge: "no claim read-back").
func TestNoOrgSuperuserClaimsSurviveTheSlotsFile(t *testing.T) {
	at := time.Date(2026, 11, 8, 14, 0, 0, 0, time.UTC)
	s := Slot{Seq: 1, Row: "22", Caller: "superuser", Principal: "superuser", Expect: expectMatch, K: 1, Method: "POST",
		Route: "/api/v1/billing/plans/pull-stripe", Path: "/api/v1/billing/plans/pull-stripe", Start: at, End: at.Add(time.Second), Status: 200,
		WantSub: WirePrincipal(claimUser), WantOrg: "-", ClaimSub: WirePrincipal(claimUser), ClaimOrg: "-", ClaimExp: farFuture,
		BodyLen: 2, BodySHA: hex64, BodyNormSHA: hex64}
	parsed, err := ParseSlots([]byte(slotsHeader + "\n" + s.line() + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c := checkClaims(parsed, Membership{}); len(c.voids) != 0 || len(c.bad) != 0 {
		t.Fatalf("a healthy no-org superuser slot read back from the slots file: voids %v bad %v", c.voids, c.bad)
	}
	parsed[0].ClaimOrg = testOrgA
	if c := checkClaims(parsed, Membership{}); !c.bad[1] {
		t.Fatalf("a no-org superuser token that carries an org must be refused: %+v", c)
	}
	parsed[0].ClaimOrg, parsed[0].ClaimSub = "", ""
	if c := checkClaims(parsed, Membership{}); len(c.voids) != 1 {
		t.Fatalf("a slot with no sub read back must still void: %+v", c)
	}
}
