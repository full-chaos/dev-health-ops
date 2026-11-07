package shadowvenue

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	testOrgA  = "11111111-1111-4111-8111-111111111111"
	testOrgB  = "22222222-2222-4222-8222-222222222222"
	testOrgC  = "33333333-3333-4333-8333-333333333333"
	testTeam  = "44444444-4444-4444-8444-444444444444"
	testOwner = "55555555-5555-4555-8555-555555555555"
)

// testPrincipals writes a principals file in the venue form, with 0600 password files that hold fake values.
func testPrincipals(t *testing.T) (Venue, string) {
	t.Helper()
	dir := t.TempDir()
	pw := func(name string) string {
		path := filepath.Join(dir, name+".pw")
		if err := os.WriteFile(path, []byte("fake-"+name+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	raw := "kind\tname\temail\tuuid\torg\trole\tpassword_file\n" +
		"org\tProof Org A\t-\t" + testOrgA + "\tA\t-\t-\n" +
		"org\tProof Org B\t-\t" + testOrgB + "\tB\t-\t-\n" +
		"org\tProof Org C (throwaway)\t-\t" + testOrgC + "\tC\t-\t-\n" +
		"principal\tmember-A\tm@example.test\t66666666-6666-4666-8666-666666666666\tA\tmember\t" + pw("member-a") + "\n" +
		"principal\tadmin-A\ta@example.test\t77777777-7777-4777-8777-777777777777\tA\towner\t" + pw("admin-a") + "\n" +
		"principal\tsuperuser\ts@example.test\t88888888-8888-4888-8888-888888888888\t-\tplatform_admin(is_superuser)\t" + pw("superuser") + "\n" +
		"principal\tadmin-B\tb@example.test\t99999999-9999-4999-8999-999999999999\tB\towner\t" + pw("admin-b") + "\n" +
		"principal\towner-C\tc@example.test\t" + testOwner + "\tC\towner\t" + pw("owner-c") + "\n" +
		"team\tp-team\t-\t" + testTeam + "\tA\t-\t-\n" +
		"setting\tgeneral/p_key\t-\t-\tA\t-\t-\n" +
		"source\tMISSING: no credential\t-\t-\tA\t-\t-\n"
	venue, err := ParsePrincipals([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return venue, raw
}

// poolIDs are the pool orgs T1..T3 and, per pool org, its owner, admin and member principal: tN-org, tN-owner, tN-admin,
// tN-member.
func poolID(n int, what string) string {
	digit := string(rune('0' + n))
	code := map[string]string{"org": "a", "owner": "b", "admin": "c", "member": "d"}[what]
	return "e" + digit + code + "00000-0000-4000-8000-0000000000" + digit + code
}

// testPoolVenue is testPrincipals plus the throwaway pool T1..T3, and the frozen ids file of the pool.
func testPoolVenue(t *testing.T) (Venue, ThrowawayIDs, string, string) {
	t.Helper()
	_, raw := testPrincipals(t)
	dir := t.TempDir()
	ids := throwawayHeader + "\n"
	for n := 1; n <= 3; n++ {
		label := "T" + string(rune('0'+n))
		raw += "org\tProof pool " + label + "\t-\t" + poolID(n, "org") + "\t" + label + "\t-\t-\n"
		ids += "org\t" + label + "\t" + poolID(n, "org") + "\n"
		for _, role := range []string{"owner", "admin", "member"} {
			pw := filepath.Join(dir, label+role+".pw")
			if err := os.WriteFile(pw, []byte("fake\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			name := role + "-" + label
			raw += "principal\t" + name + "\t" + name + "@example.test\t" + poolID(n, role) + "\t" + label + "\t" + role + "\t" + pw + "\n"
			ids += "user\t" + name + "\t" + poolID(n, role) + "\n"
		}
	}
	venue, err := ParsePrincipals([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := ParseThrowawayIDs([]byte(ids))
	if err != nil {
		t.Fatal(err)
	}
	return venue, frozen, raw, ids
}

var testPool = []string{"T1", "T2", "T3"}

func planOf(t *testing.T, rows string, allow ...string) (Plan, error) {
	t.Helper()
	venue, frozen, _, _ := testPoolVenue(t)
	return MakePlan(mustRows(t, testRowsHeader+rows), venue, PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, AllowExternal: allow, K: 3, Destructive: true})
}

func TestPlanRefusesADestructiveRowOutsideTheThrowawayPool(t *testing.T) {
	cases := map[string]string{
		"row org A":         "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tA\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n",
		"path names org A":  "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org:A}\t-\n",
		"path names T1":     "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org:T1}\t-\n",
		"body names A user": "9\tPOST\t/api/v1/admin/impersonate\tT\tyes\t-\t-\tmatch\t/api/v1/admin/impersonate\t{\"user_id\":\"{user:member-A}\"}\n",
		"path names a seed": "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{uuid:team}\t-\n",
	}
	for name, row := range cases {
		if _, err := planOf(t, row); err == nil || !strings.Contains(err.Error(), "throwaway") {
			t.Errorf("%s: %v", name, err)
		}
	}
	ok := "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n" +
		"9\tPOST\t/api/v1/admin/impersonate\tT\tyes\t-\t-\tmatch\t/api/v1/admin/impersonate\t{\"user_id\":\"{owner}\"}\n"
	if _, err := planOf(t, ok); err != nil {
		t.Fatalf("pool destructive rows refused: %v", err)
	}
	venue, frozen, _, _ := testPoolVenue(t)
	plain := mustRows(t, testRowsHeader+"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	if _, err := MakePlan(plain, venue, PlanOptions{Throwaway: "C", Pool: testPool, Frozen: frozen, K: 3, Destructive: true}); err == nil {
		t.Fatal("a throwaway label that is an org of the principals file was accepted")
	}
}

// A literal UUID in a destructive row is refused, even one of a pool org: only {org}, {owner} and {missing} name ids.
func TestPlanRefusesALiteralUUIDInADestructiveRow(t *testing.T) {
	for name, row := range map[string]string{
		"org A in the path":  "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/" + testOrgA + "\t-\n",
		"T1 in the path":     "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/" + poolID(1, "org") + "\t-\n",
		"a user in the body": "9\tPOST\t/api/v1/admin/impersonate\tT\tyes\t-\t-\tmatch\t/api/v1/admin/impersonate\t{\"user_id\":\"" + testOwner + "\"}\n",
	} {
		if _, err := planOf(t, row); err == nil || !strings.Contains(err.Error(), "literal UUID") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The pool is held to the frozen ids: one pool org per k, each in the principals file and frozen with its principals;
// an org or a principal outside the pool with a frozen id (a pool org that aliases a seeded one) is refused.
func TestPlanHoldsThePoolToTheFrozenIDs(t *testing.T) {
	venue, frozen, raw, ids := testPoolVenue(t)
	rows := mustRows(t, testRowsHeader+"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n")
	opts := func(pool []string, f ThrowawayIDs) PlanOptions {
		return PlanOptions{Throwaway: "T", Pool: pool, Frozen: f, K: 3, Destructive: true}
	}
	if _, err := MakePlan(rows, venue, opts(testPool, frozen)); err != nil {
		t.Fatal(err)
	}
	refreeze := func(text string) ThrowawayIDs {
		f, err := ParseThrowawayIDs([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	cases := map[string]PlanOptions{
		"two orgs for k=3":      opts([]string{"T1", "T2"}, frozen),
		"a pool org twice":      opts([]string{"T1", "T1", "T2"}, frozen),
		"pool org not in file":  opts([]string{"T1", "T2", "T9"}, frozen),
		"seeded org pooled":     opts([]string{"T1", "T2", "A"}, frozen),
		"no frozen ids":         opts(testPool, ThrowawayIDs{}),
		"org A frozen":          opts(testPool, refreeze(ids+"org\tA\t"+testOrgA+"\n")),
		"superuser frozen":      opts(testPool, refreeze(ids+"user\tsuperuser\t88888888-8888-4888-8888-888888888888\n")),
		"pool admin not frozen": opts(testPool, refreeze(strings.Replace(ids, "user\tadmin-T2\t"+poolID(2, "admin")+"\n", "", 1))),
		"pool org not frozen":   opts(testPool, refreeze(strings.Replace(ids, "org\tT3\t"+poolID(3, "org")+"\n", "", 1))),
	}
	for name, o := range cases {
		if _, err := MakePlan(rows, venue, o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A pool org whose id is org A's: its principals file maps T3 to org A's uuid.
	alias, err := ParsePrincipals([]byte(strings.Replace(raw, poolID(3, "org"), "a"+testOrgA[1:], 1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MakePlan(rows, alias, opts(testPool, frozen)); err == nil {
		t.Error("a pool org whose id is not frozen was accepted")
	}
}

// Controls run first, the plain rows next (row-major, callers member, admin, superuser), the destructive rows last and
// k-major: slot k of every destructive row acts in pool org k, with that org's own member and admin, never a borrowed
// caller; the superuser's org delete is the last request of each k.
func TestPlanOrdersControlsFirstAndDestructiveRowsLastKMajor(t *testing.T) {
	plan, err := planOf(t,
		"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n"+
			"9\tPOST\t/api/v1/admin/impersonate\tT\tyes\t-\t-\tmatch\t/api/v1/admin/impersonate\t{\"user_id\":\"{owner}\"}\n"+
			"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n"+
			"C2\tGET\t/api/v1/admin/teams\tA\tno\t-\tmember-A\tmismatch\t/api/v1/admin/teams\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, slot := range plan.Slots {
		got = append(got, slot.Row+"/"+slot.Caller+"/"+slot.Principal+"/"+itoa(slot.K))
	}
	want := []string{
		"C2/member/member-A/1", "C2/member/member-A/2", "C2/member/member-A/3",
		"58/member/member-A/1", "58/member/member-A/2", "58/member/member-A/3",
		"58/admin/admin-A/1", "58/admin/admin-A/2", "58/admin/admin-A/3",
		"58/superuser/superuser/1", "58/superuser/superuser/2", "58/superuser/superuser/3",
	}
	for k := 1; k <= 3; k++ {
		label, n := "T"+itoa(k), itoa(k)
		want = append(want, "13/member/member-"+label+"/"+n, "13/admin/owner-"+label+"/"+n, "13/superuser/superuser/"+n,
			"9/member/member-"+label+"/"+n, "9/admin/owner-"+label+"/"+n, "9/superuser/superuser/"+n)
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("order\n got %v\nwant %v", got, want)
	}
	for i, slot := range plan.Slots {
		if slot.Seq != i+1 || slot.CrossOrg || slot.Destructive != (slot.Row == "13" || slot.Row == "9") {
			t.Fatalf("slot %d: %+v", i, slot)
		}
		if slot.Row == "13" && slot.Path != "/api/v1/admin/orgs/"+poolID(slot.K, "org") {
			t.Fatalf("slot %d acts in %s, not pool org %d", slot.Seq, slot.Path, slot.K)
		}
		if slot.Row == "9" && slot.Body != `{"user_id":"`+poolID(slot.K, "owner")+`"}` {
			t.Fatalf("slot %d body %s, not the owner of pool org %d", slot.Seq, slot.Body, slot.K)
		}
	}
}

// A pool org without its own member is refused for a destructive row: no caller is borrowed from a seeded org.
func TestPlanRefusesABorrowedCallerOnADestructiveRow(t *testing.T) {
	venue, frozen, raw, ids := testPoolVenue(t)
	_ = venue
	cut := "principal\tmember-T2\tmember-T2@example.test\t" + poolID(2, "member")
	var kept []string
	for _, line := range strings.Split(raw, "\n") {
		if !strings.HasPrefix(line, cut) {
			kept = append(kept, line)
		}
	}
	noMember, err := ParsePrincipals([]byte(strings.Join(kept, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	frozen, _ = ParseThrowawayIDs([]byte(strings.Replace(ids, "user\tmember-T2\t"+poolID(2, "member")+"\n", "", 1)))
	rows := mustRows(t, testRowsHeader+"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n")
	if _, err := MakePlan(rows, noMember, PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, K: 3, Destructive: true}); err == nil || !strings.Contains(err.Error(), "member") {
		t.Fatalf("a pool org without a member: %v", err)
	}
}

// -only-rows sends the named rows and the controls, nothing else; an id that is no row, a control or an id twice is
// refused.
func TestPlanOnlyRows(t *testing.T) {
	venue, frozen, _, _ := testPoolVenue(t)
	rows := mustRows(t, testRowsHeader+
		"C2\tGET\t/api/v1/admin/teams\tA\tno\t-\tmember-A\tmismatch\t/api/v1/admin/teams\t-\n"+
		"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n"+
		"22\tPOST\t/api/v1/billing/plans/pull-stripe\tA\tno\tstripe\t-\tmatch\t/api/v1/billing/plans/pull-stripe\t-\n"+
		"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n")
	o := PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, AllowExternal: []string{"stripe"}, K: 3, Destructive: true, OnlyRows: []string{"22", "13"}}
	plan, err := MakePlan(rows, venue, o)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, s := range plan.Slots {
		seen[s.Row]++
	}
	if seen["58"] != 0 || seen["C2"] != 3 || seen["22"] != 9 || seen["13"] != 9 {
		t.Fatalf("slots per row %v", seen)
	}
	for name, ids := range map[string][]string{"unknown": {"22", "99"}, "control": {"C2"}, "twice": {"22", "22"}} {
		o.OnlyRows = ids
		if _, err := MakePlan(rows, venue, o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPlanResolvesEveryPlaceholderAndRefusesAnUnknownOne(t *testing.T) {
	plan, err := planOf(t,
		"61\tGET\t/api/v1/admin/teams/{team_id}\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams/{uuid:team}\t-\n"+
			"56\tGET\t/api/v1/admin/settings/{category}/{key}\tA\tno\t-\t-\tmatch\t/api/v1/admin/settings/{name:setting}\t-\n"+
			"C1\tGET\t/api/v1/admin/orgs/{org_id}/members\tA\tno\t-\tadmin-A\tmismatch\t/api/v1/admin/orgs/{org:B}/members\t-\n"+
			"3\tPOST\t/api/v1/admin/orgs/{org_id}/members\tA\tno\t-\t-\tmatch\t/api/v1/admin/orgs/{org}/members\t{\"user_id\":\"{missing}\"}\n")
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{}
	for _, slot := range plan.Slots {
		paths[slot.Row] = slot.Path + " " + slot.Body
	}
	want := map[string]string{
		"C1": "/api/v1/admin/orgs/" + testOrgB + "/members -",
		"61": "/api/v1/admin/teams/" + testTeam + " -",
		"56": "/api/v1/admin/settings/general/p_key -",
		"3":  "/api/v1/admin/orgs/" + testOrgA + "/members {\"user_id\":\"" + MissingID + "\"}",
	}
	for row, w := range want {
		if paths[row] != w {
			t.Errorf("row %s: %q, want %q", row, paths[row], w)
		}
	}
	bad := map[string]string{
		"unknown kind":      "61\tGET\t/api/v1/admin/teams/{team_id}\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams/{uuid:nothing}\t-\n",
		"seed has no uuid":  "61\tGET\t/api/v1/admin/teams/{team_id}\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams/{uuid:setting}\t-\n",
		"unknown principal": "3\tPOST\t/api/v1/admin/orgs/{org_id}/members\tA\tno\t-\t-\tmatch\t/api/v1/admin/orgs/{org}/members\t{\"u\":\"{user:nobody}\"}\n",
		"unknown name":      "61\tGET\t/api/v1/admin/teams/{team_id}\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams/{x}\t-\n",
		"path shape":        "61\tGET\t/api/v1/admin/teams/{team_id}\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams/{uuid:team}/extra\t-\n",
		"literal differs":   "61\tGET\t/api/v1/admin/teams/{team_id}\tA\tno\t-\t-\tmatch\t/api/v1/admin/team/{uuid:team}\t-\n",
		"body not json":     "3\tPOST\t/api/v1/admin/orgs/{org_id}/members\tA\tno\t-\t-\tmatch\t/api/v1/admin/orgs/{org}/members\t[1,\n",
	}
	for name, row := range bad {
		if _, err := planOf(t, row); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPlanExternalRowsAreGapsUnlessAllowed(t *testing.T) {
	row := "22\tPOST\t/api/v1/billing/plans/pull-stripe\tA\tno\tstripe\t-\tmatch\t/api/v1/billing/plans/pull-stripe\t-\n"
	plan, err := planOf(t, row)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Slots) != 3 {
		t.Fatalf("%d slots, want 3 gap slots", len(plan.Slots))
	}
	for _, slot := range plan.Slots {
		if !slot.Gap() || !strings.Contains(slot.Note, "stripe") {
			t.Fatalf("slot %+v is no stripe gap", slot)
		}
	}
	plan, err = planOf(t, row, "stripe")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Slots) != 9 || plan.Slots[0].Gap() {
		t.Fatalf("allowed: %d slots, first gap %v", len(plan.Slots), plan.Slots[0].Gap())
	}
}

func TestParsePrincipalsRefusesMalformedFiles(t *testing.T) {
	_, raw := testPrincipals(t)
	cases := map[string]string{
		"wrong header":     strings.Replace(raw, "password_file", "password", 1),
		"six columns":      raw + "team\tx\t-\t-\tA\t-\n",
		"unknown role":     strings.Replace(raw, "\tmember\t", "\tviewer\t", 1),
		"relative pw path": strings.Replace(raw, "\t/", "\trel/", 1),
		"org twice":        raw + "org\tAgain\t-\t" + testOrgA + "\tA\t-\t-\n",
		"name twice":       raw + "principal\tmember-A\tx@example.test\t" + testOrgA + "\tA\tmember\t/x.pw\n",
		"principal no org": strings.Replace(raw, "\tA\tmember\t", "\tQ\tmember\t", 1),
	}
	for name, text := range cases {
		if _, err := ParsePrincipals([]byte(text)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Without Destructive the destructive rows are gaps, one per cell, and nothing of theirs is sent; no pool is needed. A
// destructive row outside the throwaway label is still refused.
func TestPlanDestructiveRowsAreGapsUnlessAsked(t *testing.T) {
	venue, _ := testPrincipals(t)
	rows := mustRows(t, testRowsHeader+"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n"+
		"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	plan, err := MakePlan(rows, venue, PlanOptions{Throwaway: "T", K: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Slots) != 12 {
		t.Fatalf("%d slots, want 9 sent and 3 gaps", len(plan.Slots))
	}
	for _, slot := range plan.Slots {
		if (slot.Row == "13") != (slot.Gap() && strings.Contains(slot.Note, "destructive")) {
			t.Fatalf("slot %+v", slot)
		}
	}
	if _, err := MakePlan(mustRows(t, testRowsHeader+"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tA\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n"), venue, PlanOptions{Throwaway: "T", K: 3}); err == nil {
		t.Fatal("a destructive row outside the throwaway label passed while destructive rows are off")
	}
}

// The frozen ids file: a header, org and user lines with a uuid; a line of another kind, a bad uuid, an id twice, the
// missing id, or no org line refuse it. Holds refuses an id that is not frozen and lets MissingID through.
func TestParseThrowawayIDsAndHolds(t *testing.T) {
	_, frozen, _, ids := testPoolVenue(t)
	for name, text := range map[string]string{
		"header":     strings.Replace(ids, "uuid", "id", 1),
		"kind":       ids + "team\tx\t" + testTeam + "\n",
		"bad uuid":   ids + "user\tx\tnot-a-uuid\n",
		"twice":      ids + "user\tagain\t" + poolID(1, "owner") + "\n",
		"missing id": ids + "user\tm\t" + MissingID + "\n",
		"no org":     throwawayHeader + "\nuser\tx\t" + poolID(1, "owner") + "\n",
	} {
		if _, err := ParseThrowawayIDs([]byte(text)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := frozen.Holds("/api/v1/admin/orgs/"+poolID(1, "org"), `{"new_owner_id":"`+MissingID+`"}`); err != nil {
		t.Fatal(err)
	}
	if err := frozen.Holds("/api/v1/admin/orgs/" + strings.ToUpper(testOrgA)); err == nil {
		t.Fatal("org A's id passed")
	}
	if err := (ThrowawayIDs{}).Holds("/x"); err == nil {
		t.Fatal("no frozen ids passed")
	}
}
