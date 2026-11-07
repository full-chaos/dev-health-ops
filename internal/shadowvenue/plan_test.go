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

func planOf(t *testing.T, rows string, allow ...string) (Plan, error) {
	t.Helper()
	venue, _ := testPrincipals(t)
	return MakePlan(mustRows(t, testRowsHeader+rows), venue, PlanOptions{Throwaway: "C", AllowExternal: allow, K: 3, Destructive: true})
}

func TestPlanRefusesADestructiveRowOutsideTheThrowawayOrg(t *testing.T) {
	cases := map[string]string{
		"row org A":         "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tA\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n",
		"path names org A":  "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tC\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org:A}\t-\n",
		"body names A user": "9\tPOST\t/api/v1/admin/impersonate\tC\tyes\t-\t-\tmatch\t/api/v1/admin/impersonate\t{\"user_id\":\"{user:member-A}\"}\n",
		"path names a seed": "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tC\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{uuid:team}\t-\n",
	}
	for name, row := range cases {
		if _, err := planOf(t, row); err == nil || !strings.Contains(err.Error(), "throwaway") {
			t.Errorf("%s: %v", name, err)
		}
	}
	ok := "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tC\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n" +
		"9\tPOST\t/api/v1/admin/impersonate\tC\tyes\t-\t-\tmatch\t/api/v1/admin/impersonate\t{\"user_id\":\"{user:owner-C}\"}\n"
	if _, err := planOf(t, ok); err != nil {
		t.Fatalf("org C destructive rows refused: %v", err)
	}
	venue, _ := testPrincipals(t)
	plain := mustRows(t, testRowsHeader+"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	if _, err := MakePlan(plain, venue, PlanOptions{Throwaway: "Z", K: 3, Destructive: true}); err == nil {
		t.Fatal("an unknown throwaway org label accepted")
	}
}

// Controls run first, destructive rows last; inside a row the callers run member, admin, superuser, so the
// superuser's org delete is the run's last request.
func TestPlanOrdersControlsFirstAndDestructiveRowsLast(t *testing.T) {
	plan, err := planOf(t,
		"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tC\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n"+
			"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n"+
			"C2\tGET\t/api/v1/admin/teams\tA\tno\t-\tmember-A\tmismatch\t/api/v1/admin/teams\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, slot := range plan.Slots {
		got = append(got, slot.Row+"/"+slot.Caller+"/"+slot.Principal)
	}
	want := []string{
		"C2/member/member-A", "C2/member/member-A", "C2/member/member-A",
		"58/member/member-A", "58/member/member-A", "58/member/member-A",
		"58/admin/admin-A", "58/admin/admin-A", "58/admin/admin-A",
		"58/superuser/superuser", "58/superuser/superuser", "58/superuser/superuser",
		"13/member/member-A", "13/member/member-A", "13/member/member-A",
		"13/admin/owner-C", "13/admin/owner-C", "13/admin/owner-C",
		"13/superuser/superuser", "13/superuser/superuser", "13/superuser/superuser",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("order\n got %v\nwant %v", got, want)
	}
	for i, slot := range plan.Slots {
		if slot.Seq != i+1 || slot.K != i%3+1 {
			t.Fatalf("slot %d: seq %d k %d", i, slot.Seq, slot.K)
		}
	}
	// org C has no member: the member cell borrows org A's member, marked cross-org; no other cell is.
	for _, slot := range plan.Slots {
		if slot.CrossOrg != (slot.Row == "13" && slot.Caller == "member") {
			t.Fatalf("slot %+v cross-org %v", slot, slot.CrossOrg)
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

// Without Destructive the destructive rows are gaps, one per cell, and nothing of theirs is sent.
func TestPlanDestructiveRowsAreGapsUnlessAsked(t *testing.T) {
	venue, _ := testPrincipals(t)
	rows := mustRows(t, testRowsHeader+"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tC\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n"+
		"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	plan, err := MakePlan(rows, venue, PlanOptions{Throwaway: "C", K: 3})
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
	if _, err := MakePlan(mustRows(t, testRowsHeader+"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tA\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n"), venue, PlanOptions{Throwaway: "C", K: 3}); err == nil {
		t.Fatal("a destructive row outside the throwaway org passed while destructive rows are off")
	}
}

// A seed of the throwaway org may be named by a destructive row; a seed of another org still may not.
func TestPlanAllowsAThrowawayOrgSeedInADestructiveRow(t *testing.T) {
	_, raw := testPrincipals(t)
	const seedC = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	venue, err := ParsePrincipals([]byte(raw + "cteam\tc-team\t-\t" + seedC + "\tC\t-\t-\n"))
	if err != nil {
		t.Fatal(err)
	}
	row := func(path, body string) string {
		return "13\tDELETE\t/api/v1/admin/orgs/{org_id}\tC\tyes\t-\t-\tmatch\t" + path + "\t" + body + "\n"
	}
	for name, line := range map[string]string{
		"uuid in the path": row("/api/v1/admin/orgs/{uuid:cteam}", "-"),
		"name in the path": row("/api/v1/admin/orgs/{name:cteam}", "-"),
	} {
		if _, err := MakePlan(mustRows(t, testRowsHeader+line), venue, PlanOptions{Throwaway: "C", K: 1, Destructive: true}); err != nil {
			t.Errorf("%s: a throwaway-org seed was refused: %v", name, err)
		}
	}
	if _, err := MakePlan(mustRows(t, testRowsHeader+row("/api/v1/admin/orgs/{uuid:team}", "-")), venue, PlanOptions{Throwaway: "C", K: 1, Destructive: true}); err == nil {
		t.Error("a seed of org A was accepted in a destructive row")
	}
}
