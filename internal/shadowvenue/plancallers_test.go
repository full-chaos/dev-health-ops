package shadowvenue

import (
	"strings"
	"testing"
)

func superOrgPlan(t *testing.T, rowOrg string) Plan {
	t.Helper()
	venue := superOrgVenue(t)
	rows := mustRows(t, testRowsHeader+"58\tGET\t/api/v1/admin/teams\t"+rowOrg+"\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	plan, err := MakePlan(rows, venue, PlanOptions{Throwaway: "T", K: 1})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestAPlatformAdminOrgPrincipalGivesTheRowAFourthCallerInItsOrgOnly(t *testing.T) {
	var got []string
	for _, s := range superOrgPlan(t, "E").Slots {
		got = append(got, s.Caller+"/"+s.Principal)
	}
	want := "member/-,admin/-,superuser/superuser,superuser_org/superuser-E"
	if !strings.Contains(strings.Join(got, ","), "superuser/superuser,superuser_org/superuser-E") {
		t.Fatalf("callers %v, want the no-org superuser then the superuser-O (%s)", got, want)
	}
	for _, s := range superOrgPlan(t, "A").Slots {
		if s.Caller == CallerSuperuserOrg {
			t.Fatalf("a superuser-O of org E ran a row of org A: %+v", s)
		}
	}
}

func TestSuperuserOrgSlotsExpectTheOrgClaimOfTheRowOrg(t *testing.T) {
	for _, s := range superOrgPlan(t, "E").Slots {
		if s.Caller == CallerSuperuserOrg && (s.WantOrg != claimOrgE || s.WantSub != WirePrincipal(claimUser)) {
			t.Fatalf("%+v", s)
		}
		if s.Caller == "superuser" && s.WantOrg != none {
			t.Fatalf("the no-org superuser expects org %q", s.WantOrg)
		}
	}
}

func TestPrincipalsMayRepeatAUserOnlyAmongPlatformAdmins(t *testing.T) {
	_, raw := testPrincipals(t)
	const pw = "/x/p.pw"
	ok := raw + "principal\tsuperuser-B\ts@example.test\t88888888-8888-4888-8888-888888888888\tB\tplatform_admin_org(no_membership)\t" + pw + "\n"
	if _, err := ParsePrincipals([]byte(ok)); err != nil {
		t.Fatalf("a platform admin repeated by uuid and email refused: %v", err)
	}
	for name, extra := range map[string]string{
		"a member repeated by uuid":                            "principal\tmember-B\tother@example.test\t66666666-6666-4666-8666-666666666666\tB\tmember\t" + pw + "\n",
		"a member repeated by email":                           "principal\tmember-B\tm@example.test\t12345678-6666-4666-8666-666666666666\tB\tmember\t" + pw + "\n",
		"a platform admin with the uuid of a member":           "principal\tsuperuser-B\ts2@example.test\t66666666-6666-4666-8666-666666666666\tB\tplatform_admin_org(no_membership)\t" + pw + "\n",
		"a platform admin with the email of a member":          "principal\tsuperuser-B\tm@example.test\t12345678-8888-4888-8888-888888888888\tB\tplatform_admin_org(no_membership)\t" + pw + "\n",
		"a platform admin repeated with another email":         "principal\tsuperuser-B\tt@example.test\t88888888-8888-4888-8888-888888888888\tB\tplatform_admin_org(no_membership)\t" + pw + "\n",
		"a member with the uuid and email of a platform admin": "principal\tmember-B\ts@example.test\t88888888-8888-4888-8888-888888888888\tB\tmember\t" + pw + "\n",
		"a platform admin with the uuid and email of a member": "principal\tsuperuser-B\tm@example.test\t66666666-6666-4666-8666-666666666666\tB\tplatform_admin_org(no_membership)\t" + pw + "\n",
		"a name twice": "principal\tmember-A\tx@example.test\t12345678-6666-4666-8666-666666666666\tB\tmember\t" + pw + "\n",
		"a platform admin of the org kind with no org": "principal\tsuperuser-N\ts@example.test\t88888888-8888-4888-8888-888888888888\t-\tplatform_admin_org(no_membership)\t" + pw + "\n",
	} {
		if _, err := ParsePrincipals([]byte(raw + extra)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The no-org superuser stays the first superuser that has no org, wherever the superuser-O principals stand in the file.
func TestSuperuserOrgPrincipalsBeforeTheNoOrgSuperuserDoNotTakeItsCell(t *testing.T) {
	_, raw := testPrincipals(t)
	const pw = "/x/p.pw"
	first := strings.Replace(raw, "principal\tsuperuser\t", "principal\tsuperuser-E\ts@example.test\t88888888-8888-4888-8888-888888888888\tA\tplatform_admin_org(no_membership)\t"+pw+"\nprincipal\tsuperuser\t", 1)
	venue, err := ParsePrincipals([]byte(first))
	if err != nil {
		t.Fatal(err)
	}
	rows := mustRows(t, testRowsHeader+"58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	plan, err := MakePlan(rows, venue, PlanOptions{Throwaway: "T", K: 1})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range plan.Slots {
		got = append(got, s.Caller+"/"+s.Principal)
	}
	if !strings.Contains(strings.Join(got, ","), "superuser/superuser,superuser_org/superuser-E") {
		t.Fatalf("callers %v", got)
	}
}

func TestPlatformAdminOrgPrincipalOfAPoolOrgPassesThePoolCheckWithoutAFrozenUUID(t *testing.T) {
	venue, frozen, raw, _ := testPoolVenue(t)
	const pw = "/x/p.pw"
	more := raw + "principal\tsuperuser-T1\ts@example.test\t88888888-8888-4888-8888-888888888888\tT1\tplatform_admin_org(no_membership)\t" + pw + "\n"
	venue, err := ParsePrincipals([]byte(more))
	if err != nil {
		t.Fatal(err)
	}
	rows := mustRows(t, testRowsHeader+"13\tDELETE\t/api/v1/admin/orgs/{org_id}\tT\tyes\t-\t-\tmatch\t/api/v1/admin/orgs/{org}\t-\n")
	plan, err := MakePlan(rows, venue, PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, K: 3, Destructive: true})
	if err != nil {
		t.Fatalf("a superuser-O of a pool org was refused by the pool check: %v", err)
	}
	found := false
	for _, s := range plan.Slots {
		found = found || (s.Caller == CallerSuperuserOrg && s.K == 1 && s.Principal == "superuser-T1")
	}
	if !found {
		t.Fatal("no superuser-O cell in pool org T1")
	}
}

func TestPrincipalsRefuseAnEmailOfAnotherUUIDEvenAmongPlatformAdmins(t *testing.T) {
	_, raw := testPrincipals(t)
	extra := "principal\tsuperuser-B\ts@example.test\t12345678-8888-4888-8888-888888888888\tB\tplatform_admin_org(no_membership)\t/x/p.pw\n"
	if _, err := ParsePrincipals([]byte(raw + extra)); err == nil {
		t.Fatal("a platform admin took the email of another platform admin with a different uuid")
	}
}

// The superuser-O exemption is by exact uuid, declared in the principals file by a superuser_o line; the role string alone
// exempts nothing, and a second platform_admin_org uuid that is not listed is refused.
func TestSuperuserOrgExemptionNeedsAListedUUID(t *testing.T) {
	_, raw := testPrincipals(t)
	const listed, unlisted = "abababab-abab-4bab-8bab-abababababab", "cdcdcdcd-cdcd-4dcd-8dcd-cdcdcdcdcdcd"
	pw := "/var/lib/oci-cache/lane-scratch/pm-13267/none.pw"
	so := func(name, uuid string) string {
		return "principal\t" + name + "\tso-" + name + "@example.test\t" + uuid + "\tA\tplatform_admin_org\t" + pw + "\n"
	}
	if _, err := ParsePrincipals([]byte(raw + "superuser_o\tsuperuser-O\t-\t" + listed + "\t-\t-\t-\n" + so("superuser-A", listed))); err != nil {
		t.Fatalf("a listed superuser-O uuid was refused: %v", err)
	}
	for name, file := range map[string]string{
		"no superuser_o line": raw + so("superuser-A", listed),
		"another uuid listed": raw + "superuser_o\tsuperuser-O\t-\t" + listed + "\t-\t-\t-\n" + so("superuser-A", unlisted),
		"second unlisted":     raw + "superuser_o\tsuperuser-O\t-\t" + listed + "\t-\t-\t-\n" + so("superuser-A", listed) + so("superuser-B", unlisted),
	} {
		if _, err := ParsePrincipals([]byte(file)); err == nil || !strings.Contains(err.Error(), "not listed") {
			t.Errorf("%s: want a refusal naming the unlisted uuid, got %v", name, err)
		}
	}
}
