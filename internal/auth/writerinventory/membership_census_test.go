package writerinventory

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const (
	membershipRouted  = "routed by AUTH_PROXY_ORG_PROVISIONING"
	membershipRefused = "refused"
)

// membershipSite names the switches that close a go-api write and the cells,
// "file|TestName", that prove it.
type membershipSite struct {
	class string
	cells []string
}

const (
	cellInviteJoin    = "internal/api/session/invites_provision_integration_test.go|TestInviteJoinsGoThroughAuthWithNoGoAPITransaction"
	cellOnboard       = "internal/api/session/invites_provision_integration_test.go|TestOnboardCreateOrgGoesToAuthWithTheSwitchOn"
	cellSessionRefuse = "internal/api/session/usersowned_test.go|TestUsersOwnedRefusesEverySessionWriterBeforeTouchingTheDatabase"
	cellSSOCreate     = "internal/api/sso/usersowned_test.go|TestUsersOwnedRefusesTheSSOAccountCreate"
	cellSSOJoin       = "internal/api/sso/usersowned_join_integration_test.go|TestSSOJoinRefusedWhileUsersOwned"
	cellMemberRoutes  = "internal/apiservice/admin/orgs_provision_test.go|TestProvisionSwitchOnCallsAuthOnceAndWritesNothing"
	cellOperatorCLI   = "internal/apiservice/admin/operator_provision_test.go|TestOperatorRefusesMembershipChangesAndDeletesWithTheSwitchOn"
	cellOrgCreateHTTP = "internal/apiservice/admin/orgcreate_provision_test.go|TestAdminCreateGoesToAuthWithTheSwitchOn"
	cellOrgCreateCLI  = "internal/apiservice/admin/orgcreate_provision_test.go|TestOperatorCreateOrgRefusedWithTheSwitchOn"
	cellUserCreate    = "internal/apiservice/admin/usersowned_test.go|TestUsersOwnedAdminRoutesRefuseBeforeAnyDatabaseWork"
)

// Keyed File|Func|Verb on the allowlist's go-api public.memberships rows.
var membershipWriterCensus = map[string]membershipSite{
	"internal/api/session/register.go|insertMembership|INSERT": {membershipRouted, []string{cellInviteJoin, cellOnboard, cellSessionRefuse}},

	"internal/api/sso/provision.go|handlers.createAccountAndMembership|INSERT": {membershipRefused, []string{cellSSOCreate}},
	"internal/api/sso/provision.go|handlers.membershipOrJoin|INSERT":           {membershipRefused, []string{cellSSOJoin}},

	"internal/apiservice/admin/orgstore.go|pgStore.insertMembershipTx|INSERT":   {membershipRouted, []string{cellMemberRoutes, cellOrgCreateHTTP, cellOrgCreateCLI, cellUserCreate}},
	"internal/apiservice/admin/orgstore.go|pgStore.updateMembershipRole|UPDATE": {membershipRouted, []string{cellMemberRoutes, cellOperatorCLI}},
	"internal/apiservice/admin/orgstore.go|pgStore.removeMembership|DELETE":     {membershipRouted, []string{cellMemberRoutes, cellOperatorCLI}},
	"internal/apiservice/admin/orgstore.go|pgStore.transferOwnership|UPDATE":    {membershipRouted, []string{cellMemberRoutes}},
}

// The purge builds its DELETE from orgDeletionTargets, so no allowlist row holds
// it; the switch that drops the target and the lock is read here instead.
var membershipPurgeSite = struct{ targets, purge, orgsOn, target string }{
	targets: "internal/apiservice/admin/orgdeletion_targets.go",
	purge:   "internal/apiservice/admin/orgdeletion.go",
	orgsOn:  "usersowned.OrgsOn()",
	target:  `{Table: "memberships", Direct:`,
}

func goAPIMembershipWriters() map[string]bool {
	rows := map[string]bool{}
	for _, entry := range Allowlist {
		if entry.Table != "public.memberships" {
			continue
		}
		if !strings.HasPrefix(entry.File, "internal/api/") && !strings.HasPrefix(entry.File, "internal/apiservice/") {
			continue
		}
		rows[entry.File+"|"+entry.Func+"|"+entry.Verb] = true
	}
	return rows
}

func membershipCellProblem(root, cell string) string {
	file, name, ok := strings.Cut(cell, "|")
	if !ok {
		return "malformed cell " + cell
	}
	raw, err := os.ReadFile(filepath.Join(root, file))
	if err != nil {
		return "cell file: " + err.Error()
	}
	if !strings.Contains(string(raw), "func "+name+"(") {
		return "no cell " + name + " in " + file
	}
	return ""
}

func membershipCensusProblems(root string, rows map[string]bool) []string {
	var problems []string
	for key := range rows {
		site, ok := membershipWriterCensus[key]
		switch {
		case !ok:
			problems = append(problems, "unclassified go-api public.memberships writer: "+key)
		case site.class != membershipRouted && site.class != membershipRefused:
			problems = append(problems, key+": unknown class "+site.class)
		case len(site.cells) == 0:
			problems = append(problems, key+": no proving cell")
		}
		for _, cell := range site.cells {
			if why := membershipCellProblem(root, cell); why != "" {
				problems = append(problems, key+": "+why)
			}
		}
	}
	for key := range membershipWriterCensus {
		if !rows[key] {
			problems = append(problems, "stale census row (no such allowlist writer): "+key)
		}
	}
	sort.Strings(problems)
	return problems
}

func TestMembershipWriterCensus(t *testing.T) {
	root := repoRoot(t)
	rows := goAPIMembershipWriters()
	if len(rows) == 0 {
		t.Fatal("the allowlist holds no go-api public.memberships writer; the census would pass vacuously")
	}
	for _, problem := range membershipCensusProblems(root, rows) {
		t.Error(problem)
	}
	targets, err := os.ReadFile(filepath.Join(root, membershipPurgeSite.targets))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(targets), membershipPurgeSite.target) {
		t.Fatalf("%s: no memberships purge target; update this census", membershipPurgeSite.targets)
	}
	purge, err := os.ReadFile(filepath.Join(root, membershipPurgeSite.purge))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(purge), membershipPurgeSite.orgsOn) {
		t.Errorf("%s: the memberships purge does not read %s", membershipPurgeSite.purge, membershipPurgeSite.orgsOn)
	}
}

func TestMembershipCensusRedOnAnUnclassifiedWriter(t *testing.T) {
	rows := goAPIMembershipWriters()
	rows["internal/api/planted/planted.go|plant|INSERT"] = true
	problems := strings.Join(membershipCensusProblems(repoRoot(t), rows), "\n")
	if !strings.Contains(problems, "unclassified go-api public.memberships writer: internal/api/planted/planted.go|plant|INSERT") {
		t.Fatalf("a planted writer is not red: %s", problems)
	}
}
