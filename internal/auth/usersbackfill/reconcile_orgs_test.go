package usersbackfill

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

type fakeOrgs struct {
	diffs OrgDiffs
	err   error
}

func (f fakeOrgs) OrgDiffs(context.Context) (OrgDiffs, error) { return f.diffs, f.err }

func orgsReport(t *testing.T, d OrgDiffs, exact bool) OrgsReconciliation {
	t.Helper()
	rep, err := ReconcileOrgs(context.Background(), fakeOrgs{diffs: d}, exact)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func orgsHas(rep OrgsReconciliation, line string) bool {
	for _, l := range rep.Lines() {
		if l == line {
			return true
		}
	}
	return false
}

func orgID(n int) uuid.UUID { return uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", n)) }

// withClass returns diffs with one class set to n.
func withClass(class string, n int) OrgDiffs {
	var d OrgDiffs
	switch class {
	case "org_missing_in_auth":
		d.MissingInAuth = n
	case "org_extra_in_auth":
		d.ExtraInAuth = n
	case "org_slug_diff":
		d.SlugDiff = n
	case "org_name_diff":
		d.NameDiff = n
	case "org_active_diff":
		d.ActiveDiff = n
	case "membership_auth_missing":
		d.Members.AuthMissing = n
	case "membership_public_missing":
		d.Members.PublicMissing = n
	case "membership_role_diff":
		d.Members.RoleDiff = n
	case "membership_inactive_in_public":
		d.Members.InactiveInPublic = n
	case "membership_duplicate_public_source":
		d.DuplicatePublicSource = n
	}
	return d
}

// Which classes gate in the default mode and in the exact (cutover) mode.
var orgGateTable = []struct {
	class   string
	inGate  bool // gated by default
	inExact bool // gated with -exact
}{
	{"org_missing_in_auth", true, true},
	{"org_extra_in_auth", true, true},
	{"org_slug_diff", true, true},
	{"org_name_diff", true, true},
	{"org_active_diff", true, true},
	{"membership_public_missing", true, true},
	{"membership_inactive_in_public", true, true},
	{"membership_auth_missing", false, true},
	{"membership_role_diff", false, true},
	{"membership_duplicate_public_source", false, true},
}

func TestReconcileOrgsGatesTheRightClassesInEachMode(t *testing.T) {
	for _, c := range orgGateTable {
		def := orgsReport(t, withClass(c.class, 3), false)
		exact := orgsReport(t, withClass(c.class, 3), true)
		if got := def.GatedTotal(); (got == 3) != c.inGate || (got != 0 && got != 3) {
			t.Errorf("default mode, class %s: gated %d, want gated=%v", c.class, got, c.inGate)
		}
		if got := exact.GatedTotal(); (got == 3) != c.inExact || (got != 0 && got != 3) {
			t.Errorf("exact mode, class %s: gated %d, want gated=%v", c.class, got, c.inExact)
		}
		if !orgsHas(def, "diff."+c.class+"=3") || !orgsHas(exact, "diff."+c.class+"=3") {
			t.Errorf("class %s: the count line is missing:\n%s", c.class, strings.Join(def.Lines(), "\n"))
		}
	}
}

func TestReconcileOrgsCleanSnapshotPassesBothModes(t *testing.T) {
	for _, exact := range []bool{false, true} {
		rep := orgsReport(t, OrgDiffs{PublicOrgs: 4, AuthOrgs: 4, Matched: 4, PublicMemberships: 9, AuthMemberships: 9}, exact)
		if rep.GatedTotal() != 0 {
			t.Fatalf("exact=%v: a clean snapshot gated %d", exact, rep.GatedTotal())
		}
		mode := "default"
		if exact {
			mode = "exact"
		}
		for _, want := range []string{"mode=read-only", "gate=" + mode, "orgs_public=4", "orgs_auth=4", "orgs_matched=4",
			"memberships_public=9", "memberships_auth=9", "gated_diffs=0", "mismatch_orgs_total=0", "mismatch_listed=0"} {
			if !orgsHas(rep, want) {
				t.Errorf("exact=%v: line %q missing:\n%s", exact, want, strings.Join(rep.Lines(), "\n"))
			}
		}
	}
}

// The whole report is one stable text: a golden for a mixed fixture.
func TestReconcileOrgsLinesGolden(t *testing.T) {
	d := OrgDiffs{PublicOrgs: 5, AuthOrgs: 6, Matched: 3, MissingInAuth: 1, ExtraInAuth: 2, SlugDiff: 1, NameDiff: 0, ActiveDiff: 1,
		Members:               MembershipDiffs{PublicMissing: 2, InactiveInPublic: 1, RoleDiff: 4, AuthMissing: 3},
		DuplicatePublicSource: 1, NonUserPrincipalMemberships: 2, PublicMemberships: 20, AuthMemberships: 19,
		Mismatches: []OrgMismatch{
			{OrgID: orgID(1), Classes: []string{"org_slug_diff", "membership_role_diff"}},
			{OrgID: orgID(2), Classes: []string{"org_active_diff"}},
		}}
	want := strings.Join([]string{
		"mode=read-only",
		"gate=default",
		"orgs_public=5",
		"orgs_auth=6",
		"orgs_matched=3",
		"memberships_public=20",
		"memberships_auth=19",
		"diff.org_missing_in_auth=1",
		"diff.org_extra_in_auth=2",
		"diff.org_slug_diff=1",
		"diff.org_name_diff=0",
		"diff.org_active_diff=1",
		"diff.membership_public_missing=2",
		"diff.membership_inactive_in_public=1",
		"diff.membership_auth_missing=3",
		"diff.membership_role_diff=4",
		"diff.membership_duplicate_public_source=1",
		"info.membership_non_user_principals=2",
		"gated_diffs=8",
		"mismatch_orgs_total=2",
		"mismatch_listed=2",
		"mismatch.org=00000000-0000-0000-0000-000000000001 classes=org_slug_diff,membership_role_diff",
		"mismatch.org=00000000-0000-0000-0000-000000000002 classes=org_active_diff",
	}, "\n")
	if got := strings.Join(orgsReport(t, d, false).Lines(), "\n"); got != want {
		t.Fatalf("report differs\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// A capped list is never read as complete: the total and the listed count are printed.
func TestReconcileOrgsCapsTheListAndPrintsTheTotal(t *testing.T) {
	var ms []OrgMismatch
	for i := 1; i <= 60; i++ {
		ms = append(ms, OrgMismatch{OrgID: orgID(i), Classes: []string{"org_slug_diff"}})
	}
	rep := orgsReport(t, OrgDiffs{SlugDiff: 60, Mismatches: ms}, false)
	lines := rep.Lines()
	n := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "mismatch.org=") {
			n++
		}
	}
	if n != OrgMismatchCap || OrgMismatchCap != 50 {
		t.Fatalf("listed %d org lines, want %d (cap 50)", n, OrgMismatchCap)
	}
	if !orgsHas(rep, "mismatch_orgs_total=60") || !orgsHas(rep, "mismatch_listed=50") {
		t.Fatalf("total or listed count missing:\n%s", strings.Join(lines, "\n"))
	}
	if !orgsHas(rep, "mismatch.org="+orgID(1).String()+" classes=org_slug_diff") || orgsHas(rep, "mismatch.org="+orgID(51).String()+" classes=org_slug_diff") {
		t.Fatal("the first 50 ids in order must be listed, the 51st not")
	}
}

// A failed read stops the report with an error that names no driver text; the lines carry only counts, ids and class names.
func TestReconcileOrgsReadFailureAndNoValueInTheReport(t *testing.T) {
	_, err := ReconcileOrgs(context.Background(), fakeOrgs{err: errors.New("planted-driver-detail postgres://u:p@h/db")}, false)
	if err == nil {
		t.Fatal("a failed read was not reported")
	}
	if strings.Contains(err.Error(), "planted-driver-detail") || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("error carries the driver text: %v", err)
	}
	rep := orgsReport(t, OrgDiffs{SlugDiff: 1, Mismatches: []OrgMismatch{{OrgID: orgID(7), Classes: []string{"org_slug_diff"}}}}, true)
	for _, l := range rep.Lines() {
		if strings.ContainsAny(l, "@ ") && !strings.HasPrefix(l, "mismatch.org=") {
			t.Fatalf("a line carries a value: %q", l)
		}
	}
}

func orgsVerb(t *testing.T) cli.Command {
	t.Helper()
	for _, c := range Command().Children {
		if c.Name == "orgs-reconcile" {
			return c
		}
	}
	t.Fatal("no orgs-reconcile verb")
	return cli.Command{}
}

func TestOrgsReconcileVerbIsRegisteredAndDocumentsBothModes(t *testing.T) {
	v := orgsVerb(t)
	if err := cli.Validate([]cli.Command{Command()}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(Command().Summary, "orgs-reconcile") {
		t.Fatalf("the group summary does not list the verb: %q", Command().Summary)
	}
	var stderr bytes.Buffer
	code := v.Run(context.Background(), cli.Env{Args: []string{"-h"}, Lookup: func(string) (string, bool) { return "", false }, Stdout: &bytes.Buffer{}, Stderr: &stderr})
	if code != cli.ExitOK || !strings.Contains(stderr.String(), "-exact") || !strings.Contains(stderr.String(), "default") {
		t.Fatalf("help: exit %d, text %q (it must name -exact and the default mode)", code, stderr.String())
	}
}

func TestOrgsReconcileVerbRefusesBadArguments(t *testing.T) {
	for _, args := range [][]string{{"extra"}, {"-batch", "10"}, {"-dry-run"}, {"-nope"}} {
		var stderr bytes.Buffer
		code := orgsVerb(t).Run(context.Background(), cli.Env{Args: args, Lookup: func(string) (string, bool) { return "", false }, Stdout: &bytes.Buffer{}, Stderr: &stderr})
		if code != cli.ExitUsage {
			t.Fatalf("args %v: exit %d, want %d", args, code, cli.ExitUsage)
		}
	}
}

func TestOrgsReconcileVerbWithoutADatabaseFailsWithoutRunning(t *testing.T) {
	var stderr, stdout bytes.Buffer
	code := orgsVerb(t).Run(context.Background(), cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &stderr})
	if code != cli.ExitFailure || !bytes.Contains(stderr.Bytes(), []byte("AUTH_DATABASE_URI")) || stdout.Len() != 0 {
		t.Fatalf("exit %d, stderr %q, stdout %q", code, stderr.String(), stdout.String())
	}
}

// With a stubbed target: the exit code follows the gate of the chosen mode, and the lines go to stdout.
func TestOrgsReconcileVerbExitCodeFollowsTheMode(t *testing.T) {
	orig := orgsReconcileTargetFn
	t.Cleanup(func() { orgsReconcileTargetFn = orig })
	run := func(d OrgDiffs, args ...string) (int, string) {
		orgsReconcileTargetFn = func(context.Context, cli.Env, string) (OrgsTarget, func(), int) {
			return fakeOrgs{diffs: d}, func() {}, cli.ExitOK
		}
		var stdout bytes.Buffer
		code := orgsVerb(t).Run(context.Background(), cli.Env{Args: args, Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &bytes.Buffer{}})
		return code, stdout.String()
	}
	if code, out := run(OrgDiffs{}); code != cli.ExitOK || !strings.Contains(out, "gated_diffs=0") {
		t.Fatalf("clean: exit %d, out %q", code, out)
	}
	infoOnly := OrgDiffs{Members: MembershipDiffs{RoleDiff: 2}}
	if code, _ := run(infoOnly); code != cli.ExitOK {
		t.Fatalf("a role diff alone must pass the default mode, exit %d", code)
	}
	if code, out := run(infoOnly, "-exact"); code != cli.ExitFailure || !strings.Contains(out, "gate=exact") || !strings.Contains(out, "gated_diffs=2") {
		t.Fatalf("-exact must gate a role diff: exit %d, out %q", code, out)
	}
	if code, _ := run(OrgDiffs{SlugDiff: 1}); code != cli.ExitFailure {
		t.Fatalf("an org slug diff must fail the default mode, exit %d", code)
	}
	failing := func(context.Context, cli.Env, string) (OrgsTarget, func(), int) {
		return fakeOrgs{err: errors.New("planted")}, func() {}, cli.ExitOK
	}
	orgsReconcileTargetFn = failing
	var stderr bytes.Buffer
	if code := orgsVerb(t).Run(context.Background(), cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stdout: &bytes.Buffer{}, Stderr: &stderr}); code != cli.ExitFailure || strings.Contains(stderr.String(), "planted") {
		t.Fatalf("a failed read: exit %d, stderr %q", code, stderr.String())
	}
}
