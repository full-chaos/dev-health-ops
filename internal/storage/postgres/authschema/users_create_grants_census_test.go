package authschema

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The `dho auth users-create` verb runs as the auth runtime role and adds no grant. This census reads the SQL of
// identity.Store.ProvisionCLIUser and of insertPublicUser from source and proves RuntimePosture and UsersWriteGrant
// cover every table and column they write, and the platform role read of the superuser refusal.
func TestRuntimeGrantsCoverEveryWriteOfTheUsersCreateVerb(t *testing.T) {
	posture := map[string]TablePrivilege{}
	for _, p := range RuntimePosture() {
		posture[p.Table] = p
	}
	source, err := os.ReadFile("../../../auth/identity/cliuser.go")
	if err != nil {
		t.Fatal(err)
	}
	inserted := regexp.MustCompile("INSERT INTO `\\+s\\.q\\+`\\.(\\w+)").FindAllStringSubmatch(string(source), -1)
	if len(inserted) < 2 {
		t.Fatalf("expected the principals and users inserts in cliuser.go, found %d", len(inserted))
	}
	for _, m := range inserted {
		if !posture[m[1]].Insert {
			t.Errorf("the verb inserts auth.%s and the runtime role holds no INSERT there", m[1])
		}
	}
	// audit.Commit: the audit row, the outbox rows and the revision counter of the 0013 create guard.
	for table, want := range map[string]TablePrivilege{
		"security_audit_events":     {Insert: true},
		"auth_outbox_events":        {Insert: true},
		"aggregate_revisions":       {Insert: true, Update: true},
		"platform_role_assignments": {Select: true},
		"principals":                {Select: true},
	} {
		got := posture[table]
		if want.Insert && !got.Insert || want.Update && !got.Update || want.Select && !got.Select {
			t.Errorf("%s: the runtime role holds %+v, the verb needs %+v", table, got, want)
		}
	}

	public, err := os.ReadFile("../../../auth/identity/publicusers.go")
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`(?s)INSERT INTO public\.users\s*\((.*?)\)\s*VALUES`).FindStringSubmatch(string(public))
	if match == nil {
		t.Fatal("no INSERT INTO public.users in publicusers.go")
	}
	granted := map[string]bool{}
	insert, _ := UsersWriteGrant()
	for _, c := range insert {
		granted[c] = true
	}
	for _, c := range strings.Split(match[1], ",") {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if !granted[c] {
			t.Errorf("the verb writes public.users.%s and UsersWriteGrant has no INSERT on it", c)
		}
		if c == "is_superuser" {
			t.Error("the verb must not write is_superuser")
		}
	}
}
