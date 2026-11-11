package authschema

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The user-admin routes (identity adminusers.go) run as the auth runtime role. This census reads their SQL from
// source and proves the runtime posture and the users-owned grants cover every public.users column they insert or
// update, that none touches public.refresh_tokens (no grant since CHAOS-10408), and that none writes is_superuser (the platform roles live in the auth schema).
func TestRuntimeGrantsCoverEveryWriteOfTheAdminUserRoutes(t *testing.T) {
	raw, err := os.ReadFile("../../../auth/identity/adminusers.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	insert, update := UsersWriteGrant()

	inserts := regexp.MustCompile(`(?s)INSERT INTO public\.users\s*\((.*?)\)\s*VALUES`).FindAllStringSubmatch(source, -1)
	if len(inserts) != 1 {
		t.Fatalf("expected one INSERT INTO public.users in adminusers.go, found %d", len(inserts))
	}
	for _, c := range strings.Split(inserts[0][1], ",") {
		c = strings.TrimSpace(c)
		if !slices.Contains(insert, c) {
			t.Errorf("adminusers.go inserts public.users.%s and UsersWriteGrant has no INSERT on it", c)
		}
		if c == "is_superuser" {
			t.Error("adminusers.go must not insert is_superuser")
		}
	}

	updates := regexp.MustCompile("UPDATE public\\.users SET ([^`]*?) WHERE").FindAllStringSubmatch(source, -1)
	if len(updates) < 3 {
		t.Fatalf("expected the email, verified and soft delete updates of public.users in adminusers.go, found %d", len(updates))
	}
	for _, m := range updates {
		for _, assignment := range strings.Split(m[1], ",") {
			column := strings.TrimSpace(strings.SplitN(assignment, "=", 2)[0])
			if !slices.Contains(update, column) {
				t.Errorf("adminusers.go updates public.users.%s and UsersWriteGrant has no UPDATE on it", column)
			}
			if column == "is_superuser" {
				t.Error("adminusers.go must not update is_superuser")
			}
		}
	}

	// The auth runtime role holds no grant on public.refresh_tokens (CHAOS-10408 removed it): no admin write touches it.
	if strings.Contains(source, "public.refresh_tokens") {
		t.Error("adminusers.go writes public.refresh_tokens: the runtime role holds no grant there (CHAOS-10408)")
	}
	if strings.Contains(source, "DELETE FROM public.") {
		t.Error("adminusers.go deletes a public row: the runtime role holds no such DELETE for these routes")
	}
}
