//go:build integration

package admin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// ownedRenamer stands in for auth-service's rename route: it writes go-api's name as the route's commit does while
// organizations are owned (through the test's admin pool), or answers err.
type ownedRenamer struct {
	mu    sync.Mutex
	pool  *pgxpool.Pool
	err   error
	calls []string
	actor uuid.UUID
}

func (f *ownedRenamer) Rename(ctx context.Context, orgID, actor uuid.UUID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
	f.actor = actor
	if f.err != nil {
		return f.err
	}
	_, err := f.pool.Exec(ctx, `UPDATE organizations SET name = $2, updated_at = now() WHERE id = $1`, orgID, strings.TrimSpace(name))
	return err
}

// ownedRolePool is apiManifestPool with go-api's organization UPDATE narrowed to the columns go-api keeps while
// organizations are owned: no name, no is_active (the grant 10992 PR-C gives the api role). keep names the columns.
func ownedRolePool(ctx context.Context, t *testing.T, admin *pgxpool.Pool, keep ...string) *pgxpool.Pool {
	t.Helper()
	pool := apiManifestPool(ctx, t, admin)
	role := pool.Config().ConnConfig.User
	for _, statement := range []string{
		fmt.Sprintf(`REVOKE UPDATE ON public.organizations FROM %s`, role),
		fmt.Sprintf(`GRANT UPDATE (%s) ON public.organizations TO %s`, strings.Join(keep, ", "), role),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

var goAPIKeptColumns = []string{"description", "settings", "tier", "managed_by", "updated_at"}

func storedOrg(ctx context.Context, t *testing.T, pool *pgxpool.Pool, org uuid.UUID) (name, description string, active bool) {
	t.Helper()
	var desc *string
	if err := pool.QueryRow(ctx, `SELECT name, description, is_active FROM organizations WHERE id = $1`, org).Scan(&name, &desc, &active); err != nil {
		t.Fatal(err)
	}
	if desc != nil {
		description = *desc
	}
	return name, description, active
}

func ownedHandlers(pool *pgxpool.Pool, renamer orgreactivate.Renamer, auth *liveAuth) *handlers {
	h := liveHandlers(pool, auth)
	h.orgRenamer = renamer
	return h
}

// While organizations are owned the admin PATCH renames in auth-service first, then writes only the columns go-api
// keeps: it succeeds as a role that may not update name or is_active, and the is_active change is auth's alone.
func TestOrgPatchNameGoesToAuthAndWritesNoNameOrIsActiveWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	ctx, admin := startOrgsDB(t)
	org := seedOrg(ctx, t, admin, true)
	renamer := &ownedRenamer{pool: admin}
	auth := &liveAuth{status: "active", rev: 1}
	h := ownedHandlers(ownedRolePool(ctx, t, admin, goAPIKeptColumns...), renamer, auth)
	code, _ := patchLive(ctx, h, org, `{"name": " Renamed ", "description": "kept by go-api", "is_active": false}`)
	if code != http.StatusOK {
		t.Fatalf("PATCH answered %d, want 200", code)
	}
	if len(renamer.calls) != 1 || renamer.calls[0] != " Renamed " || renamer.actor.String() != validUUID {
		t.Fatalf("rename calls %q by %s, want one with the name as sent by the superuser", renamer.calls, renamer.actor)
	}
	name, description, active := storedOrg(ctx, t, admin, org)
	if name != "Renamed" || description != "kept by go-api" || !active {
		t.Fatalf("stored name %q description %q active %v; want auth's name, go-api's description, is_active left to auth", name, description, active)
	}
	if strings.Join(auth.calls, ",") != "suspend" {
		t.Fatalf("auth lifecycle calls %v, want one suspend", auth.calls)
	}
}

// Q5: the name goes to auth first; when go-api's own write then fails the PATCH answers the error and the name stays.
func TestOrgPatchRestFailsAfterTheRenameWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	ctx, admin := startOrgsDB(t)
	org := seedOrg(ctx, t, admin, true)
	renamer := &ownedRenamer{pool: admin}
	h := ownedHandlers(ownedRolePool(ctx, t, admin, "updated_at"), renamer, nil)
	if code, _ := patchLive(ctx, h, org, `{"name": "Renamed", "description": "refused"}`); code != http.StatusInternalServerError {
		t.Fatalf("PATCH answered %d, want 500", code)
	}
	if name, description, _ := storedOrg(ctx, t, admin, org); len(renamer.calls) != 1 || name != "Renamed" || description != "" {
		t.Fatalf("after the failed write: %d rename calls, name %q, description %q; want one call, the name kept, no description", len(renamer.calls), name, description)
	}
}

// Q5, the other order: a refused rename answers before go-api writes anything.
func TestOrgPatchRefusedRenameWritesNothingWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	ctx, admin := startOrgsDB(t)
	for name, tc := range map[string]struct {
		err  error
		code int
	}{
		"suspended":   {orgreactivate.ErrSuspended, http.StatusConflict},
		"bad name":    {orgreactivate.ErrNameInvalid, http.StatusUnprocessableEntity},
		"not found":   {orgreactivate.ErrNotFound, http.StatusNotFound},
		"refused":     {orgreactivate.ErrRefused, http.StatusForbidden},
		"unavailable": {orgreactivate.ErrUnavailable, http.StatusServiceUnavailable},
	} {
		org := seedOrg(ctx, t, admin, true)
		h := ownedHandlers(ownedRolePool(ctx, t, admin, goAPIKeptColumns...), &ownedRenamer{pool: admin, err: tc.err}, nil)
		if code, _ := patchLive(ctx, h, org, `{"name": "Renamed", "description": "not written"}`); code != tc.code {
			t.Fatalf("%s: PATCH answered %d, want %d", name, code, tc.code)
		}
		if stored, description, _ := storedOrg(ctx, t, admin, org); stored == "Renamed" || description != "" {
			t.Fatalf("%s: stored name %q description %q after a refused rename", name, stored, description)
		}
	}
}

// A name over auth's 100 characters is refused before auth is called; with no rename client go-api never writes the
// name itself.
func TestOrgPatchNameBoundAndNoClientWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	ctx, admin := startOrgsDB(t)
	org := seedOrg(ctx, t, admin, true)
	renamer := &ownedRenamer{pool: admin}
	h := ownedHandlers(admin, renamer, nil)
	if code, _ := patchLive(ctx, h, org, `{"name": "`+strings.Repeat("n", 101)+`"}`); code != http.StatusUnprocessableEntity || len(renamer.calls) != 0 {
		t.Fatalf("a 101 character name: %d with %d rename calls, want 422 and none", code, len(renamer.calls))
	}
	if code, _ := patchLive(ctx, ownedHandlers(admin, nil, nil), org, `{"name": "No Client"}`); code != http.StatusInternalServerError {
		t.Fatalf("no rename client: %d, want 500", code)
	}
	if name, _, _ := storedOrg(ctx, t, admin, org); name == "No Client" {
		t.Fatal("go-api wrote the name itself with no rename client")
	}
}

// With the switch off the PATCH is as before: go-api writes the name, no rename call.
func TestOrgPatchNameWithTheSwitchOffMakesNoRenameCall(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "false")
	ctx, admin := startOrgsDB(t)
	org := seedOrg(ctx, t, admin, true)
	renamer := &ownedRenamer{pool: admin}
	long := strings.Repeat("n", 150)
	if code, _ := patchLive(ctx, ownedHandlers(admin, renamer, nil), org, `{"name": "`+long+`"}`); code != http.StatusOK {
		t.Fatalf("PATCH answered %d, want 200", code)
	}
	if name, _, _ := storedOrg(ctx, t, admin, org); name != long || len(renamer.calls) != 0 {
		t.Fatalf("stored name %q with %d rename calls, want go-api's own write and no call", name, len(renamer.calls))
	}
}
