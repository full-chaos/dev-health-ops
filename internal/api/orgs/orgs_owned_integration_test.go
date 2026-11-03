//go:build integration

package orgs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// ownRenamer stands in for auth-service's rename route: it writes go-api's name as the route's commit does while
// organizations are owned, or answers err.
type ownRenamer struct {
	mu    sync.Mutex
	pool  *pgxpool.Pool
	err   error
	calls []string
	actor uuid.UUID
}

func (f *ownRenamer) Rename(ctx context.Context, orgID, actor uuid.UUID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls, f.actor = append(f.calls, name), actor
	if f.err != nil {
		return f.err
	}
	_, err := f.pool.Exec(ctx, `UPDATE organizations SET name = $2, updated_at = now() WHERE id = $1`, orgID, strings.TrimSpace(name))
	return err
}

type ownEnv struct {
	ctx         context.Context
	admin       *pgxpool.Pool
	org, member uuid.UUID
}

func startOwnEnv(t *testing.T) *ownEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	e := &ownEnv{ctx: ctx, admin: admin}
	e.seed(t)
	return e
}

// seed gives the env a fresh organization named "Before" and an owner of it.
func (e *ownEnv) seed(t *testing.T) {
	t.Helper()
	e.org, e.member = uuid.New(), uuid.New()
	pgseed.Org(e.ctx, t, e.admin, e.org.String(), "community")
	if _, err := e.admin.Exec(e.ctx, `UPDATE organizations SET name = 'Before' WHERE id = $1`, e.org); err != nil {
		t.Fatal(err)
	}
	pgseed.User(e.ctx, t, e.admin, e.member.String())
	pgseed.Membership(e.ctx, t, e.admin, uuid.NewString(), e.member.String(), e.org.String(), "owner")
}

// rolePool is a login holding SELECT on organizations and memberships and UPDATE of the organization columns in
// update only: no name while organizations are owned (the grant 10992 PR-C gives the api role).
func (e *ownEnv) rolePool(t *testing.T, update ...string) *pgxpool.Pool {
	t.Helper()
	role := "orgs_owned_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	password := uuid.NewString()
	for _, statement := range []string{
		fmt.Sprintf(`CREATE ROLE %s LOGIN NOSUPERUSER NOINHERIT PASSWORD '%s'`, role, password),
		fmt.Sprintf(`GRANT SELECT ON public.organizations, public.memberships TO %s`, role),
		fmt.Sprintf(`GRANT UPDATE (%s) ON public.organizations TO %s`, strings.Join(update, ", "), role),
	} {
		if _, err := e.admin.Exec(e.ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = e.admin.Exec(context.Background(), "DROP OWNED BY "+role)
		_, _ = e.admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
	})
	config := e.admin.Config().Copy()
	config.ConnConfig.User, config.ConnConfig.Password = role, password
	pool, err := pgxpool.NewWithConfig(e.ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func (e *ownEnv) patch(t *testing.T, pool *pgxpool.Pool, renamer Renamer, body string) (int, map[string]any) {
	t.Helper()
	value, err := pyjson.DecodeString(body)
	if err != nil {
		t.Fatal(err)
	}
	h := handlers{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), now: time.Now, renamer: renamer}
	ctx := policy.WithUser(e.ctx, &policy.User{ID: e.member, UserID: e.member.String(), OrgID: e.org.String()})
	request := httptest.NewRequest(http.MethodPatch, "/api/v1/orgs/me", nil).WithContext(policy.WithBody(ctx, pybody.Body{Value: value}))
	response := httptest.NewRecorder()
	h.updateOwnOrg(response, request)
	var out map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &out)
	return response.Code, out
}

func (e *ownEnv) stored(t *testing.T) (name, description string) {
	t.Helper()
	var desc *string
	if err := e.admin.QueryRow(e.ctx, `SELECT name, description FROM organizations WHERE id = $1`, e.org).Scan(&name, &desc); err != nil {
		t.Fatal(err)
	}
	if desc != nil {
		description = *desc
	}
	return name, description
}

// While organizations are owned PATCH /api/v1/orgs/me renames in auth-service first, acting as the member, then writes
// the description only: it succeeds as a role that may not update name, and answers auth's stored name.
func TestOwnOrgNameGoesToAuthWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	e := startOwnEnv(t)
	renamer := &ownRenamer{pool: e.admin}
	code, out := e.patch(t, e.rolePool(t, "description", "updated_at"), renamer, `{"name": " After ", "description": "go-api's"}`)
	if code != http.StatusOK || out["name"] != "After" {
		t.Fatalf("PATCH answered %d %v, want 200 with auth's stored name", code, out)
	}
	if len(renamer.calls) != 1 || renamer.calls[0] != " After " || renamer.actor != e.member {
		t.Fatalf("rename calls %q by %s, want one with the name as sent by the member", renamer.calls, renamer.actor)
	}
	if name, description := e.stored(t); name != "After" || description != "go-api's" {
		t.Fatalf("stored name %q description %q", name, description)
	}
}

// Q5: the name goes to auth first; when go-api's description write then fails the PATCH answers the error and the
// name stays.
func TestOwnOrgRestFailsAfterTheRenameWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	e := startOwnEnv(t)
	renamer := &ownRenamer{pool: e.admin}
	if code, _ := e.patch(t, e.rolePool(t, "updated_at"), renamer, `{"name": "After", "description": "refused"}`); code != http.StatusInternalServerError {
		t.Fatalf("PATCH answered %d, want 500", code)
	}
	if name, description := e.stored(t); len(renamer.calls) != 1 || name != "After" || description != "" {
		t.Fatalf("after the failed write: %d rename calls, name %q, description %q", len(renamer.calls), name, description)
	}
}

// Q5, the other order: a refused rename answers before go-api writes anything.
func TestOwnOrgRefusedRenameWritesNothingWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	e := startOwnEnv(t)
	pool := e.rolePool(t, "description", "updated_at")
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
		if code, _ := e.patch(t, pool, &ownRenamer{pool: e.admin, err: tc.err}, `{"name": "After", "description": "not written"}`); code != tc.code {
			t.Fatalf("%s: PATCH answered %d, want %d", name, code, tc.code)
		}
		if stored, description := e.stored(t); stored != "Before" || description != "" {
			t.Fatalf("%s: stored name %q description %q after a refused rename", name, stored, description)
		}
	}
}

// While organizations are owned a name of 101 to 255 characters is refused with go-api's 422 before auth is called;
// a description-only PATCH makes no rename call and leaves the stored name as it is; with no client, 500.
func TestOwnOrgNameBoundWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	e := startOwnEnv(t)
	renamer := &ownRenamer{pool: e.admin}
	pool := e.rolePool(t, "description", "updated_at")
	if code, _ := e.patch(t, pool, renamer, `{"name": "`+strings.Repeat("n", 101)+`"}`); code != http.StatusUnprocessableEntity || len(renamer.calls) != 0 {
		t.Fatalf("a 101 character name: %d with %d rename calls, want 422 and none", code, len(renamer.calls))
	}
	long := strings.Repeat("l", 150)
	if _, err := e.admin.Exec(e.ctx, `UPDATE organizations SET name = $2 WHERE id = $1`, e.org, long); err != nil {
		t.Fatal(err)
	}
	if code, out := e.patch(t, pool, renamer, `{"description": "only"}`); code != http.StatusOK || out["name"] != long || len(renamer.calls) != 0 {
		t.Fatalf("a description-only PATCH: %d %v with %d rename calls, want 200, the stored name, no call", code, out, len(renamer.calls))
	}
	if code, _ := e.patch(t, pool, nil, `{"name": "No Client"}`); code != http.StatusInternalServerError {
		t.Fatalf("no rename client: %d, want 500", code)
	}
}

// With the switch off the PATCH is as before: go-api writes a name of up to 255 characters, no rename call.
func TestOwnOrgNameWithTheSwitchOffMakesNoRenameCall(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "false")
	e := startOwnEnv(t)
	renamer := &ownRenamer{pool: e.admin}
	long := strings.Repeat("n", 255)
	if code, out := e.patch(t, e.admin, renamer, `{"name": "`+long+`"}`); code != http.StatusOK || out["name"] != long || len(renamer.calls) != 0 {
		t.Fatalf("PATCH answered %d %v with %d rename calls, want 200, go-api's own write, no call", code, out, len(renamer.calls))
	}
}
