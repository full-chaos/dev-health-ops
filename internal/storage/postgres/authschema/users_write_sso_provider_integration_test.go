//go:build integration

package authschema

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Under users-owned the runtime role inserts users.sso_provider_id (ops revision 0155) with the account it creates.
// Before 0155 the column grant is DEFERRED by Apply and PENDING in the posture check; from 0155 on it is granted, and
// a missing grant FAILS the posture check.

func (f *orderFixture) applyUsersOwned(t *testing.T, owned bool, ack string) (string, error) {
	t.Helper()
	pool, err := pgxpool.New(f.ctx, f.uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var logs bytes.Buffer
	_, err = Apply(f.ctx, pool, Options{Schema: "auth", RuntimeRole: f.role, UsersOwned: owned, AcknowledgeNarrowing: ack,
		Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	return logs.String(), err
}

func (f *orderFixture) ssoProviderInsertable(t *testing.T) bool {
	t.Helper()
	var held bool
	if err := orderConnect(t, f.ctx, f.uri).QueryRow(f.ctx, `SELECT has_column_privilege($1, 'public.users', 'sso_provider_id', 'INSERT')`, f.role).Scan(&held); err != nil {
		t.Fatal(err)
	}
	return held
}

// TestUsersOwnedApplyGrantsTheSSOProviderInsert: at ops head, the users-owned grants include INSERT on
// users.sso_provider_id; with that grant revoked the posture check FAILS naming it.
func TestUsersOwnedApplyGrantsTheSSOProviderInsert(t *testing.T) {
	f := newOrderFixture(t)
	if _, err := f.dhoMigrate(t); err != nil {
		t.Fatalf("dho migrate: %v", err)
	}
	if _, err := f.applyUsersOwned(t, true, ""); err != nil {
		t.Fatalf("Apply users-owned: %v", err)
	}
	if !f.ssoProviderInsertable(t) {
		t.Fatal("the runtime role cannot INSERT public.users.sso_provider_id under users-owned")
	}
	conn := orderConnect(t, f.ctx, f.uri)
	if _, err := conn.Exec(f.ctx, fmt.Sprintf(`REVOKE INSERT (sso_provider_id) ON public.users FROM %s`, Quote(mustValidatedIdentifier(f.role)))); err != nil {
		t.Fatal(err)
	}
	violations, err := VerifyRuntimePosture(f.ctx, conn, Options{Schema: "auth", RuntimeRole: f.role, UsersOwned: true})
	if err != nil {
		t.Fatalf("posture check with the INSERT grant revoked: %v", err)
	}
	var named bool
	for _, v := range violations {
		if v.Object == "users.sso_provider_id" && v.Privilege == "INSERT" {
			named = true
		}
	}
	if !named {
		t.Fatalf("posture check with the INSERT grant revoked = %v; want a FAIL naming users.sso_provider_id INSERT", violations)
	}
}

// TestUsersOwnedApplyDefersTheSSOProviderInsertBefore0155: on an ops schema at revision 0154 (no
// users.sso_provider_id), Apply with users-owned passes and reports the write grant of the column DEFERRED; the
// posture check holds it PENDING (no error, no violation); the grants pass refuses and names the column.
func TestUsersOwnedApplyDefersTheSSOProviderInsertBefore0155(t *testing.T) {
	f := newOrderFixture(t)
	f.dhoMigrateThrough(t, "0154")
	logs, err := f.applyUsersOwned(t, true, "")
	if err != nil {
		t.Fatalf("Apply users-owned at ops revision 0154: %v", err)
	}
	if !strings.Contains(logs, "users-owned write grant DEFERRED") || !strings.Contains(logs, "column=sso_provider_id") {
		t.Fatalf("Apply at 0154 did not report the sso_provider_id write grant as DEFERRED:\n%s", logs)
	}
	violations, err := VerifyRuntimePosture(f.ctx, orderConnect(t, f.ctx, f.uri), Options{Schema: "auth", RuntimeRole: f.role, UsersOwned: true})
	if err != nil {
		t.Fatalf("posture check at 0154: %v", err)
	}
	for _, v := range violations {
		if strings.Contains(v.Object, "sso_provider_id") {
			t.Fatalf("posture check at 0154 named %+v; want the column PENDING", v)
		}
	}
	pool, err := pgxpool.New(f.ctx, f.uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = Grants(f.ctx, pool, Options{Schema: "auth", RuntimeRole: f.role, UsersOwned: true})
	if !errors.Is(err, ErrPublicColumnMissing) || !strings.Contains(err.Error(), "public.users.sso_provider_id") {
		t.Fatalf("grants pass at ops revision 0154 = %v, want ErrPublicColumnMissing naming public.users.sso_provider_id", err)
	}
}

// TestSwitchOffNarrowingLosesTheSSOProviderInsert: switching users-owned off revokes the INSERT on
// users.sso_provider_id with the other users-owned grants, so the flip-back acknowledgement digest covers it.
func TestSwitchOffNarrowingLosesTheSSOProviderInsert(t *testing.T) {
	f := newOrderFixture(t)
	if _, err := f.dhoMigrate(t); err != nil {
		t.Fatalf("dho migrate: %v", err)
	}
	if _, err := f.applyUsersOwned(t, true, ""); err != nil {
		t.Fatalf("Apply users-owned: %v", err)
	}
	_, err := f.applyUsersOwned(t, false, "")
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("Apply with the switch off and no acknowledgement = %v, want a narrowing refusal", err)
	}
	t.Logf("flip-back narrowing digest %s over %d lost grants", narrowed.Digest, len(narrowed.Lost))
	var lost bool
	for _, g := range narrowed.Lost {
		if g.Kind == "column" && strings.HasSuffix(g.Object, "users") && g.Column == "sso_provider_id" && g.Privilege == "INSERT" {
			lost = true
		}
	}
	if !lost {
		t.Fatalf("the switch-off narrowing does not lose INSERT users.sso_provider_id: %v", narrowed.Lost)
	}
}
