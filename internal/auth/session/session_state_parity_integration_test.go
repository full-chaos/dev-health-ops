//go:build integration

package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The two session reads as they were before they took the session rule: go-api's and
// query-api's (internal/api/policy sessionStateQuery) and the CI mint pre-check's
// (internal/edgetokenmint sessionLiveSQL). Kept here only to compare answers.
const (
	paritySchema         = "acp"
	oldSessionStateQuery = `SELECT principal_id, revoked_at IS NOT NULL, legacy_token_version FROM {auth_schema}.sessions WHERE id = $1`
	oldSessionLiveSQL    = `SELECT principal_id, revoked_at IS NOT NULL, expires_at <= now(), legacy_token_version FROM %s.sessions WHERE id = $1`
)

// oldTextStore is go-api's store with the old session read.
type oldTextStore struct{ policy.PGStore }

func (s oldTextStore) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	var row policy.SessionRow
	err := s.Pool.QueryRow(ctx, strings.ReplaceAll(oldSessionStateQuery, "{auth_schema}", pgx.Identifier{s.AuthSchema}.Sanitize()), id).
		Scan(&row.PrincipalID, &row.Revoked, &row.LegacyTokenVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return policy.SessionRow{}, false, nil
	}
	return row, err == nil, err
}

// oldTextMint runs the old mint pre-check text whatever CheckSession asks for, so
// CheckSession's decision runs on the old read.
type oldTextMint struct{ pool *pgxpool.Pool }

func (d oldTextMint) QueryRow(ctx context.Context, _ string, args ...any) pgx.Row {
	return d.pool.QueryRow(ctx, fmt.Sprintf(oldSessionLiveSQL, pgx.Identifier{paritySchema}.Sanitize()), args...)
}

type parityEnv struct {
	admin    *pgxpool.Pool
	api      *pgxpool.Pool
	sessions *Store
	identity *identity.Store
	org      uuid.UUID
}

func startParityEnv(t *testing.T, ctx context.Context) *parityEnv {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	roles := map[string]string{}
	for _, base := range []string{"parity_runtime", "parity_api"} {
		role, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		roleID, err := authschema.NewValidatedIdentifier(role)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, authschema.Quote(roleID), readerStorePassword)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		roles[base] = role
	}
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: paritySchema, RuntimeRole: roles["parity_runtime"],
		SessionReaderRoles: []string{roles["parity_api"]}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	apiRole, err := authschema.NewValidatedIdentifier(roles["parity_api"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `GRANT SELECT ON public.users, public.memberships, public.organizations TO `+authschema.Quote(apiRole)); err != nil {
		t.Fatal(err)
	}
	schema, err := authschema.NewValidatedIdentifier(paritySchema)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	sessions, err := New(Config{Pool: readerStoreLogin(t, ctx, admin, roles["parity_runtime"]), Schema: schema,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
	if err != nil {
		t.Fatal(err)
	}
	ids, err := identity.New(identity.Config{Pool: admin, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	org := uuid.New()
	for _, statement := range []string{
		`INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'parity-org', 'Parity')`,
		`INSERT INTO acp.organizations (id, slug, name) VALUES ($1, 'parity-org', 'Parity')`,
	} {
		if _, err := admin.Exec(ctx, statement, org); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return &parityEnv{admin: admin, api: readerStoreLogin(t, ctx, admin, roles["parity_api"]), sessions: sessions, identity: ids, org: org}
}

// principal makes one user: auth principal and users row (password stamped three hours ago, before
// any session of the cell), public users row at token_version 2 and an admin membership.
func (e *parityEnv) principal(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	name := "parity-" + id.String()[:8]
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO acp.principals (id, kind, display_name) VALUES ($1, 'user', 'parity')`, []any{id}},
		{`INSERT INTO acp.users (principal_id, email, email_lower, password_hash, password_algorithm, password_updated_at)
VALUES ($1, $2, $2, 'parity-hash', 'parity-algorithm', now() - interval '3 hours')`, []any{id, name + "@example.test"}},
		{`INSERT INTO public.users (id, email, username, full_name, token_version) VALUES ($1, $2, $3, 'Parity User', 2)`, []any{id, name + "@example.test", name}},
		{`INSERT INTO public.memberships (id, user_id, org_id, role) VALUES (gen_random_uuid(), $1, $2, 'admin')`, []any{id, e.org}},
	} {
		if _, err := e.admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("%s: %v", statement.sql, err)
		}
	}
	return id
}

// seed writes a session row directly (a state a writer reaches by time, or one no writer makes).
func (e *parityEnv) seed(t *testing.T, ctx context.Context, principal uuid.UUID, created, expires string, revoked bool, pin int64, parent *uuid.UUID) uuid.UUID {
	t.Helper()
	var org *uuid.UUID
	if parent != nil {
		org = &e.org
	}
	revokedAt := "NULL"
	if revoked {
		revokedAt = "now()"
	}
	var id uuid.UUID
	if err := e.admin.QueryRow(ctx, `INSERT INTO acp.sessions (principal_id, organization_id, token_hash, created_at, expires_at, revoked_at, legacy_token_version, web_session_id)
VALUES ($1, $2, $3, `+created+`, `+expires+`, `+revokedAt+`, $4, $5) RETURNING id`, principal, org, []byte(uuid.NewString()), pin, parent).Scan(&id); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	return id
}

type parityAnswer struct{ goAPI, mint string }

func (e *parityEnv) answer(t *testing.T, ctx context.Context, store policy.Store, mintDB edgetokenmint.RowQuerier, principal, sid uuid.UUID) string {
	t.Helper()
	auth, mint := routedReaderAuth(t, store, principal)
	goAPI := "served"
	if _, err := auth.Authenticate(policy.WithRouteProfile(ctx, routedReaderProfile), mint(sid, &e.org)); policy.IsRefusal(err) {
		goAPI = "refused"
	} else if err != nil {
		goAPI = "error: " + err.Error()
	}
	var version int
	if err := e.admin.QueryRow(ctx, `SELECT token_version FROM public.users WHERE id = $1`, principal).Scan(&version); err != nil {
		t.Fatal(err)
	}
	mintAnswer := "live"
	if err := edgetokenmint.CheckSession(ctx, mintDB, sid, edgetokenmint.Principal{UserID: principal.String(), TokenVersion: version}, paritySchema); err != nil {
		mintAnswer = "refused"
		for name, want := range map[string]error{"not found": edgetokenmint.ErrSessionNotFound, "not live": edgetokenmint.ErrSessionNotLive,
			"another principal": edgetokenmint.ErrSessionPrincipal, "stale pin": edgetokenmint.ErrSessionPin} {
			if errors.Is(err, want) {
				mintAnswer += " (" + name + ")"
			}
		}
		if mintAnswer == "refused" {
			mintAnswer = "error: " + err.Error()
		}
	}
	return goAPI + " | mint " + mintAnswer
}

func (e *parityEnv) both(t *testing.T, ctx context.Context, principal, sid uuid.UUID) (old, now string) {
	t.Helper()
	newStore := policy.PGStore{Pool: e.api, AuthSchema: paritySchema}
	return e.answer(t, ctx, oldTextStore{newStore}, oldTextMint{e.admin}, principal, sid),
		e.answer(t, ctx, newStore, e.admin, principal, sid)
}

// Product rule 1: per row, the old read and the session rule's read give the same answer, at
// go-api/query-api (Authenticate on the session reader login) and at the CI mint pre-check
// (CheckSession on the CI owner login), on every state a writer makes. Any row that differs is a STOP.
func TestSessionStateParityOnWriterRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := startParityEnv(t, ctx)
	type row struct {
		name           string
		principal, sid uuid.UUID
		// wantOld, wantNew are set only on the one named row whose mint refusal class moves (acp: not an
		// answer change; the refusal stays); every other row must answer the same.
		wantOld, wantNew string
	}
	var rows []row
	add := func(name string, principal, sid uuid.UUID) {
		rows = append(rows, row{name: name, principal: principal, sid: sid})
	}

	p := e.principal(t, ctx)
	live, err := e.sessions.Create(ctx, p, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	add("live (session.Store.Create; signup stamp before created_at)", p, live.Session.ID)
	revoked, err := e.sessions.Create(ctx, p, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := e.sessions.Revoke(ctx, revoked.Session.ID, Actor{}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	add("own revoke (session.Store.Revoke)", p, revoked.Session.ID)
	add("expired by time (expires_at passed, unlinked)", p, e.seed(t, ctx, p, "now() - interval '2 hours'", "now() - interval '1 second'", false, 2, nil))
	add("expired and revoked", p, e.seed(t, ctx, p, "now() - interval '2 hours'", "now() - interval '1 second'", true, 2, nil))
	add("unknown session id", p, uuid.New())

	stale := e.principal(t, ctx)
	staleSession, err := e.sessions.Create(ctx, stale, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET token_version = token_version + 1 WHERE id = $1`, stale); err != nil {
		t.Fatal(err)
	}
	add("token_version bumped after the session (stale pin)", stale, staleSession.Session.ID)

	changed := e.principal(t, ctx)
	before, err := e.sessions.Create(ctx, changed, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := e.identity.SetPasswordHashFor(ctx, changed, "parity-hash-2", identity.Actor{}, identity.PasswordChanged, nil); err != nil {
		t.Fatalf("SetPasswordHashFor: %v", err)
	}
	// The writer stamps the password and bumps the public token_version in one transaction: go-api refuses
	// the older session either way (pin, now revoke: the same 401); the CI mint pre-check refuses it either way,
	// its class moving from ErrSessionPin to ErrSessionNotLive (acp ruling: accepted, named here).
	rows = append(rows, row{name: "password changed after the session (identity.SetPasswordHashFor, no session end)", principal: changed, sid: before.Session.ID,
		wantOld: "refused | mint refused (stale pin)", wantNew: "refused | mint refused (not live)"})
	after, err := e.sessions.Create(ctx, changed, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	add("session created after the password change", changed, after.Session.ID)

	for _, r := range rows {
		old, now := e.both(t, ctx, r.principal, r.sid)
		t.Logf("%s: old %q, new %q", r.name, old, now)
		switch {
		case r.wantOld != "":
			if old != r.wantOld || now != r.wantNew {
				t.Errorf("STOP (rule 1): %s: old %q, new %q; want exactly old %q, new %q", r.name, old, now, r.wantOld, r.wantNew)
			}
		case old != now:
			t.Errorf("STOP (rule 1): %s: old %q, new %q", r.name, old, now)
		}
	}
}

// The rows where the two reads differ, each a state no writer makes, named with why. The
// session rule's answer is auth-service's own (session.Store reads the same rule).
func TestSessionStateDiffersOnlyOnStatesNoWriterMakes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := startParityEnv(t, ctx)
	p := e.principal(t, ctx)
	parentRevoked := e.seed(t, ctx, p, "now() - interval '2 hours'", "now() + interval '1 hour'", true, 2, nil)
	parentExpired := e.seed(t, ctx, p, "now() - interval '2 hours'", "now() - interval '1 second'", false, 2, nil)
	clientOfRevoked := e.seed(t, ctx, p, "now()", "now() + interval '1 hour'", false, 2, &parentRevoked)
	clientOfExpired := e.seed(t, ctx, p, "now()", "now() + interval '1 hour'", false, 2, &parentExpired)

	for _, c := range []struct {
		name, why      string
		sid            uuid.UUID
		wantOld, wantN string
	}{
		{"client session of a revoked web session", "a client session's sid is only in an ACR-audience token, which go-api and query-api refuse before any read (acr_mint_refusal_test); CI proof sessions are unlinked",
			clientOfRevoked, "served | mint live", "refused | mint refused (not live)"},
		{"client session of an expired web session", "as above; go-api decides no expiry, so only the mint answer moves",
			clientOfExpired, "served | mint live", "served | mint refused (not live)"},
	} {
		old, now := e.both(t, ctx, p, c.sid)
		t.Logf("%s (%s): old %q, new %q", c.name, c.why, old, now)
		if old != c.wantOld || now != c.wantN {
			t.Errorf("%s: old %q, new %q; want old %q, new %q", c.name, old, now, c.wantOld, c.wantN)
		}
	}

	// A password stamp after the session with the pin still current: identity.SetPasswordHashFor bumps the
	// public token_version in the same transaction, and the signup stamps (local.go, cliuser.go) precede any
	// session, so no writer leaves this row.
	q := e.principal(t, ctx)
	stampAfter := e.seed(t, ctx, q, "now() - interval '2 hours'", "now() + interval '1 hour'", false, 2, nil)
	if _, err := e.admin.Exec(ctx, `UPDATE acp.users SET password_updated_at = now() - interval '1 hour' WHERE principal_id = $1`, q); err != nil {
		t.Fatal(err)
	}
	old, now := e.both(t, ctx, q, stampAfter)
	t.Logf("password stamp after the session, pin current: old %q, new %q", old, now)
	if old != "served | mint live" || now != "refused | mint refused (not live)" {
		t.Errorf("password stamp after the session, pin current: old %q, new %q", old, now)
	}
}
