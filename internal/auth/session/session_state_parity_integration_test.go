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
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/audit"
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
	schema   authschema.ValidatedIdentifier
	org      uuid.UUID
}

// guardedRow runs one auth-schema INSERT through audit.Commit (migration 0006's guard refuses it
// otherwise), with its audit row (fixture setup is off the provisioning stream). scan, when set,
// reads a RETURNING row inside the same transaction.
func (e *parityEnv) guardedRow(t *testing.T, ctx context.Context, sql string, args []any, scan func(pgx.Row) error) {
	t.Helper()
	err := audit.Commit(ctx, e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if scan != nil {
				return scan(tx.QueryRow(ctx, sql, args...))
			}
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "test.session_setup", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamSessionLifecycle,
	})
	if err != nil {
		t.Fatalf("guarded %q: %v", sql, err)
	}
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
	e := &parityEnv{admin: admin, api: readerStoreLogin(t, ctx, admin, roles["parity_api"]), sessions: sessions, identity: ids, schema: schema, org: uuid.New()}
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'parity-org', 'Parity')`, e.org); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	e.guardedRow(t, ctx, `INSERT INTO acp.organizations (id, slug, name) VALUES ($1, 'parity-org', 'Parity')`, []any{e.org}, nil)
	return e
}

// parityHash is a bcrypt verifier, the only class identity writes (validBcrypt).
func parityHash(t *testing.T) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(uuid.NewString()), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

// principal makes one local user with the rows identity.ProvisionLocal writes, as two password changes later
// (identity.SetPasswordHashFor) leave them: the stamp three hours ago, before any session of the cell, and
// token version 2 in both schemas. A public admin membership, as go-api's member add writes it.
func (e *parityEnv) principal(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	email := "parity-" + id.String()[:8] + "@example.test"
	hash := parityHash(t)
	e.guardedRow(t, ctx, `INSERT INTO acp.principals (id, kind, display_name) VALUES ($1, 'user', 'Parity User')`, []any{id}, nil)
	e.guardedRow(t, ctx, `INSERT INTO acp.users (principal_id, email, email_lower, password_hash, password_algorithm, password_updated_at, legacy_token_version)
VALUES ($1, $2, $2, $3, 'bcrypt', now() - interval '3 hours', 2)`, []any{id, email, hash}, nil)
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.users (id, email, password_hash, full_name, auth_provider, is_active, is_verified, token_version, created_at, updated_at)
VALUES ($1, $2, $3, 'Parity User', 'local', true, false, 2, now(), now())`, []any{id, email, hash}},
		{`INSERT INTO public.memberships (id, org_id, user_id, role, joined_at, created_at, updated_at) VALUES (gen_random_uuid(), $1, $2, 'admin', now(), now(), now())`, []any{e.org, id}},
	} {
		if _, err := e.admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("%s: %v", statement.sql, err)
		}
	}
	return id
}

// tokenHash is a session token_hash as the writer stores it: the digest of a fresh credential.
func tokenHash(t *testing.T) []byte {
	t.Helper()
	handle, err := newCredential()
	if err != nil {
		t.Fatal(err)
	}
	return hashCredential(handle)
}

// seed writes a web session row directly with the columns session.Store.Create writes (insertSessionAs), at a
// created stamp of its own (a state a writer reaches by time, or one no writer makes); revoked, it carries
// what Store.Revoke writes. created and expires are SQL expressions.
func (e *parityEnv) seed(t *testing.T, ctx context.Context, principal uuid.UUID, org *uuid.UUID, created, expires string, revoked bool, pin int64) uuid.UUID {
	t.Helper()
	revokedAt, updatedAt := "NULL", "c.at"
	var reason *string
	if revoked {
		revokedAt, updatedAt = "now()", "now()"
		r := revokedByCaller
		reason = &r
	}
	var id uuid.UUID
	e.guardedRow(t, ctx, `INSERT INTO acp.sessions (principal_id, organization_id, token_hash, issued_at, created_at, updated_at, expires_at, auth_time, amr,
	legacy_token_version, revoked_at, revoked_reason)
SELECT $1::uuid, $2::uuid, $3::bytea, c.at, c.at, `+updatedAt+`, `+expires+`, c.at, ARRAY['pwd'], $4::bigint, `+revokedAt+`, $5::text
FROM (SELECT `+created+` AS at) c RETURNING id`,
		[]any{principal, org, tokenHash(t), pin, reason}, func(r pgx.Row) error { return r.Scan(&id) })
	return id
}

// client writes a browser client's session under web with the row session.Store.InsertClientSession writes
// (linked): principal, organization, pin, auth_time and amr are the web session's, created now. It skips the
// writer's liveness check on web, so the row is one no writer makes once web has ended.
func (e *parityEnv) client(t *testing.T, ctx context.Context, web uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	e.guardedRow(t, ctx, `INSERT INTO acp.sessions (principal_id, organization_id, token_hash, issued_at, created_at, updated_at, expires_at,
	auth_time, amr, legacy_token_version, web_session_id)
SELECT w.principal_id, w.organization_id, $2::bytea, t.at, t.at, t.at, t.at + make_interval(secs => $3::double precision),
	w.auth_time, w.amr, w.legacy_token_version, w.id
FROM acp.sessions w CROSS JOIN (SELECT clock_timestamp() AS at) t
WHERE w.id = $1::uuid AND w.web_session_id IS NULL
RETURNING id`, []any{web, tokenHash(t), ClientSessionLifetime.Seconds()}, func(r pgx.Row) error { return r.Scan(&id) })
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
	add("expired by time (expires_at passed, unlinked)", p, e.seed(t, ctx, p, nil, "now() - interval '2 hours'", "now() - interval '1 second'", false, 2))
	add("expired and revoked", p, e.seed(t, ctx, p, nil, "now() - interval '2 hours'", "now() - interval '1 second'", true, 2))
	add("unknown session id", p, uuid.New())

	stale := e.principal(t, ctx)
	staleSession, err := e.sessions.Create(ctx, stale, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// go-api's own password set (internal/apiservice/admin userstore.go setUserPassword) moves the public row only.
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET password_hash = $2, token_version = token_version + 1, updated_at = now() WHERE id = $1`,
		stale, parityHash(t)); err != nil {
		t.Fatal(err)
	}
	add("token_version bumped after the session (stale pin)", stale, staleSession.Session.ID)

	changed := e.principal(t, ctx)
	before, err := e.sessions.Create(ctx, changed, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := e.identity.SetPasswordHashFor(ctx, changed, parityHash(t), identity.Actor{}, identity.PasswordChanged, nil); err != nil {
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
	// A client session's web session carries an organization (InsertClientSession refuses one without).
	parentRevoked := e.seed(t, ctx, p, &e.org, "now() - interval '2 hours'", "now() + interval '1 hour'", true, 2)
	parentExpired := e.seed(t, ctx, p, &e.org, "now() - interval '2 hours'", "now() - interval '1 second'", false, 2)
	clientOfRevoked := e.client(t, ctx, parentRevoked)
	clientOfExpired := e.client(t, ctx, parentExpired)

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
	stampAfter := e.seed(t, ctx, q, nil, "now() - interval '2 hours'", "now() + interval '1 hour'", false, 2)
	e.guardedRow(t, ctx, `UPDATE acp.users SET password_updated_at = now() - interval '1 hour' WHERE principal_id = $1`, []any{q}, nil)
	old, now := e.both(t, ctx, q, stampAfter)
	t.Logf("password stamp after the session, pin current: old %q, new %q", old, now)
	if old != "served | mint live" || now != "refused | mint refused (not live)" {
		t.Errorf("password stamp after the session, pin current: old %q, new %q", old, now)
	}
}
