//go:build integration

package audit_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// TestDrillAuthPassword is the third driver of scripts/drill-auth-pitr.sh (CHAOS-11834): after a restore to T, a user who
// changed the password after T must not log in with the old password, and must not log in at all until a reset. The
// attempt is the login decision auth-service runs (local.Service.Login) as the unprivileged runtime role, not a marker.
// It does nothing unless DRILL_PASSWORD_PHASE is set; it never starts a container.
//
// Phases: seed (source, before T: an auth-service user and a go-api-only user, each with a password) | change (source,
// after T: both change the password) | attempt (a copy: four logins and the hash markers).
// Passwords are made at run time, live in drill-password-secrets.json (0600) of the evidence directory, and are never
// printed or passed as an argument.
func TestDrillAuthPassword(t *testing.T) {
	phase := os.Getenv("DRILL_PASSWORD_PHASE")
	if phase == "" {
		t.Skip("DRILL_PASSWORD_PHASE not set: this is a drill driver, run by scripts/drill-auth-pitr.sh")
	}
	dsn, dir := os.Getenv("DRILL_DSN"), os.Getenv("DRILL_EVIDENCE_DIR")
	if dsn == "" || dir == "" {
		t.Fatal("DRILL_DSN and DRILL_EVIDENCE_DIR are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	role := os.Getenv("DRILL_RUNTIME_ROLE")
	if role == "" {
		role = "drill_runtime"
	}
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	defer admin.Close()
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = os.Getenv("DRILL_RUNTIME_PASSWORD")
	rt, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	defer rt.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	secrets := filepath.Join(dir, "drill-password-secrets.json")
	switch phase {
	case "seed":
		drillPasswordSeed(t, ctx, admin, rt, schema, dir, secrets)
	case "change":
		drillPasswordChange(t, ctx, admin, rt, schema, secrets)
	case "attempt":
		drillPasswordAttempt(t, ctx, admin, rt, schema, dir, secrets)
	default:
		t.Fatalf("unknown DRILL_PASSWORD_PHASE %q", phase)
	}
}

type drillPasswordUser struct {
	Email       string `json:"email"`
	PrincipalID string `json:"principal_id"`
	Old         string `json:"old"`
	New         string `json:"new"`
}

type drillPasswordSecrets struct {
	Auth   drillPasswordUser `json:"auth"`
	Legacy drillPasswordUser `json:"legacy"`
}

func drillRandomPassword(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func drillPasswordLoad(t *testing.T, path string) drillPasswordSecrets {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the drill password secrets: %v", err)
	}
	var s drillPasswordSecrets
	if err := json.Unmarshal(raw, &s); err != nil || s.Auth.PrincipalID == "" || s.Legacy.PrincipalID == "" {
		t.Fatal("the drill password secrets file is malformed")
	}
	return s
}

// drillDryUsers is the real identity lookup with no projection: a successful login must not write to the copy.
type drillDryUsers struct{ local.IdentityUsers }

func (drillDryUsers) Project(context.Context, uuid.UUID) error { return nil }

type drillFakeSessions struct{}

func (drillFakeSessions) Create(context.Context, uuid.UUID, *uuid.UUID, []string) (session.Issued, error) {
	return session.Issued{}, nil
}

// drillNoAttempts keeps a refused try from writing a lockout row or locking the user (5 failures lock for 15 minutes).
type drillNoAttempts struct{}

func (drillNoAttempts) Check(context.Context, []byte) (bool, time.Duration, error) {
	return false, 0, nil
}
func (drillNoAttempts) RecordFailure(context.Context, []byte) error { return nil }
func (drillNoAttempts) Clear(context.Context, []byte) error         { return nil }

func drillLoginService(t *testing.T, rt *pgxpool.Pool, schema authschema.ValidatedIdentifier, usersOwned bool) *local.Service {
	t.Helper()
	store, err := identity.New(identity.Config{Pool: rt, Schema: schema, UsersOwnedByAuth: usersOwned})
	if err != nil {
		t.Fatalf("identity store: %v", err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	svc, err := local.New(local.Config{
		Users:      drillDryUsers{local.IdentityUsers{Store: store}},
		Sessions:   drillFakeSessions{},
		Attempts:   drillNoAttempts{},
		LockoutKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key)),
	})
	if err != nil {
		t.Fatalf("login service: %v", err)
	}
	return svc
}

func drillLoginKind(t *testing.T, ctx context.Context, svc *local.Service, email, password string) string {
	t.Helper()
	out, err := svc.Login(ctx, local.Credentials{Email: email, Password: password})
	if err != nil {
		t.Fatalf("a login attempt failed without a decision: %v", err)
	}
	return string(out.Kind)
}

// drillPasswordTries runs the four tries in drillPasswordTryNames order.
func drillPasswordTries(t *testing.T, ctx context.Context, rt *pgxpool.Pool, schema authschema.ValidatedIdentifier, s drillPasswordSecrets) []drillPasswordOutcome {
	t.Helper()
	authSvc, legacySvc := drillLoginService(t, rt, schema, true), drillLoginService(t, rt, schema, false)
	return []drillPasswordOutcome{
		{Name: drillPasswordTryNames[0], Kind: drillLoginKind(t, ctx, authSvc, s.Auth.Email, s.Auth.Old)},
		{Name: drillPasswordTryNames[1], Kind: drillLoginKind(t, ctx, authSvc, s.Auth.Email, s.Auth.New)},
		{Name: drillPasswordTryNames[2], Kind: drillLoginKind(t, ctx, legacySvc, s.Legacy.Email, s.Legacy.Old)},
		{Name: drillPasswordTryNames[3], Kind: drillLoginKind(t, ctx, legacySvc, s.Legacy.Email, s.Legacy.New)},
	}
}

func drillPasswordSeed(t *testing.T, ctx context.Context, admin, rt *pgxpool.Pool, schema authschema.ValidatedIdentifier, dir, path string) {
	t.Helper()
	s := drillPasswordSecrets{
		Auth:   drillPasswordUser{Email: "drill-11834-auth@example.com", Old: drillRandomPassword(t), New: drillRandomPassword(t)},
		Legacy: drillPasswordUser{Email: "drill-11834-legacy@example.com", Old: drillRandomPassword(t), New: drillRandomPassword(t)},
	}
	authHash, err := passwordhash.Hash(s.Auth.Old)
	if err != nil {
		t.Fatal(err)
	}
	store, err := identity.New(identity.Config{Pool: rt, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatalf("identity store: %v", err)
	}
	authID, err := store.ProvisionLocal(ctx, identity.LocalProvisionRequest{Email: s.Auth.Email, PasswordHash: &authHash})
	if err != nil {
		t.Fatalf("provision the auth-service user: %v", err)
	}
	if err := store.SetEmailVerified(ctx, authID, time.Now(), identity.Actor{}); err != nil {
		t.Fatalf("verify the auth-service user: %v", err)
	}
	s.Auth.PrincipalID = authID.String()

	legacyID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("drill-11834-legacy-user"))
	legacyHash, err := passwordhash.Hash(s.Legacy.Old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO public.users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, 'local', true, true, false, 0, now(), now())`, legacyID, s.Legacy.Email, legacyHash); err != nil {
		t.Fatalf("seed the go-api user: %v", err)
	}
	s.Legacy.PrincipalID = legacyID.String()

	for _, try := range drillPasswordTries(t, ctx, rt, schema, s) {
		isOld := try.Name == drillPasswordTryNames[0] || try.Name == drillPasswordTryNames[2]
		if want := map[bool]string{true: drillKindSuccess, false: drillKindInvalid}[isOld]; try.Kind != want {
			t.Fatalf("source before T: %s is %q, want %q", try.Name, kindLabel(try.Kind), want)
		}
	}
	writeDrillSecrets(t, path, s)
	ids := []string{s.Auth.PrincipalID, s.Legacy.PrincipalID}
	sort.Strings(ids)
	encoded, err := json.Marshal(ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "drill-password-ids.json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeDrillSecrets(t *testing.T, path string, s drillPasswordSecrets) {
	t.Helper()
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write the drill password secrets: %v", err)
	}
}

func drillPasswordChange(t *testing.T, ctx context.Context, admin, rt *pgxpool.Pool, schema authschema.ValidatedIdentifier, path string) {
	t.Helper()
	s := drillPasswordLoad(t, path)
	authHash, err := passwordhash.Hash(s.Auth.New)
	if err != nil {
		t.Fatal(err)
	}
	store, err := identity.New(identity.Config{Pool: rt, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatalf("identity store: %v", err)
	}
	authID := uuid.MustParse(s.Auth.PrincipalID)
	if _, err := store.SetPasswordHashFor(ctx, authID, authHash, identity.Actor{PrincipalID: &authID}, identity.PasswordChanged, nil); err != nil {
		t.Fatalf("change the auth-service password: %v", err)
	}
	legacyHash, err := passwordhash.Hash(s.Legacy.New)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := admin.Exec(ctx, drillLegacyPasswordSQL, uuid.MustParse(s.Legacy.PrincipalID), legacyHash)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("change the go-api password: rows=%d err=%v", tag.RowsAffected(), err)
	}
	for _, try := range drillPasswordTries(t, ctx, rt, schema, s) {
		isNew := try.Name == drillPasswordTryNames[1] || try.Name == drillPasswordTryNames[3]
		if want := map[bool]string{true: drillKindSuccess, false: drillKindInvalid}[isNew]; try.Kind != want {
			t.Fatalf("source after the change: %s is %q, want %q", try.Name, kindLabel(try.Kind), want)
		}
	}
}

type drillPasswordResult struct {
	Expect string                 `json:"expect"`
	Fenced bool                   `json:"fenced"`
	Tries  []drillPasswordOutcome `json:"attempts"`
	// OldAccepted counts users whose password from before T still logs in: the gap (2 unfenced PITR, 0 fenced).
	OldAccepted int `json:"old_password_accepted"`
	NewAccepted int `json:"new_password_accepted"`
	// HashNull counts the restored password rows with no hash: auth.users of the auth user (hash and algorithm both
	// NULL) plus the public.users rows of both users. 3 after a fence apply, 0 otherwise.
	HashNull int `json:"hash_null_rows"`
}

func drillPasswordAttempt(t *testing.T, ctx context.Context, admin, rt *pgxpool.Pool, schema authschema.ValidatedIdentifier, dir, path string) {
	t.Helper()
	expect := os.Getenv("DRILL_EXPECT")
	fenced := os.Getenv("DRILL_FENCE") == "1"
	s := drillPasswordLoad(t, path)
	res := drillPasswordResult{Expect: expect, Fenced: fenced, Tries: drillPasswordTries(t, ctx, rt, schema, s)}
	for i, try := range res.Tries {
		if try.Kind == drillKindSuccess {
			if i%2 == 0 {
				res.OldAccepted++
			} else {
				res.NewAccepted++
			}
		}
	}
	var authNull, publicNull int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND password_hash IS NULL AND password_algorithm IS NULL`,
		uuid.MustParse(s.Auth.PrincipalID)).Scan(&authNull); err != nil {
		t.Fatalf("read the auth.users hash marker: %v", err)
	}
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM public.users WHERE id = ANY($1) AND password_hash IS NULL`,
		[]uuid.UUID{uuid.MustParse(s.Auth.PrincipalID), uuid.MustParse(s.Legacy.PrincipalID)}).Scan(&publicNull); err != nil {
		t.Fatalf("read the public.users hash marker: %v", err)
	}
	res.HashNull = authNull + publicNull
	encoded, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "attempt-password-"+expect+"-"+os.Getenv("DRILL_LABEL")+".json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkPasswordAttempts(expect, fenced, res.Tries); err != nil {
		t.Fatal(err)
	}
	if want := map[bool]int{true: 3, false: 0}[fenced]; res.HashNull != want {
		t.Fatalf("hash markers: %d row(s) with no hash, want %d (expect=%s fenced=%v)", res.HashNull, want, expect, fenced)
	}
}
