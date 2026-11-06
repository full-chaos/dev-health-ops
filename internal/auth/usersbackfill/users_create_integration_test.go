//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const createPassword = "Zq7-correct-horse-battery" // gitleaks:allow

type createRows struct{ principals, authUsers, publicUsers, audits, assignments int64 }

func (e *reconEnv) createRows(t *testing.T, ctx context.Context) createRows {
	t.Helper()
	var r createRows
	if err := e.admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.principals), (SELECT count(*) FROM auth.users),
	(SELECT count(*) FROM public.users), (SELECT count(*) FROM auth.security_audit_events),
	(SELECT count(*) FROM auth.platform_role_assignments)`).Scan(&r.principals, &r.authUsers, &r.publicUsers, &r.audits, &r.assignments); err != nil {
		t.Fatal(err)
	}
	return r
}

// runCreate runs `auth users-create` as the owned runtime login.
func (e *reconEnv) runCreate(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	owned := e.ownedRuntime(t)
	orig := connectFn
	connectFn = func(ctx context.Context, _ cli.Env, _ string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		pool, err := pgxpool.NewWithConfig(ctx, owned.Config().Copy())
		if err != nil {
			t.Fatal(err)
		}
		return pool, e.schema, cli.ExitOK
	}
	t.Cleanup(func() { connectFn = orig })
	var stdout, stderr bytes.Buffer
	code := authVerb(t, "users-create").Run(context.Background(), cli.Env{
		Args: args, Lookup: envLookup(env), Stdin: strings.NewReader(createPassword + "\n"), Stdout: &stdout, Stderr: &stderr,
	})
	if strings.Contains(stdout.String(), createPassword) || strings.Contains(stderr.String(), createPassword) {
		t.Fatal("the password reached the verb output")
	}
	return code, stdout.String(), stderr.String()
}

type noAttempts struct{}

func (noAttempts) Check(context.Context, []byte) (bool, time.Duration, error) { return false, 0, nil }
func (noAttempts) RecordFailure(context.Context, []byte) error                { return nil }
func (noAttempts) Clear(context.Context, []byte) error                        { return nil }

type fakeSessions struct{}

func (fakeSessions) Create(context.Context, uuid.UUID, *uuid.UUID, []string) (session.Issued, error) {
	return session.Issued{}, nil
}

type readOnlyUsers struct{ local.IdentityUsers }

func (readOnlyUsers) Project(context.Context, uuid.UUID) error { return nil }

func (e *reconEnv) loginKind(t *testing.T, ctx context.Context, email, password string) local.Kind {
	t.Helper()
	store, err := identity.New(identity.Config{Pool: e.ownedRuntime(t), Schema: e.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	svc, err := local.New(local.Config{Users: readOnlyUsers{local.IdentityUsers{Store: store}}, Sessions: fakeSessions{}, Attempts: noAttempts{},
		LockoutKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
	if err != nil {
		t.Fatal(err)
	}
	out, err := svc.Login(ctx, local.Credentials{Email: email, Password: password})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	return out.Kind
}

var ownedEnv = map[string]string{"AUTH_USERS_OWNED": "true", "AUTH_PLATFORM_ROLES_OWNED": "true"}

func TestUsersCreateMakesAUserWhoCanLogInThroughTheOwnedPath(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	email := createEmail("created")
	before := e.createRows(t, ctx)
	code, stdout, stderr := e.runCreate(t, ownedEnv, "-email", email, "-full-name", "Created User", "-password-stdin")
	if code != cli.ExitOK || !strings.HasPrefix(stdout, "user_created principal=") {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
	id, err := uuid.Parse(strings.TrimSpace(strings.TrimPrefix(stdout, "user_created principal=")))
	if err != nil {
		t.Fatalf("principal id: %v", err)
	}
	after := e.createRows(t, ctx)
	if after.principals != before.principals+1 || after.authUsers != before.authUsers+1 || after.publicUsers != before.publicUsers+1 || after.audits != before.audits+1 || after.assignments != before.assignments {
		t.Fatalf("rows before %+v after %+v: want one principal, one auth user, one public user, one audit row, no assignment", before, after)
	}
	var verified, publicVerified bool
	var provider string
	if err := e.admin.QueryRow(ctx, `SELECT u.email_verified_at IS NOT NULL, p.is_verified, p.auth_provider FROM auth.users u JOIN public.users p ON p.id = u.principal_id WHERE u.principal_id = $1`, id).
		Scan(&verified, &publicVerified, &provider); err != nil || !verified || !publicVerified || provider != "local" {
		t.Fatalf("verified %v public verified %v provider %q err %v", verified, publicVerified, provider, err)
	}
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.principal.provisioned'
		AND attributes->>'reason_code' = 'principal_provisioned' AND attributes->>'source' = 'operator-cli'`, id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("audit rows %d err %v", n, err)
	}
	// No organization and no membership (product rule 1).
	if err := e.admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM public.memberships WHERE user_id = $1) + (SELECT count(*) FROM auth.memberships WHERE principal_id = $1)`, id).Scan(&n); err != nil || n != 0 {
		t.Fatalf("memberships %d err %v", n, err)
	}
	if kind := e.loginKind(t, ctx, email, createPassword); kind != local.KindSuccess {
		t.Fatalf("login kind %q, want success", kind)
	}
	if kind := e.loginKind(t, ctx, email, createPassword+"x"); kind != local.KindInvalidCredentials {
		t.Fatalf("login with a wrong password: kind %q", kind)
	}
}

func TestUsersCreateRefusalsWriteNothingByRowCount(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	taken := createEmail("taken")
	if code, _, stderr := e.runCreate(t, ownedEnv, "-email", taken, "-password-stdin"); code != cli.ExitOK {
		t.Fatalf("first create: exit %d %s", code, stderr)
	}
	publicOnly := createEmail("publiconly")
	e.seed(t, ctx, publicOnly, true, true)

	cases := []struct {
		name string
		env  map[string]string
		args []string
		want int
		text string
	}{
		{"users not owned", map[string]string{"AUTH_USERS_OWNED": "false"}, []string{"-email", createEmail("a"), "-password-stdin"}, cli.ExitRefused, "AUTH_USERS_OWNED"},
		{"duplicate auth email", ownedEnv, []string{"-email", taken, "-password-stdin"}, cli.ExitRefused, "email exists"},
		{"an email only public.users holds (second write fails after the first two)", ownedEnv, []string{"-email", publicOnly, "-password-stdin"}, cli.ExitRefused, "email exists"},
		{"superuser, roles not owned", map[string]string{"AUTH_USERS_OWNED": "true"}, []string{"-email", createEmail("b"), "-superuser", "-password-stdin"}, cli.ExitRefused, "AUTH_PLATFORM_ROLES_OWNED"},
		{"superuser, no active admin", ownedEnv, []string{"-email", createEmail("c"), "-superuser", "-password-stdin"}, cli.ExitRefused, "auth-migrate platform-role-bootstrap"},
	}
	for _, c := range cases {
		before := e.createRows(t, ctx)
		code, stdout, stderr := e.runCreate(t, c.env, c.args...)
		after := e.createRows(t, ctx)
		if code != c.want || stdout != "" || !strings.Contains(stderr, c.text) {
			t.Errorf("%s: exit %d, stdout %q, stderr %q", c.name, code, stdout, stderr)
		}
		if after != before {
			t.Errorf("%s: rows before %+v after %+v", c.name, before, after)
		}
	}

	// With an active admin the refusal names platform-role-grant.
	e.seedSuperuser(t, ctx, "admin", true)
	// The backfill writes the assignment only while public is_superuser is the authority (Options{}): with the switch on it skips the role leg.
	if _, err := Run(ctx, PGSource{Pool: e.runtime}, e.store, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := e.createRows(t, ctx).assignments; got != 1 {
		t.Fatalf("the admin seed wrote %d platform_admin assignments, want 1", got)
	}
	before := e.createRows(t, ctx)
	code, stdout, stderr := e.runCreate(t, ownedEnv, "-email", createEmail("d"), "-superuser", "-password-stdin")
	after := e.createRows(t, ctx)
	if code != cli.ExitRefused || stdout != "" || !strings.Contains(stderr, "dho auth platform-role-grant") || after != before {
		t.Errorf("superuser with an admin: exit %d, stdout %q, stderr %q, rows before %+v after %+v", code, stdout, stderr, before, after)
	}

	// A weak password is refused before the database: no rows either.
	before = e.createRows(t, ctx)
	var stdoutW, stderrW bytes.Buffer
	codeW := authVerb(t, "users-create").Run(ctx, cli.Env{Args: []string{"-email", createEmail("e"), "-password-stdin"}, Lookup: envLookup(ownedEnv),
		Stdin: strings.NewReader("short1\n"), Stdout: &stdoutW, Stderr: &stderrW})
	if after = e.createRows(t, ctx); codeW != cli.ExitRefused || after != before {
		t.Errorf("weak password: exit %d, rows before %+v after %+v", codeW, before, after)
	}
}

// createEmail is a login address the shared email rule accepts: it refuses the special-use ".test" domain that uniq uses for seeded rows.
func createEmail(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8] + "@example.com"
}
