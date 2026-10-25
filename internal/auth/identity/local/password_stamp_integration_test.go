//go:build integration

package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func cheapHash(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	return strings.Replace(string(h), "$2a$", "$2b$", 1), err
}

type endRig struct {
	reader   *sdkmetric.ManualReader
	env      *pgEnv
	links    *PGLinks
	sessions *session.Store
	accounts IdentityAccounts
	flows    *Flows
}

func newEndRig(t *testing.T, ctx context.Context) *endRig {
	t.Helper()
	env, links := linkEnv(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := identity.New(identity.Config{Pool: env.runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	sessions, err := session.New(session.Config{Pool: env.runtime, Schema: schema,
		Meter:        sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"),
		SuccessorKey: contracts.NewSecretString("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")})
	if err != nil {
		t.Fatal(err)
	}
	rig := &endRig{reader: reader, env: env, links: links, sessions: sessions, accounts: IdentityAccounts{Store: store, Sessions: sessions, Links: links}}
	rig.flows, err = NewFlows(FlowsConfig{Accounts: rig.accounts, Links: links, Sessions: sessions, Mail: &liveMail{},
		Locker: links, UsersOwnedByAuth: true, Hash: cheapHash})
	if err != nil {
		t.Fatal(err)
	}
	return rig
}

// account makes the auth user and go-api's users and refresh_tokens rows of
// one principal.
func (r *endRig) account(t *testing.T, ctx context.Context, password string) uuid.UUID {
	t.Helper()
	hash, err := cheapHash(password)
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.accounts.ProvisionLocal(ctx, ProvisionLocalRequest{Email: uuid.NewString() + "@example.com", PasswordHash: hash})
	if err != nil {
		t.Fatalf("ProvisionLocal: %v", err)
	}
	r.goAPIRows(t, ctx, id)
	return id
}

// legacyJTI is the jti of the go-api refresh token seeded for a principal.
const legacyJTI = "go-api-refresh-jti"

// goAPIRows sets the principal's public.users token_version to 3 and one live refresh
// token whose digest is that of legacyJTI.
func (r *endRig) goAPIRows(t *testing.T, ctx context.Context, id uuid.UUID) {
	t.Helper()
	if _, err := r.env.admin.Exec(ctx, `UPDATE public.users SET token_version = 3 WHERE id = $1`, id); err != nil {
		t.Fatalf("seed go-api token version: %v", err)
	}
	digest := sha256.Sum256([]byte(legacyJTI))
	if _, err := r.env.admin.Exec(ctx, `INSERT INTO public.refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
		VALUES (gen_random_uuid(), $1, NULL, $2, gen_random_uuid(), now() + interval '1 day', now())`, id, hex.EncodeToString(digest[:])); err != nil {
		t.Fatalf("seed go-api refresh token: %v", err)
	}
}

// revocations is the sum of auth.session.revocations so far.
func (r *endRig) revocations(t *testing.T, ctx context.Context) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if data, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == "auth.session.revocations" {
				for _, p := range data.DataPoints {
					sum += p.Value
				}
			}
		}
	}
	return sum
}

func (r *endRig) live(t *testing.T, ctx context.Context, issued session.Issued) bool {
	t.Helper()
	record, err := r.sessions.Current(ctx, issued.Session.ID)
	if err != nil {
		t.Fatal(err)
	}
	return record.RevokedAt == nil
}

func (r *endRig) tokenVersion(t *testing.T, ctx context.Context, id uuid.UUID) int {
	t.Helper()
	return r.env.count(t, ctx, `SELECT token_version FROM public.users WHERE id = $1`, id)
}

func (r *endRig) liveLegacyRows(t *testing.T, ctx context.Context, id uuid.UUID) int {
	t.Helper()
	return r.env.count(t, ctx, `SELECT count(*) FROM public.refresh_tokens WHERE user_id = $1 AND revoked_at IS NULL`, id)
}

func (r *endRig) passwordHash(t *testing.T, ctx context.Context, id uuid.UUID) string {
	t.Helper()
	var hash string
	if err := r.env.admin.QueryRow(ctx, `SELECT password_hash FROM auth.users WHERE principal_id = $1`, id).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	return hash
}

// After a reset every credential class of the principal is ended in the same
// transaction: the auth session (physical revoked_at, what policy's
// SessionState reads), the native refresh credential, go-api's token_version
// (which ends access tokens without a sid) and the go-api refresh rows.
func TestResetEndsEveryCredentialClass(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	old, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: linkHash("reset-secret"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	before := rig.revocations(t, ctx)
	out, err := rig.flows.Reset(ctx, "reset-secret", "A-new-pass-77")
	if err != nil || out.Kind != FlowOK {
		t.Fatalf("Reset = %+v, %v", out, err)
	}
	if got := rig.revocations(t, ctx); got != before+1 {
		t.Errorf("auth.session.revocations = %d after the reset, want %d", got, before+1)
	}
	if rig.live(t, ctx, old) {
		t.Error("the pre-reset session is live")
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NOT NULL`, old.Session.ID); n != 1 {
		t.Errorf("the pre-reset session is not physically revoked (%d rows)", n)
	}
	if res, err := rig.sessions.Exchange(ctx, old.Credential); err != nil || res.Reason != session.ReasonSessionRevoked {
		t.Errorf("Exchange of the old credential = %+v, %v; want %s", res, err, session.ReasonSessionRevoked)
	}
	if v := rig.tokenVersion(t, ctx, id); v != 4 {
		t.Errorf("go-api token_version = %d, want 4", v)
	}
	if n := rig.liveLegacyRows(t, ctx, id); n != 0 {
		t.Errorf("%d go-api refresh rows are still live", n)
	}
	state, found, err := policy.PGStore{Pool: rig.env.runtime, AuthSchema: "auth"}.SessionState(ctx, old.Session.ID)
	if err != nil || !found || !state.Revoked {
		t.Errorf("policy SessionState of the old session = %+v found %v err %v; want revoked", state, found, err)
	}
	if res, err := rig.sessions.ExchangeLegacy(ctx, session.LegacyRefresh{PrincipalID: id, JTI: contracts.NewSecretString(legacyJTI)}); err != nil || res.Reason.Success() {
		t.Errorf("ExchangeLegacy after the reset = %+v, %v; want a refusal", res, err)
	}
	time.Sleep(20 * time.Millisecond)
	after, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
	if err != nil || !rig.live(t, ctx, after) {
		t.Errorf("a session created after the reset is not live: %v", err)
	}
}

// A change ends the same classes, keeps the caller's own new session and a
// later second device live.
func TestChangeEndsEveryCredentialClassAndKeepsTheCallersNewSession(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	older, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	before := rig.revocations(t, ctx)
	out, err := rig.flows.Change(ctx, ChangeRequest{PrincipalID: id, Current: "Old-pass-12345", New: "Brand-new-pass-42"})
	if err != nil || out.Kind != FlowOK || out.Issued == nil {
		t.Fatalf("Change = %+v, %v", out, err)
	}
	if got := rig.revocations(t, ctx); got != before+1 {
		t.Errorf("auth.session.revocations = %d after the change, want %d", got, before+1)
	}
	if rig.live(t, ctx, older) {
		t.Error("the older device's session is live after the change")
	}
	if !rig.live(t, ctx, *out.Issued) {
		t.Error("the session the change issued to the caller is void")
	}
	if v := rig.tokenVersion(t, ctx, id); v != 4 {
		t.Errorf("go-api token_version = %d, want 4", v)
	}
	if n := rig.liveLegacyRows(t, ctx, id); n != 0 {
		t.Errorf("%d go-api refresh rows are still live", n)
	}
}

// When any step of the transaction fails the password is unchanged, the
// sessions stay live and go-api's rows are untouched.
func TestAFailedStepRollsTheWholePasswordChangeBack(t *testing.T) {
	steps := map[string]string{
		"the refresh token revoke": `REVOKE UPDATE (revoked_at) ON public.refresh_tokens FROM %s`,
	}
	for name, revoke := range steps {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			rig := newEndRig(t, ctx)
			id := rig.account(t, ctx, "Old-pass-12345")
			old, err := rig.sessions.Create(ctx, id, nil, []string{"pwd"})
			if err != nil {
				t.Fatal(err)
			}
			before := rig.passwordHash(t, ctx, id)
			revBefore := rig.revocations(t, ctx)
			var role string
			if err := rig.env.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
				t.Fatal(err)
			}
			if _, err := rig.env.admin.Exec(ctx, fmt.Sprintf(revoke, `"`+role+`"`)); err != nil {
				t.Fatal(err)
			}
			if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: linkHash("reset-secret"), TTL: ResetTTL}); err != nil {
				t.Fatal(err)
			}
			if _, err := rig.flows.Reset(ctx, "reset-secret", "A-new-pass-77"); err == nil {
				t.Fatal("Reset succeeded although a step failed")
			}
			if _, err := rig.flows.Change(ctx, ChangeRequest{PrincipalID: id, Current: "Old-pass-12345", New: "Brand-new-pass-42"}); err == nil {
				t.Fatal("Change succeeded although a step failed")
			}
			if got := rig.passwordHash(t, ctx, id); got != before {
				t.Error("the password changed although the transaction failed")
			}
			if got := rig.revocations(t, ctx); got != revBefore {
				t.Errorf("auth.session.revocations moved %d -> %d although the transaction failed", revBefore, got)
			}
			if !rig.live(t, ctx, old) {
				t.Error("the session was revoked although the transaction failed")
			}
			if v := rig.tokenVersion(t, ctx, id); v != 3 {
				t.Errorf("go-api token_version = %d, want 3 (unchanged)", v)
			}
			if _, ok, err := rig.accounts.PeekResetLink(ctx, linkHash("reset-secret")); err != nil || !ok {
				t.Errorf("the failed write burned the reset link: ok %v, err %v", ok, err)
			}
			if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, id); n != 1 {
				t.Errorf("%d sessions exist, want only the 1 seeded (a failed change must not leave its new session)", n)
			}
			if n := rig.liveLegacyRows(t, ctx, id); n != 1 {
				t.Errorf("%d go-api refresh rows are live, want the 1 seeded", n)
			}
		})
	}
}

// A reset link issued before a password change is unknown after it, for a
// change and for a reset by another link; a verification link stays.
func TestAPasswordChangeEndsTheEarlierResetLinksOnly(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: linkHash("earlier-reset"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: id, Purpose: PurposeEmailVerification, TokenHash: linkHash("verify"), TTL: VerificationTTL}); err != nil {
		t.Fatal(err)
	}
	if out, err := rig.flows.Change(ctx, ChangeRequest{PrincipalID: id, Current: "Old-pass-12345", New: "Brand-new-pass-42"}); err != nil || out.Kind != FlowOK {
		t.Fatalf("Change = %+v, %v", out, err)
	}
	out, err := rig.flows.Reset(ctx, "earlier-reset", "A-new-pass-77")
	if err != nil || out.Kind != FlowInvalidToken {
		t.Errorf("a reset link issued before the change = %+v, %v; want it refused", out, err)
	}
	if out, err := rig.flows.Verify(ctx, "verify"); err != nil || out.Kind != FlowOK {
		t.Errorf("the verification link = %+v, %v; want it to stay valid", out, err)
	}
}

// accepts is go-api's live access-token check (policy.Authenticator.CheckClaims)
// for an access token with no sid claim and token version tv.
func (r *endRig) accepts(t *testing.T, ctx context.Context, id uuid.UUID, tv int) bool {
	t.Helper()
	verifier, err := edgetoken.New(strings.Repeat("k", 40), "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: r.env.admin, AuthSchema: "auth"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = auth.CheckClaims(ctx, jwt.MapClaims{"sub": id.String(), "type": "access", "tv": float64(tv)})
	return err == nil
}

// A go-api access token minted before a reset or a change carries no sid, so
// only the token version ends it; after either it is refused, a token at the
// new version is accepted.
func TestANoSidGoAPIAccessTokenIsRefusedAfterResetAndChange(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)

	reset := rig.account(t, ctx, "Old-pass-12345")
	if !rig.accepts(t, ctx, reset, 3) {
		t.Fatal("the token is refused before the reset")
	}
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: reset, Purpose: PurposePasswordReset, TokenHash: linkHash("reset-secret"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if out, err := rig.flows.Reset(ctx, "reset-secret", "A-new-pass-77"); err != nil || out.Kind != FlowOK {
		t.Fatalf("Reset = %+v, %v", out, err)
	}
	if rig.accepts(t, ctx, reset, 3) {
		t.Error("the pre-reset no-sid access token is accepted after the reset")
	}
	if !rig.accepts(t, ctx, reset, 4) {
		t.Error("a token at the new version is refused after the reset")
	}

	rig2 := newEndRig(t, ctx)
	change := rig2.account(t, ctx, "Old-pass-12345")
	if !rig2.accepts(t, ctx, change, 3) {
		t.Fatal("the token is refused before the change")
	}
	if out, err := rig2.flows.Change(ctx, ChangeRequest{PrincipalID: change, Current: "Old-pass-12345", New: "Brand-new-pass-42"}); err != nil || out.Kind != FlowOK {
		t.Fatalf("Change = %+v, %v", out, err)
	}
	if rig2.accepts(t, ctx, change, 3) {
		t.Error("the pre-change no-sid access token is accepted after the change")
	}
	if !rig2.accepts(t, ctx, change, 4) {
		t.Error("a token at the new version is refused after the change")
	}
}

// A reset that read its link, then lost the race to a change of the same
// account, cannot overwrite the change: the link is consumed inside the
// password transaction, after the user lock, and the change deleted it.
func TestAnOlderResetCannotOverwriteAChangeMadeBetweenItsReadAndItsWrite(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	digest := linkHash("reset-secret")
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: digest, TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := rig.accounts.PeekResetLink(ctx, digest); err != nil || !ok {
		t.Fatalf("PeekResetLink = %v, %v", ok, err)
	}
	if out, err := rig.flows.Change(ctx, ChangeRequest{PrincipalID: id, Current: "Old-pass-12345", New: "Brand-new-pass-42"}); err != nil || out.Kind != FlowOK {
		t.Fatalf("Change = %+v, %v", out, err)
	}
	afterChange := rig.passwordHash(t, ctx, id)
	hash, err := cheapHash("Older-reset-pass-77")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.accounts.SetPasswordEndingSessions(ctx, PasswordEnd{ID: id, Hash: hash, ResetLink: &digest}); !errors.Is(err, ErrResetLinkUnusable) {
		t.Fatalf("the older reset's write = %v, want ErrResetLinkUnusable", err)
	}
	if got := rig.passwordHash(t, ctx, id); got != afterChange {
		t.Error("the older reset overwrote the change")
	}
	if v := rig.tokenVersion(t, ctx, id); v != 4 {
		t.Errorf("go-api token_version = %d, want 4 (only the change raised it)", v)
	}
}

// A reset whose link was used leaves it consumed with the new password, and a
// second use is the invalid-token answer.
func TestAResetConsumesItsLinkInTheSameWriteAsThePassword(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	id := rig.account(t, ctx, "Old-pass-12345")
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: linkHash("reset-secret"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if out, err := rig.flows.Reset(ctx, "reset-secret", "A-new-pass-77"); err != nil || out.Kind != FlowOK {
		t.Fatalf("Reset = %+v, %v", out, err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND purpose = 'password_reset' AND consumed_at IS NOT NULL`, id); n != 1 {
		t.Errorf("%d consumed reset links, want 1", n)
	}
	if out, err := rig.flows.Reset(ctx, "reset-secret", "Another-pass-88"); err != nil || out.Kind != FlowInvalidToken {
		t.Errorf("replay = %+v, %v; want the invalid-token answer", out, err)
	}
}
