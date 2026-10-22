//go:build integration

package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// goUser seeds a go-api user at token_version tv and its auth principal.
func (e *env) goUser(t *testing.T, ctx context.Context, tv int) uuid.UUID {
	t.Helper()
	id := e.principal(t, ctx)
	tag := id.String()[:8]
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users (id, email, username, full_name, password_hash, auth_provider,
		is_active, is_verified, is_superuser, token_version, created_at, updated_at)
		VALUES ($1, $2, $3, 'Legacy User', NULL, 'local', true, true, false, $4, now(), now())`,
		id, "legacy-"+tag+"@example.test", "legacy-"+tag, tv); err != nil {
		t.Fatalf("seed go-api user: %v", err)
	}
	return id
}

// goRefresh seeds a go-api refresh_tokens row for jti, as go-api's
// create_refresh_token writes it.
func (e *env) goRefresh(t *testing.T, ctx context.Context, user uuid.UUID, family uuid.UUID, jti string, expiresIn time.Duration) {
	t.Helper()
	digest := sha256.Sum256([]byte(jti))
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
		VALUES (gen_random_uuid(), $1, NULL, $2, $3, now() + make_interval(secs => $4::double precision), now())`,
		user, hex.EncodeToString(digest[:]), family, expiresIn.Seconds()); err != nil {
		t.Fatalf("seed go-api refresh token: %v", err)
	}
}

func (e *env) goRevokedAt(t *testing.T, ctx context.Context, jti string) *time.Time {
	t.Helper()
	digest := sha256.Sum256([]byte(jti))
	var at *time.Time
	if err := e.admin.QueryRow(ctx, `SELECT revoked_at FROM public.refresh_tokens WHERE token_hash = $1`,
		hex.EncodeToString(digest[:])).Scan(&at); err != nil {
		t.Fatalf("read go-api refresh token: %v", err)
	}
	return at
}

func (e *env) ageGoRevocation(t *testing.T, ctx context.Context, jti string, by time.Duration) {
	t.Helper()
	digest := sha256.Sum256([]byte(jti))
	if _, err := e.admin.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = revoked_at - make_interval(secs => $2::double precision)
		WHERE token_hash = $1`, hex.EncodeToString(digest[:]), by.Seconds()); err != nil {
		t.Fatal(err)
	}
}

func legacy(user uuid.UUID, jti string) LegacyRefresh {
	return LegacyRefresh{PrincipalID: user, JTI: contracts.NewSecretString(jti)}
}

func TestCreateAndCreateAuditedPinTheUsersTokenVersion(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 5)
	plain := e.create(t, ctx, user)
	audited, err := e.store.CreateAudited(ctx, user, nil, []string{"pwd"}, signInEvent(user))
	if err != nil {
		t.Fatalf("CreateAudited: %v", err)
	}
	for name, issued := range map[string]Issued{"Create": plain, "CreateAudited": audited} {
		if issued.Session.LegacyTokenVersion == nil || *issued.Session.LegacyTokenVersion != 5 {
			t.Fatalf("%s pinned %v, want 5", name, issued.Session.LegacyTokenVersion)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND legacy_token_version = 5`, issued.Session.ID.String()); n != 1 {
			t.Fatalf("%s: the session row is not pinned at 5", name)
		}
	}
	// A principal go-api does not hold has no token to pin: 0, never NULL.
	other := e.principal(t, ctx)
	issued := e.create(t, ctx, other)
	if issued.Session.LegacyTokenVersion == nil || *issued.Session.LegacyTokenVersion != 0 {
		t.Fatalf("a principal without a go-api row pinned %v, want 0", issued.Session.LegacyTokenVersion)
	}
	current, err := e.store.Current(ctx, plain.Session.ID)
	if err != nil || current.LegacyTokenVersion == nil || *current.LegacyTokenVersion != 5 {
		t.Fatalf("Current reads pin %v (%v), want 5", current.LegacyTokenVersion, err)
	}
}

func TestExchangeLegacyRevokesTheGoAPITokenAndStartsAPinnedSession(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 3)
	e.goRefresh(t, ctx, user, uuid.New(), "jti-live", time.Hour)

	result, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-live"))
	if err != nil || result.Reason != ReasonAccepted {
		t.Fatalf("ExchangeLegacy = %s, %v; want accepted", result.Reason, err)
	}
	if e.goRevokedAt(t, ctx, "jti-live") == nil {
		t.Fatal("the go-api refresh token is still live")
	}
	record := result.Session
	if record.PrincipalID != user || record.LegacyTokenVersion == nil || *record.LegacyTokenVersion != 3 {
		t.Fatalf("session %+v, want the user's, pinned at 3", record)
	}
	digest := sha256.Sum256([]byte("jti-live"))
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND legacy_jti_hash = $2`,
		record.ID.String(), digest[:]); n != 1 {
		t.Fatal("the first credential does not record the exchanged jti")
	}
	if next := e.exchange(t, ctx, e.store, result.Credential); next.Reason != ReasonAccepted {
		t.Fatalf("the exchanged credential rotates as %s, want accepted", next.Reason)
	}
}

func TestExchangeLegacyAgainWithinTheGraceWindowAnswersTheSameCredential(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	e.goRefresh(t, ctx, user, uuid.New(), "jti-twice", time.Hour)
	first, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-twice"))
	if err != nil || first.Reason != ReasonAccepted {
		t.Fatalf("first exchange = %s, %v", first.Reason, err)
	}
	revokedAt := e.goRevokedAt(t, ctx, "jti-twice")
	second, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-twice"))
	if err != nil || second.Reason != ReasonGraceReplay {
		t.Fatalf("second exchange in the grace window = %s, %v; want refresh_grace_replay", second.Reason, err)
	}
	if second.Credential.Reveal() != first.Credential.Reveal() || second.Session.ID != first.Session.ID || !second.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatal("the replay did not answer the first exchange's credential, session and expiry")
	}
	if again := e.goRevokedAt(t, ctx, "jti-twice"); again == nil || !again.Equal(*revokedAt) {
		t.Fatal("the replay changed the go-api row's revocation")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user.String()); n != 1 {
		t.Fatalf("%d sessions, want 1", n)
	}
	current, err := e.store.Current(ctx, first.Session.ID)
	if err != nil || current.RevokedAt != nil {
		t.Fatalf("the exchanged session was revoked by a replay in the grace window (%v)", err)
	}
	if next := e.exchange(t, ctx, e.store, second.Credential); next.Reason != ReasonAccepted {
		t.Fatalf("the replayed credential rotates as %s, want accepted", next.Reason)
	}
}

func TestExchangeLegacyConcurrentlyCreatesOneSession(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	e.goRefresh(t, ctx, user, uuid.New(), "jti-race", time.Hour)
	const callers = 4
	results := make([]Result, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = e.store.ExchangeLegacy(ctx, legacy(user, "jti-race"))
		}()
	}
	wg.Wait()
	accepted := 0
	for i, result := range results {
		if errs[i] != nil || !result.Reason.Success() {
			t.Fatalf("caller %d = %s, %v; want a success", i, result.Reason, errs[i])
		}
		if result.Reason == ReasonAccepted {
			accepted++
		}
		if result.Credential.Reveal() != results[0].Credential.Reveal() {
			t.Fatalf("caller %d got another credential", i)
		}
	}
	if accepted != 1 {
		t.Fatalf("%d callers were accepted, want 1", accepted)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user.String()); n != 1 {
		t.Fatalf("%d sessions, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1`, results[0].Session.ID.String()); n != 1 {
		t.Fatalf("%d credentials, want 1", n)
	}
}

func TestExchangeLegacyReplayAfterANativeRotationRevokesTheFamily(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	family := uuid.New()
	e.goRefresh(t, ctx, user, family, "jti-rotated", time.Hour)
	e.goRefresh(t, ctx, user, family, "jti-rotated-sibling", time.Hour)
	first, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-rotated"))
	if err != nil || first.Reason != ReasonAccepted {
		t.Fatalf("first exchange = %s, %v", first.Reason, err)
	}
	if next := e.exchange(t, ctx, e.store, first.Credential); next.Reason != ReasonAccepted {
		t.Fatalf("native rotation = %s", next.Reason)
	}
	result, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-rotated"))
	if err != nil || result.Reason != ReasonRefreshReused || result.Credential.Reveal() != "" {
		t.Fatalf("replay after a rotation = %s, %v; want refresh_reused and no credential", result.Reason, err)
	}
	current, err := e.store.Current(ctx, first.Session.ID)
	if err != nil || current.RevokedAt == nil {
		t.Fatalf("the session is live after the family revoke (%v)", err)
	}
	if e.goRevokedAt(t, ctx, "jti-rotated-sibling") == nil {
		t.Fatal("the go-api family is still live")
	}
}

func TestExchangeLegacyReplayForADeactivatedPrincipalRevokesTheFamily(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	family := uuid.New()
	e.goRefresh(t, ctx, user, family, "jti-off", time.Hour)
	e.goRefresh(t, ctx, user, family, "jti-off-sibling", time.Hour)
	first, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-off"))
	if err != nil || first.Reason != ReasonAccepted {
		t.Fatalf("first exchange = %s, %v", first.Reason, err)
	}
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, user.String())
	result, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-off"))
	if err != nil || result.Reason != ReasonPrincipalDeactivated || result.Credential.Reveal() != "" {
		t.Fatalf("replay for a deactivated principal = %s, %v; want principal_deactivated and no credential", result.Reason, err)
	}
	if e.goRevokedAt(t, ctx, "jti-off-sibling") == nil {
		t.Fatal("the go-api family is still live")
	}
	current, err := e.store.Current(ctx, first.Session.ID)
	if err != nil || current.RevokedAt == nil {
		t.Fatalf("the session is live after the family revoke (%v)", err)
	}
}

func TestExchangeLegacyReplayWithAnotherSuccessorKeyAsksForARetry(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	e.goRefresh(t, ctx, user, uuid.New(), "jti-key", time.Hour)
	first, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-key"))
	if err != nil || first.Reason != ReasonAccepted {
		t.Fatalf("first exchange = %s, %v", first.Reason, err)
	}
	otherKey, _ := testKey(t)
	rotated := e.newStore(t, otherKey, Config{})
	result, err := rotated.ExchangeLegacy(ctx, legacy(user, "jti-key"))
	if !errors.Is(err, ErrGraceUnavailable) || result.Reason != ReasonDependency {
		t.Fatalf("replay under another key = %s, %v; want dependency and ErrGraceUnavailable", result.Reason, err)
	}
	current, err := e.store.Current(ctx, first.Session.ID)
	if err != nil || current.RevokedAt != nil {
		t.Fatalf("the retry answer revoked the session (%v)", err)
	}
	if again, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-key")); err != nil || again.Reason != ReasonGraceReplay {
		t.Fatalf("replay under the original key = %s, %v; want refresh_grace_replay", again.Reason, err)
	}
}

func TestExchangeLegacyReuseRevokesTheFamilyAndItsSessions(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	family := uuid.New()
	e.goRefresh(t, ctx, user, family, "jti-reused", time.Hour)
	e.goRefresh(t, ctx, user, family, "jti-sibling", time.Hour)
	unrelated := uuid.New()
	e.goRefresh(t, ctx, user, unrelated, "jti-unrelated", time.Hour)
	first, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-reused"))
	if err != nil || first.Reason != ReasonAccepted {
		t.Fatalf("first exchange = %s, %v", first.Reason, err)
	}
	e.ageGoRevocation(t, ctx, "jti-reused", GraceWindow+time.Second)

	result, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-reused"))
	if err != nil || result.Reason != ReasonRefreshReused {
		t.Fatalf("reuse = %s, %v; want refresh_reused", result.Reason, err)
	}
	current, err := e.store.Current(ctx, first.Session.ID)
	if err != nil || current.RevokedAt == nil {
		t.Fatalf("the session exchanged from the reused token is live (%v)", err)
	}
	if e.goRevokedAt(t, ctx, "jti-sibling") == nil {
		t.Fatal("the reused token's go-api family is still live")
	}
	if e.goRevokedAt(t, ctx, "jti-unrelated") != nil {
		t.Fatal("another go-api family was revoked")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = 'auth.session.refresh_reuse_revoked' AND subject_principal_id = $1`, user.String()); n != 1 {
		t.Fatalf("%d reuse audit rows, want 1", n)
	}
}

func TestExchangeLegacyReuseOfATokenGoAPIRevokedRevokesItsFamily(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	family := uuid.New()
	e.goRefresh(t, ctx, user, family, "jti-go-revoked", time.Hour)
	e.goRefresh(t, ctx, user, family, "jti-go-live", time.Hour)
	digest := sha256.Sum256([]byte("jti-go-revoked"))
	if _, err := e.admin.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = now() - interval '1 minute' WHERE token_hash = $1`,
		hex.EncodeToString(digest[:])); err != nil {
		t.Fatal(err)
	}
	result, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-go-revoked"))
	if err != nil || result.Reason != ReasonRefreshReused {
		t.Fatalf("reuse = %s, %v; want refresh_reused", result.Reason, err)
	}
	if e.goRevokedAt(t, ctx, "jti-go-live") == nil {
		t.Fatal("the family of a reused go-api token is still live")
	}
}

func TestExchangeLegacyRefusesWithoutChangingAnything(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 0)
	stranger := e.goUser(t, ctx, 0)
	e.goRefresh(t, ctx, user, uuid.New(), "jti-mine", time.Hour)
	e.goRefresh(t, ctx, user, uuid.New(), "jti-expired", -time.Minute)
	org := uuid.New()
	for name, tc := range map[string]struct {
		legacy LegacyRefresh
		jti    string
		want   Reason
	}{
		"unknown jti":          {legacy(user, "jti-nobody"), "", ReasonRefreshUnknown},
		"another subject":      {legacy(stranger, "jti-mine"), "jti-mine", ReasonRefreshUnknown},
		"another organization": {LegacyRefresh{PrincipalID: user, OrganizationID: &org, JTI: contracts.NewSecretString("jti-mine")}, "jti-mine", ReasonRefreshUnknown},
		"expired":              {legacy(user, "jti-expired"), "jti-expired", ReasonRefreshExpired},
	} {
		result, err := e.store.ExchangeLegacy(ctx, tc.legacy)
		if err != nil || result.Reason != tc.want {
			t.Fatalf("%s: %s, %v; want %s", name, result.Reason, err, tc.want)
		}
		if tc.jti != "" && e.goRevokedAt(t, ctx, tc.jti) != nil {
			t.Fatalf("%s: a refused exchange revoked the go-api token", name)
		}
	}
	e.guarded(t, ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, user.String())
	result, err := e.store.ExchangeLegacy(ctx, legacy(user, "jti-mine"))
	if err != nil || result.Reason != ReasonPrincipalDeactivated {
		t.Fatalf("disabled principal: %s, %v; want principal_deactivated", result.Reason, err)
	}
	if e.goRevokedAt(t, ctx, "jti-mine") != nil {
		t.Fatal("a refused exchange for a disabled principal revoked the go-api token")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user.String()); n != 0 {
		t.Fatalf("%d sessions after only refusals, want 0", n)
	}
}

// A session row without a pin (older than migration 0016) is refused under its
// lock before anything is written: its credential is neither consumed nor
// revoked, and the session stays as it was.
func TestExchangeRefusesAPinlessSessionWithoutRotating(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	credential, err := newCredential()
	if err != nil {
		t.Fatal(err)
	}
	sessionID := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, amr)
		VALUES ($1, $2, $3, 'infinity', '{pwd}')`, sessionID, principal, hashCredential(contracts.NewSecretString("pinless-handle"))); err != nil {
		t.Fatalf("seed a pinless session: %v", err)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
		VALUES ($1, $2, $3, now() + interval '1 hour')`, sessionID, principal, hashCredential(credential)); err != nil {
		t.Fatalf("seed its credential: %v", err)
	}

	result, err := e.store.Exchange(ctx, credential)
	if err != nil || result.Reason != ReasonRevisionStale {
		t.Fatalf("Exchange = %s, %v; want revision_stale", result.Reason, err)
	}
	if result.Credential.Reveal() != "" {
		t.Fatal("a refused exchange returned a credential")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials
		WHERE session_id = $1 AND consumed_at IS NULL AND revoked_at IS NULL`, sessionID.String()); n != 1 {
		t.Fatalf("%d untouched credentials after the refusal, want the one", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1`, sessionID.String()); n != 1 {
		t.Fatalf("%d credentials after the refusal, want no successor", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NULL AND last_seen_at IS NULL`, sessionID.String()); n != 1 {
		t.Fatal("the refusal changed the session row")
	}
	again, err := e.store.Exchange(ctx, credential)
	if err != nil || again.Reason != ReasonRevisionStale {
		t.Fatalf("the same credential again = %s, %v; want revision_stale, not reuse", again.Reason, err)
	}
}

// A go-api refresh token whose row go-api revoked (a password reset or change,
// an admin password set or a soft delete keep revoking them while
// auth-service owns sessions) is never exchanged, at any distance from the
// revocation.
func TestExchangeLegacyRefusesATokenGoAPIRevoked(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 1)
	for name, age := range map[string]time.Duration{"just revoked": 0, "revoked earlier": GraceWindow + time.Minute} {
		jti := "jti-revoked-" + strings.ReplaceAll(name, " ", "-")
		e.goRefresh(t, ctx, user, uuid.New(), jti, time.Hour)
		digest := sha256.Sum256([]byte(jti))
		if _, err := e.admin.Exec(ctx, `UPDATE public.refresh_tokens SET revoked_at = now() - make_interval(secs => $2::double precision)
			WHERE token_hash = $1`, hex.EncodeToString(digest[:]), age.Seconds()); err != nil {
			t.Fatal(err)
		}
		result, err := e.store.ExchangeLegacy(ctx, legacy(user, jti))
		if err != nil || result.Reason.Success() || result.Credential.Reveal() != "" {
			t.Fatalf("%s: exchange = %s (%v), want a refusal and no credential", name, result.Reason, err)
		}
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user.String()); n != 0 {
		t.Fatalf("%d sessions from revoked go-api tokens, want 0", n)
	}
}
