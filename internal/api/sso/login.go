package sso

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ssoUnauthenticated is D2738 (r1 P1 on #3347/CHAOS-6658: an unauthenticated
// invalid callback disabling a provider org-wide). It marks a failure that
// occurred BEFORE the caller proved anything -- OIDC's opaque state failing
// to authenticate (state.go's AEAD tag, expiry, or provider/org binding),
// or SAML's assertion failing to verify against the configured IdP
// certificate (saml.go's ParseXMLResponse, plus the payload-shape checks
// that precede it: missing/non-base64 SAMLResponse, both attacker-choosable
// with zero credentials) -- as distinct from ssoProcessing, whose failures
// all occur AFTER that proof and legitimately indicate a broken IdP
// configuration worth surfacing to an admin. See recordSSOUnauthenticated.
//
// Provider-CONFIG-derived failures that fire on every request regardless
// of caller input (SAML: missing/invalid stored certificate, an
// unparseable ACS URL built from the provider's own row) are deliberately
// NOT in this bucket -- an attacker cannot choose to trigger those, they
// are already broken for every caller including a legitimate IdP, so
// recording them is informative rather than an attacker-controlled DoS.
type ssoUnauthenticated struct{ msg string }

func (e ssoUnauthenticated) Error() string { return e.msg }
func ssoAuthErr(msg string) error          { return ssoUnauthenticated{msg: msg} }

func asSSOUnauthenticated(err error, target *ssoUnauthenticated) bool {
	if unauth, ok := err.(ssoUnauthenticated); ok {
		*target = unauth
		return true
	}
	return false
}

// ssoProcessing is SSOProcessingError (services/sso.py:37): the base
// class both OIDCProcessingError and SAMLProcessingError extend with no
// added behavior of their own, so this port uses ONE Go error type across
// provision.go (shared by both protocols) and each protocol's own
// exchange/validation path (oidc.go, saml.go) rather than two parallel,
// near-identical types. Its message is recorded onto the provider
// (record_error) and into an audit row, never surfaced verbatim to the
// caller -- the router always answers its own fixed per-branch message.
type ssoProcessing struct{ msg string }

func (e ssoProcessing) Error() string { return e.msg }
func ssoErr(msg string) error         { return ssoProcessing{msg: msg} }

func asSSOProcessing(err error, target *ssoProcessing) bool {
	if processing, ok := err.(ssoProcessing); ok {
		*target = processing
		return true
	}
	return false
}

// finishSSOLogin is the tail every successful callback (OIDC's
// oidc_callback, SAML's saml_acs_callback) shares once claims have
// resolved to a real user: record_login (sso_providers.last_login_at), an
// SSO_LOGIN audit row (status success, resource SESSION), then
// AuthService.create_token_pair's own SSOLoginResponse -- the same
// token-minting code every login path uses (see the doc comment on the
// refresh-token gap below), not a protocol-specific mechanism.
func (h handlers) finishSSOLogin(ctx context.Context, w http.ResponseWriter, r *http.Request,
	providerID uuid.UUID, orgID string, user *provisionedUser, membership *provisionedMembership, protocol string) {
	role := "member"
	loginOrgID := ""
	var orgUUID *uuid.UUID
	if membership != nil {
		role = membership.role
		loginOrgID = membership.orgID.String()
		id := membership.orgID
		orgUUID = &id
	}
	now := h.Now()
	jti, err := newJTI()
	if err != nil {
		h.fail(w, r, "mint token", err)
		return
	}
	access, err := h.Signer.Access(edgetoken.AccessClaims{
		UserID: user.id.String(), Email: user.email, OrgID: loginOrgID, Role: role, IsSuperuser: user.isSuperuser,
		Username: user.username, FullName: user.fullName, TokenVersion: user.tokenVersion,
	}, now, jti)
	if err != nil {
		h.fail(w, r, "mint access token", err)
		return
	}
	// r1 review (CHAOS-6658, P1): the earlier version of this function
	// minted a stateless refresh JWT and never inserted a refresh_tokens
	// row, faithfully matching SSOService's own AuthService.
	// create_token_pair (services/auth.py:282-283) -- but /auth/refresh
	// (api/auth/routers/refresh.py:158, ported here as refreshByHash)
	// does a real DB lookup by the token's own jti on BOTH planes, and
	// 401s when no row exists. Python's version of both SSO callback
	// routes was never actually reachable (the pre-CHAOS-6658 dead-state
	// finding), so this Python bug never had a live consequence; once this
	// Go port fixed the state bug and made the routes real, the SAME
	// stateless mint would have shipped a login whose OWN refresh_token
	// 401s on first use -- a real, executed, reproduced defect (r1's
	// second P1 on #3347), not a parity target worth preserving. Fixed
	// here (shared by both OIDC and SAML, since both call this function)
	// by storing the refresh token the same way the password-login route
	// does (session/tokens.go storeRefresh), inside the same transaction
	// as the audit/login-time writes.
	family := uuid.New()
	refreshJTI, err := newJTI()
	if err != nil {
		h.fail(w, r, "mint token", err)
		return
	}
	refreshExpiresAt := now.Add(7 * 24 * time.Hour)
	refresh, err := h.Signer.RefreshUntil(edgetoken.RefreshClaims{UserID: user.id.String(), OrgID: loginOrgID, FamilyID: family.String()},
		now, refreshExpiresAt, refreshJTI)
	if err != nil {
		h.fail(w, r, "mint refresh token", err)
		return
	}

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `UPDATE sso_providers SET last_login_at = $2, updated_at = $2 WHERE id = $1::uuid`,
		providerID, now.UTC()); err != nil {
		h.fail(w, r, "record login", err)
		return
	}
	if _, err := tx.Exec(ctx, `INSERT INTO refresh_tokens
	(id, user_id, org_id, token_hash, family_id, expires_at, revoked_at, replaced_by_hash, successor_jti,
	 ip_address, user_agent, created_at)
VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, NULL, $7, $8, $9)`,
		uuid.New(), user.id, orgUUID, hashRefreshJTI(refreshJTI), family, refreshExpiresAt.UTC(),
		clientHost(r), userAgentHeader(r), now.UTC()); err != nil {
		h.fail(w, r, "store refresh token", err)
		return
	}
	meta := pyjson.NewObject()
	meta.Set("provider_id", providerID.String())
	meta.Set("protocol", protocol)
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		h.fail(w, r, "encode audit metadata", err)
		return
	}
	if _, err := (audit.PGWriter{Now: h.Now}).Write(ctx, tx, audit.Entry{
		OrgID: mustParseUUID(orgID), UserID: uuidPtr(user.id), Action: audit.ActionSSOLogin,
		ResourceType: audit.ResourceSession, ResourceID: user.id.String(), RequestMetadata: []byte(metaJSON),
	}); err != nil {
		h.fail(w, r, "write audit log", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}

	out := pyjson.NewObject()
	out.Set("access_token", access)
	out.Set("refresh_token", refresh)
	out.Set("token_type", "bearer")
	out.Set("expires_in", pyjson.IntOf(int64(edgetoken.AccessLifetime/time.Second)))
	out.Set("user_id", user.id.String())
	out.Set("email", user.email)
	out.Set("org_id", loginOrgID)
	out.Set("role", role)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// recordSSOFailure is process_oidc_callback's / process_saml_response's /
// provision_or_get_user's `except SSOProcessingError` block, shared across
// both protocols: record_error onto the provider (last_error, status
// "error"), an SSO_LOGIN audit row (status failure, resource
// SSO_PROVIDER), commit, then the response. protocol is "oidc" or "saml";
// stage is "" for an exchange/validation failure or "provisioning" for a
// provision_or_get_user failure, matching extra_metadata's own shape.
func (h handlers) recordSSOFailure(ctx context.Context, w http.ResponseWriter, r *http.Request,
	orgID string, providerID uuid.UUID, errMsg string, status int, detail, protocol, stage string) {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin failure record", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	sanitized := pythonparity.SanitizeErrorText(errMsg, 4000)
	now := h.Now()
	if _, err := tx.Exec(ctx, `UPDATE sso_providers SET last_error = $2, last_error_at = $3, status = 'error', updated_at = $3
WHERE id = $1::uuid`, providerID, sanitized, now.UTC()); err != nil {
		h.fail(w, r, "record provider error", err)
		return
	}
	meta := pyjson.NewObject()
	meta.Set("protocol", protocol)
	if stage != "" {
		meta.Set("stage", stage)
	}
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		h.fail(w, r, "encode audit metadata", err)
		return
	}
	if _, err := (audit.PGWriter{Now: h.Now}).Write(ctx, tx, audit.Entry{
		OrgID: mustParseUUID(orgID), Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider,
		ResourceID: providerID.String(), Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
	}); err != nil {
		h.fail(w, r, "write audit log", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	policy.WriteDetail(w, status, detail, nil)
}

// recordSSOUnauthenticated is D2738's ruling on the r1 P1 (an
// unauthenticated invalid callback disabling the provider org-wide): a
// caller who has NOT proven possession of a credential this package trusts
// for this exact provider (OIDC: a state token it minted; SAML: an
// assertion signed by the configured IdP certificate) writes nothing to
// sso_providers -- no status flip, no last_error -- because at this point
// the only fact in evidence is "some request arrived with input that
// doesn't verify," which anyone can produce with zero credentials by
// hitting this public route with garbage. A single such request must
// never be able to take the whole org's SSO login offline.
//
// An audit row is still written (so a defender investigating repeated
// probing has a trail) and a log line is emitted; the HTTP response is the
// identical generic detail an authenticated-but-later-failed exchange
// gets (recordSSOFailure's own detail string, passed through unchanged),
// so a probe cannot distinguish the two cases by response shape.
//
// Deferred, not in this change (team-lead's D2738 ruling, RISK-NOTES): real
// rate limiting of this route, and a dedicated metrics counter for this
// event. Neither has an existing primitive in this codebase to build on;
// adding one here would be new infrastructure beyond the ruling's actual
// ask, which is the persistence behavior, not the route's throughput
// controls.
func (h handlers) recordSSOUnauthenticated(ctx context.Context, w http.ResponseWriter, r *http.Request,
	orgID string, providerID uuid.UUID, errMsg string, status int, detail, protocol, stage string) {
	sanitized := pythonparity.SanitizeErrorText(errMsg, 4000)
	h.Logger.WarnContext(ctx, "api sso: callback presented unauthenticated input; provider status left unchanged",
		"provider_id", providerID.String(), "protocol", protocol, "reason", sanitized)
	meta := pyjson.NewObject()
	meta.Set("protocol", protocol)
	if stage != "" {
		meta.Set("stage", stage)
	}
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		h.fail(w, r, "encode audit metadata", err)
		return
	}
	if _, err := (audit.PGWriter{Now: h.Now}).Write(ctx, h.Pool, audit.Entry{
		OrgID: mustParseUUID(orgID), Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider,
		ResourceID: providerID.String(), Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
	}); err != nil {
		h.fail(w, r, "write audit log", err)
		return
	}
	policy.WriteDetail(w, status, detail, nil)
}
