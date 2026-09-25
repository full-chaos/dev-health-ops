// PagerDuty OAuth callback (pagerduty.py's complete_pagerduty_authorization).
// Part of CHAOS-6595: it consumes the one-time state authorize stored,
// exchanges the code with PagerDuty, proves the account with a live read,
// and persists the encrypted OAuth binding, the credential descriptor and a
// durable revocation for any grant it replaces.
package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func (h *handlers) pagerDutyCallbackRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: pagerDutyPrefix + "/callback", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.completePagerDutyAuthorization))},
	}
}

// pagerDutyOAuthRegions is _PAGERDUTY_REGIONS: the order the account is
// looked up in.
var pagerDutyOAuthRegions = []string{"us", "eu"}

// storedPagerDutyTokens is OAuthTokens.model_dump_json() as the Go readers
// (providerfoundation.PagerDutyOAuthHydrator) and Python's model_validate_json
// both read it.
type storedPagerDutyTokens struct {
	AccessToken   string    `json:"access_token"`
	RefreshToken  *string   `json:"refresh_token"`
	ExpiresAt     time.Time `json:"expires_at"`
	GrantedScopes []string  `json:"granted_scopes"`
}

func (h *handlers) completePagerDutyAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	var state string
	var code, callbackError *string
	object, ok := errs.Object(body)
	if ok {
		if value, present := errs.RequiredString(object, "state", 1, 0); present {
			state = value
		}
		if value, present := errs.OptionalString(object, "code", 0, 0); present {
			code = &value
		}
		if value, present := errs.OptionalString(object, "error", 0, 0); present {
			callbackError = &value
		}
		errs.ForbidExtra(object, "state", "code", "error")
	}
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}

	// A revoke PagerDuty refused for an earlier setup is retried before this
	// one starts; it never changes this request's answer.
	h.drainPagerDutySetupRevocations(ctx, orgID)

	verifier, found, err := h.consumePagerDutyAuthorization(ctx, orgID, state)
	if err != nil {
		h.internalError(ctx, w, "consume pagerduty authorization request", err)
		return
	}
	if !found {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid or expired PagerDuty OAuth state", nil)
		return
	}

	// validate_callback: an error wins over a missing code.
	if callbackError != nil && *callbackError != "" {
		policy.WriteDetail(w, http.StatusBadRequest, "PagerDuty OAuth callback returned an error: "+*callbackError, nil)
		return
	}
	authorizationCode := ""
	if code != nil {
		authorizationCode = *code
	}
	if authorizationCode == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "PagerDuty OAuth callback code is required", nil)
		return
	}
	if h.pagerDuty.ClientID == "" {
		policy.WriteDetail(w, http.StatusInternalServerError, "PagerDuty OAuth configuration is unavailable", nil)
		return
	}

	tokens, err := providerfoundation.ExchangePagerDutyAuthorizationCode(ctx, h.upstreamDoer, h.pagerDuty, authorizationCode, verifier, h.store.now().UTC())
	switch {
	case errors.Is(err, providerfoundation.ErrPagerDutyExchangeRejected):
		policy.WriteDetail(w, http.StatusBadRequest, "PagerDuty OAuth authorization code was rejected", nil)
		return
	case errors.Is(err, providerfoundation.ErrPagerDutyExchangeBadStatus):
		policy.WriteDetail(w, http.StatusBadGateway, "PagerDuty OAuth service is unavailable", nil)
		return
	case errors.Is(err, providerfoundation.ErrPagerDutyExchangeUnavailable):
		policy.WriteDetail(w, http.StatusServiceUnavailable, "PagerDuty OAuth service is unavailable", nil)
		return
	case err != nil:
		h.internalError(ctx, w, "exchange pagerduty authorization code", err)
		return
	}

	// Until the grant is stored or revoked, the token is on record: a crash or
	// a refused revoke below must not leave it live and untracked. If it
	// cannot be recorded it is revoked at once and the setup fails loudly.
	setupID, err := h.enqueuePagerDutySetupRevocation(ctx, orgID, tokens)
	if err != nil {
		// Named limit: with the record store itself failing there is nowhere
		// to keep a refused revoke, so the token can stay live at PagerDuty
		// (Python drops it the same way). Say so loudly.
		if revokeErr := providerfoundation.RevokePagerDutyOAuthToken(ctx, h.httpDoer, h.pagerDuty, tokens.RevocationToken()); revokeErr != nil {
			kind := "access_token"
			if tokens.RefreshToken != "" {
				kind = "refresh_token"
			}
			h.logger.ErrorContext(ctx, "admin: a pagerduty token could be neither recorded nor revoked; it stays live at PagerDuty",
				slog.String("token_kind", kind), slog.String("record_error", err.Error()), slog.String("revoke_error", revokeErr.Error()))
		}
		h.internalError(ctx, w, "record pagerduty setup revocation", err)
		return
	}

	if missing := missingPagerDutyReadScopes(tokens.GrantedScopes); len(missing) > 0 {
		h.revokePagerDutySetupToken(ctx, setupID, tokens)
		policy.WriteDetail(w, http.StatusBadRequest, "Missing required PagerDuty OAuth scopes: "+strings.Join(missing, ", "), nil)
		return
	}
	validated, region, err := h.validatePagerDutyOAuthIdentity(ctx, tokens)
	if err != nil {
		h.revokePagerDutySetupToken(ctx, setupID, tokens)
		var validation *providerfoundation.PagerDutyValidationError
		if errors.As(err, &validation) {
			policy.WriteDetail(w, http.StatusBadRequest, "PagerDuty OAuth account validation failed", nil)
			return
		}
		h.internalError(ctx, w, "validate pagerduty oauth identity", err)
		return
	}

	credentialName := validated.AccountDisplay
	if credentialName == "" {
		credentialName = "default"
	}
	if err := h.persistPagerDutyOAuthBinding(ctx, orgID, credentialName, setupID, tokens, validated, region); err != nil {
		if errors.Is(err, errPagerDutySetupSuperseded) {
			// A later callback's drain already revoked this token and removed
			// its record: nothing was stored, nothing is left to revoke.
			h.internalError(ctx, w, "persist pagerduty oauth binding", err)
			return
		}
		// The local state was rolled back; the grant PagerDuty just issued
		// must not outlive the failed setup.
		h.revokePagerDutySetupToken(ctx, setupID, tokens)
		h.internalError(ctx, w, "persist pagerduty oauth binding", err)
		return
	}
	if _, err := h.retryPagerDutyRevocations(ctx, orgID, credentialName); err != nil {
		h.internalError(ctx, w, "retry pagerduty revocations", err)
		return
	}

	out := pyjson.NewObject()
	out.Set("connected", true)
	out.Set("credential_name", credentialName)
	out.Set("region", region)
	out.Set("subdomain", validated.Subdomain)
	out.Set("granted_scopes", stringValues(tokens.GrantedScopes))
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func stringValues(values []string) []pyjson.Value {
	out := make([]pyjson.Value, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

// missingPagerDutyReadScopes is sorted(READ_SCOPES - granted).
func missingPagerDutyReadScopes(granted []string) []string {
	have := make(map[string]struct{}, len(granted))
	for _, scope := range granted {
		have[scope] = struct{}{}
	}
	var missing []string
	for _, scope := range providerfoundation.PagerDutyReadScopeSet() {
		if _, ok := have[scope]; !ok {
			missing = append(missing, scope)
		}
	}
	sort.Strings(missing)
	return missing
}

// consumePagerDutyAuthorization is PagerDutyAuthorizationRequestStore.consume
// plus the route's session.commit(): the one-time row is deleted and the
// deletion committed only when the state was known, this organisation's and
// unexpired. Any other outcome rolls back (the router's session dependency
// rolls back on the HTTPException), so an expired row is left in place.
func (h *handlers) consumePagerDutyAuthorization(ctx context.Context, orgID, state string) (string, bool, error) {
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	digest := sha256.Sum256([]byte(state))
	var encrypted string
	var expiresAt time.Time
	err = tx.QueryRow(ctx,
		`DELETE FROM pagerduty_oauth_authorization_requests WHERE state_hash = $1 AND org_id = $2 RETURNING code_verifier_encrypted, expires_at`,
		hex.EncodeToString(digest[:]), orgID).Scan(&encrypted, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !expiresAt.After(h.store.now().UTC()) {
		return "", false, nil
	}
	verifier, err := h.decryptor.Decrypt(secrets.NewValue(encrypted))
	if err != nil {
		return "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", false, err
	}
	return string(verifier), true, nil
}

// validatePagerDutyOAuthIdentity is _validate_oauth_identity: the account is
// looked up in each server-controlled region in order; the first proof wins
// and the last failure is the answer when none does.
func (h *handlers) validatePagerDutyOAuthIdentity(ctx context.Context, tokens providerfoundation.PagerDutyOAuthTokens) (providerfoundation.ValidatedPagerDutyCredential, string, error) {
	var last error
	for _, region := range pagerDutyOAuthRegions {
		validated, err := providerfoundation.ValidatePagerDutyCredential(ctx, h.upstreamDoer, h.pagerDuty, providerfoundation.PagerDutyCredentialCandidate{
			AuthMode: "oauth", AccessToken: tokens.AccessToken, GrantedScopes: tokens.GrantedScopes, Region: region,
		}, providerfoundation.PagerDutyReadScopeSet())
		if err == nil {
			return validated, region, nil
		}
		last = err
	}
	return providerfoundation.ValidatedPagerDutyCredential{}, "", last
}

// persistPagerDutyOAuthBinding is the callback's one unit of work:
// PagerDutyOAuthCredentialRepository.replace_and_capture, the replaced
// grant's durable revocation, and IntegrationCredentialsService.set, all
// committed together or not at all.
func (h *handlers) persistPagerDutyOAuthBinding(ctx context.Context, orgID, credentialName string, setupID uuid.UUID, tokens providerfoundation.PagerDutyOAuthTokens, validated providerfoundation.ValidatedPagerDutyCredential, region string) error {
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Take the setup record first: a drain (which locks the same row for the
	// length of its revoke) that judged this callback dead holds it until the
	// token is revoked and the row gone. Finding the row gone means that
	// happened: the token is no longer good, so the grant must not be stored.
	var held int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM provider_oauth_revocations WHERE id = $1 FOR UPDATE`, setupID).Scan(&held); errors.Is(err, pgx.ErrNoRows) {
		return errPagerDutySetupSuperseded
	} else if err != nil {
		return err
	}
	now := h.store.now().UTC()
	bindingID := strings.ReplaceAll(uuid.NewString(), "-", "")

	// _locked_credential + the predecessor's tokens, captured under the lock.
	var previousEncrypted *string
	err = tx.QueryRow(ctx,
		`SELECT token_encrypted FROM provider_oauth_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2 FOR UPDATE`,
		orgID, credentialName).Scan(&previousEncrypted)
	exists := true
	if errors.Is(err, pgx.ErrNoRows) {
		exists = false
	} else if err != nil {
		return err
	}
	var previousToken string
	if exists {
		plaintext, err := h.decryptor.Decrypt(secrets.NewValue(derefString(previousEncrypted)))
		if err != nil {
			return err
		}
		if previousToken, err = pagerDutyRevokeToken(plaintext); err != nil {
			return err
		}
	}

	var refresh *string
	if tokens.RefreshToken != "" {
		refresh = &tokens.RefreshToken
	}
	encoded, err := json.Marshal(storedPagerDutyTokens{AccessToken: tokens.AccessToken, RefreshToken: refresh, ExpiresAt: tokens.ExpiresAt, GrantedScopes: tokens.GrantedScopes})
	if err != nil {
		return err
	}
	sealed, err := h.decryptor.Encrypt(encoded)
	if err != nil {
		return err
	}
	scopesText, err := pyjson.Dumps(stringValues(tokens.GrantedScopes))
	if err != nil {
		return err
	}
	if !exists {
		_, err = tx.Exec(ctx,
			`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, binding_id, expires_at, granted_scopes, has_refresh_token, account_id, account_display)
VALUES ($1, 'pagerduty', $2, $3, 1, $4, $4, $5, $6, $7::json, $8, $9, $10)`,
			orgID, credentialName, sealed.Reveal(), now, bindingID, tokens.ExpiresAt, scopesText, refresh != nil, validated.AccountID, validated.AccountDisplay)
	} else {
		_, err = tx.Exec(ctx,
			`UPDATE provider_oauth_credentials SET token_encrypted = $3, version = version + 1, binding_id = $4, updated_at = $5, expires_at = $6, granted_scopes = $7::json, has_refresh_token = $8, account_id = $9, account_display = $10
WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`,
			orgID, credentialName, sealed.Reveal(), bindingID, now, tokens.ExpiresAt, scopesText, refresh != nil, validated.AccountID, validated.AccountDisplay)
	}
	if err != nil {
		return err
	}

	if exists {
		revocation, err := h.decryptor.Encrypt([]byte(previousToken))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, 'replacement', $4, 'v1', 'pending', 0, $5, $5)`,
			uuid.New(), orgID, credentialName, revocation.Reveal(), now); err != nil {
			return err
		}
	}

	credentialsObject := pyjson.NewObject()
	credentialsObject.Set("auth_mode", "oauth")
	credentialsObject.Set("oauth_credential_name", credentialName)
	credentialsObject.Set("oauth_binding_id", bindingID)
	credentialsObject.Set("subdomain", validated.Subdomain)
	credentialsObject.Set("region", region)
	credentialsObject.Set("account_id", validated.AccountID)
	config := pyjson.NewObject()
	config.Set("auth_mode", "oauth")
	config.Set("region", region)
	config.Set("subdomain", validated.Subdomain)
	config.Set("account_id", validated.AccountID)
	config.Set("account_display", validated.AccountDisplay)
	config.Set("granted_scopes", stringValues(tokens.GrantedScopes))
	if err := credentials.NewSaver(h.decryptor, h.store.now).Set(ctx, tx, orgID, "pagerduty", credentialName, credentialsObject, config, true); err != nil {
		return err
	}
	// The grant is stored: the setup record for its token goes with it, in
	// the same commit.
	if _, err := tx.Exec(ctx, `DELETE FROM provider_oauth_revocations WHERE id = $1`, setupID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
