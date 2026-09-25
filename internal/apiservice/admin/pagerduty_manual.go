// PagerDuty client-credentials and API-token setup (pagerduty.py's
// set_pagerduty_client_credentials and set_pagerduty_api_token). Part of
// CHAOS-6595: each proves the credential with a live read, requires the
// proven account to be the subdomain the caller named, retires any OAuth
// binding under the same name, and stores the descriptor.
package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

func (h *handlers) pagerDutyManualRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: pagerDutyPrefix + "/client-credentials", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.setPagerDutyClientCredentials))},
		{Method: http.MethodPost, Pattern: pagerDutyPrefix + "/api-token", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.setPagerDutyAPIToken))},
	}
}

// pagerDutyManualBody is the two request models' shared shape: a credential
// name, a subdomain and a region, plus the mode's own secret fields.
type pagerDutyManualBody struct {
	CredentialName string
	Subdomain      string
	Region         string
	Secrets        map[string]string
}

// parsePagerDutyManualBody validates one of the two models. Field order is
// the models' declared order (credential_name, the secret fields, subdomain,
// region), which is the order pydantic reports errors in.
func parsePagerDutyManualBody(body pybody.Body, secretFields []string) (pagerDutyManualBody, pybody.Errors) {
	var errs pybody.Errors
	out := pagerDutyManualBody{CredentialName: "default", Region: "us", Secrets: map[string]string{}}
	object, ok := errs.Object(body)
	if !ok {
		return out, errs
	}
	if raw, present := errs.DefaultedString(object, "credential_name", 0, 0); present {
		normalized := pythonparity.Strip(raw)
		if normalized == "" {
			errs = append(errs, pydanticValueError([]pyjson.Value{"body", "credential_name"}, raw, "value must not be blank"))
		} else {
			out.CredentialName = normalized
		}
	}
	for _, name := range secretFields {
		if value, present := errs.RequiredString(object, name, 1, 0); present {
			out.Secrets[name] = value
		}
	}
	if raw, present := errs.RequiredString(object, "subdomain", 1, 0); present {
		normalized := pythonparity.Strip(raw)
		if normalized == "" {
			errs = append(errs, pydanticValueError([]pyjson.Value{"body", "subdomain"}, raw, "value must not be blank"))
		} else {
			out.Subdomain = normalized
		}
	}
	if raw, present := object.Get("region"); present {
		if text, isString := raw.(string); isString && (text == "us" || text == "eu") {
			out.Region = text
		} else {
			ctx := pyjson.NewObject()
			ctx.Set("expected", "'us' or 'eu'")
			errs = append(errs, pybody.Error{Type: "literal_error", Loc: []pyjson.Value{"body", "region"},
				Msg: "Input should be 'us' or 'eu'", Input: raw, Ctx: ctx})
		}
	}
	errs.ForbidExtra(object, append(append([]string{"credential_name"}, secretFields...), "subdomain", "region")...)
	return out, errs
}

func (h *handlers) setPagerDutyClientCredentials(w http.ResponseWriter, r *http.Request) {
	parsed, errs := parsePagerDutyManualBody(bodyFromContext(r.Context()), []string{"client_id", "client_secret"})
	h.setPagerDutyManualCredential(w, r, "client_credentials", parsed, errs)
}

func (h *handlers) setPagerDutyAPIToken(w http.ResponseWriter, r *http.Request) {
	parsed, errs := parsePagerDutyManualBody(bodyFromContext(r.Context()), []string{"api_token"})
	h.setPagerDutyManualCredential(w, r, "api_token", parsed, errs)
}

func (h *handlers) setPagerDutyManualCredential(w http.ResponseWriter, r *http.Request, authMode string, body pagerDutyManualBody, errs pybody.Errors) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	if !h.requireCanonicalIncidentIngestion(ctx, w, orgID) {
		return
	}

	candidate := providerfoundation.PagerDutyCredentialCandidate{
		AuthMode: authMode, Subdomain: body.Subdomain, Region: body.Region,
		APIToken: body.Secrets["api_token"], ClientID: body.Secrets["client_id"], ClientSecret: body.Secrets["client_secret"],
	}
	validated, err := providerfoundation.ValidatePagerDutyCredential(ctx, h.upstreamDoer, h.pagerDuty, candidate, providerfoundation.PagerDutyReadScopeSet())
	if err != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "PagerDuty credential validation failed", nil)
		return
	}
	// _require_verified_subdomain compares casefolded values; pythonparity.Fold
	// is that key (its one named difference, a literal lowercase final sigma,
	// cannot occur in a PagerDuty subdomain).
	if pythonparity.Fold(validated.Subdomain) != pythonparity.Fold(body.Subdomain) {
		policy.WriteDetail(w, http.StatusBadRequest, "PagerDuty credential belongs to a different account", nil)
		return
	}

	tokenToRevoke, err := h.storePagerDutyManualCredential(ctx, orgID, authMode, body, validated)
	if err != nil {
		h.internalError(ctx, w, "store pagerduty credential", err)
		return
	}
	if tokenToRevoke != "" {
		_ = providerfoundation.RevokePagerDutyOAuthToken(ctx, h.httpDoer, h.pagerDuty, tokenToRevoke)
	}

	out := pyjson.NewObject()
	out.Set("connected", true)
	out.Set("credential_name", body.CredentialName)
	out.Set("auth_mode", authMode)
	out.Set("region", body.Region)
	out.Set("subdomain", validated.Subdomain)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// storePagerDutyManualCredential is _remove_oauth_binding, then
// IntegrationCredentialsService.set and update_test_result, in one
// transaction. The returned token (empty when there is none, or no app
// client is configured) is revoked by the caller only after the commit.
func (h *handlers) storePagerDutyManualCredential(ctx context.Context, orgID, authMode string, body pagerDutyManualBody, validated providerfoundation.ValidatedPagerDutyCredential) (string, error) {
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// The binding is deleted unconditionally; only its revoke token depends
	// on the stored value being readable (a ValueError -- corrupt ciphertext
	// or token JSON -- leaves the token unknown, never blocks the delete).
	var tokenToRevoke string
	var encrypted *string
	err = tx.QueryRow(ctx,
		`SELECT token_encrypted FROM provider_oauth_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`,
		orgID, body.CredentialName).Scan(&encrypted)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if encrypted != nil {
		if plaintext, decErr := h.decryptor.Decrypt(secrets.NewValue(*encrypted)); decErr == nil {
			if token, tokErr := pagerDutyRevokeToken(plaintext); tokErr == nil && h.pagerDuty.ClientID != "" {
				tokenToRevoke = token
			}
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM provider_oauth_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`,
		orgID, body.CredentialName); err != nil {
		return "", err
	}

	credentialsObject := pyjson.NewObject()
	credentialsObject.Set("auth_mode", authMode)
	if authMode == "client_credentials" {
		credentialsObject.Set("client_id", body.Secrets["client_id"])
		credentialsObject.Set("client_secret", body.Secrets["client_secret"])
	} else {
		credentialsObject.Set("api_token", body.Secrets["api_token"])
	}
	credentialsObject.Set("subdomain", validated.Subdomain)
	credentialsObject.Set("region", body.Region)
	config := pyjson.NewObject()
	config.Set("auth_mode", authMode)
	config.Set("region", body.Region)
	config.Set("subdomain", validated.Subdomain)
	config.Set("account_id", validated.AccountID)
	config.Set("account_display", validated.AccountDisplay)
	if err := credentials.NewSaver(h.decryptor, h.store.now).Set(ctx, tx, orgID, "pagerduty", body.CredentialName, credentialsObject, config, true); err != nil {
		return "", err
	}
	// update_test_result(success=True): sanitize_error_text(None) is None.
	now := h.store.now().UTC()
	if _, err := tx.Exec(ctx,
		`UPDATE integration_credentials SET last_test_at = $4, last_test_success = true, last_test_error = NULL WHERE org_id = $1 AND provider = 'pagerduty' AND name = $2 AND $3 <> ''`,
		orgID, body.CredentialName, body.CredentialName, now); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return tokenToRevoke, nil
}
