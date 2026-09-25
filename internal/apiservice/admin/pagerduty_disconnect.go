// PagerDuty disconnect (pagerduty.py's disconnect_pagerduty via
// IntegrationCredentialsService.disconnect_pagerduty). Part of CHAOS-6591:
// unlike status/preflight (CHAOS-6590), this route detaches
// pagerduty_webhook_bindings rows and makes a live outbound PagerDuty
// revoke call, so it needs the org-deletion route's own PagerDuty
// dependencies (h.decryptor/h.pagerDuty/h.httpDoer, CHAOS-6306) and its
// venue test's fake-revoke-endpoint seam (VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE,
// testsupport/venueoracle's pythonProgram) rather than a new one.
package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

func (h *handlers) pagerDutyDisconnectRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: pagerDutyPrefix + "/disconnect", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.disconnectPagerDuty))},
	}
}

// disconnectPagerDuty is pagerduty.py's disconnect_pagerduty.
func (h *handlers) disconnectPagerDuty(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	credentialName := "default"
	if ok {
		if raw, present := errs.DefaultedString(object, "credential_name", 0, 0); present {
			normalized := pythonparity.Strip(raw)
			if normalized == "" {
				errs = append(errs, pydanticValueError([]pyjson.Value{"body", "credential_name"}, raw, "value must not be blank"))
			} else {
				credentialName = normalized
			}
		}
		errs.ForbidExtra(object, "credential_name")
	}
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}

	disconnected, err := h.disconnectPagerDutyCredential(ctx, orgID, credentialName)
	if err != nil {
		h.internalError(ctx, w, "disconnect pagerduty", err)
		return
	}
	if disconnected != nil && !*disconnected {
		policy.WriteDetail(w, http.StatusServiceUnavailable, "PagerDuty remote revocation is pending retry", nil)
		return
	}
	out := pyjson.NewObject()
	out.Set("disconnected", true)
	out.Set("credential_name", credentialName)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// disconnectPagerDutyCredential is
// IntegrationCredentialsService.disconnect_pagerduty: local removal
// (webhook-binding detach, OAuth token delete, credential deactivate)
// commits FIRST, in its own transaction -- so a crash or a slow revoke
// attempt afterward never stops the local disconnect from being durable --
// then a live PagerDuty revoke attempt runs, in a second transaction,
// against whatever `provider_oauth_revocations` rows are pending (this
// disconnect's own enqueued row, and any earlier org's still-unretried
// ones for this same credential). Returns nil for "no credential row
// existed" (Python's None, itself never a failure), else whether the
// revocation queue finished draining.
func (h *handlers) disconnectPagerDutyCredential(ctx context.Context, orgID, credentialName string) (*bool, error) {
	// A revoke PagerDuty refused for an earlier OAuth setup is retried too;
	// it never changes this disconnect's answer.
	h.drainPagerDutySetupRevocations(ctx, orgID)

	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	var credentialID *uuid.UUID
	var id uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM integration_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND name = $2`, orgID, credentialName).Scan(&id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		credentialID = nil
	case err != nil:
		return nil, err
	default:
		credentialID = &id
	}

	if credentialID != nil {
		if _, err := tx.Exec(ctx,
			`UPDATE pagerduty_webhook_bindings SET status = 'inactive', revoked_at = COALESCE(revoked_at, now()), updated_at = now(), credential_id = NULL WHERE credential_id = $1`,
			*credentialID); err != nil {
			return nil, err
		}
	}

	// revokeCandidate mirrors `versioned.tokens.refresh_token or
	// versioned.tokens.access_token` from a decryptable OAuth row; any
	// decrypt/decode failure (ValueError in Python) leaves it nil, exactly
	// as Python's `except ValueError: revoke_candidate = None` does --
	// local deletion below never depends on it.
	var revokeCandidate *string
	var tokenEncrypted *string
	err = tx.QueryRow(ctx, `SELECT token_encrypted FROM provider_oauth_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`,
		orgID, credentialName).Scan(&tokenEncrypted)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if tokenEncrypted != nil {
		if plaintext, decErr := h.decryptor.Decrypt(secrets.NewValue(*tokenEncrypted)); decErr == nil {
			if token, tokErr := pagerDutyRevokeToken(plaintext); tokErr == nil {
				revokeCandidate = &token
			}
		}
	}

	// The revocation row is queued whether or not PagerDuty is configured
	// (CHAOS-6619): without a client id the revoke cannot be attempted now,
	// but dropping the token would leave it live at PagerDuty with nothing
	// to say a revoke is still owed. Python queues nothing then (named
	// divergence); the row is retried once the api is configured, by the
	// next callback or disconnect of this credential.
	configPresent := h.pagerDuty.ClientID != ""
	if revokeCandidate != nil {
		sealed, encErr := h.decryptor.Encrypt([]byte(*revokeCandidate))
		if encErr != nil {
			return nil, encErr
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, 'disconnect', $4, 'v1', 'pending', 0, now(), now())`,
			uuid.New(), orgID, credentialName, sealed.Reveal()); err != nil {
			return nil, err
		}
	}

	if _, err := tx.Exec(ctx, `DELETE FROM provider_oauth_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`,
		orgID, credentialName); err != nil {
		return nil, err
	}

	if credentialID == nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		committed = true
		return nil, nil
	}

	if _, err := tx.Exec(ctx, `UPDATE integration_credentials SET is_active = false, credentials_encrypted = NULL WHERE id = $1`, *credentialID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	committed = true

	if !configPresent {
		result := revokeCandidate == nil
		return &result, nil
	}
	complete, err := h.retryPagerDutyRevocations(ctx, orgID, credentialName)
	if err != nil {
		return nil, err
	}
	return &complete, nil
}

// retryPagerDutyRevocations is PagerDutyOAuthRevocationRepository.retry_pending:
// every pending revocation row for (org, pagerduty, credentialName), oldest
// first, gets ONE live revoke attempt; a decrypt failure or a failed revoke
// call is retained (attempts incremented, is_complete=false) rather than
// raised, matching Python's `except (ValueError, httpx.HTTPError)`.
func (h *handlers) retryPagerDutyRevocations(ctx context.Context, orgID, credentialName string) (bool, error) {
	return h.retryPagerDutyRevocationRows(ctx, orgID, `SELECT id, token_encrypted FROM provider_oauth_revocations WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2 AND status = 'pending' ORDER BY created_at FOR UPDATE`, credentialName)
}

// retryPagerDutyRevocationRows is retryPagerDutyRevocations over the rows a
// query selects (id, token_encrypted; $1 is the organisation, further
// arguments follow).
func (h *handlers) retryPagerDutyRevocationRows(ctx context.Context, orgID, query string, args ...any) (bool, error) {
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	type pendingRevocation struct {
		id        uuid.UUID
		encrypted string
	}
	rows, err := tx.Query(ctx, query, append([]any{orgID}, args...)...)
	if err != nil {
		return false, err
	}
	var pending []pendingRevocation
	for rows.Next() {
		var row pendingRevocation
		if err := rows.Scan(&row.id, &row.encrypted); err != nil {
			rows.Close()
			return false, err
		}
		pending = append(pending, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}

	complete := true
	for _, row := range pending {
		plaintext, decErr := h.decryptor.Decrypt(secrets.NewValue(row.encrypted))
		revokeErr := decErr
		if decErr == nil {
			revokeErr = providerfoundation.RevokePagerDutyOAuthToken(ctx, h.httpDoer, h.pagerDuty, string(plaintext))
		}
		if revokeErr != nil {
			if _, err := tx.Exec(ctx, `UPDATE provider_oauth_revocations SET attempts = attempts + 1, last_error = 'remote_revoke_failed', updated_at = now() WHERE id = $1`, row.id); err != nil {
				return false, err
			}
			complete = false
			continue
		}
		if _, err := tx.Exec(ctx, `DELETE FROM provider_oauth_revocations WHERE id = $1`, row.id); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	committed = true
	return complete, nil
}
