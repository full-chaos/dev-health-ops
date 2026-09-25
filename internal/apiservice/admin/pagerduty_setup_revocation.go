package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The PagerDuty OAuth callback's setup revocations (CHAOS-6631).
//
// A row in provider_oauth_revocations with purpose 'setup' exists exactly
// while a token PagerDuty issued to a callback is neither the org's active
// grant nor revoked: it is written right after the code exchange, deleted in
// the same transaction that stores the grant, and deleted after a compensating
// revoke PagerDuty accepted. A refused compensating revoke leaves it pending
// (attempts and last_error say so) for the next callback, which retries it.
// Python drops the token instead (both planes did): the durable row is the
// named divergence.

// errPagerDutySetupSuperseded: the callback's setup record is gone at the
// moment it would store its grant, so a drain revoked the token first.
var errPagerDutySetupSuperseded = errors.New("pagerduty setup revocation superseded: the token was revoked before the grant was stored")

// setupRevocationStaleAfter is how old a setup row that never failed a
// revoke (its request died between the exchange and the outcome) must be
// before a later callback retries it. A callback stops on its own deadline
// (setupCallbackDeadline, from the moment the row is written), so a row
// older than a few deadlines belongs to a request that is gone: an in-flight
// callback's token is never revoked under it. The constant expression below
// fails to compile if the cutoff is ever shortened below twice the deadline.
const (
	setupRevocationStaleAfter = 15 * time.Minute
	setupCallbackDeadline     = 5 * time.Minute
	_                         = uint(setupRevocationStaleAfter - 2*setupCallbackDeadline)
)

// setupCallbackTimeout is the deadline a callback runs under after its setup
// row is written; a var only so a test can shorten it.
var setupCallbackTimeout = setupCallbackDeadline

// setupRevocationStaleInterval is the cutoff as the SQL interval.
var setupRevocationStaleInterval = fmt.Sprintf("%d seconds", int64(setupRevocationStaleAfter/time.Second))

// enqueuePagerDutySetupRevocation durably records a token PagerDuty just
// issued, in its own transaction, before the callback does anything else
// with it.
func (h *handlers) enqueuePagerDutySetupRevocation(ctx context.Context, orgID string, tokens providerfoundation.PagerDutyOAuthTokens) (uuid.UUID, error) {
	sealed, err := h.decryptor.Encrypt([]byte(tokens.RevocationToken()))
	if err != nil {
		return uuid.Nil, err
	}
	id := uuid.New()
	if _, err := h.store.Pool.Exec(ctx,
		`INSERT INTO provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', '', 'setup', $3, 'v1', 'pending', 0, now(), now())`,
		id, orgID, sealed.Reveal()); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// revokePagerDutySetupToken is _revoke_tokens with a memory: the compensating
// revoke of a grant the callback will not keep. PagerDuty accepting it
// removes the setup row; a refusal keeps the row pending for a later retry.
// It never obscures the setup outcome the route answers.
func (h *handlers) revokePagerDutySetupToken(ctx context.Context, setupID uuid.UUID, tokens providerfoundation.PagerDutyOAuthTokens) {
	if err := providerfoundation.RevokePagerDutyOAuthToken(ctx, h.httpDoer, h.pagerDuty, tokens.RevocationToken()); err != nil {
		// The request's own context may be the reason the revoke failed.
		if _, dbErr := h.store.Pool.Exec(context.WithoutCancel(ctx),
			`UPDATE provider_oauth_revocations SET attempts = attempts + 1, last_error = 'remote_revoke_failed', updated_at = now() WHERE id = $1`, setupID); dbErr != nil {
			h.logger.ErrorContext(ctx, "admin: recording a refused pagerduty setup revoke failed", slog.String("error", dbErr.Error()))
			return
		}
		h.logger.ErrorContext(ctx, "admin: pagerduty setup revoke refused; the token is kept for retry", slog.String("revocation_id", setupID.String()))
		return
	}
	if _, err := h.store.Pool.Exec(context.WithoutCancel(ctx), `DELETE FROM provider_oauth_revocations WHERE id = $1`, setupID); err != nil {
		h.logger.ErrorContext(ctx, "admin: removing a completed pagerduty setup revocation failed", slog.String("error", err.Error()))
	}
}

// drainPagerDutySetupRevocations retries the org's setup revocations that
// are due: a refused compensating revoke (attempts > 0), or a row old enough
// that its callback died without an outcome. Its result never changes what
// the calling route answers.
func (h *handlers) drainPagerDutySetupRevocations(ctx context.Context, orgID string) {
	if _, err := h.retryPagerDutyRevocationRows(ctx, orgID,
		`SELECT id, token_encrypted FROM provider_oauth_revocations
WHERE org_id = $1 AND provider = 'pagerduty' AND purpose = 'setup' AND status = 'pending'
  AND (attempts > 0 OR created_at < now() - interval '`+setupRevocationStaleInterval+`')
ORDER BY created_at FOR UPDATE`); err != nil {
		h.logger.ErrorContext(ctx, "admin: draining pagerduty setup revocations failed", slog.String("error", err.Error()))
	}
}
