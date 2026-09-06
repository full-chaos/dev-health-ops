package operational

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// isNativeGithubAppEvent reports whether eventType is one of the two GitHub
// App lifecycle events WebhookHandler.Work handles entirely inside this
// process (CHAOS-5312 PR1) instead of dispatching to the Python HTTP bridge.
// Every other github event type (push, pull_request, issue_*, deployment,
// workflow_run, ...) is unaffected: it still dispatches to Python's
// _process_github_event, which still triggers the full-repo sync pipeline --
// that pipeline is explicitly out of this family's scope (see PR1's body).
func isNativeGithubAppEvent(eventType string) bool {
	return eventType == "installation" || eventType == "marketplace_purchase"
}

// InstallationWriter is an OPTIONAL capability a DeliveryStore may implement:
// native handling of the two GitHub App lifecycle events above, with no HTTP
// round trip. WebhookHandler.Work type-asserts for it -- a store that does
// not implement it (a test fake, for instance) falls through to the existing
// HTTP dispatch path unchanged, the same optional-capability pattern
// internal/scheduler/sync/occurrence_reconciler.go already uses for
// prometheusWriter/executedProofLoadReporter.
type InstallationWriter interface {
	UpsertGithubAppEvent(
		ctx context.Context, eventType string, payload []byte, now time.Time,
	) (GithubAppEventResult, error)
}

// GithubAppEventResult mirrors the shape of Python's
// _process_github_installation_event/_process_github_event's
// marketplace_purchase branch closely enough for a caller to log the same
// facts; it carries no error itself (a genuine failure is returned as the
// method's error value, matching every other Store method in this package).
type GithubAppEventResult struct {
	Processed      bool
	Reason         string
	InstallationID int64
	Action         string
}

// UpsertGithubAppEvent is the native port of
// src/dev_health_ops/workers/system_webhooks.py's
// _process_github_installation_event (the "installation" branch) and the
// _process_github_event marketplace_purchase no-op branch.
//
// marketplace_purchase: CHAOS-2236 entitlement mapping is still deferred --
// this is a logged no-op on both sides, ported verbatim.
//
// installation: unlike Python's select-then-insert-then-catch-IntegrityError
// dance (a real TOCTOU window under concurrent installs, exercised by
// tests/api/webhooks/test_github_app_installation.py's
// test_installation_webhook_recovers_when_callback_created_row_concurrently),
// this upserts via `INSERT ... ON CONFLICT (installation_id) DO NOTHING`,
// which is atomic -- there is no race to recover from. The observable end
// state is identical: the row exists exactly once per installation_id,
// account_login/account_type/suspended_at/updated_at reflect this event, and
// on a "deleted" action with a resolved org_id, the matching
// (org_id, provider='github', name='github-app') integration_credentials row
// is deactivated.
func (store *PostgresStore) UpsertGithubAppEvent(
	ctx context.Context, eventType string, rawPayload []byte, now time.Time,
) (GithubAppEventResult, error) {
	if store == nil || store.pool == nil {
		return GithubAppEventResult{}, errors.New("github app event store is unavailable")
	}

	if eventType == "marketplace_purchase" {
		return GithubAppEventResult{Processed: true, Action: eventType}, nil
	}

	var payload map[string]any
	_ = json.Unmarshal(rawPayload, &payload)

	installationRaw, _ := payload["installation"].(map[string]any)
	idFloat, ok := installationRaw["id"].(float64)
	if !ok {
		return GithubAppEventResult{Processed: false, Reason: "missing_installation_id"}, nil
	}
	installationID := int64(idFloat)
	action, _ := payload["action"].(string)
	accountRaw, _ := installationRaw["account"].(map[string]any)
	login, _ := accountRaw["login"].(string)
	accountType, _ := accountRaw["type"].(string)

	suspend := action == "suspend" || action == "deleted"
	unsuspend := action == "created" || action == "unsuspend"

	if err := upsertGithubAppInstallation(ctx, store.pool, installationID, action, login, accountType, suspend, unsuspend, now); err != nil {
		return GithubAppEventResult{}, err
	}

	return GithubAppEventResult{Processed: true, InstallationID: installationID, Action: action}, nil
}

func upsertGithubAppInstallation(
	ctx context.Context, pool *pgxpool.Pool,
	installationID int64, action, login, accountType string, suspend, unsuspend bool, now time.Time,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
INSERT INTO public.github_app_installations (id, installation_id, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $2)
ON CONFLICT (installation_id) DO NOTHING`, installationID, now); err != nil {
		return err
	}

	updateSQL := `UPDATE public.github_app_installations SET updated_at = $2`
	args := []any{installationID, now}
	next := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if login != "" {
		updateSQL += ", account_login = " + next(login)
	}
	if accountType != "" {
		updateSQL += ", account_type = " + next(accountType)
	}
	if suspend {
		updateSQL += ", suspended_at = " + next(now)
	} else if unsuspend {
		updateSQL += ", suspended_at = NULL"
	}
	updateSQL += " WHERE installation_id = $1 RETURNING org_id"

	var orgID *string
	if err := tx.QueryRow(ctx, updateSQL, args...).Scan(&orgID); err != nil {
		return err
	}

	if action == "deleted" && orgID != nil && *orgID != "" {
		if _, err := tx.Exec(ctx, `
UPDATE public.integration_credentials
SET is_active = false, updated_at = $1
WHERE org_id = $2 AND provider = 'github' AND name = 'github-app'`, now, *orgID); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
