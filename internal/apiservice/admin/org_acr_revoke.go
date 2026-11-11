package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
)

// OrgCredentialRevoker is acr's organization revoke: every live acr credential of the organization, for the admin who
// acts. *acrrevoke.Client has it.
type OrgCredentialRevoker interface {
	RevokeOrganization(ctx context.Context, orgID, actorID, reason string) (acrrevoke.OrganizationRevocation, error)
}

// UnusableOrgCredentialRevoker is the revoker of a set but unusable ACR_REVOKE_* configuration: every call fails with
// acrrevoke.ErrClientConfigInvalid, so the disable answers 503 and the delete purges nothing. refused names the first
// refused variable (never a value).
func UnusableOrgCredentialRevoker(refused string) OrgCredentialRevoker {
	return unusableOrgRevoker{refused: refused}
}

type unusableOrgRevoker struct{ refused string }

func (u unusableOrgRevoker) RevokeOrganization(context.Context, string, string, string) (acrrevoke.OrganizationRevocation, error) {
	return acrrevoke.OrganizationRevocation{}, fmt.Errorf("%w: %s", acrrevoke.ErrClientConfigInvalid, u.refused)
}

// The operator-facing answers when acr did not revoke an organization's credentials. The disable is stored and the
// delete purged nothing; each repeat is safe (acr revokes what is left).
const (
	orgDisableACRRevokeDetail            = "Organization disabled; its acr credentials were not revoked; repeat the request."
	orgDisableACRRevokeIncompleteDetail  = "Organization disabled; its acr credentials were not all revoked: more remains; repeat the request."
	orgDeletionACRRevokeDetail           = "The acr credentials of this organization were not revoked; the organization is now disabled; run the delete again. Nothing was deleted."
	orgDeletionDisableFailedDetail       = "The organization could not be disabled; it is unchanged and its acr credentials were not revoked; run the delete again. Nothing was deleted."
	orgDeletionACRRevokeIncompleteDetail = "The acr credentials of this organization were not all revoked: more remains; the organization is now disabled; run the delete again. Nothing was deleted."
)

// acrOperatorActor is the acr actor of `dho admin orgs delete`, which acts with no request user.
const acrOperatorActor = "operator"

// errOrgDeletionACRRevoke is a delete stopped before any purge because acr did not revoke the organization's
// credentials (or the disable before it failed: errOrgDeletionDisable).
var (
	errOrgDeletionACRRevoke = errors.New("acr credential revoke of the organization failed")
	errOrgDeletionDisable   = errors.New("the organization delete could not disable the organization")
)

const (
	orgACREventDisable = "disable"
	orgACREventDelete  = "delete"
)

var orgACRRevokeSkipped = func() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/admin")
	counter, err := meter.Int64Counter("devhealth_api_org_acr_revoke_skipped_total",
		metric.WithDescription("Organization disables and deletes that revoked no acr credential because no acr revoke client is configured, by event and reason; the disable or delete went on."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_api_org_acr_revoke_skipped_total")
	}
	return counter
}()

func acrActor(ctx context.Context) string {
	if user := policy.UserFrom(ctx); user != nil && user.ID != uuid.Nil {
		return user.ID.String()
	}
	return acrOperatorActor
}

// revokeOrgACRCredentials asks acr to revoke every credential of the disabled organization. No client is one ERROR
// line and one count, and nil: the disable or delete goes on, and acr refuses the credentials at use.
func (h *handlers) revokeOrgACRCredentials(ctx context.Context, orgID uuid.UUID, event, reason string) error {
	if h.acrOrgRevoker == nil {
		orgACRRevokeSkipped.Add(ctx, 1, metric.WithAttributes(attribute.String("event", event), attribute.String("reason", "no_client")))
		h.logger.ErrorContext(ctx, "admin: org acr credential revoke not configured; acr refuses the credentials at use",
			"org_id", orgID.String(), "event", event, "reason", "no_client")
		return nil
	}
	got, err := h.acrOrgRevoker.RevokeOrganization(ctx, orgID.String(), acrActor(ctx), reason)
	if err != nil {
		attrs := []any{"org_id", orgID.String(), "event", event, "calls", got.Calls, "revoked", got.Revoked}
		var failure *acrrevoke.Error
		switch {
		case errors.Is(err, acrrevoke.ErrOrganizationLive):
			attrs = append(attrs, "decision", "organization_live")
		case errors.Is(err, acrrevoke.ErrOrganizationIncomplete):
			attrs = append(attrs, "decision", "more_remains")
		case errors.Is(err, acrrevoke.ErrClientConfigInvalid):
			attrs = append(attrs, "decision", "client_config_invalid")
		case errors.As(err, &failure):
			attrs = append(attrs, "decision", "failed", "failure_kind", string(failure.Kind), "status", failure.Status)
		default:
			attrs = append(attrs, "decision", "failed", "failure_kind", string(acrlisting.KindTransport))
		}
		h.logger.ErrorContext(ctx, "admin: org acr credential revoke failed", attrs...)
		return err
	}
	h.logger.InfoContext(ctx, "admin: org acr credentials revoked", slog.String("org_id", orgID.String()), slog.String("event", event),
		slog.Int("revoked", got.Revoked), slog.Int("caller_revoked", got.CallerRevoked), slog.Int("calls", got.Calls))
	return nil
}

// revokeACRAfterDisable runs after a stored disable.
func (h *handlers) revokeACRAfterDisable(ctx context.Context, orgID uuid.UUID) error {
	return h.revokeOrgACRCredentials(ctx, orgID, orgACREventDisable, acrrevoke.ReasonOrganizationDisabled)
}

// revokeACRBeforeDelete runs on a real delete before any count or purge: it disables the organization, so acr
// refuses its credentials at once, then revokes them. A failure stops the delete with the organization disabled.
// An organization with no row has nothing to disable and nothing acr can bind: the delete goes on as before.
func (h *handlers) revokeACRBeforeDelete(ctx context.Context, orgID uuid.UUID) error {
	tag, err := h.store.Pool.Exec(ctx, `UPDATE organizations SET is_active = false, updated_at = $2 WHERE id = $1`, orgID, h.now().UTC())
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: org deletion could not disable the organization", "org_id", orgID.String(), "error", err)
		return fmt.Errorf("%w: %w: %w", errOrgDeletionACRRevoke, errOrgDeletionDisable, err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if err := h.revokeOrgACRCredentials(ctx, orgID, orgACREventDelete, acrrevoke.ReasonOrganizationDeleted); err != nil {
		return fmt.Errorf("%w: %w", errOrgDeletionACRRevoke, err)
	}
	return nil
}

// orgDeletionACRRevokeAnswer is the operator-facing text of a delete stopped by errOrgDeletionACRRevoke.
func orgDeletionACRRevokeAnswer(err error) string {
	if errors.Is(err, errOrgDeletionDisable) {
		return orgDeletionDisableFailedDetail
	}
	if errors.Is(err, acrrevoke.ErrOrganizationIncomplete) {
		return orgDeletionACRRevokeIncompleteDetail
	}
	return orgDeletionACRRevokeDetail
}

// orgDisableACRRevokeAnswer is the operator-facing text of a stored disable whose acr revoke failed.
func orgDisableACRRevokeAnswer(err error) string {
	if errors.Is(err, acrrevoke.ErrOrganizationIncomplete) {
		return orgDisableACRRevokeIncompleteDetail
	}
	return orgDisableACRRevokeDetail
}
