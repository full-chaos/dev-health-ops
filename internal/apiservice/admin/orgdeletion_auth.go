package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// OrgDeprovisioner is the one auth-service verb the organization delete needs.
type OrgDeprovisioner interface {
	Deprovision(ctx context.Context, org, actor uuid.UUID) error
}

// The reasons a delete with AUTH_PROXY_ORG_PROVISIONING off left the auth organization live: a closed set, one
// counter label each.
const (
	deprovisionSkipNoClient        = "no_client"
	deprovisionSkipRoutesOff       = "routes_off"
	deprovisionSkipUsersNotOwned   = "users_not_owned"
	deprovisionSkipRefused         = "refused"
	deprovisionSkipAuthUnavailable = "auth_unavailable"
)

var orgDeletionDeprovisionSkipped = func() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/admin")
	counter, err := meter.Int64Counter("devhealth_api_orgdelete_auth_deprovision_skipped_total",
		metric.WithDescription("Organization deletes with AUTH_PROXY_ORG_PROVISIONING off whose auth deprovision did not run, by reason; the purge went on."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_api_orgdelete_auth_deprovision_skipped_total")
	}
	return counter
}()

// deprovisionBeforeDelete runs before any purge on a real delete. With the switch on it is deprovisionInAuth, whose
// refusal stops the delete. With it off and an auth-service configured it asks auth-service to deprovision; a
// failure never stops the delete or changes its answer: it is one ERROR line and one count, and the purge goes on.
func (h *handlers) deprovisionBeforeDelete(ctx context.Context, orgID uuid.UUID) error {
	if h.orgProvision != nil {
		return h.deprovisionInAuth(ctx, orgID)
	}
	if !h.orgDeletionAuth {
		return nil
	}
	reason, code := h.deprovisionSwitchOff(ctx, orgID)
	if reason == "" {
		return nil
	}
	orgDeletionDeprovisionSkipped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	h.logger.ErrorContext(ctx, "admin: org deletion left the auth organization live",
		"org_id", orgID.String(), "reason", reason, "reason_code", code)
	return nil
}

// deprovisionSwitchOff returns "" when auth-service deprovisioned the organization or does not hold it, else the
// skip reason and the auth reason code (empty when there is none).
func (h *handlers) deprovisionSwitchOff(ctx context.Context, orgID uuid.UUID) (reason, code string) {
	if h.orgDeprovision == nil {
		return deprovisionSkipNoClient, ""
	}
	user := policy.UserFrom(ctx)
	if user == nil {
		return deprovisionSkipRefused, "no_acting_user"
	}
	err := h.orgDeprovision.Deprovision(ctx, orgID, user.ID)
	if err == nil {
		return "", ""
	}
	var refused *authprovision.Refused
	if !errors.As(err, &refused) {
		return deprovisionSkipAuthUnavailable, ""
	}
	switch {
	case refused.Reason == "organization_not_found":
		return "", ""
	case refused.Status == http.StatusNotFound && refused.Reason == "":
		return deprovisionSkipRoutesOff, ""
	case refused.Reason == "users_not_owned":
		return deprovisionSkipUsersNotOwned, refused.Reason
	}
	return deprovisionSkipRefused, refused.Reason
}
