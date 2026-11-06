package apiservice

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

const goAPIAccessService = accessrouting.Service

var (
	goAPIAccessRows = accessrouting.Rows
	parseAccessRows = accessrouting.ParseRows
)

// routedAccessVerify is accessrouting.Adapter over validator.
type routedAccessVerify struct{ validator *accessverify.Validator }

func (r routedAccessVerify) Validate(ctx context.Context, service, profile, bearer string) (*policy.RoutedResult, error) {
	return accessrouting.Adapter{Validator: r.validator}.Validate(ctx, service, profile, bearer)
}
