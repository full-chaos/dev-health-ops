package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

// analyticsUnavailableCode is the error code of an analytics answer that is unavailable by a bound, not failed.
const analyticsUnavailableCode = "UNAVAILABLE"

// analyticsError is the client error of a failed analytics batch. A pooled TestOps read past its row bound is an
// authored refusal: the answer is null and the error names the reason and the measure, never a partial value or a 0.
// Every other error stays the wrapped resolver error the presenter masks.
func analyticsError(ctx context.Context, err error) (error, bool) {
	var refused *analytics.PooledReadBoundError
	if !errors.As(err, &refused) {
		return fmt.Errorf("analytics: %w", err), false
	}
	return &gqlerror.Error{
		Message: refused.Error(),
		Path:    graphql.GetPath(ctx),
		Extensions: map[string]any{
			"code":    analyticsUnavailableCode,
			"reason":  refused.Reason(),
			"measure": strings.ToUpper(string(refused.Measure)),
		},
	}, true
}
