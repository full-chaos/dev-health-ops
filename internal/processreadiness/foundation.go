// Package processreadiness provides fail-closed readiness placeholders for
// process foundations whose runtime clients have not been composed yet.
package processreadiness

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

var errFoundationDependencyUnavailable = errors.New("process foundation dependency is unavailable")

// RegisterUnavailable registers the named dependencies as required and
// unavailable. A concrete runtime must replace each placeholder with its real
// readiness probe before the process can become ready.
func RegisterUnavailable(registry *health.Registry, names ...string) error {
	if registry == nil {
		return errFoundationDependencyUnavailable
	}
	for _, name := range names {
		if err := registry.RegisterRequired(name, func(context.Context) error {
			return errFoundationDependencyUnavailable
		}); err != nil {
			return err
		}
	}
	return nil
}
