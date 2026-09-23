package admin

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// pyTimeString is pydantic's JSON form of an aware UTC instant (Z suffix,
// microseconds only when non-zero) -- internal/api/pytime is the shared,
// canonical implementation (also consumed by gwc-w1-health's telemetry
// area); this package never carries its own copy.
func pyTimeString(t time.Time) string {
	return pytime.Pydantic(pytime.UTC(t))
}
