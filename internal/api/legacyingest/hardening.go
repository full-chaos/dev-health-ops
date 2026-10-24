package legacyingest

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const SignatureTimestampHeader = "X-Signature-Timestamp"

type Options struct {
	Deprecation               bool
	Sunset                    time.Time
	RequireSignatureTimestamp bool
	Now                       func() time.Time
}

func Harden(routes []httpapi.Route, _ Options) []httpapi.Route { return routes }

func CredentialConfigured(getenv func(string) string) bool { return true }
