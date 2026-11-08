package authruntime

import (
	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// newRecordingIssuer is the one production builder of the access issuer: the
// issuer of cfg's class, recording the signing kid of every mint on its session.
// It also starts the HS256 logout-accept counter of this service.
func newRecordingIssuer(cfg authconfig.Config, signer authapi.AccessSigner, recorder authapi.KidRecorder) (*authapi.Issuer, error) {
	issuer, err := newAccessIssuer(cfg, signer)
	if err != nil {
		return nil, err
	}
	logouts, err := accessverify.NewLogoutCounter(otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify"), authapi.AccessService)
	if err != nil {
		return nil, err
	}
	authapi.SetLogoutAccepted(logouts)
	return issuer.WithKidRecorder(recorder), nil
}
