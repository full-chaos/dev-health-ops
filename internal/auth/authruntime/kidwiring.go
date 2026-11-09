package authruntime

import (
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// newRecordingIssuer is the one production builder of the access issuer: the
// issuer of cfg's class, recording the signing kid of every mint on its session.
func newRecordingIssuer(cfg authconfig.Config, signer authapi.AccessSigner, recorder authapi.KidRecorder) (*authapi.Issuer, error) {
	issuer, err := newAccessIssuer(cfg, signer)
	if err != nil {
		return nil, err
	}
	return issuer.WithKidRecorder(recorder), nil
}
