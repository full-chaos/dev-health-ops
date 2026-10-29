package authruntime

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// withAccessCheck is the one production builder of the session routes' access
// check: with a token issuer URL set and a signer that reads the key set, the
// issuer validates a bearer by its route's endpoint-profile row (the EdDSA user
// access token for audience auth-service, plus the HS256 token where the row
// opts in). Without them the routes verify the HS256 token only.
func withAccessCheck(cfg authconfig.Config, logger *slog.Logger, signer authapi.AccessSigner, issuer *authapi.Issuer) (*authapi.Issuer, error) {
	keys, ok := signer.(authapi.KeySetSource)
	if cfg.TokenIssuerURL == "" || !ok {
		return issuer, nil
	}
	check, err := authapi.NewAccessCheck(authapi.AccessCheckConfig{
		IssuerURL: cfg.TokenIssuerURL, Keys: keys, Legacy: issuer, Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return issuer.WithAccessCheck(check), nil
}

// credentialIssuer is the issuer of the password routes: the one production
// builder of the session routes' issuer, so the access token a password change
// returns is of the configured class and its signing kid is recorded.
func credentialIssuer(cfg authconfig.Config, logger *slog.Logger, signer authapi.AccessSigner, recorder authapi.KidRecorder) (*authapi.Issuer, error) {
	issuer, err := newRecordingIssuer(cfg, signer, recorder)
	if err != nil {
		return nil, err
	}
	return withAccessCheck(cfg, logger, signer, issuer)
}
