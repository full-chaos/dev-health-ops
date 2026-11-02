package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/api/githubapp"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

func statePurposeSecrets(cfg config.Config) (ssoState string, githubState githubapp.Signer) {
	jwt := cfg.APIJWTSecret.Reveal()
	return jwt, githubapp.Signer{Secret: jwt, Issuer: cfg.APIJWTIssuer, Audience: cfg.APIJWTAudience}
}
