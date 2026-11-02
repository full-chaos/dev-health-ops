package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/api/githubapp"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// statePurposeSecrets returns the SSO login-state key and the GitHub App
// install-state signer. Each reads its own successor key; JWT_SECRET_KEY is the
// fallback while a successor is unset, until the session key is retired.
func statePurposeSecrets(cfg config.Config) (ssoState string, githubState githubapp.Signer) {
	jwt := cfg.APIJWTSecret.Reveal()
	ssoState, githubSecret := jwt, jwt
	if cfg.SSOStateSecret.Configured() {
		ssoState = cfg.SSOStateSecret.Reveal()
	}
	if cfg.GitHubAppStateSecret.Configured() {
		githubSecret = cfg.GitHubAppStateSecret.Reveal()
	}
	return ssoState, githubapp.Signer{Secret: githubSecret, Issuer: cfg.APIJWTIssuer, Audience: cfg.APIJWTAudience}
}
