package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/api/githubapp"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// statePurposeSecrets returns the SSO login-state key and the GitHub App
// install-state signer. Each reads its own successor key; JWT_SECRET_KEY is the
// fallback while a successor is unset, until the session key is retired. With a
// successor set, states minted under the fallback still verify (planned rotation overlap).
func statePurposeSecrets(cfg config.Config) (ssoState, ssoFallback string, githubState githubapp.Signer) {
	jwt := cfg.APIJWTSecret.Reveal()
	ssoState, githubSecret := jwt, jwt
	signer := githubapp.Signer{Issuer: cfg.APIJWTIssuer, Audience: cfg.APIJWTAudience}
	if cfg.SSOStateSecret.Configured() {
		ssoState, ssoFallback = cfg.SSOStateSecret.Reveal(), jwt
	}
	if cfg.GitHubAppStateSecret.Configured() {
		githubSecret, signer.FallbackSecret = cfg.GitHubAppStateSecret.Reveal(), jwt
	}
	signer.Secret = githubSecret
	return ssoState, ssoFallback, signer
}
