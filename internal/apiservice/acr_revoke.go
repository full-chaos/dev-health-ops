package apiservice

import (
	"log/slog"
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/acrcredentials"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
)

// The acr client of the credential revoke route (acrcredentials): acr's internal API origin, its token endpoint and
// the projected ServiceAccount token of go-api's workload, which acr binds with the role "revoke" in each
// organization (one binding per organization, provisioned out of band).
const (
	EnvACRRevokeURL         = "ACR_REVOKE_URL"
	EnvACRRevokeTokenURL    = "ACR_REVOKE_TOKEN_URL"
	EnvACRRevokeSubjectFile = "ACR_REVOKE_SUBJECT_FILE"
)

// newACRRevoker builds the revoke route's acr client from the three variables. All unset is "not configured": the
// route answers 503. A partial or unusable set never stops start-up: it logs which variable is wrong (never a value)
// and the route answers 503, the same as not configured.
func newACRRevoker(lookup func(string) (string, bool), logger *slog.Logger) acrcredentials.Revoker {
	read := func(name string) string {
		value, _ := lookup(name)
		return strings.TrimSpace(value)
	}
	base, token, subject := read(EnvACRRevokeURL), read(EnvACRRevokeTokenURL), read(EnvACRRevokeSubjectFile)
	if base == "" && token == "" && subject == "" {
		logger.Info("api: acr credential revoke not configured; the revoke route answers 503")
		return nil
	}
	refuse := func(variable string) acrcredentials.Revoker {
		logger.Error("api: acr credential revoke configuration refused; the revoke route answers 503", slog.String("variable", variable))
		return nil
	}
	baseURL, err := url.Parse(base)
	if err != nil || !acrlisting.ValidOrigin(baseURL) || baseURL.RawQuery != "" {
		return refuse(EnvACRRevokeURL)
	}
	tokenURL, err := url.Parse(token)
	if err != nil || !acrlisting.ValidOrigin(tokenURL) {
		return refuse(EnvACRRevokeTokenURL)
	}
	client, err := acrrevoke.New(acrrevoke.Config{BaseURL: baseURL, TokenEndpoint: tokenURL, SubjectTokenFile: subject})
	if err != nil {
		return refuse(EnvACRRevokeSubjectFile)
	}
	return client
}
