package apiservice

import (
	"log/slog"
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/acrcredentials"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
)

// The acr client of the credential revoke route (acrcredentials): acr's internal API origin, its token endpoint and
// the projected ServiceAccount token of go-api's workload, which acr binds with the role "revoke" in each
// organization (one binding per organization, provisioned out of band).
const (
	EnvACRRevokeURL         = "ACR_REVOKE_URL"
	EnvACRRevokeTokenURL    = "ACR_REVOKE_TOKEN_URL"
	EnvACRRevokeSubjectFile = "ACR_REVOKE_SUBJECT_FILE"
	// EnvACRRevokeAllowClusterHTTP "true" also accepts plain-http origins on a cluster-local Service host
	// (<name>.svc.cluster.local); unset or "false" accepts https (and loopback http) only. Any other set value, blank
	// included, is refused.
	EnvACRRevokeAllowClusterHTTP = "ACR_REVOKE_ALLOW_CLUSTER_HTTP"
)

// newACRRevoker builds the revoke route's acr client from the four variables. All unset is "not configured": the
// route answers 503. A partial or unusable set never stops start-up: it logs which variable is wrong (never a value)
// and the route answers 503, the same as not configured.
func newACRRevoker(lookup func(string) (string, bool), logger *slog.Logger) acrcredentials.Revoker {
	read := func(name string) string {
		value, _ := lookup(name)
		return strings.TrimSpace(value)
	}
	base, token, subject := read(EnvACRRevokeURL), read(EnvACRRevokeTokenURL), read(EnvACRRevokeSubjectFile)
	allowValue, allowSet := lookup(EnvACRRevokeAllowClusterHTTP)
	if base == "" && token == "" && subject == "" && !allowSet {
		logger.Info("api: acr credential revoke not configured; the revoke route answers 503")
		return nil
	}
	refuse := func(variable string) acrcredentials.Revoker {
		logger.Error("api: acr credential revoke configuration refused; the revoke route answers 503", slog.String("variable", variable))
		return nil
	}
	var allow bool
	switch {
	case !allowSet:
	case strings.TrimSpace(allowValue) == "true":
		allow = true
	case strings.TrimSpace(allowValue) != "false":
		return refuse(EnvACRRevokeAllowClusterHTTP)
	}
	// A value that does not parse is a nil URL, which ValidOrigin refuses.
	baseURL, _ := url.Parse(base)
	if !acrrevoke.ValidOrigin(baseURL, allow) || baseURL.RawQuery != "" {
		return refuse(EnvACRRevokeURL)
	}
	tokenURL, _ := url.Parse(token)
	if !acrrevoke.ValidOrigin(tokenURL, allow) {
		return refuse(EnvACRRevokeTokenURL)
	}
	client, err := acrrevoke.New(acrrevoke.Config{BaseURL: baseURL, TokenEndpoint: tokenURL, SubjectTokenFile: subject, AllowClusterHTTP: allow})
	if err != nil {
		return refuse(EnvACRRevokeSubjectFile)
	}
	return client
}
