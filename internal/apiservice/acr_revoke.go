package apiservice

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/api/acrcredentials"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
)

// The acr client of the credential revoke route (acrcredentials): acr's internal API origin, its token endpoint and
// the projected ServiceAccount token of go-api's workload, which acr binds with the role "revoke" in each
// organization (one binding per organization, provisioned out of band).
const (
	EnvACRRevokeURL         = acrrevoke.EnvURL
	EnvACRRevokeTokenURL    = acrrevoke.EnvTokenURL
	EnvACRRevokeSubjectFile = acrrevoke.EnvSubjectFile
	// EnvACRRevokeAllowClusterHTTP "true" also accepts plain-http origins on a cluster-local Service host
	// (<name>.svc.cluster.local); unset or "false" accepts https (and loopback http) only. Any other set value, blank
	// included, is refused.
	EnvACRRevokeAllowClusterHTTP = acrrevoke.EnvAllowClusterHTTP
)

// newACRRevoker builds the revoke route's acr client from the four variables. All unset is "not configured": the
// route answers 503. A partial or unusable set never stops start-up: it logs which variable is wrong (never a value)
// and the route answers 503, the same as not configured.
func newACRRevoker(lookup func(string) (string, bool), logger *slog.Logger) acrcredentials.Revoker {
	revoker, _ := acrRevokers(lookup, logger)
	return revoker
}

// acrRevokers builds the credential revoke route's client and the admin organization revoke from the same four
// variables: both nil when unset.
func acrRevokers(lookup func(string) (string, bool), logger *slog.Logger) (acrcredentials.Revoker, admin.OrgCredentialRevoker) {
	client, configured, refused := acrrevoke.FromEnv(lookup)
	switch {
	case !configured:
		logger.Info("api: acr credential revoke not configured; the revoke route answers 503")
		return nil, nil
	case refused != "":
		logger.Error("api: acr credential revoke configuration refused; the revoke route answers 503", slog.String("variable", refused))
		return nil, admin.UnusableOrgCredentialRevoker(refused)
	}
	return client, client
}
