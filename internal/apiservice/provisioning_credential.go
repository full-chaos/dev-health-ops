package apiservice

import (
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
)

// The credential go-api's provisioning clients (member, invite, organization
// deprovision, reactivate and suspend) present to auth-service.
const (
	// EnvAuthProvisioningCredential names it: unset, empty or credentialWorkload. The session credential is retired
	// on the provisioning routes, so credentialSessionCompat (or any other value) is a start-up error.
	EnvAuthProvisioningCredential = "AUTH_PROVISIONING_CREDENTIAL"
	// EnvAuthSessionIssueCredential names the credential of go-api's session issue client: unset, empty or
	// credentialWorkload. The session credential is retired on the session issue routes, so credentialSessionCompat
	// (or any other value) is a start-up error.
	EnvAuthSessionIssueCredential = "AUTH_SESSION_ISSUE_CREDENTIAL"
	// The workload inputs, read by workloadinput.NewClient.
	EnvAuthWorkloadSubjectFile   = workloadinput.EnvSubjectFile
	EnvAuthWorkloadBootstrapFile = workloadinput.EnvBootstrapFile
	EnvAuthWorkloadIssuer        = workloadinput.EnvIssuer
)

const (
	credentialWorkload      = workloadinput.CredentialWorkload
	credentialSessionCompat = "session_compat"
)

// provisioningCredential is the provisioning clients' credential, made once at start-up: the workload token, set
// whenever AUTH_PROXY_ORG_PROVISIONING is on. The zero value (the switch off) builds no client.
type provisioningCredential struct {
	workload *workloadtoken.Client
}

// sessionCredential is the session issue client's credential, made once at start-up: the workload token, set whenever
// AUTH_PROXY_SESSIONS is on. The zero value (the switch off) builds no client.
type sessionCredential struct {
	workload *workloadtoken.Client
}

// workloadAsk is one switch that is on and one selector that chose the workload credential.
type workloadAsk = workloadinput.Ask

// newCredentials takes the workload credential whenever AUTH_PROXY_ORG_PROVISIONING is on (AUTH_PROVISIONING_CREDENTIAL,
// when set, must say workload) and whenever AUTH_PROXY_SESSIONS is on (AUTH_SESSION_ISSUE_CREDENTIAL, when set, must
// say workload). When either asks for workload ONE workloadtoken.Client
// is built from the shared inputs and both clients share it. A selected workload credential with a missing or invalid
// input is a start-up error naming it; there is no fallback to the session credential here or per request.
func newCredentials(cfg AuthProxyConfig, lookup func(string) (string, bool)) (provisioningCredential, sessionCredential, error) {
	var asks []workloadAsk
	provisioningAsks, sessionAsks := false, false
	if cfg.OrgProvisioningOn() {
		value, _ := lookup(EnvAuthProvisioningCredential)
		if strings.TrimSpace(value) != "" && strings.TrimSpace(value) != credentialWorkload {
			return provisioningCredential{}, sessionCredential{}, fmt.Errorf("%s is on: %s must be %s (the %s credential is retired on the provisioning routes)",
				EnvAuthProxyOrgProvisioning, EnvAuthProvisioningCredential, credentialWorkload, credentialSessionCompat)
		}
		provisioningAsks = true
		asks = append(asks, workloadAsk{Flag: EnvAuthProxyOrgProvisioning, Selector: EnvAuthProvisioningCredential})
	}
	if cfg.Enabled(AuthSessions) {
		sessionsFlag := AuthProxyFlagEnv(AuthSessions)
		// The value is compared as it is: a padded or whitespace-only value is not one of the two.
		if value, _ := lookup(EnvAuthSessionIssueCredential); value != "" && value != credentialWorkload {
			return provisioningCredential{}, sessionCredential{}, fmt.Errorf("%s is on: %s must be %s (the %s credential is retired on the session issue routes)",
				sessionsFlag, EnvAuthSessionIssueCredential, credentialWorkload, credentialSessionCompat)
		}
		sessionAsks = true
		asks = append(asks, workloadAsk{Flag: sessionsFlag, Selector: EnvAuthSessionIssueCredential})
	}
	if len(asks) == 0 {
		return provisioningCredential{}, sessionCredential{}, nil
	}
	client, err := newWorkloadClient(cfg, lookup, asks)
	if err != nil {
		return provisioningCredential{}, sessionCredential{}, err
	}
	var provisioning provisioningCredential
	var session sessionCredential
	if provisioningAsks {
		provisioning.workload = client
	}
	if sessionAsks {
		session.workload = client
	}
	return provisioning, session, nil
}

// newWorkloadClient builds the one workloadtoken.Client over the origin of AUTH_PROXY_URL (workloadinput.NewClient).
func newWorkloadClient(cfg AuthProxyConfig, lookup func(string) (string, bool), asks []workloadAsk) (*workloadtoken.Client, error) {
	return workloadinput.NewClient(cfg.Upstream, cfg.Timeout, lookup, asks)
}
