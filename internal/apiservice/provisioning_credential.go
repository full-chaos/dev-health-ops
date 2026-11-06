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
	// EnvAuthProvisioningCredential selects it: credentialWorkload or
	// credentialSessionCompat. Unset is credentialSessionCompat.
	EnvAuthProvisioningCredential = "AUTH_PROVISIONING_CREDENTIAL"
	// EnvAuthSessionIssueCredential selects the credential of go-api's session issue client, the same two values; unset
	// is credentialSessionCompat.
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

// provisioningCredential is the provisioning clients' credential choice, made once at start-up. The zero value is
// session_compat: the clients present AUTH_SESSION_ISSUE_TOKEN. With workload set they present its workload token and
// read no session credential.
type provisioningCredential struct {
	workload *workloadtoken.Client
}

// sessionCredential is the session issue client's credential choice, made once at start-up, as provisioningCredential.
type sessionCredential struct {
	workload *workloadtoken.Client
}

// workloadAsk is one switch that is on and one selector that chose the workload credential.
type workloadAsk = workloadinput.Ask

// selectsWorkload reads selector when flag is on: false for unset, empty and session_compat, true for workload, and a
// start-up error naming the selector for any other value. The value is compared as it is, except that trimSpace
// (the provisioning selector only, as on main) trims it first: for any other selector a padded or whitespace-only
// value is not one of the three.
func selectsWorkload(lookup func(string) (string, bool), flag, selector string, trimSpace bool) (bool, error) {
	value, _ := lookup(selector)
	if trimSpace {
		value = strings.TrimSpace(value)
	}
	switch value {
	case "", credentialSessionCompat:
		return false, nil
	case credentialWorkload:
		return true, nil
	}
	return false, fmt.Errorf("%s is on: %s must be %s or %s", flag, selector, credentialWorkload, credentialSessionCompat)
}

// newCredentials reads AUTH_PROVISIONING_CREDENTIAL when AUTH_PROXY_ORG_PROVISIONING is on and
// AUTH_SESSION_ISSUE_CREDENTIAL when AUTH_PROXY_SESSIONS is on. When either selects workload ONE workloadtoken.Client
// is built from the shared inputs and both clients share it. A selected workload credential with a missing or invalid
// input is a start-up error naming it; there is no fallback to the session credential here or per request.
func newCredentials(cfg AuthProxyConfig, lookup func(string) (string, bool)) (provisioningCredential, sessionCredential, error) {
	var asks []workloadAsk
	provisioningAsks, sessionAsks := false, false
	if cfg.OrgProvisioningOn() {
		ok, err := selectsWorkload(lookup, EnvAuthProxyOrgProvisioning, EnvAuthProvisioningCredential, true)
		if err != nil {
			return provisioningCredential{}, sessionCredential{}, err
		}
		if ok {
			provisioningAsks = true
			asks = append(asks, workloadAsk{Flag: EnvAuthProxyOrgProvisioning, Selector: EnvAuthProvisioningCredential})
		}
	}
	if cfg.Enabled(AuthSessions) {
		sessionsFlag := AuthProxyFlagEnv(AuthSessions)
		ok, err := selectsWorkload(lookup, sessionsFlag, EnvAuthSessionIssueCredential, false)
		if err != nil {
			return provisioningCredential{}, sessionCredential{}, err
		}
		if ok {
			sessionAsks = true
			asks = append(asks, workloadAsk{Flag: sessionsFlag, Selector: EnvAuthSessionIssueCredential})
		}
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
