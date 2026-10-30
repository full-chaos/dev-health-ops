package apiservice

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
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
	// EnvAuthWorkloadSubjectFile is a projected ServiceAccount token file,
	// the workload exchange's subject on Kubernetes.
	EnvAuthWorkloadSubjectFile = "AUTH_WORKLOAD_SUBJECT_FILE"
	// EnvAuthWorkloadBootstrapFile is a workload bootstrap credential file,
	// the workload exchange's subject on Compose.
	EnvAuthWorkloadBootstrapFile = "AUTH_WORKLOAD_BOOTSTRAP_FILE"
	// EnvAuthWorkloadIssuer is the issuer (auth-service's token issuer URL)
	// every workload token must carry.
	EnvAuthWorkloadIssuer = "AUTH_WORKLOAD_ISSUER"
)

const (
	credentialWorkload      = "workload"
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
type workloadAsk struct{ flag, selector string }

// selectsWorkload reads selector when flag is on: false for unset and session_compat, true for workload, and a
// start-up error naming the selector for any other value.
func selectsWorkload(lookup func(string) (string, bool), flag, selector string) (bool, error) {
	value, _ := lookup(selector)
	switch strings.TrimSpace(value) {
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
		ok, err := selectsWorkload(lookup, EnvAuthProxyOrgProvisioning, EnvAuthProvisioningCredential)
		if err != nil {
			return provisioningCredential{}, sessionCredential{}, err
		}
		if ok {
			provisioningAsks = true
			asks = append(asks, workloadAsk{EnvAuthProxyOrgProvisioning, EnvAuthProvisioningCredential})
		}
	}
	if cfg.Enabled(AuthSessions) {
		sessionsFlag := AuthProxyFlagEnv(AuthSessions)
		ok, err := selectsWorkload(lookup, sessionsFlag, EnvAuthSessionIssueCredential)
		if err != nil {
			return provisioningCredential{}, sessionCredential{}, err
		}
		if ok {
			sessionAsks = true
			asks = append(asks, workloadAsk{sessionsFlag, EnvAuthSessionIssueCredential})
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

// newWorkloadClient builds the one workloadtoken.Client from AUTH_WORKLOAD_SUBJECT_FILE | AUTH_WORKLOAD_BOOTSTRAP_FILE,
// AUTH_WORKLOAD_ISSUER and the origin of AUTH_PROXY_URL. Every error names the switches and selectors that asked for
// it and the missing or invalid variable, never a path.
func newWorkloadClient(cfg AuthProxyConfig, lookup func(string) (string, bool), asks []workloadAsk) (*workloadtoken.Client, error) {
	parts := make([]string, 0, len(asks))
	for _, ask := range asks {
		parts = append(parts, fmt.Sprintf("%s is on and %s=%s", ask.flag, ask.selector, credentialWorkload))
	}
	fail := func(format string, args ...any) (*workloadtoken.Client, error) {
		return nil, fmt.Errorf("%s: %s", strings.Join(parts, ", "), fmt.Sprintf(format, args...))
	}
	subjectPath, _ := lookup(EnvAuthWorkloadSubjectFile)
	bootstrapPath, _ := lookup(EnvAuthWorkloadBootstrapFile)
	subjectPath, bootstrapPath = strings.TrimSpace(subjectPath), strings.TrimSpace(bootstrapPath)
	var subject workloadtoken.Subject
	var subjectEnv, path string
	switch {
	case subjectPath != "" && bootstrapPath != "":
		return fail("set only one of %s and %s", EnvAuthWorkloadSubjectFile, EnvAuthWorkloadBootstrapFile)
	case subjectPath != "":
		subject, subjectEnv, path = workloadtoken.KubernetesTokenFile(subjectPath), EnvAuthWorkloadSubjectFile, subjectPath
	case bootstrapPath != "":
		subject, subjectEnv, path = workloadtoken.BootstrapCredentialFile(bootstrapPath), EnvAuthWorkloadBootstrapFile, bootstrapPath
	default:
		return fail("set %s or %s", EnvAuthWorkloadSubjectFile, EnvAuthWorkloadBootstrapFile)
	}
	if !readableFile(path) {
		return fail("%s must name a readable file", subjectEnv)
	}
	issuer, _ := lookup(EnvAuthWorkloadIssuer)
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		return fail("%s is not set", EnvAuthWorkloadIssuer)
	}
	if u, err := url.Parse(issuer); err != nil || u.Scheme != "https" || u.Host == "" {
		return fail("%s must be an https URL", EnvAuthWorkloadIssuer)
	}
	client, err := workloadtoken.New(workloadtoken.Config{
		ExchangeURL:    cfg.Upstream.String(),
		AllowPlainHTTP: cfg.Upstream.Scheme == "http",
		Subject:        subject,
		Issuer:         issuer,
		Timeout:        cfg.Timeout,
		Transport:      exchangeTransport(),
	})
	if err != nil {
		return fail("workload token client: %v", err)
	}
	return client, nil
}

// exchangeTransport carries the workload exchange: the shared outbound transport, no environment proxy.
func exchangeTransport() *http.Transport { return httpguard.SharedTransport() }

// readableFile reports whether path is a regular file this process can open.
// It reads nothing from it, and callers never put the path in an error.
func readableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	_ = file.Close()
	return true
}
