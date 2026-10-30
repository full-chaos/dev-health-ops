package apiservice

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
)

// The credential go-api's provisioning clients (member, invite, organization
// deprovision, reactivate and suspend) present to auth-service.
const (
	// EnvAuthProvisioningCredential selects it: credentialWorkload or
	// credentialSessionCompat. Unset is credentialSessionCompat.
	EnvAuthProvisioningCredential = "AUTH_PROVISIONING_CREDENTIAL"
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

// provisioningCredential is the credential choice, made once at start-up. The
// zero value is session_compat: the clients present AUTH_SESSION_ISSUE_TOKEN.
// With workload set they present its workload token and read no session
// credential.
type provisioningCredential struct {
	workload *workloadtoken.Client
}

// newProvisioningCredential reads AUTH_PROVISIONING_CREDENTIAL when
// AUTH_PROXY_ORG_PROVISIONING is on. A selected workload credential with a
// missing or invalid input is a start-up error naming it; there is no
// fallback to the session credential here or per request.
func newProvisioningCredential(cfg AuthProxyConfig, lookup func(string) (string, bool)) (provisioningCredential, error) {
	if !cfg.OrgProvisioningOn() {
		return provisioningCredential{}, nil
	}
	selector, _ := lookup(EnvAuthProvisioningCredential)
	switch strings.TrimSpace(selector) {
	case "", credentialSessionCompat:
		return provisioningCredential{}, nil
	case credentialWorkload:
	default:
		return provisioningCredential{}, fmt.Errorf("%s is on: %s must be %s or %s",
			EnvAuthProxyOrgProvisioning, EnvAuthProvisioningCredential, credentialWorkload, credentialSessionCompat)
	}
	fail := func(format string, args ...any) (provisioningCredential, error) {
		return provisioningCredential{}, fmt.Errorf("%s is on and %s=%s: %s", EnvAuthProxyOrgProvisioning,
			EnvAuthProvisioningCredential, credentialWorkload, fmt.Sprintf(format, args...))
	}
	subjectPath, _ := lookup(EnvAuthWorkloadSubjectFile)
	bootstrapPath, _ := lookup(EnvAuthWorkloadBootstrapFile)
	subjectPath, bootstrapPath = strings.TrimSpace(subjectPath), strings.TrimSpace(bootstrapPath)
	var subject workloadtoken.Subject
	switch {
	case subjectPath != "" && bootstrapPath != "":
		return fail("set only one of %s and %s", EnvAuthWorkloadSubjectFile, EnvAuthWorkloadBootstrapFile)
	case subjectPath != "":
		subject = workloadtoken.KubernetesTokenFile(subjectPath)
	case bootstrapPath != "":
		subject = workloadtoken.BootstrapCredentialFile(bootstrapPath)
	default:
		return fail("set %s or %s", EnvAuthWorkloadSubjectFile, EnvAuthWorkloadBootstrapFile)
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
	return provisioningCredential{workload: client}, nil
}

// exchangeTransport carries the workload exchange: a dedicated transport with
// no environment proxy, as the provisioning transport.
func exchangeTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          2,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
}
