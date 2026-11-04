// Package workloadinput reads a service's workload-exchange inputs and builds its one workloadtoken.Client. go-api
// and query-api share it, so both read the same variables with the same checks.
package workloadinput

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

const (
	// EnvSubjectFile is a projected ServiceAccount token file,
	// the workload exchange's subject on Kubernetes.
	EnvSubjectFile = "AUTH_WORKLOAD_SUBJECT_FILE"
	// EnvBootstrapFile is a workload bootstrap credential file,
	// the workload exchange's subject on Compose.
	EnvBootstrapFile = "AUTH_WORKLOAD_BOOTSTRAP_FILE"
	// EnvIssuer is the issuer (auth-service's token issuer URL)
	// every workload token must carry.
	EnvIssuer = "AUTH_WORKLOAD_ISSUER"
	// EnvProxyURL is the auth-service origin the workload exchange is sent to.
	EnvProxyURL = "AUTH_PROXY_URL"
)

// CredentialWorkload is the selector value that chose the workload credential.
const CredentialWorkload = "workload"

// Ask is one switch that is on and one selector that chose the workload credential.
type Ask struct{ Flag, Selector string }

// InputError is a workload client error that names the bad input; its text is what the error always said.
type InputError struct {
	Variable string
	text     string
}

func (e *InputError) Error() string { return e.text }

// ParseOrigin reads raw as a bare http(s) origin: a scheme and a host, no credentials, path, query or fragment.
func ParseOrigin(raw string) (*url.URL, bool) {
	upstream, err := url.Parse(raw)
	if err != nil || (upstream.Scheme != "http" && upstream.Scheme != "https") || upstream.Host == "" || upstream.User != nil ||
		(upstream.Path != "" && upstream.Path != "/") || upstream.RawQuery != "" || upstream.Fragment != "" || upstream.Opaque != "" {
		return nil, false
	}
	return &url.URL{Scheme: upstream.Scheme, Host: upstream.Host}, true
}

// NewClient builds the one workloadtoken.Client from AUTH_WORKLOAD_SUBJECT_FILE | AUTH_WORKLOAD_BOOTSTRAP_FILE,
// AUTH_WORKLOAD_ISSUER and the auth-service origin upstream. Every error names the switches and selectors that asked
// for it and the missing or invalid variable, never a path.
func NewClient(upstream *url.URL, timeout time.Duration, lookup func(string) (string, bool), asks []Ask) (*workloadtoken.Client, error) {
	parts := make([]string, 0, len(asks))
	for _, ask := range asks {
		parts = append(parts, fmt.Sprintf("%s is on and %s=%s", ask.Flag, ask.Selector, CredentialWorkload))
	}
	fail := func(variable, format string, args ...any) (*workloadtoken.Client, error) {
		return nil, &InputError{Variable: variable, text: fmt.Sprintf("%s: %s", strings.Join(parts, ", "), fmt.Sprintf(format, args...))}
	}
	bothFiles := EnvSubjectFile + "," + EnvBootstrapFile
	subjectPath, _ := lookup(EnvSubjectFile)
	bootstrapPath, _ := lookup(EnvBootstrapFile)
	subjectPath, bootstrapPath = strings.TrimSpace(subjectPath), strings.TrimSpace(bootstrapPath)
	var subject workloadtoken.Subject
	var subjectEnv, path string
	switch {
	case subjectPath != "" && bootstrapPath != "":
		return fail(bothFiles, "set only one of %s and %s", EnvSubjectFile, EnvBootstrapFile)
	case subjectPath != "":
		subject, subjectEnv, path = workloadtoken.KubernetesTokenFile(subjectPath), EnvSubjectFile, subjectPath
	case bootstrapPath != "":
		subject, subjectEnv, path = workloadtoken.BootstrapCredentialFile(bootstrapPath), EnvBootstrapFile, bootstrapPath
	default:
		return fail(bothFiles, "set %s or %s", EnvSubjectFile, EnvBootstrapFile)
	}
	if !readableFile(path) {
		return fail(subjectEnv, "%s must name a readable file", subjectEnv)
	}
	issuer, _ := lookup(EnvIssuer)
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		return fail(EnvIssuer, "%s is not set", EnvIssuer)
	}
	if u, err := url.Parse(issuer); err != nil || u.Scheme != "https" || u.Host == "" {
		return fail(EnvIssuer, "%s must be an https URL", EnvIssuer)
	}
	client, err := workloadtoken.New(workloadtoken.Config{
		ExchangeURL:    upstream.String(),
		AllowPlainHTTP: upstream.Scheme == "http",
		Subject:        subject,
		Issuer:         issuer,
		Timeout:        timeout,
		Transport:      Transport(),
	})
	if err != nil {
		var refused *workloadtoken.ConfigFieldError
		if errors.As(err, &refused) {
			if variable := refusedConfigVariable(refused.Field, subjectEnv); variable != "" {
				return fail(variable, "%s is refused by the workload token client: %v", variable, err)
			}
		}
		return fail("", "workload token client: %v", err)
	}
	return client, nil
}

// refusedConfigVariable names the environment variable a refused workloadtoken.Config field was read from. The
// timeout and the transport are set in code, so they name none.
func refusedConfigVariable(field workloadtoken.ConfigField, subjectEnv string) string {
	switch field {
	case workloadtoken.FieldIssuer:
		return EnvIssuer
	case workloadtoken.FieldExchangeURL:
		return EnvProxyURL
	case workloadtoken.FieldSubject:
		return subjectEnv
	}
	return ""
}

// Transport carries the workload exchange: the shared outbound transport, no environment proxy.
func Transport() *http.Transport { return httpguard.SharedTransport() }

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
