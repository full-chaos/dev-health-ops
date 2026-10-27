package authconfig

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The workload-token exchange settings. The exchange is off unless
// EnvWorkloadExchange is "true"; on, every other setting is required and
// startup refuses without it. No setting has a default: the chart that turns
// the exchange on supplies each value.
const (
	// EnvWorkloadExchange ("true" or "false", default false) mounts the
	// workload-token exchange route.
	EnvWorkloadExchange = "AUTH_WORKLOAD_EXCHANGE"
	// EnvWorkloadTrustDomain names this cluster in the workload bindings.
	EnvWorkloadTrustDomain = "AUTH_WORKLOAD_TRUST_DOMAIN"
	// EnvWorkloadClusterIssuer is the ServiceAccount token issuer of the
	// cluster whose API server reviews the tokens.
	EnvWorkloadClusterIssuer = "AUTH_WORKLOAD_EXCHANGE_CLUSTER_ISSUER"
	// EnvWorkloadKubeAPIURL is that API server's https origin.
	EnvWorkloadKubeAPIURL = "AUTH_WORKLOAD_EXCHANGE_KUBE_API_URL"
	// EnvWorkloadKubeCAFile is the PEM bundle the API server must chain to.
	EnvWorkloadKubeCAFile = "AUTH_WORKLOAD_EXCHANGE_KUBE_CA_FILE"
	// EnvWorkloadReviewerTokenFile is auth-service's own token file, allowed
	// to create tokenreviews.
	EnvWorkloadReviewerTokenFile = "AUTH_WORKLOAD_EXCHANGE_REVIEWER_TOKEN_FILE"
)

// WorkloadExchange is the exchange's configuration; Enabled false leaves every
// other field empty.
type WorkloadExchange struct {
	Enabled           bool
	TrustDomain       string
	ClusterIssuer     string
	KubeAPIURL        string
	KubeCAFile        string
	ReviewerTokenFile string
}

// workloadExchange reads the exchange settings. Errors name the variable,
// never its value.
func workloadExchange(lookup secrets.LookupEnv, tokenIssuerURL string) (WorkloadExchange, error) {
	enabled, err := strictBool(lookup, EnvWorkloadExchange)
	if err != nil || !enabled {
		return WorkloadExchange{}, err
	}
	if tokenIssuerURL == "" {
		return WorkloadExchange{}, fmt.Errorf("%s=true needs %s", EnvWorkloadExchange, EnvTokenIssuerURL)
	}
	out := WorkloadExchange{Enabled: true}
	for _, field := range []struct {
		name string
		max  int
		into *string
	}{
		{EnvWorkloadTrustDomain, 253, &out.TrustDomain},
		{EnvWorkloadClusterIssuer, 512, &out.ClusterIssuer},
	} {
		value := valueOf(lookup, field.name)
		if value == "" || len(value) > field.max || strings.TrimSpace(value) != value {
			return WorkloadExchange{}, fmt.Errorf("%s=true needs %s: non-empty, at most %d bytes, no surrounding whitespace",
				EnvWorkloadExchange, field.name, field.max)
		}
		*field.into = value
	}
	out.KubeAPIURL = valueOf(lookup, EnvWorkloadKubeAPIURL)
	if err := HTTPSOrigin(out.KubeAPIURL); err != nil {
		return WorkloadExchange{}, fmt.Errorf("%s=true needs %s: an https origin with no path, query or credentials",
			EnvWorkloadExchange, EnvWorkloadKubeAPIURL)
	}
	for _, field := range []struct {
		name string
		into *string
	}{
		{EnvWorkloadKubeCAFile, &out.KubeCAFile},
		{EnvWorkloadReviewerTokenFile, &out.ReviewerTokenFile},
	} {
		value := valueOf(lookup, field.name)
		if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) != value {
			return WorkloadExchange{}, fmt.Errorf("%s=true needs %s: an absolute, clean path", EnvWorkloadExchange, field.name)
		}
		*field.into = value
	}
	return out, nil
}

// HTTPSOrigin accepts only "https://host[:port]" with at most one trailing
// slash: the value must equal its own scheme and host, so a path, a query (a
// bare "?" included), a fragment or userinfo is refused, and a port, when
// present, is 1 to 65535 in decimal digits (an empty port "host:" is
// refused). It is the one origin rule of the workload exchange: its config
// check and its TokenReview client both call it.
func HTTPSOrigin(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" {
		return errNotHTTPSOrigin
	}
	origin := "https://" + parsed.Host
	if raw != origin && raw != origin+"/" {
		return errNotHTTPSOrigin
	}
	if strings.HasSuffix(parsed.Host, ":") {
		return errNotHTTPSOrigin
	}
	if port := parsed.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
			return errNotHTTPSOrigin
		}
	}
	return nil
}

var errNotHTTPSOrigin = errors.New("not an https origin")
