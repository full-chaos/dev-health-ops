package acrrevoke

import (
	"net/url"
	"strings"
)

// The four variables that configure the client: acr's internal API origin, its token endpoint and the projected
// ServiceAccount token of the workload acr binds with the role "revoke".
const (
	EnvURL         = "ACR_REVOKE_URL"
	EnvTokenURL    = "ACR_REVOKE_TOKEN_URL"
	EnvSubjectFile = "ACR_REVOKE_SUBJECT_FILE"
	// EnvAllowClusterHTTP "true" also accepts plain-http origins on a cluster-local Service host
	// (<name>.svc.cluster.local); unset or "false" accepts https (and loopback http) only. Any other set value, blank
	// included, is refused.
	EnvAllowClusterHTTP = "ACR_REVOKE_ALLOW_CLUSTER_HTTP"
)

// FromEnv builds a Client from the four variables. All unset is (nil, false, ""): not configured. A partial or
// unusable set is (nil, true, the first refused variable's name); it never names a value.
func FromEnv(lookup func(string) (string, bool)) (client *Client, configured bool, refused string) {
	read := func(name string) string {
		value, _ := lookup(name)
		return strings.TrimSpace(value)
	}
	base, token, subject := read(EnvURL), read(EnvTokenURL), read(EnvSubjectFile)
	allowValue, allowSet := lookup(EnvAllowClusterHTTP)
	if base == "" && token == "" && subject == "" && !allowSet {
		return nil, false, ""
	}
	var allow bool
	switch {
	case !allowSet:
	case strings.TrimSpace(allowValue) == "true":
		allow = true
	case strings.TrimSpace(allowValue) != "false":
		return nil, true, EnvAllowClusterHTTP
	}
	// A value that does not parse is a nil URL, which ValidOrigin refuses. The raw checks also catch an empty "?" or "#",
	// which the parsed URL no longer shows.
	baseURL, _ := url.Parse(base)
	if strings.ContainsAny(base, "?#") || !ValidOrigin(baseURL, allow) {
		return nil, true, EnvURL
	}
	tokenURL, _ := url.Parse(token)
	if strings.Contains(token, "#") || !ValidOrigin(tokenURL, allow) {
		return nil, true, EnvTokenURL
	}
	c, err := New(Config{BaseURL: baseURL, TokenEndpoint: tokenURL, SubjectTokenFile: subject, AllowClusterHTTP: allow})
	if err != nil {
		return nil, true, EnvSubjectFile
	}
	return c, true, ""
}
