package teamsidentity

import (
	"context"
	"net/http"
	"net/netip"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// discoveryStatusError is a pre-flight refusal the discover route answers
// with an HTTPException-shaped {"detail": ...} body, exactly as
// teams.py:189-316 does before it ever calls a provider.
type discoveryStatusError struct {
	Status int
	Detail string
}

func (e *discoveryStatusError) Error() string { return e.Detail }

const defaultGitHubAPIBase = "https://api.github.com"

// discoveryHostLookup resolves a hostname for the SSRF guard; tests replace it.
var discoveryHostLookup func(context.Context, string) ([]netip.Addr, error) = resolveHostAddrs

// prepareDiscoveryCredential turns a resolved stored credential into the
// credential discovery is allowed to send to a provider, applying
// teams.py's per-provider rules (teams.py:189-316) in the same order:
//
//   - github: a PAT, or GitHub App auth. Python ignores any stored base
//     URL for a PAT (PyGithub's default host) and for the discovery calls
//     themselves even under App auth: the App's base_url (decrypted
//     mapping only) is used solely to mint the installation token, and
//     only after _validate_external_url accepts it. The returned
//     credential therefore never carries a base URL, so discovery always
//     talks to api.github.com -- CHAOS-6311 r1 finding 1 sent the stored
//     token to whatever host config.base_url named.
//   - gitlab: token required; the host is config["url"] only (Python reads
//     nothing else) and is NOT SSRF-validated there.
//   - linear: apiKey (preferred) or api_key; LinearClient's default host,
//     never a stored base URL.
//   - jira: the Jira client's own email/api_token/base_url aliasing; a
//     credential it refuses is Python's 400.
func prepareDiscoveryCredential(ctx context.Context, provider string, credential providerfoundation.Credential) (providerfoundation.Credential, error) {
	switch provider {
	case "github":
		return prepareGitHubDiscovery(ctx, credential)
	case "gitlab":
		token, ok := credential.Secret("token")
		if !ok || !token.Configured() {
			return providerfoundation.Credential{}, &discoveryStatusError{http.StatusBadRequest, "GitLab credentials require a token"}
		}
		config := map[string]string{}
		if value, present := credential.Config["url"]; present {
			config["url"] = value
		}
		return providerfoundation.NewCredential("gitlab", credential.ID, config, map[string]secrets.Value{"token": token}), nil
	case "linear":
		key, ok := credential.Secret("apiKey")
		if !ok || !key.Configured() {
			key, ok = credential.Secret("api_key")
		}
		if !ok || !key.Configured() {
			return providerfoundation.Credential{}, &discoveryStatusError{http.StatusBadRequest, "Linear credentials require apiKey"}
		}
		return providerfoundation.NewCredential("linear", credential.ID, nil, map[string]secrets.Value{"api_key": key}), nil
	case "jira":
		credential = mergeJiraConfigIntoSecrets(credential)
		if _, err := providerfoundation.NewJiraClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{}); err != nil {
			return providerfoundation.Credential{}, &discoveryStatusError{http.StatusBadRequest, "Jira credentials require email, api_token, and url"}
		}
		return credential, nil
	}
	return credential, nil
}

func prepareGitHubDiscovery(ctx context.Context, credential providerfoundation.Credential) (providerfoundation.Credential, error) {
	if providerfoundation.ValidateCredentialShape(credential) != nil {
		return providerfoundation.Credential{}, &discoveryStatusError{http.StatusBadRequest,
			"GitHub credentials require either token or app_id + private_key + installation_id"}
	}
	if token, ok := credential.Secret("token"); ok && token.Configured() {
		return githubTokenCredential(credential, token), nil
	}
	base := defaultGitHubAPIBase
	if value, ok := credential.Secret("base_url"); ok && value.Configured() {
		base = value.Reveal()
	}
	if valid, detail := validateExternalURL(ctx, base, discoveryHostLookup); !valid {
		return providerfoundation.Credential{}, &discoveryStatusError{http.StatusBadRequest, detail}
	}
	unauthorized := &discoveryStatusError{http.StatusUnauthorized, "GitHub App authentication failed"}
	auth, err := providerfoundation.NewGitHubAppAuth(credential, base, discoveryHTTPClient)
	if err != nil {
		return providerfoundation.Credential{}, unauthorized
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base, nil)
	if err != nil {
		return providerfoundation.Credential{}, unauthorized
	}
	if err := auth.Apply(request); err != nil {
		return providerfoundation.Credential{}, unauthorized
	}
	installationToken := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if installationToken == "" {
		return providerfoundation.Credential{}, &discoveryStatusError{http.StatusBadRequest, "GitHub credential missing token"}
	}
	return githubTokenCredential(credential, secrets.NewValue(installationToken)), nil
}

func githubTokenCredential(source providerfoundation.Credential, token secrets.Value) providerfoundation.Credential {
	return providerfoundation.NewCredential("github", source.ID, nil, map[string]secrets.Value{"token": token})
}

// jiraMappingKeys are every key jira_credentials_from_mapping reads
// (resolver.py:375-388).
var jiraMappingKeys = []string{"email", "api_token", "apiToken", "token", "base_url", "baseUrl", "url", "server_url"}

// mergeJiraConfigIntoSecrets reproduces the route's
// `jira_credentials_from_mapping({**config, **decrypted})` (teams.py:303):
// each Jira key comes from the decrypted secret when it is PRESENT there
// (even empty -- dict merge lets an explicit empty value shadow config and
// the resolver's `or` chain then falls through to the next alias) and from
// the config column otherwise. The Jira client reads email and the token
// only from secrets, so a credential keeping its email or URL in config was
// refused here with Python's 400 wording where Python accepts it.
func mergeJiraConfigIntoSecrets(credential providerfoundation.Credential) providerfoundation.Credential {
	fields := map[string]secrets.Value{}
	for _, key := range jiraMappingKeys {
		if value, ok := credential.Secret(key); ok {
			fields[key] = value
		} else if value, ok := credential.Config[key]; ok {
			fields[key] = secrets.NewValue(value)
		}
	}
	return providerfoundation.NewCredential("jira", credential.ID, credential.Config, fields)
}
