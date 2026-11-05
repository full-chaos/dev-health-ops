package workflows_test

import (
	"net/url"
	"slices"
	"sort"
	"testing"
)

// CHAOS-13107: the local stack issues EdDSA sessions through its own auth-service, as prod does. go-api proxies
// sessions to auth-service (it never falls back to minting HS256 itself), auth-service mints the eddsa class, every
// auth service starts wherever go-api starts, and go-api verifies against the issuer auth-service signs under. No
// docker runs here: the file is read as YAML and ${VAR:-default} resolves to its default, as with nothing set.

// eddsaComposeAuthServices are the services go-api's sessions need: auth-service, its one-shots and the https
// issuer front with its TLS step.
var eddsaComposeAuthServices = []string{
	"auth-keys-init", "auth-provision", "auth-owner-grants", "auth-migrate", "auth-grants", "auth-fence-init",
	"auth-workload-volume", "auth-workload-seed", "auth-workload-bootstrap", "auth-service",
	"auth-issuer", "auth-issuer-tls-init",
}

// runsWherever reports whether a service with profiles dep starts in every profile set that starts a service with
// profiles svc. No profiles means every profile set, the default one included.
func runsWherever(dep, svc []string) bool {
	if len(dep) == 0 {
		return true
	}
	if len(svc) == 0 {
		return false
	}
	for _, p := range svc {
		if !slices.Contains(dep, p) {
			return false
		}
	}
	return true
}

func TestComposeIssuesEdDSASessionsThroughAuthService(t *testing.T) {
	c := readComposeFile(t, "compose.yml")
	goAPI, ok := c.Services["go-api"]
	if !ok {
		t.Fatal("compose.yml has no service go-api")
	}
	authService, ok := c.Services["auth-service"]
	if !ok {
		t.Fatal("compose.yml has no service auth-service")
	}
	resolved := func(svc composeService, name string) string {
		v, _ := envString(t, svc, name)
		return expandDefaults(v, nil)
	}

	if got := resolved(goAPI, "AUTH_PROXY_SESSIONS"); got != "true" {
		t.Errorf("go-api AUTH_PROXY_SESSIONS resolves to %q, want \"true\": without it go-api mints HS256 sessions itself", got)
	}
	if got := resolved(authService, "AUTH_ACCESS_TOKEN_CLASS"); got != "eddsa" {
		t.Errorf("auth-service AUTH_ACCESS_TOKEN_CLASS resolves to %q, want \"eddsa\": the default class is hs256_compat", got)
	}

	for _, name := range eddsaComposeAuthServices {
		svc, ok := c.Services[name]
		if !ok {
			t.Errorf("compose.yml has no service %s", name)
			continue
		}
		if !runsWherever(svc.Profiles, goAPI.Profiles) {
			t.Errorf("%s profiles = %v, go-api profiles = %v: %s must start in every profile go-api starts in", name, svc.Profiles, goAPI.Profiles, name)
		}
	}
	// Every service go-api waits for, directly or not, starts where go-api starts.
	seen := map[string]bool{}
	queue := []string{"go-api"}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		deps := make([]string, 0, len(c.Services[name].DependsOn))
		for dep := range c.Services[name].DependsOn {
			deps = append(deps, dep)
		}
		sort.Strings(deps)
		for _, dep := range deps {
			if seen[dep] {
				continue
			}
			seen[dep] = true
			queue = append(queue, dep)
			if svc, ok := c.Services[dep]; ok && !runsWherever(svc.Profiles, goAPI.Profiles) {
				t.Errorf("go-api waits for %s (profiles %v), which does not start in every profile go-api starts in", dep, svc.Profiles)
			}
		}
	}
	if got := goAPI.DependsOn["auth-service"].Condition; got != "service_healthy" {
		t.Errorf("go-api depends_on auth-service condition = %q, want service_healthy: go-api must not take a login before auth-service is ready", got)
	}

	issuerGo := resolved(goAPI, "AUTH_ACCESS_TOKEN_ISSUER")
	issuerAuth := resolved(authService, "AUTH_TOKEN_ISSUER_URL")
	if issuerGo != issuerAuth {
		t.Errorf("go-api AUTH_ACCESS_TOKEN_ISSUER resolves to %q, auth-service AUTH_TOKEN_ISSUER_URL to %q: they must be equal, a token carries the issuer it was signed under", issuerGo, issuerAuth)
	}
	if u, err := url.Parse(issuerGo); err != nil || u.Scheme != "https" || u.Host == "" {
		t.Errorf("go-api AUTH_ACCESS_TOKEN_ISSUER resolves to %q, want an https URL: go-api fetches the issuer's JWKS over https only", issuerGo)
	}
}
