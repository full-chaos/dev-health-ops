package teamsidentity

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// recordingDoer answers every request 404 (the calls under test are
// expected to fail after being observed) and records where each went.
type recordingDoer struct{ hosts []string }

func (d *recordingDoer) Do(request *http.Request) (*http.Response, error) {
	d.hosts = append(d.hosts, request.URL.Host)
	return (&githubStubDoer{}).Do(request)
}

func withDiscoveryClient(t *testing.T, doer providerfoundation.HTTPDoer) {
	t.Helper()
	previous, previousExchange := discoveryHTTPClient, discoveryAppExchangeClient
	discoveryHTTPClient, discoveryAppExchangeClient = doer, doer
	t.Cleanup(func() { discoveryHTTPClient, discoveryAppExchangeClient = previous, previousExchange })
}

func withHostLookup(t *testing.T, lookup func(context.Context, string) ([]netip.Addr, error)) {
	t.Helper()
	previous := discoveryHostLookup
	discoveryHostLookup = lookup
	t.Cleanup(func() { discoveryHostLookup = previous })
}

func assertStatus(t *testing.T, err error, status int, detail string) {
	t.Helper()
	var statusErr *discoveryStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("err = %v, want *discoveryStatusError", err)
	}
	if statusErr.Status != status || statusErr.Detail != detail {
		t.Errorf("got %d %q, want %d %q", statusErr.Status, statusErr.Detail, status, detail)
	}
}

// TestDiscoveryNeverSendsCredentialsToAStoredBaseURL is CHAOS-6311 r1
// finding 1: Python's github (PAT) and linear discovery talk to their
// providers' standard hosts and ignore any stored base URL, but the Go
// clients used to honor base_url/url/gitlab_url from the decrypted secret
// or the config column, so a stored credential pointing at an arbitrary
// host received the provider token.
func TestDiscoveryNeverSendsCredentialsToAStoredBaseURL(t *testing.T) {
	const evil = "https://evil.example"
	config := map[string]string{"base_url": evil, "url": evil, "gitlab_url": evil}
	baseFields := func(fields map[string]secrets.Value) map[string]secrets.Value {
		fields["base_url"] = secrets.NewValue(evil)
		fields["url"] = secrets.NewValue(evil)
		return fields
	}

	t.Run("github PAT", func(t *testing.T) {
		doer := &recordingDoer{}
		withDiscoveryClient(t, doer)
		credential := providerfoundation.NewCredential("github", "c", config,
			baseFields(map[string]secrets.Value{"token": secrets.NewValue("tok")}))
		prepared, err := prepareDiscoveryCredential(context.Background(), "github", credential)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = discoverGitHub(context.Background(), prepared, "acme")
		if len(doer.hosts) == 0 {
			t.Fatal("no request observed")
		}
		for _, host := range doer.hosts {
			if host != "api.github.com" {
				t.Errorf("github discovery contacted %q, want only api.github.com", host)
			}
		}
	})

	t.Run("linear", func(t *testing.T) {
		doer := &recordingDoer{}
		withDiscoveryClient(t, doer)
		credential := providerfoundation.NewCredential("linear", "c", config,
			baseFields(map[string]secrets.Value{"api_key": secrets.NewValue("key")}))
		prepared, err := prepareDiscoveryCredential(context.Background(), "linear", credential)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = discoverLinear(context.Background(), prepared)
		if len(doer.hosts) == 0 {
			t.Fatal("no request observed")
		}
		for _, host := range doer.hosts {
			if host != "api.linear.app" {
				t.Errorf("linear discovery contacted %q, want only api.linear.app", host)
			}
		}
	})

	t.Run("gitlab uses config url only", func(t *testing.T) {
		doer := &recordingDoer{}
		withDiscoveryClient(t, doer)
		credential := providerfoundation.NewCredential("gitlab", "c",
			map[string]string{"url": "https://gitlab.internal.example", "base_url": evil, "gitlab_url": evil},
			baseFields(map[string]secrets.Value{"token": secrets.NewValue("tok")}))
		prepared, err := prepareDiscoveryCredential(context.Background(), "gitlab", credential)
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, _ = discoverGitLab(context.Background(), prepared, "grp")
		if len(doer.hosts) == 0 || doer.hosts[0] != "gitlab.internal.example" {
			t.Errorf("gitlab discovery hosts = %v, want config url's host first", doer.hosts)
		}
	})
}

func TestPrepareDiscoveryCredentialShapeErrorsMatchPython(t *testing.T) {
	ctx := context.Background()
	empty := func(provider string, fields map[string]secrets.Value) providerfoundation.Credential {
		return providerfoundation.NewCredential(provider, "c", nil, fields)
	}
	_, err := prepareDiscoveryCredential(ctx, "github", empty("github", map[string]secrets.Value{"other": secrets.NewValue("x")}))
	assertStatus(t, err, 400, "GitHub credentials require either token or app_id + private_key + installation_id")
	_, err = prepareDiscoveryCredential(ctx, "gitlab", empty("gitlab", map[string]secrets.Value{"token": secrets.NewValue("")}))
	assertStatus(t, err, 400, "GitLab credentials require a token")
	_, err = prepareDiscoveryCredential(ctx, "linear", empty("linear", map[string]secrets.Value{"other": secrets.NewValue("x")}))
	assertStatus(t, err, 400, "Linear credentials require apiKey")
	_, err = prepareDiscoveryCredential(ctx, "jira", empty("jira", map[string]secrets.Value{"email": secrets.NewValue("a@b.c")}))
	assertStatus(t, err, 400, "Jira credentials require email, api_token, and url")

	// Linear prefers apiKey over api_key (`decrypted.get("apiKey") or
	// decrypted.get("api_key")`), and accepts either alone.
	prepared, err := prepareDiscoveryCredential(ctx, "linear", empty("linear", map[string]secrets.Value{
		"apiKey": secrets.NewValue("camel"), "api_key": secrets.NewValue("snake"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if key, _ := prepared.Secret("api_key"); key.Reveal() != "camel" {
		t.Errorf("linear key = %q, want the apiKey value", key.Reveal())
	}
}

// TestGitHubAppBaseURLIsSSRFGuarded: Python validates a GitHub App
// credential's base_url with _validate_external_url before minting the
// installation token against it (teams.py:207-208).
func TestGitHubAppBaseURLIsSSRFGuarded(t *testing.T) {
	app := func(baseURL string) providerfoundation.Credential {
		fields := map[string]secrets.Value{
			"app_id": secrets.NewValue("1"), "installation_id": secrets.NewValue("2"),
			"private_key": secrets.NewValue("not-a-real-key"),
		}
		if baseURL != "" {
			fields["base_url"] = secrets.NewValue(baseURL)
		}
		return providerfoundation.NewCredential("github", "c", nil, fields)
	}
	withDiscoveryClient(t, &recordingDoer{})
	lookup := func(_ context.Context, host string) ([]netip.Addr, error) {
		switch host {
		case "internal.example":
			return []netip.Addr{netip.MustParseAddr("10.1.2.3")}, nil
		case "public.example":
			return []netip.Addr{netip.MustParseAddr("140.82.112.5")}, nil
		case "mapped.example":
			// Go's resolver returns plain IPv4 as 16-byte "4in6" values; a
			// public one must pass, not land in ::ffff:0:0/96's private
			// table (the first cut of the guard rejected every public IPv4).
			return []netip.Addr{netip.AddrFrom16(netip.MustParseAddr("140.82.112.5").As16())}, nil
		}
		return nil, errors.New("no such host")
	}
	withHostLookup(t, lookup)
	ctx := context.Background()

	_, err := prepareDiscoveryCredential(ctx, "github", app("http://localhost:8080"))
	assertStatus(t, err, 400, "Connection to localhost is not allowed")
	_, err = prepareDiscoveryCredential(ctx, "github", app("https://internal.example"))
	assertStatus(t, err, 400, "Connection to private/internal networks is not allowed")
	_, err = prepareDiscoveryCredential(ctx, "github", app("https://nowhere.example"))
	assertStatus(t, err, 400, "Cannot resolve hostname: nowhere.example")
	_, err = prepareDiscoveryCredential(ctx, "github", app("ftp://public.example"))
	assertStatus(t, err, 400, "Invalid URL scheme - only http and https are allowed")
	_, err = prepareDiscoveryCredential(ctx, "github", app("https://user:pw@public.example"))
	assertStatus(t, err, 400, "Credentials must not be embedded in the URL")
	// A valid, public base URL passes the guard; the (bogus) key then fails
	// the token mint, which is Python's 401.
	_, err = prepareDiscoveryCredential(ctx, "github", app("https://public.example"))
	assertStatus(t, err, 401, "GitHub App authentication failed")
	_, err = prepareDiscoveryCredential(ctx, "github", app("https://mapped.example"))
	assertStatus(t, err, 401, "GitHub App authentication failed")
}

// TestJiraConfigEmailIsMergedLikePython: Python builds Jira credentials from
// {**config, **decrypted}, so a credential with email and URL in config and
// only the token in the decrypted secret is valid there. The Go client read
// email from secrets only and refused it with Python's own 400 wording.
func TestJiraConfigEmailIsMergedLikePython(t *testing.T) {
	ctx := context.Background()
	config := map[string]string{"email": "a@b.c", "url": "https://acme.atlassian.net"}
	secret := func(name, value string) map[string]secrets.Value {
		return map[string]secrets.Value{name: secrets.NewValue(value)}
	}

	prepared, err := prepareDiscoveryCredential(ctx, "jira", providerfoundation.NewCredential("jira", "c", config, secret("api_token", "tok")))
	if err != nil {
		t.Fatalf("email+url in config, token in secrets must be accepted: %v", err)
	}
	if email, _ := prepared.Secret("email"); email.Reveal() != "a@b.c" {
		t.Errorf("email = %q, want the config value", email.Reveal())
	}

	// A decrypted key that is PRESENT shadows config even when empty (dict
	// merge), so an explicit empty email is refused, not silently rescued.
	fields := secret("api_token", "tok")
	fields["email"] = secrets.NewValue("")
	_, err = prepareDiscoveryCredential(ctx, "jira", providerfoundation.NewCredential("jira", "c", config, fields))
	assertStatus(t, err, 400, "Jira credentials require email, api_token, and url")

	// Nothing anywhere: still Python's 400.
	_, err = prepareDiscoveryCredential(ctx, "jira", providerfoundation.NewCredential("jira", "c", nil, secret("api_token", "tok")))
	assertStatus(t, err, 400, "Jira credentials require email, api_token, and url")
}
