package apiservice

import (
	"os"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/githubapp"
)

// githubAppConfig is github_app_config.py over the process environment: the
// slug, App id, OAuth client, callback URL, and the private key from the
// inline value (a single-line PEM with escaped newlines is accepted) or the
// key file. The environment is read here, when the api starts; Python reads
// each variable per request.
func githubAppConfig(lookup func(string) (string, bool)) githubapp.Config {
	get := func(name string) string {
		value, _ := lookup(name)
		return value
	}
	inline, path := get("GITHUB_APP_PRIVATE_KEY"), get("GITHUB_APP_PRIVATE_KEY_PATH")
	privateKey := func() (string, error) {
		if inline != "" {
			return strings.ReplaceAll(inline, `\n`, "\n"), nil
		}
		if path != "" {
			raw, err := os.ReadFile(path)
			return string(raw), err
		}
		return "", nil
	}
	return githubapp.Config{
		Slug: get("GITHUB_APP_SLUG"), AppID: get("GITHUB_APP_ID"),
		ClientID: get("GITHUB_APP_CLIENT_ID"), ClientSecret: get("GITHUB_APP_CLIENT_SECRET"),
		CallbackURL: get("GITHUB_APP_CALLBACK_URL"), PrivateKey: privateKey,
	}
}
