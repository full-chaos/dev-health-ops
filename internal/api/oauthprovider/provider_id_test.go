package oauthprovider

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// CHAOS-11619: every profile parser answers a missing or JSON-null user id with ErrMissingProviderID, which still
// is an ErrUnexpected for the callers that test for that; a present id, even the string "None", parses, and a
// missing email keeps its own error.
func TestEveryParserRefusesAMissingOrNullProviderID(t *testing.T) {
	profile := map[string]func(id string) string{
		GitHub: func(id string) string { return `{` + id + `"login": "a", "email": "a@x.com"}` },
		GitLab: func(id string) string {
			return `{` + id + `"username": "u", "email": "a@x.com", "confirmed_at": "2026-01-01T00:00:00Z"}`
		},
		Google: func(id string) string { return `{` + id + `"email": "a@x.com", "verified_email": true}` },
	}
	path := map[string]string{GitHub: "/gh/user", GitLab: "/gl/api/v4/user", Google: "/google"}
	for provider, build := range profile {
		for name, id := range map[string]string{"no id key": ``, "null id": `"id": null, `} {
			client := fakeClient(t, map[string]route{path[provider]: {200, build(id)}})
			_, err := client.FetchUserInfo(context.Background(), provider, "tok")
			if !errors.Is(err, ErrMissingProviderID) || !errors.Is(err, ErrUnexpected) {
				t.Errorf("%s/%s: err = %v, want ErrMissingProviderID that also is ErrUnexpected", provider, name, err)
			}
		}
		client := fakeClient(t, map[string]route{path[provider]: {200, build(`"id": "None", `)}})
		if info, err := client.FetchUserInfo(context.Background(), provider, "tok"); err != nil || info.ProviderUserID != "None" {
			t.Errorf("%s: a present string \"None\" id: %+v %v", provider, info, err)
		}
	}
	for _, provider := range []string{GitLab, Google} {
		client := fakeClient(t, map[string]route{path[provider]: {200, `{"id": 5}`}})
		var userInfoErr *UserInfoError
		if _, err := client.FetchUserInfo(context.Background(), provider, "tok"); !errors.As(err, &userInfoErr) || errors.Is(err, ErrMissingProviderID) {
			t.Errorf("%s: a missing email keeps its UserInfoError, got %v", provider, err)
		}
	}
}

// CHAOS-11678: the free text of a profile is storable before any caller writes it. A lone surrogate in a name or an
// avatar URL becomes U+FFFD; in the email it makes the address unverified; a valid pair and an unread member stay.
func TestProfileTextLoneSurrogates(t *testing.T) {
	const lone = `\ud800`
	profile := map[string]string{
		GitHub: `{"id": 1, "login": "l` + lone + `", "name": "n` + lone + `", "avatar_url": "https://a/` + lone + `", "email": "e%s@x.com", "bio": "b` + lone + `"}`,
		GitLab: `{"id": 1, "username": "l` + lone + `", "name": "n` + lone + `", "avatar_url": "https://a/` + lone + `", "email": "e%s@x.com", "confirmed_at": "2026-01-01T00:00:00Z"}`,
		Google: `{"id": "1", "name": "n` + lone + `", "picture": "https://a/` + lone + `", "email": "e%s@x.com", "verified_email": true}`,
	}
	path := map[string]string{GitHub: "/gh/user", GitLab: "/gl/api/v4/user", Google: "/google"}
	for provider, template := range profile {
		fill := func(email string) string { return strings.Replace(template, "%s", email, 1) }
		info, err := fakeClient(t, map[string]route{path[provider]: {200, fill("")}}).FetchUserInfo(context.Background(), provider, "tok")
		if err != nil || !info.EmailVerified {
			t.Fatalf("%s clean email: %+v %v", provider, info, err)
		}
		for name, value := range map[string]pyjson.Value{"username": info.Username, "full name": info.FullName, "avatar": info.AvatarURL} {
			text, isText := value.(string)
			if !isText {
				continue
			}
			if pyjson.HasLoneSurrogate(text) || !strings.Contains(text, "\uFFFD") {
				t.Errorf("%s %s = %q, want the surrogate replaced by U+FFFD", provider, name, text)
			}
		}
		bad, err := fakeClient(t, map[string]route{path[provider]: {200, fill(lone)}}).FetchUserInfo(context.Background(), provider, "tok")
		if err != nil || bad.EmailVerified {
			t.Errorf("%s: a lone surrogate in the email must leave it unverified: %+v %v", provider, bad, err)
		}
		pair, err := fakeClient(t, map[string]route{path[provider]: {200, strings.Replace(fill(""), lone, `😀`, -1)}}).FetchUserInfo(context.Background(), provider, "tok")
		if err != nil || !pair.EmailVerified {
			t.Fatalf("%s pair: %+v %v", provider, pair, err)
		}
		if text, _ := pair.FullName.(string); !strings.Contains(text, "\U0001F600") {
			t.Errorf("%s: a valid pair must stay: %q", provider, text)
		}
	}
}

// A numeric id, zero included, is str(id): non-empty, so it is a subject and never the empty string that the sign-in
// edge rule refuses.
func TestEveryParserKeepsANumericProviderIDAsANonEmptyText(t *testing.T) {
	profile := map[string]func(id string) string{
		GitHub: func(id string) string { return `{"id": ` + id + `, "login": "a", "email": "a@x.com"}` },
		GitLab: func(id string) string {
			return `{"id": ` + id + `, "username": "u", "email": "a@x.com", "confirmed_at": "2026-01-01T00:00:00Z"}`
		},
		Google: func(id string) string { return `{"id": ` + id + `, "email": "a@x.com", "verified_email": true}` },
	}
	path := map[string]string{GitHub: "/gh/user", GitLab: "/gl/api/v4/user", Google: "/google"}
	for provider, build := range profile {
		for id, want := range map[string]string{"0": "0", "12": "12"} {
			client := fakeClient(t, map[string]route{path[provider]: {200, build(id)}})
			info, err := client.FetchUserInfo(context.Background(), provider, "tok")
			if err != nil || PyStr(info.ProviderUserID) != want {
				t.Errorf("%s: id %s: %+v %v, want the text %q", provider, id, info, err, want)
			}
		}
	}
}
