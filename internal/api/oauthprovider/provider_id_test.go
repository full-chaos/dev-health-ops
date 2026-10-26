package oauthprovider

import (
	"context"
	"errors"
	"testing"
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
