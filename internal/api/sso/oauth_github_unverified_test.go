package sso

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
)

func githubClientFor(t *testing.T, emailsStatus int, emailsBody string) *oauthprovider.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/gh/user":
			_, _ = w.Write([]byte(`{"id": 1, "login": "u", "email": null}`))
		case "/gh/emails":
			w.WriteHeader(emailsStatus)
			_, _ = w.Write([]byte(emailsBody))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return &oauthprovider.Client{HTTP: server.Client(), Endpoints: oauthprovider.Endpoints{
		GitHubUser: server.URL + "/gh/user", GitHubEmails: server.URL + "/gh/emails"}}
}

func TestFetchOAuthUserInfoGitHubWithoutAVerifiedAddressIsTheNoVerifiedEmailRefusal(t *testing.T) {
	for name, body := range map[string]string{
		"unverified only": `[{"email": "a@x.com", "primary": true, "verified": false}]`,
		"empty list":      `[]`,
	} {
		t.Run(name, func(t *testing.T) {
			client := githubClientFor(t, http.StatusOK, body)
			_, err := fetchOAuthUserInfo(context.Background(), client, oauthprovider.GitHub, "tok")
			if !errors.Is(err, errOAuthNoVerifiedEmail) {
				t.Fatalf("err = %v, want errOAuthNoVerifiedEmail", err)
			}
		})
	}
}

func TestFetchOAuthUserInfoGitHubEmailsFailureStaysAProviderFailure(t *testing.T) {
	client := githubClientFor(t, http.StatusInternalServerError, `{}`)
	_, err := fetchOAuthUserInfo(context.Background(), client, oauthprovider.GitHub, "tok")
	if err == nil || errors.Is(err, errOAuthNoVerifiedEmail) {
		t.Fatalf("err = %v, want a provider failure that is not errOAuthNoVerifiedEmail", err)
	}
}
