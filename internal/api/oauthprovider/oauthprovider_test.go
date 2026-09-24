package oauthprovider

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func fakeClient(t *testing.T, routes map[string]struct {
	status int
	body   string
}) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, ok := routes[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(route.status)
		_, _ = io.Copy(w, strings.NewReader(route.body))
	}))
	t.Cleanup(server.Close)
	return &Client{HTTP: server.Client(), Endpoints: Endpoints{
		GitHubUser: server.URL + "/gh/user", GitHubEmails: server.URL + "/gh/emails",
		GitLabBase: server.URL + "/gl/", GoogleUser: server.URL + "/google"}}
}

type route = struct {
	status int
	body   string
}

func TestGitHubProfile(t *testing.T) {
	cases := []struct {
		name      string
		user      route
		emails    *route
		wantEmail pyjson.Value
		wantID    string
		wantErr   string
	}{
		{name: "email on the profile", user: route{200, `{"id": 12, "login": "a", "email": "a@x.com"}`}, wantEmail: "a@x.com", wantID: "12"},
		{name: "primary verified email", user: route{200, `{"id": 13, "email": null}`},
			emails: &route{200, `[{"email": "b@x.com", "verified": true}, {"email": "c@x.com", "primary": true, "verified": true}]`}, wantEmail: "c@x.com", wantID: "13"},
		{name: "first verified email", user: route{200, `{"id": 14, "email": ""}`},
			emails: &route{200, `[{"email": "d@x.com", "primary": true}, {"email": "e@x.com", "verified": 1}]`}, wantEmail: "e@x.com", wantID: "14"},
		{name: "first email", user: route{200, `{"id": 15}`}, emails: &route{200, `[{"email": "f@x.com"}]`}, wantEmail: "f@x.com", wantID: "15"},
		{name: "no email at all", user: route{200, `{"id": 16}`}, emails: &route{200, `[]`}, wantErr: "userinfo"},
		{name: "emails refused", user: route{200, `{"id": 17}`}, emails: &route{403, `{}`}, wantErr: "userinfo"},
		{name: "a non-object entry before a match", user: route{200, `{"id": 18}`}, emails: &route{200, `[1, {"email": "g@x.com", "primary": true, "verified": true}]`}, wantErr: "unexpected"},
		{name: "a non-object entry after a match", user: route{200, `{"id": 19}`}, emails: &route{200, `[{"email": "h@x.com", "primary": true, "verified": true}, 1]`}, wantEmail: "h@x.com", wantID: "19"},
		{name: "matched entry without email", user: route{200, `{"id": 20}`}, emails: &route{200, `[{"primary": true, "verified": true}]`}, wantErr: "unexpected"},
		{name: "emails is a dict", user: route{200, `{"id": 21}`}, emails: &route{200, `{"a": 1}`}, wantErr: "unexpected"},
		{name: "emails is an int", user: route{200, `{"id": 22}`}, emails: &route{200, `5`}, wantErr: "unexpected"},
		{name: "no id", user: route{200, `{"email": "i@x.com"}`}, wantErr: "unexpected"},
		{name: "id is a float", user: route{200, `{"id": 1.5, "email": "j@x.com"}`}, wantEmail: "j@x.com", wantID: "1.5"},
		{name: "id is a list", user: route{200, `{"id": [1, "a", null, true], "email": "k@x.com"}`}, wantEmail: "k@x.com", wantID: "[1, 'a', None, True]"},
		{name: "profile is a list", user: route{200, `[]`}, wantErr: "unexpected"},
		{name: "profile not json", user: route{200, `nope`}, wantErr: "unexpected"},
		{name: "profile refused", user: route{401, `{}`}, wantErr: "userinfo"},
		{name: "profile redirect", user: route{302, ``}, wantErr: "userinfo"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			routes := map[string]route{"/gh/user": c.user}
			if c.emails != nil {
				routes["/gh/emails"] = *c.emails
			}
			info, err := fakeClient(t, routes).FetchUserInfo(context.Background(), GitHub, "tok")
			var userInfoErr *UserInfoError
			switch c.wantErr {
			case "userinfo":
				if !errors.As(err, &userInfoErr) {
					t.Fatalf("want a UserInfoError, got %v", err)
				}
				return
			case "unexpected":
				if !errors.Is(err, ErrUnexpected) {
					t.Fatalf("want ErrUnexpected, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if info.Email != c.wantEmail || info.ProviderUserID != c.wantID {
				t.Fatalf("got email %v id %v", info.Email, info.ProviderUserID)
			}
		})
	}
}

func TestGitLabAndGoogleRequiredFields(t *testing.T) {
	client := fakeClient(t, map[string]route{
		"/gl/api/v4/user": {200, `{"id": 7, "username": "u", "name": "N", "email": "m@x.com"}`},
		"/google":         {200, `{"id": 9, "email": "g@x.com", "name": "G"}`},
	})
	info, err := client.FetchUserInfo(context.Background(), GitLab, "tok")
	if err != nil || info.ProviderUserID != "7" || info.Email != "m@x.com" || info.Username != "u" {
		t.Fatalf("gitlab: %+v %v", info, err)
	}
	info, err = client.FetchUserInfo(context.Background(), Google, "tok")
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := info.ProviderUserID.(pyjson.Int); !ok || id.Int64() != 9 || info.Username != nil {
		t.Fatalf("google keeps the raw id and has no username: %+v", info)
	}
	for _, body := range []string{`{"id": 1}`, `{"email": "x@x.com"}`, `[]`, `"text"`, `5`, `null`} {
		client := fakeClient(t, map[string]route{"/gl/api/v4/user": {200, body}})
		var userInfoErr *UserInfoError
		if _, err := client.FetchUserInfo(context.Background(), GitLab, "tok"); !errors.As(err, &userInfoErr) {
			t.Errorf("gitlab %s: want a UserInfoError, got %v", body, err)
		}
	}
	if _, err := fakeClient(t, map[string]route{"/google": {200, `{`}}).FetchUserInfo(context.Background(), Google, "tok"); !errors.Is(err, ErrUnexpected) {
		t.Errorf("google non-json: want ErrUnexpected, got %v", err)
	}
}

func TestTruthyAndPyStr(t *testing.T) {
	truthy := []pyjson.Value{true, "x", pyjson.IntOf(-1), pyjson.Float(0.5), []pyjson.Value{nil}}
	falsy := []pyjson.Value{nil, false, "", pyjson.IntOf(0), pyjson.Int{}, pyjson.Float(0), []pyjson.Value{}, pyjson.NewObject()}
	for _, value := range truthy {
		if !Truthy(value) {
			t.Errorf("Truthy(%#v) = false", value)
		}
	}
	for _, value := range falsy {
		if Truthy(value) {
			t.Errorf("Truthy(%#v) = true", value)
		}
	}
	object := pyjson.NewObject()
	object.Set("k", "it's")
	for value, want := range map[string]pyjson.Value{"None": nil, "False": false, "5": pyjson.IntOf(5), "1e+100": pyjson.Float(1e100), "{'k': \"it's\"}": object} {
		if got := PyStr(want); got != value {
			t.Errorf("PyStr(%#v) = %q, want %q", want, got, value)
		}
	}
}

// A Client without its own http.Client uses a default one (with the
// request timeout) instead of failing.
func TestNilHTTPClientUsesTheDefault(t *testing.T) {
	client := fakeClient(t, map[string]route{"/google": {200, `{"id": "g-1", "email": "g@example.com"}`}})
	client.HTTP = nil
	info, err := client.FetchUserInfo(context.Background(), Google, "tok")
	if err != nil || info.ProviderUserID != "g-1" {
		t.Fatalf("%+v %v", info, err)
	}
}
