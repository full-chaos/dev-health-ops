//go:build integration

package sso_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Over a real, migrated Postgres, a provider id uuid.UUID() refuses gets exactly the answer a well-formed id with no
// row gets on the same route: status, headers and body.
func TestSignInRoutesAnswerAMalformedProviderIDAsAnUnknownOne(t *testing.T) {
	ctx := context.Background()
	st := startStack(t, ctx)
	send := func(t *testing.T, method, path, body string) (int, http.Header, string) {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, method, st.server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		text, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		response.Header.Del("Date")
		return response.StatusCode, response.Header, string(text)
	}
	unknown := uuid.NewString()
	for _, route := range []struct{ name, method, family, verb, body string }{
		{"oauth callback", http.MethodPost, "oauth", "callback", `{"code":"c","state":"s"}`},
		{"oidc authorize", http.MethodPost, "oidc", "authorize", `{}`},
		{"oidc callback", http.MethodPost, "oidc", "callback", `{"code":"c","state":"s"}`},
		{"saml metadata", http.MethodGet, "saml", "metadata", ""},
		{"saml initiate", http.MethodPost, "saml", "initiate", `{}`},
		{"saml acs", http.MethodPost, "saml", "acs", `{"SAMLResponse":"x"}`},
	} {
		t.Run(route.name, func(t *testing.T) {
			wantStatus, wantHeader, wantBody := send(t, route.method, "/api/v1/auth/"+route.family+"/"+unknown+"/"+route.verb, route.body)
			if wantStatus != http.StatusNotFound {
				t.Fatalf("unknown id: %d %s, want 404", wantStatus, wantBody)
			}
			status, header, body := send(t, route.method, "/api/v1/auth/"+route.family+"/not-a-uuid/"+route.verb, route.body)
			if status != wantStatus || body != wantBody || len(header) != len(wantHeader) {
				t.Fatalf("malformed id %d %v %s, unknown id %d %v %s", status, header, body, wantStatus, wantHeader, wantBody)
			}
			for name := range wantHeader {
				if strings.Join(header.Values(name), "\n") != strings.Join(wantHeader.Values(name), "\n") {
					t.Fatalf("header %s: malformed id %q, unknown id %q", name, header.Values(name), wantHeader.Values(name))
				}
			}
		})
	}
}
