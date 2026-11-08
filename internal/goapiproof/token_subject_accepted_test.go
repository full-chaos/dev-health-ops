package goapiproof

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// acceptedTestToken is a user-access-token-shaped value (typ at+jwt) naming sub; unsigned, built at run time so no
// token literal is in the tree.
func acceptedTestToken(sub string) string {
	return strings.Join([]string{
		base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"EdDSA","typ":"at+jwt"}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"` + sub + `"}`)),
		base64.RawURLEncoding.EncodeToString([]byte("signature")),
	}, ".")
}

func writeAcceptedTestToken(t *testing.T, path, sub string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(acceptedTestToken(sub)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// The audit subject is the subject of the token /buildinfo accepted. The handler rotates the file to another valid
// token before it answers 200, so a second read of the file would name the other subject.
func TestTokenSubjectIsTheSubjectOfTheTokenBuildinfoAccepted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-token")
	writeAcceptedTestToken(t, path, "prn_admin_a")
	sent := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent <- r.Header.Get("Authorization")
		writeAcceptedTestToken(t, path, "prn_admin_b")
		_, _ = w.Write([]byte(`{"commit":"b18e56fa79cfe20ce0f75df148144b832d92be36","modified":false}`))
	}))
	t.Cleanup(server.Close)
	credential := UserAccessTokenFileCredential("platform-admin user access token", "routing access token", path)

	if _, err := FetchBuildIdentity(context.Background(), NewLegClient(0), server.URL, credential); err != nil {
		t.Fatalf("FetchBuildIdentity: %v", err)
	}
	verified, err := TokenSubject(<-sent)
	if err != nil || verified != "prn_admin_a" {
		t.Fatalf("the /buildinfo request carried subject (%q, %v), want prn_admin_a", verified, err)
	}
	got, err := credential.TokenSubject(context.Background())
	if err != nil {
		t.Fatalf("TokenSubject: %v", err)
	}
	if got != verified {
		t.Fatalf("audit subject = %q, /buildinfo verified %q: the subject came from a second read of the file", got, verified)
	}
}

// A credential that no /buildinfo read accepted has no subject to name: TokenSubject refuses rather than decode a
// token nobody verified.
func TestTokenSubjectRefusesATokenBuildinfoDidNotAccept(t *testing.T) {
	for name, answer := range map[string]func(http.ResponseWriter){
		"401": func(w http.ResponseWriter) { w.WriteHeader(http.StatusUnauthorized) },
		"404": func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
		"modified tree": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"commit":"b18e56fa79cfe20ce0f75df148144b832d92be36","modified":true}`))
		},
		"no build stamp":     func(w http.ResponseWriter) { _, _ = w.Write([]byte(`{"commit":"unknown","modified":false}`)) },
		"no /buildinfo read": nil,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "access-token")
			writeAcceptedTestToken(t, path, "prn_admin_a")
			credential := UserAccessTokenFileCredential("platform-admin user access token", "routing access token", path)
			if answer != nil {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { answer(w) }))
				t.Cleanup(server.Close)
				if _, err := FetchBuildIdentity(context.Background(), NewLegClient(0), server.URL, credential); err == nil {
					t.Fatal("FetchBuildIdentity accepted the answer")
				}
			}
			got, err := credential.TokenSubject(context.Background())
			if err == nil {
				t.Fatalf("TokenSubject = %q for a token /buildinfo did not accept", got)
			}
			if strings.Contains(err.Error(), acceptedTestToken("prn_admin_a")) {
				t.Fatal("the refusal carries the credential")
			}
		})
	}
}
