package authruntime

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// CHAOS-11207 (C1): the web approver reference kind is on only with all three
// settings and the caller admitted; any other state is off with one ERROR line
// that names the missing settings (never a value), and auth-service starts.
func TestWebApproverReferences_partialConfigIsOffWithOneError(t *testing.T) {
	const caller = "prn_BBBBBBBBBBBBBBBBBBBBBB"
	full := authconfig.WebApprover{Issuer: "https://www.example.test", JWKSFile: "/var/run/auth/web-approver/jwks.json", Caller: caller}
	for _, c := range []struct {
		name     string
		settings authconfig.WebApprover
		subjects []string
		missing  string
	}{
		{"no issuer", authconfig.WebApprover{JWKSFile: full.JWKSFile, Caller: caller}, []string{caller}, authconfig.EnvAuthorizeWebApproverIssuer},
		{"no JWKS file", authconfig.WebApprover{Issuer: full.Issuer, Caller: caller}, []string{caller}, authconfig.EnvAuthorizeWebApproverJWKSFile},
		{"no caller", authconfig.WebApprover{Issuer: full.Issuer, JWKSFile: full.JWKSFile}, []string{caller}, authconfig.EnvAuthorizeWebApproverCaller},
		{"caller not admitted", full, []string{"prn_AAAAAAAAAAAAAAAAAAAAAA"}, authconfig.EnvAuthorizeWebApproverCaller + " in " + authconfig.EnvAuthorizeCallerSubjects},
		{"caller not a principal id", authconfig.WebApprover{Issuer: full.Issuer, JWKSFile: full.JWKSFile, Caller: "acr-api"}, []string{"acr-api"}, "settings invalid"},
	} {
		t.Run(c.name, func(t *testing.T) {
			logs := &bytes.Buffer{}
			cfg := authconfig.Config{AuthorizeWebApprover: c.settings, AuthorizeCallerSubjects: c.subjects}

			got := webApproverReferences(cfg, slog.New(slog.NewJSONHandler(logs, nil)))

			if got != nil {
				t.Fatal("the kind is on with a partial configuration")
			}
			if strings.Count(logs.String(), `"level":"ERROR"`) != 1 || !strings.Contains(logs.String(), c.missing) {
				t.Fatalf("want one ERROR line naming %q:\n%s", c.missing, logs.String())
			}
			if strings.Contains(logs.String(), "www.example.test") || strings.Contains(logs.String(), "jwks.json") {
				t.Fatalf("the ERROR line echoes a value:\n%s", logs.String())
			}
		})
	}
}

func TestWebApproverReferences_unsetIsOffAndSilentFullIsOn(t *testing.T) {
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	if webApproverReferences(authconfig.Config{}, logger) != nil || logs.Len() != 0 {
		t.Fatalf("unset: want off and no line:\n%s", logs.String())
	}
	caller := session.WirePrincipalID(uuid.MustParse("5e5510aa-6a1e-4c4e-9d3b-2b7f0c1a9e01"))
	cfg := authconfig.Config{AuthorizeCallerSubjects: []string{caller},
		AuthorizeWebApprover: authconfig.WebApprover{Issuer: "https://www.example.test", JWKSFile: "/var/run/auth/web-approver/jwks.json", Caller: caller}}
	if got := webApproverReferences(cfg, logger); got == nil || got.Caller() != caller || !strings.Contains(logs.String(), "web approver reference on") {
		t.Fatalf("full: want on:\n%s", logs.String())
	}
}
