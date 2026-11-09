package apiservice

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/acrcredentials"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func revokerFrom(t *testing.T, env map[string]string) (any, string) {
	t.Helper()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	got := newACRRevoker(func(name string) (string, bool) { value, ok := env[name]; return value, ok }, logger)
	return got, logs.String()
}

func TestTheACRRevokerIsBuiltOnlyFromAUsableSet(t *testing.T) {
	const (
		base    = "https://acr.internal.test"
		token   = "https://acr.internal.test/api/v1/oauth/token"
		subject = "/var/run/secrets/acr/token"
	)
	cases := []struct {
		name     string
		env      map[string]string
		built    bool
		variable string
	}{
		{"all set", map[string]string{EnvACRRevokeURL: base, EnvACRRevokeTokenURL: token, EnvACRRevokeSubjectFile: subject}, true, ""},
		{"all set with edge space", map[string]string{EnvACRRevokeURL: " " + base + " ", EnvACRRevokeTokenURL: token + "\n", EnvACRRevokeSubjectFile: " " + subject}, true, ""},
		{"loopback http", map[string]string{EnvACRRevokeURL: "http://127.0.0.1:8080", EnvACRRevokeTokenURL: "http://127.0.0.1:8080/api/v1/oauth/token", EnvACRRevokeSubjectFile: subject}, true, ""},
		{"no base", map[string]string{EnvACRRevokeTokenURL: token, EnvACRRevokeSubjectFile: subject}, false, EnvACRRevokeURL},
		{"only the base", map[string]string{EnvACRRevokeURL: base}, false, EnvACRRevokeTokenURL},
		{"only the token endpoint", map[string]string{EnvACRRevokeTokenURL: token}, false, EnvACRRevokeURL},
		{"only the subject file", map[string]string{EnvACRRevokeSubjectFile: subject}, false, EnvACRRevokeURL},
		{"plain http base", map[string]string{EnvACRRevokeURL: "http://acr.internal.test", EnvACRRevokeTokenURL: token, EnvACRRevokeSubjectFile: subject}, false, EnvACRRevokeURL},
		{"base with a query", map[string]string{EnvACRRevokeURL: base + "?x=1", EnvACRRevokeTokenURL: token, EnvACRRevokeSubjectFile: subject}, false, EnvACRRevokeURL},
		{"base does not parse", map[string]string{EnvACRRevokeURL: "https://acr\x7f", EnvACRRevokeTokenURL: token, EnvACRRevokeSubjectFile: subject}, false, EnvACRRevokeURL},
		{"no token endpoint", map[string]string{EnvACRRevokeURL: base, EnvACRRevokeSubjectFile: subject}, false, EnvACRRevokeTokenURL},
		{"plain http token endpoint", map[string]string{EnvACRRevokeURL: base, EnvACRRevokeTokenURL: "http://acr.internal.test/t", EnvACRRevokeSubjectFile: subject}, false, EnvACRRevokeTokenURL},
		{"token endpoint does not parse", map[string]string{EnvACRRevokeURL: base, EnvACRRevokeTokenURL: "https://acr\x7f/t", EnvACRRevokeSubjectFile: subject}, false, EnvACRRevokeTokenURL},
		{"no subject file", map[string]string{EnvACRRevokeURL: base, EnvACRRevokeTokenURL: token}, false, EnvACRRevokeSubjectFile},
		{"blank subject file", map[string]string{EnvACRRevokeURL: base, EnvACRRevokeTokenURL: token, EnvACRRevokeSubjectFile: "   "}, false, EnvACRRevokeSubjectFile},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, logs := revokerFrom(t, tc.env)
			client, isClient := got.(*acrrevoke.Client)
			if tc.built {
				if !isClient || client == nil {
					t.Fatalf("revoker = %#v, want a client; logs %s", got, logs)
				}
				if strings.Contains(logs, "level=ERROR") {
					t.Fatalf("a usable set logged an error: %s", logs)
				}
				return
			}
			if got != nil {
				t.Fatalf("revoker = %#v, want nil (an untyped nil interface)", got)
			}
			if !strings.Contains(logs, "level=ERROR") || !strings.Contains(logs, "variable="+tc.variable+"\n") {
				t.Fatalf("logs %q do not name %s", logs, tc.variable)
			}
			for _, value := range tc.env {
				if trimmed := strings.TrimSpace(value); trimmed != "" && strings.Contains(logs, trimmed) {
					t.Fatalf("logs %q carry the value %q", logs, trimmed)
				}
			}
		})
	}
}

func TestAnUnconfiguredACRRevokerIsNilAndSaysSo(t *testing.T) {
	got, logs := revokerFrom(t, map[string]string{})
	if got != nil {
		t.Fatalf("revoker = %#v", got)
	}
	if !strings.Contains(logs, "level=INFO") || !strings.Contains(logs, "not configured") || strings.Contains(logs, "level=ERROR") {
		t.Fatalf("logs %q", logs)
	}
	got, _ = revokerFrom(t, map[string]string{EnvACRRevokeURL: " ", EnvACRRevokeTokenURL: "", EnvACRRevokeSubjectFile: "\t"})
	if got != nil {
		t.Fatalf("blank values built %#v", got)
	}
}

// configure builds the revoker from the process environment: a usable set gives the route a client, an unset one
// leaves it nil (503), and neither stops start-up.
func TestConfigureBuildsTheACRRevokerFromTheEnvironment(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run := func(t *testing.T) (acrcredentials.Revoker, error) {
		t.Helper()
		var seen acrcredentials.Revoker
		components, err := configureWith(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second),
			quietLogger(), func(deps *Deps) { seen = deps.ACRRevoker })
		closeComponents(components)
		return seen, err
	}
	for _, name := range []string{EnvACRRevokeURL, EnvACRRevokeTokenURL, EnvACRRevokeSubjectFile} {
		t.Setenv(name, "")
	}
	if got, err := run(t); err != nil || got != nil {
		t.Fatalf("unset: revoker %#v, err %v", got, err)
	}
	t.Setenv(EnvACRRevokeURL, "https://acr.internal.test")
	if got, err := run(t); err != nil || got != nil {
		t.Fatalf("a partial set: revoker %#v, err %v; want nil and no start-up error", got, err)
	}
	t.Setenv(EnvACRRevokeTokenURL, "https://acr.internal.test/api/v1/oauth/token")
	t.Setenv(EnvACRRevokeSubjectFile, "/var/run/secrets/acr/token")
	got, err := run(t)
	if client, ok := got.(*acrrevoke.Client); err != nil || !ok || client == nil {
		t.Fatalf("a usable set: revoker %#v, err %v", got, err)
	}
}

// A plain-http acr origin is accepted only when BOTH clauses hold: the explicit opt-in ACR_REVOKE_ALLOW_CLUSTER_HTTP=true and
// a cluster-local Service host (<name>.svc.cluster.local). Each clause alone is refused, https is unchanged, and an opt-in
// value other than "true" or "false" is refused by name. A refusal is a nil revoker (503) and never a start-up failure.
func TestTheClusterHTTPOptInNeedsBothClauses(t *testing.T) {
	const (
		optIn       = "ACR_REVOKE_ALLOW_CLUSTER_HTTP"
		clusterBase = "http://dev-health-acr.dev-health.svc.cluster.local:8080"
		clusterTok  = "http://dev-health-acr.dev-health.svc.cluster.local:8080/api/v1/oauth/token"
		httpsBase   = "https://acr.internal.test"
		httpsTok    = "https://acr.internal.test/api/v1/oauth/token"
		subject     = "/var/run/dev-health/acr-revoke/token"
	)
	set := func(base, token, allow string) map[string]string {
		env := map[string]string{EnvACRRevokeURL: base, EnvACRRevokeTokenURL: token, EnvACRRevokeSubjectFile: subject}
		if allow != "-" {
			env[optIn] = allow
		}
		return env
	}
	cases := []struct {
		name     string
		env      map[string]string
		built    bool
		variable string
	}{
		{"both clauses", set(clusterBase, clusterTok, "true"), true, ""},
		{"both clauses, upper-case host", set("http://ACR.DEV-HEALTH.SVC.CLUSTER.LOCAL:8080", "http://ACR.DEV-HEALTH.SVC.CLUSTER.LOCAL:8080/api/v1/oauth/token", "true"), true, ""},
		{"opt-in with space", set(clusterBase, clusterTok, " true\n"), true, ""},
		{"https with the opt-in", set(httpsBase, httpsTok, "true"), true, ""},
		{"https with opt-in false", set(httpsBase, httpsTok, "false"), true, ""},
		{"cluster host without the opt-in", set(clusterBase, clusterTok, "-"), false, EnvACRRevokeURL},
		{"cluster host with opt-in false", set(clusterBase, clusterTok, "false"), false, EnvACRRevokeURL},
		{"opt-in without the cluster host", set("http://acr.internal.test", httpsTok, "true"), false, EnvACRRevokeURL},
		{"opt-in, bare suffix host", set("http://svc.cluster.local", httpsTok, "true"), false, EnvACRRevokeURL},
		{"opt-in, dot-led suffix host", set("http://.svc.cluster.local", httpsTok, "true"), false, EnvACRRevokeURL},
		{"opt-in, suffix inside a longer host", set("http://acr.svc.cluster.local.evil.test", httpsTok, "true"), false, EnvACRRevokeURL},
		{"opt-in, trailing-dot host", set("http://acr.ns.svc.cluster.local.", httpsTok, "true"), false, EnvACRRevokeURL},
		{"opt-in, user info", set("http://u@acr.ns.svc.cluster.local", httpsTok, "true"), false, EnvACRRevokeURL},
		{"opt-in, fragment", set("http://acr.ns.svc.cluster.local#f", httpsTok, "true"), false, EnvACRRevokeURL},
		{"opt-in, query on the base", set(clusterBase+"?x=1", clusterTok, "true"), false, EnvACRRevokeURL},
		{"opt-in, other scheme", set("ftp://acr.ns.svc.cluster.local", httpsTok, "true"), false, EnvACRRevokeURL},
		{"cluster base, token endpoint off-cluster http", set(clusterBase, "http://acr.internal.test/api/v1/oauth/token", "true"), false, EnvACRRevokeTokenURL},
		{"cluster token endpoint without the opt-in", set(httpsBase, clusterTok, "-"), false, EnvACRRevokeTokenURL},
		{"opt-in TRUE", set(clusterBase, clusterTok, "TRUE"), false, optIn},
		{"opt-in 1", set(httpsBase, httpsTok, "1"), false, optIn},
		{"opt-in yes", set(httpsBase, httpsTok, "yes"), false, optIn},
		{"only the opt-in", map[string]string{optIn: "true"}, false, EnvACRRevokeURL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, logs := revokerFrom(t, tc.env)
			client, isClient := got.(*acrrevoke.Client)
			if tc.built {
				if !isClient || client == nil {
					t.Fatalf("revoker = %#v, want a client; logs %s", got, logs)
				}
				if strings.Contains(logs, "level=ERROR") {
					t.Fatalf("a usable set logged an error: %s", logs)
				}
				return
			}
			if got != nil {
				t.Fatalf("revoker = %#v, want nil", got)
			}
			if !strings.Contains(logs, "level=ERROR") || !strings.Contains(logs, "variable="+tc.variable+"\n") {
				t.Fatalf("logs %q do not name %s", logs, tc.variable)
			}
		})
	}
}
