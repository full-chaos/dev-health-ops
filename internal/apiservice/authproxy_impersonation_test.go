package apiservice

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

func TestImpersonationFlagNameAndDefault(t *testing.T) {
	if EnvAuthProxyImpersonation != "AUTH_PROXY_IMPERSONATION" {
		t.Fatalf("EnvAuthProxyImpersonation = %q", EnvAuthProxyImpersonation)
	}
	for name, env := range map[string]map[string]string{
		"unset":                               {},
		"empty":                               {EnvAuthProxyImpersonation: " "},
		"url only":                            {EnvAuthProxyURL: "http://auth-service:8095"},
		"explicit false":                      {EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyImpersonation: "false"},
		"every aggregate on, this flag unset": allOn("http://auth-service:8095"),
	} {
		if cfg := mustProxyConfig(t, env); cfg.Impersonation || cfg.ImpersonationOn() {
			t.Errorf("%s: the switch is on", name)
		}
	}
	if (AuthProxyConfig{Impersonation: true}).ImpersonationOn() {
		t.Error("the switch counts as on with no upstream URL")
	}
}

func TestImpersonationFlagIsStrictAndStandsAlone(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"without a url": {EnvAuthProxyImpersonation: "true"},
		"not a boolean": {EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyImpersonation: "yes"},
	} {
		if _, err := ParseAuthProxyConfig(lookupFrom(env)); err == nil {
			t.Errorf("%s: accepted %v", name, env)
		}
	}
	cfg := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyImpersonation: "true"})
	if !cfg.ImpersonationOn() {
		t.Fatal("the switch is off with the flag and the URL set")
	}
	for _, aggregate := range AuthAggregates {
		if cfg.Enabled(aggregate) {
			t.Errorf("the impersonation switch moved the %s routes", aggregate)
		}
	}
}

func impersonationLogger() (*slog.Logger, *bytes.Buffer) {
	var out bytes.Buffer
	return slog.New(slog.NewTextHandler(&out, nil)), &out
}

func TestImpersonationDelegatorFollowsTheSwitchAndTheSessionsFlag(t *testing.T) {
	url := "http://auth-service:8095"
	ctx := context.Background()

	off := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: url, AuthProxyFlagEnv(AuthSessions): "true"})
	logger, logs := impersonationLogger()
	_, session, err := newCredentials(off, issueEnv(workloadEnv(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := sessionIssuer(off, session)
	if err != nil {
		t.Fatal(err)
	}
	if got := impersonationDelegator(ctx, off, issuer, logger); got != nil || logs.Len() != 0 {
		t.Fatalf("switch off: delegator %v, log %q", got, logs.String())
	}

	on := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: url, AuthProxyFlagEnv(AuthSessions): "true", EnvAuthProxyImpersonation: "true"})
	_, session, err = newCredentials(on, issueEnv(workloadEnv(t, nil)))
	if err != nil {
		t.Fatal(err)
	}
	issuer, err = sessionIssuer(on, session)
	if err != nil {
		t.Fatal(err)
	}
	logger, logs = impersonationLogger()
	got := impersonationDelegator(ctx, on, issuer, logger)
	client, ok := got.(*sessionissue.Client)
	if !ok || client != issuer {
		t.Fatalf("switch and sessions on: delegator %T, want the session issue client", got)
	}
	if logs.Len() != 0 {
		t.Errorf("a valid combination logged: %s", logs.String())
	}
}

// AUTH_PROXY_IMPERSONATION without AUTH_PROXY_SESSIONS does not stop the start-up (D6465): one ERROR line, and every call
// answers unavailable, never a go-api session.
func TestImpersonationWithoutSessionsLogsOnceAndRefusesEveryCall(t *testing.T) {
	on := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyImpersonation: "true"})
	issuer, err := sessionIssuer(on, sessionCredential{})
	if err != nil || issuer != nil {
		t.Fatalf("sessions off built an issuer: %v %v", issuer, err)
	}
	logger, logs := impersonationLogger()
	got := impersonationDelegator(context.Background(), on, issuer, logger)
	if got == nil {
		t.Fatal("no delegator: the routes would fall back to go-api sessions")
	}
	if strings.Count(logs.String(), "level=ERROR") != 1 || !strings.Contains(logs.String(), EnvAuthProxyImpersonation) {
		t.Errorf("log = %q", logs.String())
	}
	if _, err := got.StartDelegated(context.Background(), nil, sessionissue.DelegatedRequest{}); !errors.Is(err, sessionissue.ErrUnavailable) {
		t.Errorf("start err = %v", err)
	}
	if err := got.EndDelegated(context.Background(), nil, uuid.New()); !errors.Is(err, sessionissue.ErrUnavailable) {
		t.Errorf("end err = %v", err)
	}
}
