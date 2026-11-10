package apiservice

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func deprovisionerFor(t *testing.T, env map[string]string) (bool, bool, string) {
	t.Helper()
	cfg := mustProxyConfig(t, env)
	lookup := issueEnv(env)
	provisioning, session, err := newCredentials(cfg, lookup)
	if err != nil {
		t.Fatalf("newCredentials: %v", err)
	}
	provisioner, err := orgProvisioner(cfg, provisioning)
	if err != nil {
		t.Fatalf("orgProvisioner: %v", err)
	}
	var logs bytes.Buffer
	configured, client := orgDeletionDeprovisioner(context.Background(), slog.New(slog.NewTextHandler(&logs, nil)), cfg, provisioner,
		provisioning, session, lookup)
	return configured, client != nil, logs.String()
}

// The organization delete's switch-off client: built from AUTH_PROXY_URL and the workload inputs with no switch read;
// none (and not configured) without AUTH_PROXY_URL; configured with no client, one WARN line and no error when the
// inputs are missing or invalid; not built with the switch on (the provisioner deprovisions then).
func TestOrgDeletionDeprovisionerFollowsTheProxyURLNotTheSwitch(t *testing.T) {
	upstream := "http://auth-service:8095"
	inputs := func(extra map[string]string) map[string]string {
		env := map[string]string{EnvAuthProxyURL: upstream, EnvAuthWorkloadSubjectFile: subjectFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer}
		for k, v := range extra {
			env[k] = v
		}
		return env
	}
	for name, tc := range map[string]struct {
		env              map[string]string
		configured, real bool
		warn             string
	}{
		"switch off, URL and inputs": {env: inputs(nil), configured: true, real: true},
		"switch explicitly false":    {env: inputs(map[string]string{EnvAuthProxyOrgProvisioning: "false"}), configured: true, real: true},
		"no AUTH_PROXY_URL":          {env: map[string]string{EnvAuthWorkloadSubjectFile: subjectFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer}},
		"no workload inputs":         {env: map[string]string{EnvAuthProxyURL: upstream}, configured: true, warn: "no workload inputs"},
		"subject file unreadable": {env: inputs(map[string]string{EnvAuthWorkloadSubjectFile: filepath.Join(t.TempDir(), "absent")}),
			configured: true, warn: "invalid"},
		"no issuer": {env: map[string]string{EnvAuthProxyURL: upstream, EnvAuthWorkloadSubjectFile: subjectFile(t)}, configured: true, warn: "invalid"},
		"switch on": {env: inputs(map[string]string{EnvAuthProxyOrgProvisioning: "true"}), configured: true},
	} {
		t.Run(name, func(t *testing.T) {
			configured, real, logs := deprovisionerFor(t, tc.env)
			if configured != tc.configured || real != tc.real {
				t.Fatalf("configured=%v client=%v, want %v %v (log %q)", configured, real, tc.configured, tc.real, logs)
			}
			if tc.warn == "" && strings.Contains(logs, "level=WARN") {
				t.Fatalf("unexpected WARN: %q", logs)
			}
			if tc.warn != "" && (!strings.Contains(logs, "level=WARN") || !strings.Contains(logs, tc.warn)) {
				t.Fatalf("log %q, want one WARN naming %q", logs, tc.warn)
			}
		})
	}
}

// configureWith with the switch off: AUTH_PROXY_URL and the workload inputs give the admin delete its client; the
// inputs missing never fail the start.
func TestConfigureGivesTheOrgDeleteAClientWithTheSwitchOff(t *testing.T) {
	for name, tc := range map[string]struct {
		subject bool
		real    bool
	}{
		"workload inputs":    {subject: true, real: true},
		"no workload inputs": {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(EnvAuthProxyOrgProvisioning, "")
			t.Setenv(EnvAuthProxyURL, "http://auth-service:8095")
			t.Setenv(EnvAuthWorkloadIssuer, testWorkloadIssuer)
			t.Setenv(EnvAuthWorkloadBootstrapFile, "")
			t.Setenv(EnvAuthWorkloadSubjectFile, "")
			if tc.subject {
				t.Setenv(EnvAuthWorkloadSubjectFile, subjectFile(t))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var deps Deps
			components, err := configureWith(ctx, apiTestConfig(), health.NewRegistry(time.Second), quietLogger(), func(built *Deps) { deps = *built })
			for index := len(components) - 1; index >= 0; index-- {
				_ = components[index].Shutdown(context.Background())
			}
			if err != nil {
				t.Fatalf("configureWith: %v", err)
			}
			if deps.OrgProvisioner != nil || !deps.OrgDeletionAuth || (deps.OrgDeprovisioner != nil) != tc.real {
				t.Fatalf("OrgProvisioner=%v OrgDeletionAuth=%v OrgDeprovisioner set=%v, want nil, true, %v",
					deps.OrgProvisioner, deps.OrgDeletionAuth, deps.OrgDeprovisioner != nil, tc.real)
			}
		})
	}
}
