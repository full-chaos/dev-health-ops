package authconfig

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	testInviteSecret   = "invite-config-test-signing-key-01"
	testInvitePrevious = "invite-config-test-previous-key-02"
)

func provisioningEnv(set map[string]string) map[string]string {
	env := baseEnv()
	env[EnvAccessTokenSecret], env[EnvSessionSuccessorKey], env[EnvValkeyURI] = testAccessSecret, testSuccessorKey, testValkeyURI
	for k, v := range set {
		if v == "<unset>" {
			delete(env, k)
			continue
		}
		env[k] = v
	}
	return env
}

func TestMembershipRoutesSwitch(t *testing.T) {
	both := map[string]string{EnvInviteTokenSecret: testInviteSecret, EnvInviteTokenSecretPrevious: testInvitePrevious}
	for _, c := range []struct {
		name    string
		env     map[string]string
		want    bool
		wantErr string
	}{
		{name: "unset is off", env: map[string]string{}},
		{name: "false is off and needs no key", env: map[string]string{EnvMembershipRoutes: "false"}},
		{name: "keys alone mount nothing", env: both},
		{name: "true with both keys", env: merge(both, map[string]string{EnvMembershipRoutes: "true"}), want: true},
		{name: "true without the signing key", env: map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecretPrevious: testInvitePrevious},
			wantErr: EnvMembershipRoutes + "=true needs " + EnvInviteTokenSecret},
		{name: "true without the previous key", env: map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecret: testInviteSecret},
			wantErr: EnvMembershipRoutes + "=true needs " + EnvInviteTokenSecretPrevious},
		{name: "true with a blank signing key", env: merge(both, map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecret: ""}),
			wantErr: EnvMembershipRoutes + "=true needs " + EnvInviteTokenSecret},
		{name: "true without the session secrets", env: merge(both, map[string]string{EnvMembershipRoutes: "true",
			EnvAccessTokenSecret: "<unset>", EnvSessionSuccessorKey: "<unset>", EnvValkeyURI: "<unset>"}),
			wantErr: EnvMembershipRoutes + " needs the session secrets"},
		{name: "a typo is refused", env: merge(both, map[string]string{EnvMembershipRoutes: "yes"}),
			wantErr: EnvMembershipRoutes + ` must be "true" or "false"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(c.env))})
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("Load = %v, want an error containing %q", err, c.wantErr)
				}
				for _, secret := range []string{testInviteSecret, testInvitePrevious, testAccessSecret} {
					if strings.Contains(err.Error(), secret) {
						t.Fatalf("the error names a key value: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Provisioning.Routes != c.want {
				t.Fatalf("routes = %v, want %v", cfg.Provisioning.Routes, c.want)
			}
		})
	}
}

func merge(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// The accept keys are the signing key, then the previous key; repeats once.
// The session JWT key is never one: an invite token signed with it is refused.
// The signing key is the only one that signs.
func TestInviteAcceptSecretsOrder(t *testing.T) {
	for _, c := range []struct {
		name     string
		previous string
		want     []string
	}{
		{"two distinct keys", testInvitePrevious, []string{testInviteSecret, testInvitePrevious}},
		{"previous equal to the signing key", testInviteSecret, []string{testInviteSecret}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(map[string]string{EnvMembershipRoutes: "true",
				EnvInviteTokenSecret: testInviteSecret, EnvInviteTokenSecretPrevious: c.previous}))})
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := cfg.InviteAcceptSecrets(); !slices.Equal(got, c.want) {
				t.Fatalf("accept keys differ from the invite keys in order (%d keys, want %d)", len(got), len(c.want))
			}
			if cfg.Provisioning.InviteSecret.Reveal() != testInviteSecret {
				t.Fatal("the signing key is not AUTH_INVITE_TOKEN_SECRET")
			}
		})
	}
	off, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(nil))})
	if err != nil {
		t.Fatal(err)
	}
	if got := off.InviteAcceptSecrets(); got != nil {
		t.Fatalf("routes off: %d accept keys, want none", len(got))
	}
}

// With the routes on, neither invite key may be the session JWT key
// (AUTH_JWT_SECRET_KEY), compared as the loaders read them (a _FILE value
// loses its final newline). The refusal names the variables, never a value.
// With the routes off, the keys are not read for this rule.
func TestInviteKeysMayNotBeTheJWTKey(t *testing.T) {
	file := filepath.Join(t.TempDir(), "invite-key")
	if err := os.WriteFile(file, []byte(testAccessSecret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	on := map[string]string{EnvMembershipRoutes: "true"}
	for _, c := range []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"the signing key is the JWT key", merge(on, map[string]string{EnvInviteTokenSecret: testAccessSecret,
			EnvInviteTokenSecretPrevious: testInvitePrevious}), EnvInviteTokenSecret + " must not equal " + EnvAccessTokenSecret},
		{"the previous key is the JWT key", merge(on, map[string]string{EnvInviteTokenSecret: testInviteSecret,
			EnvInviteTokenSecretPrevious: testAccessSecret}), EnvInviteTokenSecretPrevious + " must not equal " + EnvAccessTokenSecret},
		{"the signing key file holds the JWT key", merge(on, map[string]string{EnvInviteTokenSecret + "_FILE": file,
			EnvInviteTokenSecretPrevious: testInvitePrevious}), EnvInviteTokenSecret + " must not equal " + EnvAccessTokenSecret},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(c.env))})
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("Load = %v, want an error containing %q", err, c.wantErr)
			}
			for _, secret := range []string{testInviteSecret, testInvitePrevious, testAccessSecret} {
				if strings.Contains(err.Error(), secret) {
					t.Fatal("the error names a key value")
				}
			}
		})
	}
	off, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(map[string]string{EnvInviteTokenSecret: testAccessSecret,
		EnvInviteTokenSecretPrevious: testAccessSecret}))})
	if err != nil {
		t.Fatalf("routes off: Load = %v, want no refusal", err)
	}
	if off.Provisioning.Routes {
		t.Fatal("routes off: the routes are on")
	}
}

func TestSafeAttrsNamesTheRoutesSwitchAndNoInviteKey(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(map[string]string{EnvMembershipRoutes: "true",
		EnvInviteTokenSecret: testInviteSecret, EnvInviteTokenSecretPrevious: testInvitePrevious}))})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, attr := range cfg.SafeAttrs() {
		if attr.Key == "membership_routes_enabled" && attr.Value.Bool() {
			found = true
		}
		if v := attr.Value.String(); strings.Contains(v, testInviteSecret) || strings.Contains(v, testInvitePrevious) {
			t.Fatalf("start line attribute %s carries an invite key", attr.Key)
		}
	}
	if !found {
		t.Fatal("start line has no membership_routes_enabled=true")
	}
}
