package authconfig

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

const (
	testInviteSecret   = "invite-config-test-signing-key-01"
	testInvitePrevious = "invite-config-test-previous-key-02"
)

func provisioningEnv(set map[string]string) map[string]string {
	env := baseEnv()
	env[EnvRateLimitKey], env[EnvSessionSuccessorKey], env[EnvValkeyURI] = testRateKey, testSuccessorKey, testValkeyURI
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
		{name: "true without the previous key", env: map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecret: testInviteSecret}, want: true},
		{name: "true with a blank previous key", env: merge(both, map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecretPrevious: ""}), want: true},
		{name: "true with neither key", env: map[string]string{EnvMembershipRoutes: "true"},
			wantErr: EnvMembershipRoutes + "=true needs " + EnvInviteTokenSecret},
		{name: "true with a blank signing key", env: merge(both, map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecret: ""}),
			wantErr: EnvMembershipRoutes + "=true needs " + EnvInviteTokenSecret},
		{name: "true without the session secrets", env: merge(both, map[string]string{EnvMembershipRoutes: "true",
			EnvRateLimitKey: "<unset>", EnvSessionSuccessorKey: "<unset>", EnvValkeyURI: "<unset>"}),
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
				for _, secret := range []string{testInviteSecret, testInvitePrevious, testRateKey} {
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

// The accept keys are go-api's AcceptSecrets order: the signing key, then the
// previous key; repeats once. The signing key is the only one that signs, and
// the access key is never an accept key.
func TestInviteAcceptSecretsOrder(t *testing.T) {
	for _, c := range []struct {
		name     string
		previous string
		want     []string
	}{
		{"two distinct keys", testInvitePrevious, []string{testInviteSecret, testInvitePrevious}},
		{"previous equal to the signing key", testInviteSecret, []string{testInviteSecret}},
		{"previous equal to the rate-limit key", testRateKey, []string{testInviteSecret, testRateKey}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(map[string]string{EnvMembershipRoutes: "true",
				EnvInviteTokenSecret: testInviteSecret, EnvInviteTokenSecretPrevious: c.previous}))})
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := cfg.InviteAcceptSecrets(); !slices.Equal(got, c.want) {
				t.Fatalf("accept keys differ from go-api's order (%d keys, want %d)", len(got), len(c.want))
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

func TestInviteAcceptSecretsNeverHoldTheRateLimitKey(t *testing.T) {
	env := provisioningEnv(map[string]string{EnvMembershipRoutes: "true",
		EnvInviteTokenSecret: testInviteSecret, EnvInviteTokenSecretPrevious: testInvitePrevious})
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.InviteAcceptSecrets(); slices.Contains(got, testRateKey) || len(got) != 2 {
		t.Fatalf("accept list holds %d keys, rate-limit key among them: %v", len(got), slices.Contains(got, testRateKey))
	}
}

// With no previous key the signing key is the only accept key: a token signed
// with a retired key is refused, one signed with the signing key accepted.
func TestInviteAcceptSecretsWithoutAPreviousKey(t *testing.T) {
	for _, c := range []struct {
		name string
		env  map[string]string
	}{
		{"previous unset", map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecret: testInviteSecret}},
		{"previous blank", map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecret: testInviteSecret, EnvInviteTokenSecretPrevious: ""}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(c.env))})
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			got := cfg.InviteAcceptSecrets()
			if !slices.Equal(got, []string{testInviteSecret}) {
				t.Fatalf("accept keys: %d, want the signing key only", len(got))
			}
			current, _ := signedtoken.Build(uuid.New(), testInviteSecret)
			if ok, err := signedtoken.ValidAny(current, got); err != nil || !ok {
				t.Fatalf("a token signed with the signing key is refused (ok=%v err=%v)", ok, err)
			}
			retired, _ := signedtoken.Build(uuid.New(), testInvitePrevious)
			if ok, _ := signedtoken.ValidAny(retired, got); ok {
				t.Fatal("a token signed with a retired key is accepted with no previous key set")
			}
		})
	}
}

// A previous-key file that is empty once the resolver drops its final newline
// is an unset previous key (D6465: a hygiene key never refuses a start). An
// unreadable previous file, a previous key set twice, and an empty signing-key
// file are still refused.
func TestInvitePreviousKeyFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		path := dir + "/" + name
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	routes := map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecret: testInviteSecret}
	for _, c := range []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{name: "empty file", env: merge(routes, map[string]string{EnvInviteTokenSecretPrevious + "_FILE": write("zero", "")})},
		{name: "file holding LF", env: merge(routes, map[string]string{EnvInviteTokenSecretPrevious + "_FILE": write("lf", "\n")})},
		{name: "file holding CR", env: merge(routes, map[string]string{EnvInviteTokenSecretPrevious + "_FILE": write("cr", "\r")})},
		{name: "file holding CRLF", env: merge(routes, map[string]string{EnvInviteTokenSecretPrevious + "_FILE": write("crlf", "\r\n")})},
		{name: "unreadable file", env: merge(routes, map[string]string{EnvInviteTokenSecretPrevious + "_FILE": dir + "/missing"}),
			wantErr: EnvInviteTokenSecretPrevious + "_FILE could not be read"},
		{name: "env and file both set", env: merge(routes, map[string]string{EnvInviteTokenSecretPrevious: testInvitePrevious,
			EnvInviteTokenSecretPrevious + "_FILE": write("both", testInvitePrevious+"\n")}),
			wantErr: EnvInviteTokenSecretPrevious + " and " + EnvInviteTokenSecretPrevious + "_FILE are mutually exclusive"},
		{name: "empty signing-key file", env: map[string]string{EnvMembershipRoutes: "true", EnvInviteTokenSecret + "_FILE": write("signing", "\n")},
			wantErr: EnvInviteTokenSecret + "_FILE contains an empty value"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Load(Spec{LookupEnv: lookupFrom(provisioningEnv(c.env))})
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("Load = %v, want an error containing %q", err, c.wantErr)
				}
				if strings.Contains(err.Error(), testInvitePrevious) || strings.Contains(err.Error(), testInviteSecret) {
					t.Fatalf("the error names a key value: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := cfg.InviteAcceptSecrets(); !slices.Equal(got, []string{testInviteSecret}) {
				t.Fatalf("accept keys: %d, want the signing key only", len(got))
			}
		})
	}
}
