package authconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testLimitedValkeyURI = "redis://auth-ratelimit:valkey-secret-pw-4711@valkey.internal:6379/1"

func withValkey(env map[string]string, uri string) map[string]string {
	env[EnvValkeyURI] = uri
	return env
}

// The one mount set prod runs today: JWKS and discovery, nothing limited.
func TestTheDormantMountSetNeedsNoValkey(t *testing.T) {
	env := baseEnv()
	env[EnvTokenIssuerURL] = "https://auth.example.com"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("prod's mount set without AUTH_VALKEY_URI: %v", err)
	}
	if cfg.LimitedRoutesMounted() {
		t.Fatal("a limited route is reported mounted with no session secret")
	}
	if cfg.ValkeyURI.Configured() {
		t.Fatal("a Valkey URI appeared from nowhere")
	}
	// Session secrets alone mount the session, compatibility refresh and
	// logout routes: none of them counts in the store.
	session := sessionEnv(testAccessSecret, testSuccessorKey)
	delete(session, EnvValkeyURI)
	cfg, err = Load(Spec{LookupEnv: lookupFrom(session)})
	if err != nil || cfg.LimitedRoutesMounted() {
		t.Fatalf("session routes: err %v, limited mounted %v", err, cfg.LimitedRoutesMounted())
	}
}

func TestEveryLimitedRouteNeedsTheValkeyURI(t *testing.T) {
	mount := map[string]func(map[string]string){
		"login":       func(env map[string]string) { env[EnvLoginLockoutKey] = testLockoutKey },
		"credentials": func(env map[string]string) { env[EnvUsersOwned] = "true" },
		"social link": func(env map[string]string) { env[EnvSocialLinkRoutes] = "true" },
	}
	for name, set := range mount {
		bare := sessionEnv(testAccessSecret, testSuccessorKey)
		delete(bare, EnvValkeyURI)
		set(bare)
		_, err := Load(Spec{LookupEnv: lookupFrom(bare)})
		if err == nil || !strings.Contains(err.Error(), EnvValkeyURI) {
			t.Errorf("%s without the URI: err %v, want a refusal naming %s", name, err, EnvValkeyURI)
		}
		withURI := sessionEnv(testAccessSecret, testSuccessorKey)
		set(withURI)
		cfg, err := Load(Spec{LookupEnv: lookupFrom(withValkey(withURI, testLimitedValkeyURI))})
		if err != nil || !cfg.LimitedRoutesMounted() || cfg.ValkeyURI.Reveal() != testLimitedValkeyURI {
			t.Errorf("%s with the URI: err %v, mounted %v", name, err, cfg.LimitedRoutesMounted())
		}
	}
	// users-owned without the session secrets mounts nothing.
	env := baseEnv()
	env[EnvUsersOwned] = "true"
	if cfg, err := Load(Spec{LookupEnv: lookupFrom(env)}); err != nil || cfg.LimitedRoutesMounted() {
		t.Errorf("users-owned without session secrets: err %v, mounted %v", err, cfg.LimitedRoutesMounted())
	}
}

func TestAValkeyURIWithNoLimitedRouteIsInertButMustBeWellFormed(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(withValkey(baseEnv(), testLimitedValkeyURI))})
	if err != nil || cfg.LimitedRoutesMounted() {
		t.Fatalf("err %v, limited mounted %v", err, cfg.LimitedRoutesMounted())
	}
	for name, uri := range map[string]string{
		"http scheme":   "http://valkey.internal:6379/1",
		"database 0":    "redis://valkey.internal:6379/0",
		"no database":   "redis://valkey.internal:6379",
		"database 15":   "redis://valkey.internal:6379/15",
		"no host":       "redis:///1",
		"not a URI":     "valkey secret-pw-4711",
		"unix socket":   "unix:///run/valkey.sock",
		"extra segment": "redis://valkey.internal:6379/1/2",
	} {
		_, err := Load(Spec{LookupEnv: lookupFrom(withValkey(baseEnv(), uri))})
		if err == nil || !strings.Contains(err.Error(), EnvValkeyURI) {
			t.Errorf("%s: err %v, want a refusal naming %s", name, err, EnvValkeyURI)
			continue
		}
		if strings.Contains(err.Error(), "secret-pw-4711") || strings.Contains(err.Error(), "valkey.internal") {
			t.Errorf("%s: the refusal repeats the value: %v", name, err)
		}
	}
}

func TestTheValkeyURIComesFromTheEnvironmentOrAFileNeverBoth(t *testing.T) {
	file := filepath.Join(t.TempDir(), "valkey-uri")
	if err := os.WriteFile(file, []byte(testLimitedValkeyURI+"\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	env := baseEnv()
	env[EnvValkeyURI+"_FILE"] = file
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil || cfg.ValkeyURI.Reveal() != testLimitedValkeyURI {
		t.Fatalf("from a file: err %v", err)
	}
	env[EnvValkeyURI] = testLimitedValkeyURI
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
		t.Fatal("both AUTH_VALKEY_URI and its _FILE were accepted")
	}
}

func TestTheValkeyURIReachesNoPrintedForm(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(withValkey(baseEnv(), testLimitedValkeyURI))})
	if err != nil {
		t.Fatal(err)
	}
	printed := cfg.SafeAttrs()
	for _, attr := range printed {
		if strings.Contains(attr.String(), "valkey-secret-pw-4711") || strings.Contains(attr.String(), "valkey.internal") {
			t.Fatalf("the URI reached the startup log attribute %s", attr)
		}
	}
}
