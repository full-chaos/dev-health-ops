package authconfig

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClientAddrVerifyKeyRefusesEmbeddedLineBreaks(t *testing.T) {
	good := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("d", 32)))
	file := func(content string) string {
		path := filepath.Join(t.TempDir(), "verify-key")
		if err := os.WriteFile(path, []byte(content), 0o400); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for name, sep := range map[string]string{"LF": "\n", "CRLF": "\r\n"} {
		broken := "k1:" + good[:20] + sep + good[20:]
		env := baseEnv()
		env[EnvClientAddrVerifyKey] = broken
		if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
			t.Errorf("env, %s: accepted", name)
		}
		env = baseEnv()
		env[EnvClientAddrVerifyKey+"_FILE"] = file(broken + "\n")
		if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
			t.Errorf("_FILE, %s: accepted", name)
		}
	}
	env := baseEnv()
	env[EnvClientAddrVerifyKey+"_FILE"] = file("k1:" + good + "\n")
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil || len(cfg.ClientAddrKeys) != 1 {
		t.Fatalf("_FILE with a trailing newline: keys %d, err %v", len(cfg.ClientAddrKeys), err)
	}
	env = baseEnv()
	env[EnvClientAddrVerifyKey] = "k1:" + good
	if cfg, err = Load(Spec{LookupEnv: lookupFrom(env)}); err != nil || len(cfg.ClientAddrKeys) != 1 {
		t.Fatalf("valid env key: keys %d, err %v", len(cfg.ClientAddrKeys), err)
	}
}
