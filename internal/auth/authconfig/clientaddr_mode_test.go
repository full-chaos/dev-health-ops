package authconfig

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

func TestClientAddrModeAndKeysAreStartChecks(t *testing.T) {
	entry := "k1:" + base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("c", 32)))
	short := "k1:" + base64.RawURLEncoding.EncodeToString([]byte("short"))
	for _, c := range []struct {
		name, mode, keys string
		want             clientaddr.Mode
		ok               bool
	}{
		{"unset = cidr", "", "", clientaddr.ModeCIDR, true},
		{"cidr with a key (count only)", "cidr", entry, clientaddr.ModeCIDR, true},
		{"both with a key", "both", entry, clientaddr.ModeBoth, true},
		{"signed with a key", "signed", entry, clientaddr.ModeSigned, true},
		{"both without a key", "both", "", "", false},
		{"signed without a key", "signed", "", "", false},
		{"unknown mode", "proof", entry, "", false},
		{"short key", "cidr", short, "", false},
	} {
		env := baseEnv()
		if c.mode != "" {
			env[EnvClientAddrMode] = c.mode
		}
		if c.keys != "" {
			env[EnvClientAddrVerifyKey] = c.keys
		}
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if (err == nil) != c.ok {
			t.Errorf("%s: err %v, want ok %v", c.name, err, c.ok)
			continue
		}
		if err != nil {
			if strings.Contains(err.Error(), entry[3:]) || strings.Contains(err.Error(), short[3:]) {
				t.Errorf("%s: the error echoes the key: %v", c.name, err)
			}
			continue
		}
		if cfg.ClientAddrMode != c.want {
			t.Errorf("%s: mode %q, want %q", c.name, cfg.ClientAddrMode, c.want)
		}
		line := fmt.Sprint(slog.GroupValue(cfg.SafeAttrs()...))
		if !strings.Contains(line, "client_addr_mode="+string(c.want)) || strings.Contains(line, entry[3:]) {
			t.Errorf("%s: startup line %q, want the mode and no key", c.name, line)
		}
	}
}
