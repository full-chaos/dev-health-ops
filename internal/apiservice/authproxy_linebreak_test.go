package apiservice

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

func TestAuthProxySigningKeyRefusesEmbeddedLineBreaks(t *testing.T) {
	good := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("e", 32)))
	parse := func(value string) (AuthProxyConfig, error) {
		return ParseAuthProxyConfig(func(name string) (string, bool) {
			if name == clientaddr.EnvSigningKey {
				return value, true
			}
			return "", false
		})
	}
	for name, sep := range map[string]string{"LF": "\n", "CRLF": "\r\n"} {
		if _, err := parse("k1:" + good[:20] + sep + good[20:]); err == nil {
			t.Errorf("go-api signing env, %s: accepted", name)
		}
	}
	cfg, err := parse("k1:" + good)
	if err != nil || cfg.ClientAddrKey == nil {
		t.Fatalf("valid signing key: %v", err)
	}
}
