package apiservice

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestInviteConfigNeverUsesTheJWTOrEncryptionKeyForLinks(t *testing.T) {
	t.Setenv("EMAIL_PROVIDER", "console")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	cfg := config.Config{APIJWTSecret: secrets.NewValue("jwt-key"), SettingsEncryptionKey: secrets.NewValue("settings-key")}
	got := inviteConfig(cfg, logger, inviteLookup(nil))
	if got.Configured() || got.Secret() != "" || len(got.AcceptSecrets()) != 0 {
		t.Fatalf("link tokens configured from the JWT or encryption key: secret %d bytes, accepts %d", len(got.Secret()), len(got.AcceptSecrets()))
	}
	if !strings.Contains(logs.String(), "LINK_TOKEN_SECRET") {
		t.Fatalf("no startup line names LINK_TOKEN_SECRET: %q", logs.String())
	}
	logs.Reset()
	set := inviteConfig(config.Config{LinkTokenSecret: secrets.NewValue("link-new"), LinkTokenSecretPrevious: secrets.NewValue("link-old"),
		APIJWTSecret: secrets.NewValue("jwt-key")}, logger, inviteLookup(nil))
	if !set.Configured() || len(set.AcceptSecrets()) != 2 || strings.Contains(logs.String(), "LINK_TOKEN_SECRET") {
		t.Fatalf("configured case: configured %v accepts %v log %q", set.Configured(), set.AcceptSecrets(), logs.String())
	}
}
