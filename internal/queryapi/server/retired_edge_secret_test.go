package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// edgeJWTSecretEnvVar is the name of the retired HS256 edge secret. No production code reads it; tests set it to show
// that a leftover value changes nothing.
const edgeJWTSecretEnvVar = "GO_API_EDGE_JWT_SECRET"

// A leftover GO_API_EDGE_JWT_SECRET is read by nothing: Build starts, and no log line names it or carries its value.
func TestQueryAPIBuildIgnoresRetiredEdgeSecret(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	settings := map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, "DEV_HEALTH_ENV": "ci"}
	plane, err := Build(func(key string) string { return settings[key] })
	if err != nil {
		t.Fatalf("Build with only the retired edge secret set: %v", err)
	}
	defer plane.Close()
	if strings.Contains(logged.String(), edgeJWTSecretEnvVar) || strings.Contains(logged.String(), edgeTestSecret) {
		t.Errorf("a log line names %s or carries its value:\n%s", edgeJWTSecretEnvVar, logged.String())
	}
}
