package cigates_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The postgres image's entrypoint runs initdb scripts on a temporary server that listens on the unix
// socket only. A probe that uses the socket reports Ready while that server is still starting up or
// restarting, and the proof's first psql fails with "the database system is starting up". The probe
// must go over TCP, which only the real server opens.
func TestAuthK8sProofPostgresReadinessProbeUsesTCP(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "auth_k8s", "postgres.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var probe string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, "pg_isready") {
			probe = line
		}
	}
	if probe == "" {
		t.Fatal("the proof's postgres has no pg_isready readiness probe")
	}
	if !strings.Contains(probe, "-h, 127.0.0.1") {
		t.Errorf("readiness probe %q uses the unix socket, which the init-time server also answers", strings.TrimSpace(probe))
	}
}
