package principal

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// pythonIssuedEnvelopeProgram mints a REAL effective-principal envelope with
// principal_envelope.issue_effective_principal_envelope (the exact function
// query-api's Go verifier is written against) and exports its REAL JWKS via
// build_envelope_jwks -- not a hand-authored payload. Both entry points are
// DB-free: get_impersonation_context reads an unset contextvar (None) and
// get_user_permissions computes purely from the role, so a plain
// AuthenticatedUser is enough, no app/DB bootstrap required.
const pythonIssuedEnvelopeProgram = `
import json
import os

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat

key = Ed25519PrivateKey.generate()
os.environ["GO_API_ENVELOPE_PRIVATE_KEY"] = key.private_bytes(
    encoding=Encoding.PEM,
    format=PrivateFormat.PKCS8,
    encryption_algorithm=NoEncryption(),
).decode("utf-8")

from dev_health_ops.api.graphql import principal_envelope
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing.types import LicenseTier

user = AuthenticatedUser(
    user_id="11111111-1111-4111-8111-111111111111",
    email="dev@example.com",
    org_id="org-1",
    role="admin",
    is_superuser=False,
    is_superuser_verified=False,
    token_version=3,
)
token = principal_envelope.issue_effective_principal_envelope(
    user, tier=LicenseTier.TEAM, licensed_features=["ai_review"],
)
jwks = principal_envelope.build_envelope_jwks()
print(json.dumps({
    "token": token,
    "jwks": jwks,
    "issuer": principal_envelope.ENVELOPE_ISSUER,
    "audience": principal_envelope.ENVELOPE_AUDIENCE,
}))
`

// TestVerifierMatchesLivePythonIssuedEnvelope proves query-api's Go verifier
// accepts an envelope actually minted by the Python edge and actually keyed
// from the Python edge's own JWKS export -- closing the gap the Go-only
// fixture tests in verifier_test.go cannot: they sign with Go's own JWT
// library and build the JWKS in Go, so they would stay green even if
// Python's issuer serialization or key export drifted from what this
// package expects (CHAOS-4366 PR #1956 review finding).
func TestVerifierMatchesLivePythonIssuedEnvelope(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve principal package path")
	}
	// .../cmd/query-api/internal/principal -> repository root.
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", "..", ".."))
	python := filepath.Join(repositoryRoot, ".venv", "bin", "python")
	if _, err := os.Stat(python); err != nil {
		python, err = exec.LookPath("python3")
		if err != nil {
			t.Fatalf("live Python oracle interpreter: %v", err)
		}
	}

	command := exec.Command(python, "-c", pythonIssuedEnvelopeProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repositoryRoot, "src"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python envelope oracle: %v: %s", err, output)
	}

	var result struct {
		Token    string          `json:"token"`
		JWKS     json.RawMessage `json:"jwks"`
		Issuer   string          `json:"issuer"`
		Audience string          `json:"audience"`
	}
	// The oracle's own stdout can carry warnings above the JSON line (e.g.
	// otel exporter noise seen elsewhere in this repo's Python subprocess
	// oracles); take the last line, which is this program's only print().
	lines := splitNonEmptyLines(output)
	if len(lines) == 0 {
		t.Fatalf("live Python envelope oracle produced no output: %s", output)
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil {
		t.Fatalf("decode live Python envelope oracle output: %v: %s", err, output)
	}

	jwksPath := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(jwksPath, result.JWKS, 0o600); err != nil {
		t.Fatalf("write jwks: %v", err)
	}

	v, err := NewVerifier(jwksPath, result.Issuer, result.Audience)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	claims, err := v.Verify(result.Token)
	if err != nil {
		t.Fatalf("Verify: real Python-issued envelope was rejected: %v", err)
	}
	if claims.UserID() != "11111111-1111-4111-8111-111111111111" {
		t.Errorf("UserID = %q", claims.UserID())
	}
	if claims.OrgID != "org-1" {
		t.Errorf("OrgID = %q, want org-1", claims.OrgID)
	}
	if claims.Role != "admin" {
		t.Errorf("Role = %q, want admin", claims.Role)
	}
	if claims.Tier != "team" {
		t.Errorf("Tier = %q, want team", claims.Tier)
	}
	if len(claims.Permissions) == 0 {
		t.Error("Permissions: got none, want the admin role's permission set")
	}

	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(
		filepath.Join(proofDir, "query-api-principal-envelope"),
		[]byte("executed"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
}

func splitNonEmptyLines(b []byte) []string {
	var lines []string
	start := 0
	for i, c := range b {
		if c == '\n' {
			if line := string(b[start:i]); line != "" {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	if line := string(b[start:]); line != "" {
		lines = append(lines, line)
	}
	return lines
}
