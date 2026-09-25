//go:build integration

package sync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// fingerprintVenueKey is the SETTINGS_ENCRYPTION_KEY both planes share.
const fingerprintVenueKey = "venue-oracle-credential-fingerprint-key-32bytes!"

// pythonCredentialStampProgram runs the planner's own
// _resolve_credential_stamp for each integration and prints the stamped
// credential_fingerprint and auth_source, or "raise <class>".
const pythonCredentialStampProgram = `
import json, sys, uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool
from dev_health_ops.models.integrations import Integration
from dev_health_ops.sync import planner
payload = json.loads(sys.stdin.read())
engine = create_engine(payload["uri"], poolclass=NullPool)
out = []
for integration_id in payload["integrations"]:
    with Session(engine) as session:
        try:
            integration = session.get(Integration, uuid.UUID(integration_id))
            _, fingerprint, auth_source = planner._resolve_credential_stamp(session, integration)
            out.append([fingerprint, auth_source])
        except Exception as exc:
            out.append(["raise " + type(exc).__name__, ""])
engine.dispose()
print(json.dumps(out))
`

type fingerprintCase struct {
	integrationID string
	provider      string
	credentialID  *string
}

// TestCredentialFingerprintVenueOracleMatchesLivePython holds the
// materializer's credential_fingerprint stamp to the planner's own
// _resolve_credential_stamp over the same seeded rows: credentials of every
// provider shape encrypted by the Python plane's encrypt_value (config
// merged under the decrypted fields), environment auth, an undecryptable
// credential and a credential whose payload is not a mapping. Each
// integration gets the same fingerprint, or fails on both planes.
func TestCredentialFingerprintVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the credential fingerprint oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	ctx := context.Background()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	const org = "00000000-0000-4000-8000-0000000f1a11"
	var cases []fingerprintCase
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		PythonEnv: []string{"SETTINGS_ENCRYPTION_KEY=" + fingerprintVenueKey},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			cases = seedFingerprintCases(t, ctx, admin, venue, org)
			return nil
		},
	})

	python := pyoracle.Resolve(t, root)
	parsed, err := url.Parse(venue.AdminURI(t, venue.SourceDB))
	if err != nil {
		t.Fatal(err)
	}
	parsed.Scheme = "postgresql+psycopg2"
	ids := make([]string, len(cases))
	for index, c := range cases {
		ids[index] = c.integrationID
	}
	input, _ := json.Marshal(map[string]any{"uri": parsed.String(), "integrations": ids})
	command := exec.Command(python, "-c", pythonCredentialStampProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_SDK_DISABLED=true", "ENVIRONMENT=test",
		"SETTINGS_ENCRYPTION_KEY="+fingerprintVenueKey)
	for _, name := range []string{"GITHUB_TOKEN", "GITHUB_URL", "GITHUB_APP_ID", "GITHUB_APP_PRIVATE_KEY_PATH", "GITHUB_APP_INSTALLATION_ID",
		"GITLAB_TOKEN", "GITLAB_URL", "JIRA_BASE_URL", "JIRA_EMAIL", "JIRA_API_TOKEN", "LINEAR_API_KEY"} {
		if os.Getenv(name) != "" {
			t.Fatalf("%s is set in the test environment; the environment-auth case needs it unset on both planes", name)
		}
	}
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][2]string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(cases) {
		t.Fatalf("decode: %v\n%s", err, output)
	}

	pool, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(fingerprintVenueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	materializer := &NativeMaterializer{domainPool: pool}
	materializer.WithCredentialFingerprint(decryptor)
	stamped, raised := 0, 0
	for index, c := range cases {
		auth := "integration_credential"
		if c.credentialID == nil {
			auth = "environment"
		}
		loaded := &loadedMaterializationPlan{input: PlannerInput{OrgID: org, IntegrationID: c.integrationID}, provider: c.provider,
			credentialID: c.credentialID, authSource: &auth}
		got := ""
		if err := materializer.stampCredentialFingerprint(ctx, loaded); err != nil {
			got = "raise"
		} else if loaded.credentialFingerprint != nil {
			got = *loaded.credentialFingerprint
		}
		expected := want[index][0]
		if strings.HasPrefix(expected, "raise") {
			expected = "raise"
			raised++
		} else {
			stamped++
			if want[index][1] != auth {
				t.Errorf("case %d: python auth_source %q, go %q", index, want[index][1], auth)
			}
		}
		if got != expected {
			t.Errorf("case %d (%s): go %s, python %s", index, c.provider, got, want[index][0])
		}
	}
	if stamped == 0 || raised == 0 {
		t.Fatalf("want both stamped and refused cases, got %d stamped, %d refused", stamped, raised)
	}
	t.Logf("%d integrations: %d stamped identically, %d refused on both planes", len(cases), stamped, raised)
	venueoracle.WriteProof(t)
}

// seedFingerprintCases writes the org and one integration per credential
// shape, each credential encrypted by the Python plane, and returns the
// cases in order.
func seedFingerprintCases(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, org string) []fingerprintCase {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES ($1, 'fp', 'fp', '{}', 'enterprise', true, $2, $2)`, org, at)
	shapes := []struct {
		provider, plaintext, config string
		raw                         bool // plaintext stored as-is (not encrypted)
	}{
		{"github", `{"token": "ghp_abc"}`, `{}`, false},
		{"github", `{"token": "ghp_abc", "base_url": " https://ghe.example.com/api/v3/ "}`, `{}`, false},
		{"github", `{"app_id": 12, "installation_id": 34, "private_key": "-----BEGIN KEY-----"}`, `{}`, false},
		{"github", `{"app_id": "12", "installation_id": "34", "private_key": "pem", "token": ""}`, `{"base_url": "https://ghe/"}`, false},
		{"gitlab", `{"token": "glpat-x"}`, `{"url": "https://gitlab.example.com", "group_id": 9}`, false},
		{"gitlab", `{"private_token": "p", "project_id": 7}`, `{"base_url": "https://gl.example/"}`, false},
		{"jira", `{"email": "e@example.com", "api_token": "t<&>", "base_url": "https://x.atlassian.net/"}`, `{"cloud_id": "c-1"}`, false},
		{"linear", `{"api_key": "lin_api_x", "workspace_id": "w"}`, `{}`, false},
		{"launchdarkly", `{"api_key": "ld", "environment": "production", "project_key": "default"}`, `{}`, false},
		{"github", `{"unknown": "x"}`, `{}`, false},
		{"github", `{}`, `{}`, false},
		{"github", `{"token": 123, "username": null}`, `{}`, false},
		{"jira", `{"refresh_token": "r", "client_id": "cid", "client_secret": "cs", "oauth_binding_id": "b"}`, `{}`, false},
		{"github", `["not", "a", "mapping"]`, `{}`, false},
		{"github", `not a fernet token`, `{}`, true},
	}
	var calls []venueoracle.PythonCall
	for _, shape := range shapes {
		if !shape.raw {
			calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{shape.plaintext}})
		}
	}
	encrypted := venue.CallPython(t, calls...)
	var cases []fingerprintCase
	next := 0
	for _, shape := range shapes {
		var ciphertext string
		if shape.raw {
			ciphertext = base64.StdEncoding.EncodeToString([]byte(shape.plaintext))
		} else {
			if err := json.Unmarshal(encrypted[next], &ciphertext); err != nil {
				t.Fatal(err)
			}
			next++
		}
		credentialID, integrationID := uuid.New(), uuid.New()
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, $5, $6::json, $7, $7)`, credentialID, org, shape.provider, "cred-"+credentialID.String()[:8], ciphertext, shape.config, at)
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '{}', true, $6, $6)`, integrationID, org, shape.provider, credentialID, "int-"+integrationID.String()[:8], at)
		id := credentialID.String()
		cases = append(cases, fingerprintCase{integrationID: integrationID.String(), provider: shape.provider, credentialID: &id})
	}
	// Environment auth: no credential; both planes read no environment
	// credentials, so both stamp the fallback scope's fingerprint.
	for _, provider := range []string{"github", "jira"} {
		integrationID := uuid.New()
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, NULL, $4, '{}', true, $5, $5)`, integrationID, org, provider, "env-"+integrationID.String()[:8], at)
		cases = append(cases, fingerprintCase{integrationID: integrationID.String(), provider: provider})
	}
	return cases
}
