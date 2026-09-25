//go:build integration

package workerservice

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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

// teamCatalogFingerprintVenueKey is the SETTINGS_ENCRYPTION_KEY both planes share.
const teamCatalogFingerprintVenueKey = "venue-oracle-team-catalog-fingerprint-key-32b!"

// pythonTeamCatalogStampProgram runs the planner's own
// _resolve_credential_stamp for each integration and prints the stamp a
// new sync run would carry: [credential_id, credential_fingerprint,
// auth_source], or ["raise <class>", "", ""].
const pythonTeamCatalogStampProgram = `
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
            credential_id, fingerprint, auth_source = planner._resolve_credential_stamp(session, integration)
            out.append([str(credential_id) if credential_id is not None else "", fingerprint or "", auth_source or ""])
        except Exception as exc:
            out.append(["raise " + type(exc).__name__, "", ""])
engine.dispose()
print(json.dumps(out))
`

type teamCatalogFingerprintCase struct {
	integrationID, credentialID, provider string
	// resolverRefuses names why providerfoundation.CredentialResolver
	// refuses this shape before the fingerprint check runs; empty when the
	// shape reaches the check.
	resolverRefuses string
}

// TestTeamCatalogFingerprintVenueOracleMatchesLivePython holds the team
// catalog seam's stamped-run check to the planner's own stamp. Python's
// _resolve_credential_stamp stamps a run over each seeded credential (every
// team-catalog provider, several credential shapes, config merged under the
// decrypted fields); the Go resolver must accept every such stamp as its
// own. A credential whose secret content is edited in place after the stamp
// must still fail closed with the distinct mismatch error.
func TestTeamCatalogFingerprintVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the team catalog fingerprint oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	ctx := context.Background()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	const org = "00000000-0000-4000-8000-0000000fca71"
	var cases []teamCatalogFingerprintCase
	var tampered string
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		PythonEnv: []string{"SETTINGS_ENCRYPTION_KEY=" + teamCatalogFingerprintVenueKey},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			cases, tampered = seedTeamCatalogFingerprintCases(t, ctx, admin, venue, org)
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
	command := exec.Command(python, "-c", pythonTeamCatalogStampProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_SDK_DISABLED=true", "ENVIRONMENT=test",
		"SETTINGS_ENCRYPTION_KEY="+teamCatalogFingerprintVenueKey)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var stamps [][3]string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &stamps); err != nil || len(stamps) != len(cases) {
		t.Fatalf("decode: %v\n%s", err, output)
	}

	pool, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(teamCatalogFingerprintVenueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	resolver := teamCatalogClientResolver{
		pool: pool,
		credentials: providerfoundation.CredentialResolver{
			Repository: providerfoundation.PostgresCredentialRepository{Pool: pool},
			Decryptor:  decryptor,
		},
		doer:  &http.Client{},
		retry: providerfoundation.DefaultRetryPolicy(),
	}

	run := func(c teamCatalogFingerprintCase, stamp [3]string) error {
		runID := uuid.NewString()
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id, org_id, integration_id, credential_id, credential_fingerprint, auth_source, triggered_by, mode, status,
	total_units, completed_units, failed_units, created_at)
VALUES ($1, $2, $3, $4, $5, $6, 'scheduled', 'incremental', 'running', 0, 0, 0, now())`,
			runID, org, c.integrationID, stamp[0], stamp[1], stamp[2]); err != nil {
			t.Fatalf("insert sync run: %v", err)
		}
		_, _, _, err := resolver.ResolveClient(ctx, org, runID, c.provider)
		return err
	}

	verified, resolved, refused := 0, 0, 0
	for index, c := range cases {
		stamp := stamps[index]
		if strings.HasPrefix(stamp[0], "raise") {
			t.Fatalf("case %d (%s): python refused to stamp: %s", index, c.provider, stamp[0])
		}
		if stamp[0] != c.credentialID || stamp[2] != "integration_credential" || stamp[1] == "" {
			t.Fatalf("case %d (%s): python stamp %v, want credential %s with a fingerprint", index, c.provider, stamp, c.credentialID)
		}
		// The check runs after the credential resolves and before the client
		// is built, and both of those fail with the same error. A twin run
		// with a wrong witness must fail with the mismatch, which proves the
		// check ran on this shape rather than an earlier resolve error.
		wrong := stamp
		wrong[1] = strings.Repeat("0", 64)
		wrongErr := run(c, wrong)
		if c.resolverRefuses != "" {
			if errors.Is(wrongErr, errTeamCatalogCredentialFingerprintMismatch) || !errors.Is(wrongErr, providerfoundation.ErrCredentialInvalid) {
				t.Fatalf("case %d (%s): want the resolver to refuse it before the check (%s), got %v", index, c.provider, c.resolverRefuses, wrongErr)
			}
			refused++
			continue
		}
		if !errors.Is(wrongErr, errTeamCatalogCredentialFingerprintMismatch) {
			t.Fatalf("case %d (%s): a wrong witness gave %v, so the check did not run on this shape", index, c.provider, wrongErr)
		}
		err := run(c, stamp)
		if errors.Is(err, errTeamCatalogCredentialFingerprintMismatch) {
			t.Errorf("case %d (%s): go refused python's own stamp: %v", index, c.provider, err)
			continue
		}
		verified++
		if err == nil {
			resolved++
		}
	}

	// The in-place secret edit: stamp case 0, then rewrite its ciphertext.
	stamp := stamps[0]
	if _, err := pool.Exec(ctx, `UPDATE integration_credentials SET credentials_encrypted = $1 WHERE id = $2`,
		tampered, cases[0].credentialID); err != nil {
		t.Fatal(err)
	}
	if err := run(cases[0], stamp); !errors.Is(err, errTeamCatalogCredentialFingerprintMismatch) {
		t.Fatalf("an in-place secret edit after the stamp: err = %v, want the fingerprint mismatch", err)
	}
	// The edit-and-restore race (#3099 r1): the resolver's read sees an
	// edited secret, and the row is back to the stamped content by the time
	// anything else could read it. The check must hash the row the client
	// was built from, so it must refuse; a second read of the restored row
	// would have matched the stamp and sent the edited secret.
	var original string
	if err := pool.QueryRow(ctx, `SELECT credentials_encrypted FROM integration_credentials WHERE id = $1`, cases[1].credentialID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	raced := resolver
	raced.credentials.Repository = editedAtReadRepository{inner: resolver.credentials.Repository, ciphertext: tampered}
	racedRunID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id, org_id, integration_id, credential_id, credential_fingerprint, auth_source, triggered_by, mode, status,
	total_units, completed_units, failed_units, created_at)
VALUES ($1, $2, $3, $4, $5, $6, 'scheduled', 'incremental', 'running', 0, 0, 0, now())`,
		racedRunID, org, cases[1].integrationID, stamps[1][0], stamps[1][1], stamps[1][2]); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := raced.ResolveClient(ctx, org, racedRunID, cases[1].provider); !errors.Is(err, errTeamCatalogCredentialFingerprintMismatch) {
		t.Fatalf("a secret edited at the resolver's read and restored after it: err = %v, want the fingerprint mismatch", err)
	}
	var after string
	if err := pool.QueryRow(ctx, `SELECT credentials_encrypted FROM integration_credentials WHERE id = $1`, cases[1].credentialID).Scan(&after); err != nil || after != original {
		t.Fatalf("the raced row changed in the database (%v); the race must be at read time only", err)
	}
	t.Logf("%d credentials: %d python stamps accepted by go (%d resolved a client), %d refused by the resolver before the check; the edited secret fails closed", len(cases), verified, resolved, refused)
	venueoracle.WriteProof(t)
}

// seedTeamCatalogFingerprintCases writes the org and one integration per
// credential shape, each credential encrypted by the Python plane, and
// returns the cases plus a Python-encrypted replacement secret for case 0.
func seedTeamCatalogFingerprintCases(
	t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, org string,
) ([]teamCatalogFingerprintCase, string) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES ($1, 'tcfp', 'tcfp', '{}', 'enterprise', true, $2, $2)`, org, at)
	shapes := []struct{ provider, plaintext, config, resolverRefuses string }{
		{"linear", `{"api_key": "lin_api_x", "workspace_id": "w"}`, `{}`, ""},
		{"linear", `{"api_key": "lin_api_y"}`, `{"team_id": "t-1", "base_url": "https://api.linear.app/"}`, ""},
		{"github", `{"token": "ghp_abc"}`, `{}`, ""},
		{"github", `{"token": "ghp_abc", "base_url": " https://ghe.example.com/api/v3/ "}`, `{}`, ""},
		{"github", `{"app_id": 12, "installation_id": 34, "private_key": "-----BEGIN KEY-----"}`, `{}`, ""},
		{"github", `{"app_id": "12", "installation_id": "34", "private_key": "pem", "token": ""}`, `{"base_url": "https://ghe/"}`, ""},
		{"gitlab", `{"token": "glpat-x"}`, `{"url": "https://gitlab.example.com", "group_id": 9}`, ""},
		{"gitlab", `{"private_token": "p", "project_id": 7}`, `{"base_url": "https://gl.example/"}`, "a GitLab private_token without token"},
		{"jira", `{"email": "e@example.com", "api_token": "t<&>", "base_url": "https://x.atlassian.net/"}`, `{"cloud_id": "c-1"}`, ""},
		{"jira", `{"refresh_token": "r", "client_id": "cid", "client_secret": "cs", "oauth_binding_id": "b"}`, `{}`, ""},
		{"github", `["not", "a", "mapping"]`, `{}`, "a payload that is not a mapping"},
		// No identifier or secret field: Python hashes the
		// {credential_id, integration_id} fallback scope, and the resolver
		// accepts the credential, so the check sees that scope.
		{"linear", `{"unknown": "x"}`, `{}`, ""},
	}
	calls := make([]venueoracle.PythonCall, 0, len(shapes)+1)
	for _, shape := range shapes {
		calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{shape.plaintext}})
	}
	calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value",
		Args: []any{`{"api_key": "lin_api_rotated_in_place", "workspace_id": "w"}`}})
	encrypted := venue.CallPython(t, calls...)
	var cases []teamCatalogFingerprintCase
	for index, shape := range shapes {
		var ciphertext string
		if err := json.Unmarshal(encrypted[index], &ciphertext); err != nil {
			t.Fatal(err)
		}
		credentialID, integrationID := uuid.New(), uuid.New()
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, $5, $6::json, $7, $7)`, credentialID, org, shape.provider, "cred-"+credentialID.String()[:8], ciphertext, shape.config, at)
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '{}', true, $6, $6)`, integrationID, org, shape.provider, credentialID, "int-"+integrationID.String()[:8], at)
		cases = append(cases, teamCatalogFingerprintCase{integrationID: integrationID.String(), credentialID: credentialID.String(),
			provider: shape.provider, resolverRefuses: shape.resolverRefuses})
	}
	var tampered string
	if err := json.Unmarshal(encrypted[len(shapes)], &tampered); err != nil {
		t.Fatal(err)
	}
	return cases, tampered
}

// editedAtReadRepository returns its inner repository's row with the
// ciphertext replaced: the row as an edit-and-restore race would have
// shown it to the resolver, while the database keeps the stamped content.
type editedAtReadRepository struct {
	inner      providerfoundation.CredentialRepository
	ciphertext string
}

func (repository editedAtReadRepository) ResolveEncrypted(
	ctx context.Context, scope providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	record, err := repository.inner.ResolveEncrypted(ctx, scope)
	if err == nil {
		record.Ciphertext = secrets.NewValue(repository.ciphertext)
	}
	return record, err
}
