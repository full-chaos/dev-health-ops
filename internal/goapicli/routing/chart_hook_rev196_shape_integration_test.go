//go:build integration

package routing

// D3001/D2855: the chart's pre-upgrade routing-carry hook, driven end to end through the
// REAL rendered shell script AND the real `dho` binary, against a real Postgres seeded in
// the exact rev196 shape (a live row whose candidate_build lags the actually-running
// build, no schema change involved in that staleness) -- stale_build -> repoint ->
// retry -> success. The existing Go suite (TestCarryJSONReasonIsStaleBuildWhenARowNamesA
// BuildNotRunning, this package) already proves the CLI's own -json/reason contract for
// this shape; test_helm_routing_carry_hooks_behavior.py already proves the chart SCRIPT's
// shell control flow reacts to it correctly, with a STUBBED dho. Neither proves the
// SHELL SCRIPT and the REAL CLI together reach the right end state -- that gap is exactly
// what an earlier repro this session got wrong in the other direction (concluding -json
// did not exist at all, from a stale branch) -- so this closes it directly, once, with
// both halves real.
//
// The chart script passes no -catalog/-documents override, so it resolves against the
// real production catalog/documents dump (goapiproof.DefaultCatalogPath/
// DefaultDocumentsPath) -- this test therefore uses a REAL registered operation from
// those files, never a synthetic one, and runs the real dho binary with its cwd at the
// repo root so those relative default paths resolve exactly as they do in production.

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func buildRealDho(t *testing.T, root string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "dho")
	cmd := exec.Command("go", "build", "-o", binary, "./cmd/dho")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("building the real dho binary: %v\n%s", err, out)
	}
	return binary
}

// realRuntimeRoot builds a directory laid out exactly like the go-api-tools image's own
// workdir: the real catalog at its real relative path, and a documents.json generated the
// SAME way docker/go-api-tools.Dockerfile generates it at image build time (`registrydump
// -file internal/queryapi/server/query_route.go`) -- never hand-copied. The chart script
// passes no -catalog/-documents override, so this is what makes the real dho binary's
// defaults resolve to the SAME registered operations production does, from a directory
// this test fully controls and can seed data to exactly match.
func realRuntimeRoot(t *testing.T, root string) string {
	t.Helper()
	dir := t.TempDir()
	catalogDst := filepath.Join(dir, goapiproof.DefaultCatalogPath)
	if err := os.MkdirAll(filepath.Dir(catalogDst), 0o755); err != nil {
		t.Fatal(err)
	}
	catalogSrc, err := os.ReadFile(filepath.Join(root, goapiproof.DefaultCatalogPath))
	if err != nil {
		t.Fatalf("reading the real catalog: %v", err)
	}
	if err := os.WriteFile(catalogDst, catalogSrc, 0o644); err != nil {
		t.Fatal(err)
	}

	registrydump := filepath.Join(t.TempDir(), "registrydump")
	build := exec.Command("go", "build", "-o", registrydump, "./cmd/registrydump")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building registrydump: %v\n%s", err, out)
	}
	dump := exec.Command(registrydump, "-file", "internal/queryapi/server/query_route.go")
	dump.Dir = root
	documentsOut, err := dump.Output()
	if err != nil {
		t.Fatalf("running registrydump: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, goapiproof.DefaultDocumentsPath), documentsOut, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// aRealRegisteredOperation picks one operation + its real document text out of
// runtimeRoot's catalog/documents pair -- so the fake registry/Postgres seed below names
// something the real dho binary, run with no -catalog/-documents override and this same
// cwd, will independently resolve to the SAME digest.
func aRealRegisteredOperation(t *testing.T, runtimeRoot string) (operation, document, digest string) {
	t.Helper()
	catalog, err := goapiproof.LoadOperationCatalog(filepath.Join(runtimeRoot, goapiproof.DefaultCatalogPath))
	if err != nil {
		t.Fatalf("loading the real catalog: %v", err)
	}
	documents, err := goapiproof.LoadDocuments(filepath.Join(runtimeRoot, goapiproof.DefaultDocumentsPath))
	if err != nil {
		t.Fatalf("loading the real documents dump: %v", err)
	}
	for name, text := range documents {
		if catalog[name] != "" {
			return name, text, goapidigest.Document(text)
		}
	}
	t.Fatal("no operation appears in both the real catalog and the real documents dump")
	return "", "", ""
}

// renderPreUpgradeCarryScript renders the chart's real pre-upgrade routing-carry Job the
// same way `helm template` would for a real upgrade, and returns its container's shell
// script verbatim -- never a hand-copied guess of what the chart says.
func renderPreUpgradeCarryScript(t *testing.T, chartRoot string) string {
	t.Helper()
	const pinnedToolsImage = "ghcr.io/full-chaos/dev-health-go-api-tools@sha256:df5bb659aa5d38624de3c5f33cd66a929d9196f8baee07bb164c3d64b5772c38"
	cmd := exec.Command("helm", "template", "rev196", chartRoot,
		"--is-upgrade",
		"--set", "queryApi.enabled=true",
		"--set", "web.env.BACKEND_URL=http://backend.test:8000", // CHAOS-8310: required, and irrelevant to this render
		"--set", "migrations.hook.goApiRoutingTools.image="+pinnedToolsImage,
		"--set", "migrations.hook.goApiRoutingTools.mintOrg=00000000-0000-0000-0000-000000000000",
	)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("helm template: %v", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var doc map[string]any
		if decErr := dec.Decode(&doc); decErr != nil {
			break
		}
		if doc == nil {
			continue
		}
		meta, _ := doc["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if doc["kind"] == "Job" && strings.HasSuffix(name, "-dev-health-routing-carry") {
			spec := doc["spec"].(map[string]any)
			template := spec["template"].(map[string]any)
			podSpec := template["spec"].(map[string]any)
			containers := podSpec["containers"].([]any)
			container := containers[0].(map[string]any)
			args := container["args"].([]any)
			return args[0].(string)
		}
	}
	t.Fatal("no routing-carry Job found in the rendered chart")
	return ""
}

// TestPreUpgradeCarryHookRealDhoRev196ShapeStaleBuildRepointRetrySucceeds is the full
// chain: a real Postgres row lagging the actually-running build (no schema change
// involved in that staleness), driven through the chart's REAL rendered shell script with
// the REAL dho binary on PATH. Asserts the script reaches "OK after repoint-then-retry"
// and that the row's candidate_build is actually corrected in Postgres, not merely that
// the script printed a success line.
func TestPreUpgradeCarryHookRealDhoRev196ShapeStaleBuildRepointRetrySucceeds(t *testing.T) {
	root := repoRoot(t)
	runtimeRoot := realRuntimeRoot(t, root)
	operation, _, digest := aRealRegisteredOperation(t, runtimeRoot)

	pool, dsn := startVerbPostgres(t)
	const staleBuild = "0000000000000000000000000000000000000000"
	const runningBuild = "1111111111111111111111111111111111111111" // deliberately != staleBuild
	// Mirrors seedLiveRow's shape, parameterized on the REAL operation picked above --
	// seedLiveRow itself hardcodes verbTestOperation, and this test cannot use that fixed
	// name paired with a real digest, because the chart script under test passes no
	// -catalog/-documents override: it resolves against the REAL production files, so the
	// operation name AND its digest must both be real and consistent with each other.
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		 VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
		carryDeployedSchemaDigest, digest, operation, staleBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build,
			 owner, mode, rollout_percentage, review_evidence, recorded_by)
		 VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'the original decision', 'operator')`,
		carryDeployedSchemaDigest, digest, operation, staleBuild); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/registry", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{
			"schema_digest": carryDeployedSchemaDigest,
			"operations":    []map[string]string{{"operation": operation, "document_digest": digest}},
		})
	})
	mux.HandleFunc("/buildinfo", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		writeJSON(t, w, map[string]any{"commit": runningBuild, "modified": false, "version": "test", "build_time": "2026-09-10T00:00:00Z"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	realDho := buildRealDho(t, root)
	script := renderPreUpgradeCarryScript(t, filepath.Join(root, "deploy", "helm", "dev-health"))

	binDir := t.TempDir()
	// The chart mints an envelope via `dho mint envelope -key-file ...`, which needs a
	// real Ed25519 key this test has no reason to provision -- /buildinfo above only
	// checks the header is non-empty, so a stub answers the mint call and execs straight
	// through to the real binary for everything else, the same pattern the real-dho chart
	// test in tests/workers/ already uses.
	stubPath := filepath.Join(binDir, "dho")
	if err := os.WriteFile(stubPath, []byte(fmt.Sprintf(
		"#!/usr/bin/env bash\nset -u\n"+
			"if [ \"$1 $2\" = \"mint envelope\" ]; then echo "+verbTestBearer+"; exit 0; fi\n"+
			"exec %q \"$@\"\n", realDho)), 0o755); err != nil {
		t.Fatal(err)
	}

	harness := filepath.Join(t.TempDir(), "harness.sh")
	if err := os.WriteFile(harness, []byte("#!/usr/bin/env bash\nset -u\n"+script+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	env := append(os.Environ(),
		"PATH="+binDir+":/usr/bin:/bin",
		"MINT_ORG=test-org",
		"QUERY_API_URL="+server.URL,
		"RELEASE_NAME=rev196-test",
		"POSTGRES_URI="+dsn,
	)

	cmd := exec.Command("bash", harness)
	cmd.Dir = runtimeRoot // the real dho's default -catalog/-documents paths resolve from here
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	t.Logf("script output:\n%s", out)
	if err != nil {
		t.Fatalf("the pre-upgrade carry hook script failed: %v", err)
	}
	if !strings.Contains(string(out), "OK after repoint-then-retry") {
		t.Fatalf("script did not report success after the repoint-then-retry chain:\n%s", out)
	}

	var candidateBuild string
	if err := pool.QueryRow(ctx,
		`SELECT current_candidate_build FROM go_api_routing_state WHERE schema_digest = $1 AND selected_operation = $2`,
		carryDeployedSchemaDigest, operation,
	).Scan(&candidateBuild); err != nil {
		t.Fatal(err)
	}
	if candidateBuild != runningBuild {
		t.Fatalf("row's candidate_build = %q after the chain, want %q (the real running build repoint corrected it to)", candidateBuild, runningBuild)
	}
}
