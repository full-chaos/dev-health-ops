//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonProviderResolutionProgram runs the REAL get_provider for each case
// with the venue's Python database (the org's stored LLM settings) and
// answers as POST /api/v1/work-units/{id}/explain did before it moved to
// the query-api (main.py, work_unit_explain_endpoint): get_provider runs
// first, and `except (ValueError, LLMError) as exc: raise
// HTTPException(status_code=422, detail=str(exc))`. A provider that
// resolves lets the route go on to its work-unit read, which on this empty
// store is the route's own 404 "Work unit {id} not found".
const pythonProviderResolutionProgram = `
import json, sys
from dev_health_ops.llm.providers import get_provider
from dev_health_ops.llm.errors import LLMError

out = []
for case in json.loads(sys.stdin.read()):
    try:
        get_provider(case["llm_provider"], model=None, org_id=case["org_id"])
    except (ValueError, LLMError) as exc:
        detail = str(exc)
        status = 422
    else:
        detail = "Work unit " + case["work_unit_id"] + " not found"
        status = 404
    out.append({"status": status, "body": json.dumps({"detail": detail}, ensure_ascii=False, separators=(",", ":"))})
print("RESULT " + json.dumps(out))
`

// llmEnvironment is every variable get_provider or its Go port reads to
// pick or configure a platform provider. The oracle clears all of them on
// both sides, so an org's stored settings decide.
var llmEnvironment = []string{
	"LLM_PROVIDER", "LLM_MODEL", "LLM_API_KEY", "LLM_BASE_URL",
	"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "QWEN_API_KEY", "DASHSCOPE_API_KEY",
	"LOCAL_LLM_BASE_URL", "OLLAMA_BASE_URL", "OLLAMA_MODEL", "LMSTUDIO_BASE_URL",
}

// TestVenueOracleWorkUnitExplainProviderResolution compares the work-unit
// explain route's answer to a provider it cannot resolve with the Python
// api's, byte for byte, for orgs whose stored LLM settings name a BYO
// provider with a legacy base_url: one urllib.parse.urlsplit cannot parse
// (a ValueError Python does not catch, answered as 422 with its text), one
// SSRF validation refuses (a fallback to the platform default), and an
// explicit provider request. Both planes read the same seeded settings
// rows (the venue copies the seeded database for Go).
func TestVenueOracleWorkUnitExplainProviderResolution(t *testing.T) {
	ctx := context.Background()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	unparsable, refused, unsupported := uuid.New(), uuid.New(), uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: uuid.NewString() + uuid.NewString(),
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			for _, org := range []struct {
				id       uuid.UUID
				slug     string
				provider string
				apiKey   string
				baseURL  string
			}{
				// A legacy row: the llm-settings routes refuse this value,
				// so only an older save can hold it.
				{unparsable, "venue-llm-unparsable", "local", "", "https://[::1"},
				{refused, "venue-llm-refused", "local", "", "https://[::1]/v1"},
				// A provider Python serves and the Go port answers 501 for,
				// with the same unparsable base_url: Python raises the
				// ValueError before it builds any provider.
				{unsupported, "venue-llm-unsupported", "anthropic", "sk-ant-venue", "https://[::1"},
			} {
				if _, err := admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, org.id, org.slug); err != nil {
					t.Fatalf("seed organization: %v", err)
				}
				settings := map[string]string{"provider": org.provider, "base_url": org.baseURL}
				if org.apiKey != "" {
					settings["api_key"] = org.apiKey
				}
				for key, value := range settings {
					if _, err := admin.Exec(ctx, `INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES ($1, $2, 'llm', $3, $4, false, now(), now())`, uuid.New(), org.id.String(), key, value); err != nil {
						t.Fatalf("seed settings %s: %v", key, err)
					}
				}
			}
			return nil
		},
	})

	for _, name := range llmEnvironment {
		t.Setenv(name, "")
	}
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	handler := newWorkUnitExplainHandler(newTestWorkUnitsReader(t), nil, llmorgsettings.Store{Pool: pool})

	type providerCase struct {
		Name        string `json:"name"`
		OrgID       string `json:"org_id"`
		LLMProvider string `json:"llm_provider"`
		WorkUnitID  string `json:"work_unit_id"`
	}
	cases := []providerCase{
		{Name: "auto, stored base_url urlsplit cannot parse", OrgID: unparsable.String(), LLMProvider: "auto"},
		{Name: "explicit local, stored base_url urlsplit cannot parse", OrgID: unparsable.String(), LLMProvider: "local"},
		{Name: "explicit mock, stored base_url urlsplit cannot parse", OrgID: unparsable.String(), LLMProvider: "mock"},
		{Name: "auto, stored base_url SSRF refuses", OrgID: refused.String(), LLMProvider: "auto"},
		{Name: "explicit local, stored base_url SSRF refuses", OrgID: refused.String(), LLMProvider: "local"},
		{Name: "explicit anthropic, stored base_url urlsplit cannot parse", OrgID: unsupported.String(), LLMProvider: "anthropic"},
	}
	for index := range cases {
		cases[index].WorkUnitID = "wu-provider-" + cases[index].LLMProvider
	}

	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	async := strings.Replace(venue.AdminURI(t, venue.SourceDB), "postgres://", "postgresql+asyncpg://", 1)
	async = strings.Replace(async, "postgresql://", "postgresql+asyncpg://", 1)
	command := exec.Command("python3", "-c", pythonProviderResolutionProgram)
	command.Dir = root
	command.Env = append(withoutLLMEnvironment(os.Environ()), "PYTHONPATH="+filepath.Join(root, "src"), "POSTGRES_URI="+async)
	command.Stdin = bytes.NewReader(payload)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("python: %v\n%s", err, stderr.String())
	}
	var python []struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, "RESULT "); ok {
			if err := json.Unmarshal([]byte(rest), &python); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(python) != len(cases) {
		t.Fatalf("python answered %d of %d cases\n%s", len(python), len(cases), stderr.String())
	}

	same := 0
	for index, tc := range cases {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/work-units/"+tc.WorkUnitID+"/explain?llm_provider="+tc.LLMProvider, nil)
		request.SetPathValue("work_unit_id", tc.WorkUnitID)
		request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: tc.OrgID, Role: "owner"}))
		recorder := httptest.NewRecorder()
		handler(recorder, request)
		goBody := strings.TrimSpace(recorder.Body.String())
		if recorder.Code != python[index].Status || goBody != python[index].Body {
			t.Errorf("%s: DIFF\n python %d %s\n go     %d %s", tc.Name, python[index].Status, python[index].Body, recorder.Code, goBody)
			continue
		}
		same++
	}
	t.Logf("%d of %d cases byte-identical to the Python api's answer", same, len(cases))
	venueoracle.WriteProof(t)
}

func withoutLLMEnvironment(environ []string) []string {
	out := make([]string, 0, len(environ))
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		keep := true
		for _, cleared := range llmEnvironment {
			if name == cleared {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, entry)
		}
	}
	return out
}
