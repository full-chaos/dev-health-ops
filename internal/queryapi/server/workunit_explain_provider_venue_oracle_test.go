//go:build integration

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// llmEnvironment is every variable get_provider or its Go port reads to
// pick or configure a platform provider. The oracle clears all of them on
// both sides, so an org's stored settings decide.
var llmEnvironment = []string{
	"LLM_PROVIDER", "LLM_MODEL", "LLM_API_KEY", "LLM_BASE_URL",
	"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "QWEN_API_KEY", "DASHSCOPE_API_KEY",
	"LOCAL_LLM_BASE_URL", "OLLAMA_BASE_URL", "OLLAMA_MODEL", "LMSTUDIO_BASE_URL",
}

// NOT pinned by this oracle (its 6 cases decide only what they decide; the other clauses are pinned, where they are, by the Go-only tests of
// internal/llmorgsettings): a stored provider "auto", "mock" or "none" (resolve.go:120, each of its four clauses); a stored org with neither key
// nor base_url (:127); a stored provider that differs from the requested one (:163, pinned by TestCredentials_SourceBound); credentialsComplete's
// per-provider branches (:130: the one stored anthropic org has its key; :130 is pinned by TestResolveUsableProvider_PrecedenceMatrix and the
// function itself by TestCredentialsComplete; :170, the check inside Store.Credentials, is pinned by no test the vet ran); a stored provider that
// is not a known provider (:123); resolve.go:167, :177 (the SSRF refusal in Store.Credentials: no test of llmorgsettings, categorize or
// investmentexplain pins it; finding with the lead), :205, :230; provider_org.go:42, :78, :87, :113, :128; providerkind.go:148,
// :168, :189, :193; and workunit_explain_route.go:299. Adding a case (a stored "auto"; a stored anthropic org with a base_url and no key; an
// explicit openai request against a stored anthropic org; a provider with neither key nor base_url) needs a re-record.
//
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
	_, file, _, _ := moduleroot.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	// The ids are named, not random: the recording and every frozen run seed the same ones.
	unparsable, refused, unsupported := stableVenueID("provider/unparsable"), stableVenueID("provider/refused"), stableVenueID("provider/unsupported")
	storedAuto, storedMock, storedUnknown := stableVenueID("provider/stored-auto"), stableVenueID("provider/stored-mock"), stableVenueID("provider/stored-unknown")
	keyless, bare, usableLocal := stableVenueID("provider/keyless-openai"), stableVenueID("provider/bare-openai"), stableVenueID("provider/usable-local")
	golden := venueoracle.OpenGolden(t, venueGolden("workunit-explain-provider", t.Name(), providerGoldenPin))
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden: golden, Root: root,
		// No route here checks a token; the key is the one the other venue oracles of this package use.
		JWTKey: oracleJWTKey,
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
				// Stored providers the resolution treats as not usable or as another provider (resolve.go), with a public IP-literal base_url
				// (no name lookup) so only the stored provider and the key decide.
				{storedAuto, "venue-llm-stored-auto", "auto", "dummy-venue-key-a", "https://8.8.8.8/v1"},
				{storedMock, "venue-llm-stored-mock", "mock", "dummy-venue-key-b", "https://8.8.8.8/v1"},
				{storedUnknown, "venue-llm-stored-unknown", "frobnicate", "dummy-venue-key-c", "https://8.8.8.8/v1"},
				// A key-required provider with a base_url and no key; one with neither; a complete local provider.
				{keyless, "venue-llm-keyless-openai", "openai", "", "https://8.8.8.8/v1"},
				{bare, "venue-llm-bare-openai", "openai", "", ""},
				{usableLocal, "venue-llm-usable-local", "local", "", "https://8.8.8.8/v1"},
			} {
				if _, err := admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, org.id, org.slug); err != nil {
					t.Fatalf("seed organization: %v", err)
				}
				settings := map[string]string{"provider": org.provider}
				if org.baseURL != "" {
					settings["base_url"] = org.baseURL
				}
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
	handler := newWorkUnitExplainHandler(newTestWorkUnitsReader(t), nil, llmorgsettings.Store{Pool: pool}, nil, testPlatformGate{})

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
		{Name: "auto, stored provider auto with key and base_url", OrgID: storedAuto.String(), LLMProvider: "auto"},
		{Name: "auto, stored provider mock with key and base_url", OrgID: storedMock.String(), LLMProvider: "auto"},
		{Name: "auto, stored provider unknown", OrgID: storedUnknown.String(), LLMProvider: "auto"},
		{Name: "auto, stored openai with base_url and no key", OrgID: keyless.String(), LLMProvider: "auto"},
		{Name: "explicit openai, stored openai with base_url and no key", OrgID: keyless.String(), LLMProvider: "openai"},
		{Name: "auto, stored openai with neither key nor base_url", OrgID: bare.String(), LLMProvider: "auto"},
		{Name: "auto, stored local complete", OrgID: usableLocal.String(), LLMProvider: "auto"},
		{Name: "explicit local, stored local complete", OrgID: usableLocal.String(), LLMProvider: "local"},
		{Name: "explicit openai, stored local complete", OrgID: usableLocal.String(), LLMProvider: "openai"},
		{Name: "explicit mock, stored local complete", OrgID: usableLocal.String(), LLMProvider: "mock"},
	}
	for index := range cases {
		cases[index].WorkUnitID = "wu-provider-" + cases[index].LLMProvider
	}

	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	// The launcher form: the harness's own producer starts the program in the closed environment (no LLM variable reaches it); the address of the
	// run's own Python database is the one per-run entry, passed as an extra (it is not part of the request's key).
	request := venueoracle.ProgramRequest("provider resolution", payload)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	var python []struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	for _, line := range strings.Split(answers[0].Body, "\n") {
		if rest, ok := strings.CutPrefix(line, "RESULT "); ok {
			if err := json.Unmarshal([]byte(rest), &python); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(python) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(python), len(cases))
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
	golden.SkipDiff(t)
	venueoracle.WriteGoOnlyProof(t, "the Go work-unit explain route's provider resolution against the frozen answers of the Python api")
	golden.Finish(t)
}

// The golden's pin is the digest the record verb writes; PIN: names the golden until it does.
const providerGoldenPin = "5aa77c57c36482f7d61d28f012d3e9f486139f3b0ddefa743b355880aa63c21c"

// providerDeclared holds the environment entries that shape the Python program's answers. The address of the run's own database is not one of
// them: it is made for the run and passed as a per-run entry (POSTGRES_URI).
var providerDeclared = map[string]string{"OTEL_SDK_DISABLED": "true", "ENVIRONMENT": "test"}
