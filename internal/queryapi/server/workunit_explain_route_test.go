package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// newTestWorkUnitExplainHandler builds the work handler over the shared
// zero-rows reader, a nil token-usage writer (no accounting row is
// attempted, matching the reference's own `if db_url` guard) and a nil
// org-settings resolver, which every "ForOrg" entry point treats exactly
// as "this org configured no BYO provider".
func newTestWorkUnitExplainHandler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return newWorkUnitExplainHandler(newTestWorkUnitsReader(t), nil, nil)
}

func newWorkUnitExplainRequest(t *testing.T, workUnitID, query string) *http.Request {
	t.Helper()
	target := "/api/v1/work-units/" + workUnitID + "/explain"
	if query != "" {
		target += "?" + query
	}
	req := httptest.NewRequest(http.MethodPost, target, nil)
	req.SetPathValue("work_unit_id", workUnitID)
	return req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-ABC-123"}))
}

// readWorkUnitExplainFixture takes the fixture's path RELATIVE TO THIS
// DIRECTORY, spelled out at the call site as one string literal, rather
// than a bare basename joined onto a prefix here.
//
// That is what keeps the fixtures visible to the Go-workflow path-filter
// oracle (tests/tooling/test_go_workflow_path_filters.py). The oracle
// resolves a data-suffixed literal against the naming file's own directory,
// so a full relative path resolves exactly. A bare basename resolves only
// through its unique-basename fallback, which needs the name to be unique
// in the whole repository, and one of these fixtures shares its basename
// with a file under tests/acceptance. An ambiguous basename is REPORTED
// rather than guessed at, because a fixture the oracle cannot place is a
// fixture whose PR can be classified non-Go, leaving the Go test that reads
// it unrun. Spelling the path is a property that holds for ANY fixture
// name; basename uniqueness is luck that runs out.
func readWorkUnitExplainFixture(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return data
}

// TestWorkUnitExplainRequiresAuthContext pins that the work handler --
// reached only after the entry handler has already authenticated -- still
// refuses a request carrying no claims, rather than proceeding with an
// empty org id.
func TestWorkUnitExplainRequiresAuthContext(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/work-units/wu-ABC-123/explain", nil)
	req.SetPathValue("work_unit_id", "wu-ABC-123")
	rec := httptest.NewRecorder()
	newTestWorkUnitExplainHandler(t).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Errorf("WWW-Authenticate = %q, want %q", got, "Bearer")
	}
}

// TestWorkUnitExplainValidationErrorsMatchPython replays each captured
// 422 against the handler. The fixtures are verbatim bodies from the real
// Python app (FastAPI TestClient with the auth dependency overridden); the
// capture command is quoted in this change's TEST-EVIDENCE.
//
// Each of the three typed query parameters is covered. scope_type,
// scope_id, llm_provider and llm_model are plain `str` parameters, which
// Pydantic never rejects, so none of them can appear here -- an unknown
// scope_type's own (503) behaviour is pinned separately below.
func TestWorkUnitExplainValidationErrorsMatchPython(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		fixture string
	}{
		{"non-numeric range_days", "range_days=not-a-number", "testdata/work_unit_explain/non_numeric_range_days.json"},
		{"malformed start_date", "start_date=not-a-date", "testdata/work_unit_explain/malformed_start_date.json"},
		{"malformed end_date", "end_date=2024-13-40", "testdata/work_unit_explain/malformed_end_date.json"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			newTestWorkUnitExplainHandler(t).ServeHTTP(rec, newWorkUnitExplainRequest(t, "wu-ABC-123", testCase.query))

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			got := decodeValidationErrorBody(t, rec.Body.Bytes())
			want := decodeValidationErrorBody(t, readWorkUnitExplainFixture(t, testCase.fixture))
			if !reflect.DeepEqual(got, want) {
				t.Errorf("body mismatch\n got=%+v\nwant=%+v", got, want)
			}
		})
	}
}

// TestWorkUnitExplainNotFoundBodyMatchesPython pins the 404 against the
// captured reference bodies, and it does so in TWO ways because this route
// reproduces the reference's encoding on one axis and not the other.
//
// BYTES, for the cases the encoding path reproduces exactly. The reference
// interpolates the caller's own work_unit_id into the message, and the
// escaping case carries `"`, `<`, `>`, `&` and a non-ASCII character --
// Go's default encoder escapes three of those where the reference escapes
// none, which is what this route's disabled HTML escaping closes and only a
// byte comparison catches.
//
// DECODED VALUE, for the line-separator case. U+2028/U+2029 are governed by
// an option this encoder type exposes no setter for, so they leave escaped
// where the reference writes their raw bytes. The fixture stays as the
// record of what the reference emits; the assertions below state exactly
// what this route sends instead -- the escaped spelling on the wire, the
// SAME string once decoded -- so the accepted difference is measured rather
// than described.
func TestWorkUnitExplainNotFoundBodyMatchesPython(t *testing.T) {
	// The capture ran with LLM_PROVIDER=mock and no llm_provider parameter,
	// so "auto" resolves to mock and get_provider succeeds before the
	// not-found check is ever reached. Reproduced here rather than papered
	// over with an explicit ?llm_provider=mock, so this test exercises the
	// same resolution path the capture did.
	t.Setenv("LLM_PROVIDER", "mock")

	cases := []struct {
		name       string
		workUnitID string
		fixture    string
		// byteExact is false for the one case whose encoding this path
		// cannot reproduce; its decoded value is compared instead.
		byteExact bool
	}{
		{"plain id", "wu-ABC-123", "testdata/work_unit_explain/not_found.json", true},
		{"id needing json escaping", "wu-\"q\"-<tag>&\u00e9", "testdata/work_unit_explain/not_found_id_needing_json_escaping.json", true},
		{"id with line separators", "wu-\u2028-\u2029-end", "testdata/work_unit_explain/not_found_id_with_line_separators.json", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			newTestWorkUnitExplainHandler(t).ServeHTTP(rec, newWorkUnitExplainRequest(t, testCase.workUnitID, ""))

			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusNotFound, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}

			want := readWorkUnitExplainFixture(t, testCase.fixture)
			// The reference body carries no trailing newline; the encoder
			// appends exactly one, which is insignificant JSON whitespace.
			if !bytes.HasSuffix(rec.Body.Bytes(), []byte("\n")) {
				t.Errorf("body carries no trailing newline: %q", rec.Body.Bytes())
			}
			got := bytes.TrimSuffix(rec.Body.Bytes(), []byte("\n"))
			if bytes.HasSuffix(got, []byte("\n")) {
				t.Errorf("body carries more than one trailing newline: %q", rec.Body.Bytes())
			}

			if testCase.byteExact {
				if !bytes.Equal(got, bytes.TrimRight(want, "\n")) {
					t.Errorf("body mismatch\n got=%s\nwant=%s", got, want)
				}
				return
			}

			// The escaped spelling is on the wire, the raw bytes are not.
			for _, separator := range []string{"\u2028", "\u2029"} {
				if bytes.Contains(got, []byte(separator)) {
					t.Errorf("body carries the raw bytes of %q; this encoding path cannot emit them: %q", separator, got)
				}
			}
			if !bytes.Contains(got, []byte(`\u2028`)) || !bytes.Contains(got, []byte(`\u2029`)) {
				t.Errorf("body does not carry the escaped separators: %q", got)
			}
			// And the DECODED value equals the reference's, which is the
			// contract a client of this route actually depends on.
			var candidate, reference struct {
				Detail string `json:"detail"`
			}
			if err := json.Unmarshal(got, &candidate); err != nil {
				t.Fatalf("decode candidate: %v\nbody=%s", err, got)
			}
			if err := json.Unmarshal(want, &reference); err != nil {
				t.Fatalf("decode fixture: %v\nbody=%s", err, want)
			}
			if candidate.Detail != reference.Detail {
				t.Errorf("decoded detail = %q, want %q", candidate.Detail, reference.Detail)
			}
		})
	}
}

// TestWorkUnitExplainUnknownScopeTypeAnswers503 pins that an unknown
// scope_type is NOT a validation error. The reference constructs
// ScopeFilter(level=scope_type) inside the endpoint's own try block, so
// the Pydantic literal_error it raises is swallowed by the generic
// handler and answered as 503 "Explanation unavailable" -- confirmed
// against the live app, whose body this fixture is.
func TestWorkUnitExplainUnknownScopeTypeAnswers503(t *testing.T) {
	t.Setenv("LLM_PROVIDER", "mock")

	rec := httptest.NewRecorder()
	newTestWorkUnitExplainHandler(t).ServeHTTP(rec,
		newWorkUnitExplainRequest(t, "wu-ABC-123", "scope_type=not-a-real-scope"))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	want := readWorkUnitExplainFixture(t, "testdata/work_unit_explain/unknown_scope_type_is_not_a_validation_error.json")
	if got := bytes.TrimRight(rec.Body.Bytes(), "\n"); !bytes.Equal(got, bytes.TrimRight(want, "\n")) {
		t.Errorf("body mismatch\n got=%s\nwant=%s", got, want)
	}
}

// llmProviderEnvNames are the environment variable NAMES the provider
// resolver consults. Only their names appear here and no value of any of
// them is ever read: the tests below clear them so the resolver reaches
// its own not-configured branch.
var llmProviderEnvNames = []string{
	"LLM_PROVIDER", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY",
	"LOCAL_LLM_BASE_URL", "DASHSCOPE_API_KEY", "QWEN_API_KEY",
	"OLLAMA_MODEL", "OLLAMA_BASE_URL", "LMSTUDIO_MODEL", "LMSTUDIO_BASE_URL",
	"LLM_MODEL", "LLM_MODEL_OPENAI", "LLM_API_KEY", "LLM_BASE_URL",
}

func clearLLMProviderEnv(t *testing.T) {
	t.Helper()
	for _, name := range llmProviderEnvNames {
		// t.Setenv first so the harness restores the original on cleanup;
		// Unsetenv then makes it ABSENT rather than empty, which is what the
		// resolver's own presence checks read.
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
}

// TestWorkUnitExplainUnconfiguredProviderBodiesMatchPython pins the two
// not-configured branches this route ACTUALLY reaches, byte for byte
// against bodies captured from the live reference app.
//
// Both answer 422 before any ClickHouse read, so a request naming a work
// unit that does not exist still answers 422 rather than 404 -- which is
// also what makes these cases capturable without a database. The detail is
// str(exc) of the reference's own LLMAuthError, so it carries that
// exception's provider/model suffix as well as its message, and the
// message names environment VARIABLES, never a value of one.
//
// Only "auto" and a provider kind this plane implements get here. A kind
// the reference knows but this plane has no client for answers 501 first
// -- see TestWorkUnitExplainGoUnsupportedProviderAnswers501 below, and
// TestMissingLLMProviderDetailMatchesPythonForEveryEnvHint for the proof
// that the ported message table is right for those kinds anyway.
func TestWorkUnitExplainUnconfiguredProviderBodiesMatchPython(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		fixture string
	}{
		// resolve_provider_name's own failure: nothing configured, so
		// "auto" resolves to nothing at all.
		{"auto resolves to nothing", "", "testdata/work_unit_explain/provider_unconfigured_auto.json"},
		// get_provider's _provider_has_required_config branch, for the one
		// named kind this plane both implements and can check.
		{"openai", "llm_provider=openai", "testdata/work_unit_explain/provider_unconfigured_openai.json"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			clearLLMProviderEnv(t)

			rec := httptest.NewRecorder()
			newTestWorkUnitExplainHandler(t).ServeHTTP(rec,
				newWorkUnitExplainRequest(t, "wu-ABC-123", testCase.query))

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			want := readWorkUnitExplainFixture(t, testCase.fixture)
			if got := bytes.TrimRight(rec.Body.Bytes(), "\n"); !bytes.Equal(got, bytes.TrimRight(want, "\n")) {
				t.Errorf("body mismatch\n got=%s\nwant=%s", got, want)
			}
		})
	}
}

// TestWorkUnitExplainGoUnsupportedProviderAnswers501 pins the one place
// this route deliberately answers a status the reference never does.
//
// anthropic, gemini and qwen are providers the reference genuinely serves
// and this plane has no client for at all -- it cannot even check their
// credentials, so it cannot tell a configured one from an unconfigured
// one. Answering the unconfigured 422 would therefore be a guess that is
// WRONG whenever the credential is present, which is the silent-wrong-
// answer class the sibling explain route's own guard exists to prevent. So
// the guard runs first and answers 501 for all three.
//
// The cost, pinned here so it is not rediscovered: where the credential is
// genuinely absent, the reference answers the 422 body
// provider_unconfigured_<kind>.json carries and this route answers 501.
// Same outcome for a caller -- no explanation -- by a different status.
func TestWorkUnitExplainGoUnsupportedProviderAnswers501(t *testing.T) {
	for _, provider := range []string{"anthropic", "gemini", "qwen"} {
		t.Run(provider, func(t *testing.T) {
			clearLLMProviderEnv(t)

			rec := httptest.NewRecorder()
			newTestWorkUnitExplainHandler(t).ServeHTTP(rec,
				newWorkUnitExplainRequest(t, "wu-ABC-123", "llm_provider="+provider))

			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusNotImplemented, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
		})
	}
}

// TestMissingLLMProviderDetailMatchesPythonForEveryEnvHint proves the
// ported message table against the reference for EVERY entry it has,
// including the three kinds the route's own 501 guard reaches first. The
// table is a port of a reference function, so it is verified as one; that
// the route does not reach three of its arms is a property of the route,
// not a reason to leave those arms unverified.
func TestMissingLLMProviderDetailMatchesPythonForEveryEnvHint(t *testing.T) {
	// Each fixture path is its own literal rather than a name built from
	// the provider, so every one of these five files is a path the
	// Go-workflow oracle can resolve -- see readWorkUnitExplainFixture.
	cases := []struct {
		provider string
		fixture  string
	}{
		{"auto", "testdata/work_unit_explain/provider_unconfigured_auto.json"},
		{"openai", "testdata/work_unit_explain/provider_unconfigured_openai.json"},
		{"anthropic", "testdata/work_unit_explain/provider_unconfigured_anthropic.json"},
		{"gemini", "testdata/work_unit_explain/provider_unconfigured_gemini.json"},
		{"qwen", "testdata/work_unit_explain/provider_unconfigured_qwen.json"},
	}
	for _, testCase := range cases {
		provider := testCase.provider
		t.Run(provider, func(t *testing.T) {
			var reference struct {
				Detail string `json:"detail"`
			}
			raw := readWorkUnitExplainFixture(t, testCase.fixture)
			if err := json.Unmarshal(raw, &reference); err != nil {
				t.Fatalf("decode fixture: %v\nbody=%s", err, raw)
			}
			if got := missingLLMProviderDetail(provider); got != reference.Detail {
				t.Errorf("= %q, want %q", got, reference.Detail)
			}
		})
	}
}

// TestMissingLLMProviderDetailFallsBackForAnUnnamedProvider pins the
// env-hint table's own default arm, which no captured fixture reaches:
// the reference's `else` gives "LLM_PROVIDER" for any provider name its
// four-entry table does not carry.
func TestMissingLLMProviderDetailFallsBackForAnUnnamedProvider(t *testing.T) {
	got := missingLLMProviderDetail("lmstudio")
	want := "LLM provider 'lmstudio' is not configured. Set LLM_PROVIDER or choose " +
		"--llm-provider mock for fixtures/testing. | provider=lmstudio | model=none"
	if got != want {
		t.Errorf("= %q, want %q", got, want)
	}
}

// TestWorkUnitExplainProviderNoneIsNotACredentialFailure pins the half of
// the "none" provider path that lives in this handler: get_provider
// constructs NoneProvider WITHOUT consulting any credential
// (providers/__init__.py:254-259 returns before
// _provider_has_required_config), so "none" must pass the provider gate
// rather than be rejected as unusable. The empty 200 it eventually
// produces needs a reader that returns a row, which the golden test in
// internal/workunitexplain covers.
func TestWorkUnitExplainProviderNoneIsNotACredentialFailure(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestWorkUnitExplainHandler(t).ServeHTTP(rec,
		newWorkUnitExplainRequest(t, "wu-ABC-123", "llm_provider=none"))

	// The zero-rows reader means this stops at 404, which is the point: it
	// got PAST the provider gate rather than answering 422.
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d (past the provider gate, stopped at not-found)\nbody=%s",
			rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

// TestParseWorkUnitExplainQueryDefaults pins the endpoint's own parameter
// defaults (main.py:695-703).
func TestParseWorkUnitExplainQueryDefaults(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/work-units/wu-ABC-123/explain", nil)
	parsed, validationErrors := parseWorkUnitExplainQuery(req)
	if len(validationErrors) != 0 {
		t.Fatalf("validationErrors = %+v, want none", validationErrors)
	}
	if parsed.scopeType != "org" {
		t.Errorf("scopeType = %q, want %q", parsed.scopeType, "org")
	}
	if parsed.scopeID != "" {
		t.Errorf("scopeID = %q, want empty", parsed.scopeID)
	}
	if parsed.rangeDays != 14 {
		t.Errorf("rangeDays = %d, want 14", parsed.rangeDays)
	}
	if parsed.startDate != nil || parsed.endDate != nil {
		t.Errorf("startDate/endDate = %v/%v, want nil/nil", parsed.startDate, parsed.endDate)
	}
	if parsed.llmProvider != "auto" {
		t.Errorf("llmProvider = %q, want %q", parsed.llmProvider, "auto")
	}
	if parsed.llmModel != "" {
		t.Errorf("llmModel = %q, want empty", parsed.llmModel)
	}
}

// TestWorkUnitExplainLLMErrorStatus pins _http_exception_from_llm_error's
// own four branches (main.py:414-421).
func TestWorkUnitExplainLLMErrorStatus(t *testing.T) {
	cases := []struct {
		class categorize.LLMErrorClass
		want  int
	}{
		{categorize.LLMErrorClassAuth, http.StatusUnprocessableEntity},
		{categorize.LLMErrorClassRateLimit, http.StatusTooManyRequests},
		{categorize.LLMErrorClassServer, http.StatusServiceUnavailable},
		{categorize.LLMErrorClassOther, http.StatusUnprocessableEntity},
	}
	for _, testCase := range cases {
		if got := workUnitExplainLLMErrorStatus(testCase.class); got != testCase.want {
			t.Errorf("class %v -> %d, want %d", testCase.class, got, testCase.want)
		}
	}
}

// TestWorkUnitExplainSwitchDefaultsOff pins the route's reachability: with
// the toggle unset, its routeswitch operation is disabled.
func TestWorkUnitExplainSwitchDefaultsOff(t *testing.T) {
	if err := os.Unsetenv(workUnitExplainEnabledEnvVar); err != nil {
		t.Fatalf("unset %s: %v", workUnitExplainEnabledEnvVar, err)
	}
	if workUnitExplainSwitchFromEnv(os.Getenv).Enabled(workUnitExplainOperation) {
		t.Fatalf("%s is enabled with %s unset, want disabled", workUnitExplainOperation, workUnitExplainEnabledEnvVar)
	}

	t.Setenv(workUnitExplainEnabledEnvVar, "true")
	if !workUnitExplainSwitchFromEnv(os.Getenv).Enabled(workUnitExplainOperation) {
		t.Fatalf("%s is disabled with %s=true, want enabled", workUnitExplainOperation, workUnitExplainEnabledEnvVar)
	}
}

// newCapturingWorkUnitExplainHandler builds the work handler over a
// client that records every statement it is asked to run, so a test can
// assert what reached the SQL rather than what the handler intended.
func newCapturingWorkUnitExplainHandler(t *testing.T) (http.HandlerFunc, *capturingWorkUnitsClient) {
	t.Helper()
	client := &capturingWorkUnitsClient{}
	reader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatalf("investmentexplain.NewReader: %v", err)
	}
	return newWorkUnitExplainHandler(reader, nil, nil), client
}

// TestWorkUnitExplainTeamScopePushesConditionIntoTheWorkUnitQuery is the
// guard for a defect this route shipped without: it resolved repo-level
// scope ids and then never applied the TEAM-level scope at all, so a
// team-scoped request for a unit outside that team answered 200 with an
// explanation where the reference answers 404. Resolving repo filter ids
// is not enough on its own -- that resolver ignores a team-level id BY
// DESIGN (see its own doc comment), and the team's membership test is a
// separate condition pushed into the work-unit read, exactly as the
// work-units GET and the investment-explain POST both do it.
//
// Asserting on the EMITTED STATEMENT is the point. Every earlier test
// here drove the same handler through a zero-rows client and passed while
// the predicate was missing, because a missing narrowing predicate
// changes no status and no body when the result set is empty either way.
func TestWorkUnitExplainTeamScopePushesConditionIntoTheWorkUnitQuery(t *testing.T) {
	t.Setenv("LLM_PROVIDER", "mock")

	handler, client := newCapturingWorkUnitExplainHandler(t)
	rec := httptest.NewRecorder()
	handler(rec, newWorkUnitExplainRequest(t, "wu-ABC-123", "llm_provider=mock&scope_type=team&scope_id=team-42"))

	// The zero-rows client means the handler reaches its not-found answer;
	// what matters is the statement it emitted on the way there.
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	found := false
	for index, statement := range client.queries {
		if !strings.Contains(statement, teamscope.Marker) {
			continue
		}
		// The membership test belongs INSIDE the work-unit investments
		// read, never as its own standalone statement: a team's matching
		// repo count must not cross back to the caller as a query result of
		// its own, where it would be subject to the read client's
		// per-statement row ceiling.
		if !strings.Contains(statement, "work_unit_investments") {
			t.Fatalf("a query carries the team-scope membership subquery as its OWN standalone statement, not nested inside the work-unit investments read:\n%s", statement)
		}
		found = true
		raw, present := bindingValue(client.bindings[index], teamscope.BindingTeamIDs)
		if !present {
			t.Fatalf("the work-unit investments query carries no %s binding", teamscope.BindingTeamIDs)
		}
		ids, ok := raw.([]string)
		if !ok || len(ids) != 1 || ids[0] != "team-42" {
			t.Errorf("%s binding = %#v, want [\"team-42\"]", teamscope.BindingTeamIDs, raw)
		}
	}
	if !found {
		t.Fatal("no emitted query carries the pushed-down team-scope condition -- scope_type=team never reached teamscope.RepoCondition, so the team scope is silently dropped")
	}
}

// TestWorkUnitExplainOrgScopeEmitsNoTeamCondition is the other half: the
// condition must appear ONLY for a team scope. An unconditional team
// predicate would narrow every org-scoped request to whatever an empty
// team-id list resolves to.
func TestWorkUnitExplainOrgScopeEmitsNoTeamCondition(t *testing.T) {
	t.Setenv("LLM_PROVIDER", "mock")

	handler, client := newCapturingWorkUnitExplainHandler(t)
	rec := httptest.NewRecorder()
	handler(rec, newWorkUnitExplainRequest(t, "wu-ABC-123", "llm_provider=mock"))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	for index, statement := range client.queries {
		if strings.Contains(statement, teamscope.Marker) {
			t.Errorf("an org-scoped request emitted the team-scope membership subquery:\n%s", statement)
		}
		if _, present := bindingValue(client.bindings[index], teamscope.BindingTeamIDs); present {
			t.Errorf("an org-scoped request carries a %s binding", teamscope.BindingTeamIDs)
		}
	}
}

// ollamaFixtureRowScanner is a RowScanner over a single fixed row, its
// values matching one query's Scan destination count/order exactly --
// package main's own copy of the same fixture-row pattern
// internal/queryapi/investmentexplain/explain_golden_test.go uses,
// needed here because that package's version is unexported.
type ollamaFixtureRowScanner struct {
	rows  [][]any
	index int
}

func (s *ollamaFixtureRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *ollamaFixtureRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		switch typed := d.(type) {
		case *string:
			*typed, _ = row[i].(string)
		case **string:
			if v, ok := row[i].(*string); ok {
				*typed = v
			} else {
				*typed = nil
			}
		case *float64:
			*typed, _ = row[i].(float64)
		case **float64:
			if v, ok := row[i].(*float64); ok {
				*typed = v
			} else {
				*typed = nil
			}
		case *time.Time:
			*typed, _ = row[i].(time.Time)
		case *[]string:
			*typed, _ = row[i].([]string)
		case *[]float64:
			*typed, _ = row[i].([]float64)
		}
	}
	return nil
}

func (s *ollamaFixtureRowScanner) Err() error   { return nil }
func (s *ollamaFixtureRowScanner) Close() error { return nil }

// ollamaSingleWorkUnitClient answers the work-unit-investments query
// (identified the same way explain_golden_test.go's explainFixtureClient
// does, by its distinctive ORDER BY clause) with exactly one real row for
// work_unit_id "wu-ABC-123", and every other query with zero rows -- the
// auxiliary lookups (repo scopes, repo identities, team assignments,
// evidence quotes) all degrade to their own documented empty-result
// defaults ("unassigned", no textual quotes) when they return nothing,
// which is enough for ExplainWorkUnit's prompt to build without error.
type ollamaSingleWorkUnitClient struct{}

func (ollamaSingleWorkUnitClient) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "ORDER BY effort_value DESC, work_unit_id ASC") {
		fromTS := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		toTS := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
		return &ollamaFixtureRowScanner{rows: [][]any{
			{
				"wu-ABC-123", strPtrForTest("issue"), strPtrForTest("Ship the new thing"),
				fromTS, toTS,
				strPtrForTest("repo-1"), strPtrForTest("github"),
				strPtrForTest("churn_loc"), floatPtrForTest(40.0),
				[]string{"velocity"}, []float64{40.0}, []string{"velocity.feature"}, []float64{40.0},
				strPtrForTest(`{"issues": [], "prs": []}`),
				floatPtrForTest(0.8), strPtrForTest("high"),
				strPtrForTest("complete"), strPtrForTest("v1"), strPtrForTest("run-1"),
				toTS,
			},
		}}, nil
	}
	return &ollamaFixtureRowScanner{}, nil
}

func strPtrForTest(s string) *string     { return &s }
func floatPtrForTest(f float64) *float64 { return &f }

// TestWorkUnitExplainOllamaExplicitProviderSucceeds is the work-unit-explain
// sibling of TestInvestmentExplainWorkHandlerOllamaExplicitProviderSucceeds:
// an explicit llm_provider=ollama request, with OLLAMA_BASE_URL pointed at a
// real server standing in for Ollama's native /api/chat endpoint and a real
// (non-zero-row) work unit to explain, must reach a genuine completion
// through the SAME production constructor
// (investmentexplain.CompleteWorkUnitExplanationForOrg ->
// categorize.NewOllamaProvider) rather than stop at the 422
// missingLLMProviderDetail body providerHasRequiredConfig's missing ollama
// case used to force regardless of configuration.
func TestWorkUnitExplainOllamaExplicitProviderSucceeds(t *testing.T) {
	const completionText = "SUMMARY: This work unit shipped a small fix.\n\nREASONS: Primarily velocity work.\n"

	requestReached := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			t.Errorf("ollama request path = %q, want /api/chat", r.URL.Path)
		}
		requestReached = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":{"role":"assistant","content":` + strconv.Quote(completionText) + `},"done":true}`))
	}))
	defer server.Close()
	t.Setenv("OLLAMA_BASE_URL", server.URL)

	reader, err := investmentexplain.NewReader(ollamaSingleWorkUnitClient{})
	if err != nil {
		t.Fatalf("investmentexplain.NewReader: %v", err)
	}
	handler := newWorkUnitExplainHandler(reader, nil, nil)

	rec := httptest.NewRecorder()
	handler(rec, newWorkUnitExplainRequest(t, "wu-ABC-123", "llm_provider=ollama"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !requestReached {
		t.Fatal("the fake ollama server never received a request -- the availability gate refused the request before any completion was attempted")
	}
	var decoded struct {
		WorkUnitID  string `json:"work_unit_id"`
		AIGenerated bool   `json:"ai_generated"`
		Summary     string `json:"summary"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response: %v\nbody=%s", err, rec.Body.String())
	}
	if !decoded.AIGenerated {
		t.Fatalf("ai_generated = false, want true -- got the non-AI/refusal shape instead of a real completion\nbody=%s", rec.Body.String())
	}
	if decoded.WorkUnitID != "wu-ABC-123" {
		t.Errorf("work_unit_id = %q, want %q", decoded.WorkUnitID, "wu-ABC-123")
	}
	if decoded.Summary == "" {
		t.Fatalf("summary is empty -- want the completion's real summary text\nbody=%s", rec.Body.String())
	}
}

// TestWorkUnitExplainOpenAIUnconfiguredStaysRefused is the negative half on
// this route: TestWorkUnitExplainUnconfiguredProviderBodiesMatchPython's
// "openai" case above already pins this exact 422 body byte for byte, so
// this test only asserts the property that fix must not disturb --
// providerHasRequiredConfig's openai branch is unaffected by widening the
// switch to also cover ollama.
func TestWorkUnitExplainOpenAIUnconfiguredStaysRefused(t *testing.T) {
	clearLLMProviderEnv(t)

	rec := httptest.NewRecorder()
	newTestWorkUnitExplainHandler(t).ServeHTTP(rec,
		newWorkUnitExplainRequest(t, "wu-ABC-123", "llm_provider=openai"))

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	want := readWorkUnitExplainFixture(t, "testdata/work_unit_explain/provider_unconfigured_openai.json")
	if got := bytes.TrimRight(rec.Body.Bytes(), "\n"); !bytes.Equal(got, bytes.TrimRight(want, "\n")) {
		t.Errorf("body mismatch\n got=%s\nwant=%s", got, want)
	}
}
