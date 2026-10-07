package decisioneval

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func bugfixBehaviour() behaviour {
	return behaviour{levels: map[string]int{"quality.bugfix": 3, "quality.testing": 1}, evidence: map[string]string{"quality": "E2_1"}}
}

// ---- end to end, each adapter ----

func TestJevEndToEndLedgerAndRawFiles(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	cfg := newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev)
	s := mustRun(t, cfg)
	if s.Arms[ArmJev].Sent != 1 || len(s.StoppedArms()) != 0 {
		t.Fatalf("%+v", s.Arms[ArmJev])
	}
	d := readLedgerT(t, env.out)
	c := classOf(t, d, ArmJev, fx.bugfix.BundleID)
	if c.State != StateOK || c.Status != categorize.StatusOK || len(c.AttemptIDs) != 1 || c.LLMCalls != 1 {
		t.Fatalf("%+v", c)
	}
	if math.Abs(c.Subcategories["quality.bugfix"]-0.8) > 1e-12 || math.Abs(c.Subcategories["quality.testing"]-0.2) > 1e-12 {
		t.Fatalf("mix = %v", c.Subcategories)
	}
	a := d.AttemptsOf(c)[0]
	wantCost := 7000 * 0.042 / 1e6
	if a.Phase != PhaseCompleted || a.AttemptState != AttemptHTTPOK || a.HTTPStatus != 200 || math.Abs(a.BilledCostUSD-wantCost) > 1e-12 ||
		a.CostBasis != "usage_x_published_rate" || a.Usage.InputTokens != 7000 || a.Usage.OutputTokens != 50 {
		t.Fatalf("%+v", a)
	}
	if a.RequestID != "req_test_1" || a.ModelRequested != DefaultJevModel || a.ModelReturned != DefaultJevModel || a.APIMode != APIModeSystemOne ||
		a.Provider != ProviderTypeSafe || a.QuestionCount != 21 || a.SpanCount == 0 || a.Attempt != 1 || a.RetryOf != 0 || a.FallbackUsed {
		t.Fatalf("%+v", a)
	}
	if a.Rubric != "decision-support-v1" || a.Map != "support-map-v1" || a.Adapter != "decision-adapter-v1" || a.Rates != "rates-2026-10-06" ||
		a.Span != "span-candidates-v1" || a.Eval != EvalVersion || !strings.Contains(a.ModelVersionStamp, "model=jev-1.13.0;") || a.RubricSHA256 != env.r.SHA256 {
		t.Fatalf("versions: %+v", a.Versions)
	}
	// raw bodies are saved verbatim, without auth
	sent := env.jev.bodies()[0]
	raw, err := os.ReadFile(filepath.Join(env.out, a.RawRequestPath))
	if err != nil || string(raw) != string(sent) {
		t.Fatalf("raw request differs from what was sent (%v)", err)
	}
	resp, err := os.ReadFile(filepath.Join(env.out, a.RawResponsePath))
	if err != nil || !strings.Contains(string(resp), `"answers"`) {
		t.Fatalf("raw response: %v", err)
	}
	hdr, _ := os.ReadFile(filepath.Join(env.out, a.RawHeadersPath))
	if !strings.HasPrefix(string(hdr), "STATUS 200\n") || !strings.Contains(string(hdr), "X-Typesafe-Request-Id: req_test_1") {
		t.Fatalf("headers file: %s", hdr)
	}
	if env.jev.auth[0] != "Bearer "+testJevToken {
		t.Fatal("the token did not reach the provider")
	}
	assertNoSecret(t, env.out, testJevToken, testOAIKey)
	// reserved line before the send, completed line after
	data, _ := os.ReadFile(filepath.Join(env.out, LedgerFile))
	if n := strings.Count(string(data), `"phase":"reserved"`); n != 1 {
		t.Fatalf("reserved lines = %d", n)
	}
	if strings.Index(string(data), `"phase":"reserved"`) > strings.Index(string(data), `"phase":"completed"`) {
		t.Fatal("the reservation must be written before the completed line")
	}
}

func TestDecisionsEndToEnd(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.dec = newFakeDecisions(t, env.r, allBehave(bugfixBehaviour()))
	s := mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmDecisions))
	if s.Arms[ArmDecisions].States[StateOK] != 1 {
		t.Fatalf("%+v", s.Arms[ArmDecisions])
	}
	d := readLedgerT(t, env.out)
	c := classOf(t, d, ArmDecisions, fx.bugfix.BundleID)
	a := d.AttemptsOf(c)[0]
	if a.Provider != ProviderOpenAI || a.APIMode != APIModeDecisions || a.ModelRequested != DefaultLunaModel || a.QuestionCount != 21 ||
		math.Abs(a.BilledCostUSD-7000*0.10/1e6) > 1e-12 {
		t.Fatalf("%+v", a)
	}
	var req struct {
		Model string `json:"model"`
		Input string `json:"input"`
	}
	_ = json.Unmarshal(env.dec.bodies()[0], &req)
	if req.Model != "gpt-6-luna" || !strings.HasPrefix(req.Input, "SOURCE_BLOCK\n") || !strings.HasSuffix(req.Input, "END_EVIDENCE_SPANS") {
		t.Fatalf("request: %.100q", req.Input)
	}
	assertNoSecret(t, env.out, testJevToken, testOAIKey)
}

func TestIncumbentEndToEndKeepsProductionRequest(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.oai = newFakeResponses(t, func(string) string { return incumbentPayload(t, fx.bugfix, "quality.bugfix") }, "gpt-5-nano-2025-08-07")
	s := mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmIncumbent))
	if s.Arms[ArmIncumbent].States[categorize.StatusOK] != 1 {
		t.Fatalf("%+v", s.Arms[ArmIncumbent])
	}
	d := readLedgerT(t, env.out)
	c := classOf(t, d, ArmIncumbent, fx.bugfix.BundleID)
	a := d.AttemptsOf(c)[0]
	if a.APIMode != APIModeResponses || a.ModelRequested != "gpt-5-nano" || a.ModelReturned != "gpt-5-nano-2025-08-07" ||
		a.Usage.InputTokens != 1165 || a.Usage.OutputTokens != 802 || a.Usage.CachedInputTokens != 100 {
		t.Fatalf("%+v", a)
	}
	wantCost := (1065*0.05 + 100*0.005 + 802*0.40) / 1e6
	if math.Abs(a.BilledCostUSD-wantCost) > 1e-12 || a.CostNoCacheDiscountUSD <= a.BilledCostUSD {
		t.Fatalf("cost %v want %v (no discount %v)", a.BilledCostUSD, wantCost, a.CostNoCacheDiscountUSD)
	}
	var body struct {
		Model     string `json:"model"`
		Input     string `json:"input"`
		MaxOut    int    `json:"max_output_tokens"`
		Reasoning struct{ Effort string }
		Text      struct {
			Verbosity string
			Format    struct {
				Type, Name string
				Strict     bool
			}
		}
	}
	if err := json.Unmarshal(env.oai.bodies()[0], &body); err != nil {
		t.Fatal(err)
	}
	if body.Model != "gpt-5-nano" || body.MaxOut != 2048 || body.Reasoning.Effort != "low" || body.Text.Verbosity != "low" ||
		body.Text.Format.Type != "json_schema" || body.Text.Format.Name != "categorization" || !body.Text.Format.Strict {
		t.Fatalf("request is not the production request: %+v", body)
	}
	// The production prompt is untouched.
	if body.Input != categorize.BuildPrompt(mustBundle(t, fx.bugfix).SourceBlock) {
		t.Fatal("the incumbent arm changed the production prompt")
	}
	if env.oai.auth[0] != "Bearer "+testOAIKey {
		t.Fatal("the key did not reach the provider")
	}
	assertNoSecret(t, env.out, testJevToken, testOAIKey)
}

// incumbent+defs sends the production request plus ONE inserted block.
func TestIncumbentDefsArmOnlyInsertsDefinitions(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.oai = newFakeResponses(t, func(string) string { return incumbentPayload(t, fx.bugfix, "quality.bugfix") }, "gpt-5-nano-2025-08-07")
	mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmIncumbent, ArmIncumbentDefs))
	bodies := env.oai.bodies()
	if len(bodies) != 2 {
		t.Fatalf("requests = %d", len(bodies))
	}
	input := func(b []byte) string {
		var v struct{ Input string }
		_ = json.Unmarshal(b, &v)
		return v.Input
	}
	plain, defs := input(bodies[0]), input(bodies[1])
	block := IncumbentDefsText(env.r)
	if !strings.Contains(defs, block) || strings.Contains(plain, "Category definitions") {
		t.Fatal("the definitions block is missing from the defs arm or leaked into the incumbent")
	}
	if strings.Replace(defs, "\n\n"+block, "", 1) != plain {
		t.Fatal("the defs arm differs from the incumbent by more than the inserted block")
	}
	for _, k := range SortedKeys() {
		if !strings.Contains(block, "- "+k+" (") {
			t.Fatalf("definition of %s missing", k)
		}
	}
	d := readLedgerT(t, env.out)
	if got := classOf(t, d, ArmIncumbentDefs, fx.bugfix.BundleID); got.Prompt != "investment-categorization-v2+incumbent-defs-v1/decision-support-v1" {
		t.Fatalf("prompt stamp = %q", got.Prompt)
	}
	if got := classOf(t, d, ArmIncumbent, fx.bugfix.BundleID); got.Prompt != categorize.PromptVersion {
		t.Fatalf("prompt stamp = %q", got.Prompt)
	}
	// The production constant is untouched by the experiment.
	if !strings.Contains(categorize.BuildPrompt("x"), "Source text (quotes must be exact substrings):\nx") {
		t.Fatal("production prompt changed: the defs insertion point is stale")
	}
}

func TestIncumbentRepairIsOneMoreAttemptInTheLedger(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	n := 0
	env.oai = newFakeResponses(t, func(string) string {
		n++
		if n == 1 {
			return `{"subcategories":{},"evidence_quotes":[],"uncertainty":"x"}` // invalid: first call
		}
		return incumbentPayload(t, fx.bugfix, "quality.bugfix")
	}, "gpt-5-nano-2025-08-07")
	mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmIncumbent))
	d := readLedgerT(t, env.out)
	c := classOf(t, d, ArmIncumbent, fx.bugfix.BundleID)
	if c.Status != categorize.StatusRepaired || len(c.AttemptIDs) != 2 || c.LLMCalls != 2 {
		t.Fatalf("%+v", c)
	}
	if got := d.AttemptsOf(c)[0].BilledCostUSD + d.AttemptsOf(c)[1].BilledCostUSD; math.Abs(got-c.BilledCostUSD) > 1e-15 {
		t.Fatalf("classification cost %v is not the sum of attempts %v", c.BilledCostUSD, got)
	}
}

// ---- failure shapes over HTTP ----

func runJevWithScript(t *testing.T, script []int, hdr map[int]map[string]string, fixtures ...FixtureRecord) (*testEnv, RunSummary, *fakeProvider) {
	env := newEnv(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	env.jev.script = script
	for code, h := range hdr {
		env.jev.headers[code] = map[string][]string{}
		for k, v := range h {
			env.jev.headers[code][k] = []string{v}
		}
	}
	return env, mustRun(t, newCfg(t, env, fixtures, ArmJev)), env.jev
}

func TestHTTPFailuresAndRetryPolicy(t *testing.T) {
	fx := makeFixtures(t)
	cases := []struct {
		name       string
		script     []int
		hdr        map[int]map[string]string
		wantCalls  int // GUARD: a sender that retries 4xx, or does not retry 429/529/5xx, changes this count
		wantState  string
		wantCode   string
		wantStop   string
		wantSleeps []time.Duration
	}{
		{"401 stops the arm, no retry", []int{401}, nil, 1, StateRequestFailed, "request_failed:http_401", "auth_401", nil},
		{"402 stops the arm, no retry", []int{402}, nil, 1, StateRequestFailed, "request_failed:http_402", "auth_402", nil},
		{"403 stops the arm, no retry", []int{403}, nil, 1, StateRequestFailed, "request_failed:http_403", "auth_403", nil},
		{"422 is not retried", []int{422}, nil, 1, StateRequestFailed, "request_failed:http_422", "", nil},
		{"400 is not retried", []int{400}, nil, 1, StateRequestFailed, "request_failed:http_400", "", nil},
		{"429 then 200 retries one time", []int{429}, nil, 2, StateOK, "", "", []time.Duration{defaultRetryWait}},
		{"429 honours retry-after", []int{429}, map[int]map[string]string{429: {"Retry-After": "7"}}, 2, StateOK, "", "", []time.Duration{7 * time.Second}},
		{"retry-after is capped at 60 s", []int{429}, map[int]map[string]string{429: {"Retry-After": "600"}}, 2, StateOK, "", "", []time.Duration{60 * time.Second}},
		{"529 twice fails after one retry", []int{529, 529}, nil, 2, StateRequestFailed, "request_failed:http_529", "", []time.Duration{defaultRetryWait}},
		{"500 then 200", []int{500}, nil, 2, StateOK, "", "", []time.Duration{defaultRetryWait}},
		{"503 twice", []int{503, 503}, nil, 2, StateRequestFailed, "request_failed:http_5xx", "", []time.Duration{defaultRetryWait}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env, s, fake := runJevWithScript(t, c.script, c.hdr, fx.bugfix)
			if fake.count() != c.wantCalls {
				t.Fatalf("provider saw %d requests, want %d", fake.count(), c.wantCalls)
			}
			d := readLedgerT(t, env.out)
			rec := classOf(t, d, ArmJev, fx.bugfix.BundleID)
			if rec.State != c.wantState || rec.StopArm != c.wantStop {
				t.Fatalf("state=%s stop=%q codes=%v", rec.State, rec.StopArm, rec.ErrorCodes)
			}
			if c.wantCode != "" && !strings.Contains(strings.Join(rec.ErrorCodes, " "), c.wantCode) {
				t.Fatalf("codes = %v, want %s", rec.ErrorCodes, c.wantCode)
			}
			if len(env.sleeps) != len(c.wantSleeps) || (len(c.wantSleeps) > 0 && env.sleeps[0] != c.wantSleeps[0]) {
				t.Fatalf("sleeps = %v, want %v", env.sleeps, c.wantSleeps)
			}
			if (c.wantStop != "") != (s.Arms[ArmJev].Stopped != "") {
				t.Fatalf("arm stop = %q", s.Arms[ArmJev].Stopped)
			}
			// every HTTP attempt has its own ledger entry, a retry is linked
			if len(rec.AttemptIDs) != c.wantCalls {
				t.Fatalf("attempt ids = %v", rec.AttemptIDs)
			}
			if c.wantCalls == 2 {
				at := d.AttemptsOf(rec)
				if at[1].RetryOf != 1 || at[0].RetryOf != 0 || at[0].AttemptState != AttemptHTTPError {
					t.Fatalf("retry link: %+v %+v", at[0], at[1])
				}
			}
			for _, a := range d.AttemptsOf(rec) {
				if a.AttemptState == AttemptHTTPError && (a.BilledCostUSD != 0 || a.ErrorClass == "") {
					t.Fatalf("failed attempt: %+v", a)
				}
			}
			if c.wantState == StateRequestFailed && rec.Status != categorize.StatusLLMTaskFailed {
				t.Fatalf("status = %s", rec.Status)
			}
		})
	}
}

func TestArmStopsAfterAuthFailureAndSkipsTheRest(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	env.jev.script = []int{401}
	s := mustRun(t, newCfg(t, env, fx.gated(), ArmJev))
	a := s.Arms[ArmJev]
	if env.jev.count() != 1 || a.Stopped != "auth_401" || a.NotRun != 3 {
		t.Fatalf("calls=%d %+v", env.jev.count(), a)
	}
}

func TestArmStopsAfterFiveRequestFailuresInARow(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	env.jev.script = []int{422, 422, 422, 422, 422}
	var fixtures []FixtureRecord
	for i := 0; i < 7; i++ {
		f := fx.bugfix
		f.FixtureID, f.BundleID = f.FixtureID+string(rune('a'+i)), f.BundleID+string(rune('a'+i))
		fixtures = append(fixtures, f)
	}
	s := mustRun(t, newCfg(t, env, fixtures, ArmJev))
	a := s.Arms[ArmJev]
	if a.Stopped != "5_request_failures_in_a_row" || env.jev.count() != 5 || a.NotRun != 2 {
		t.Fatalf("calls=%d %+v", env.jev.count(), a)
	}
}

func TestModelMismatchAndNotJSON(t *testing.T) {
	fx := makeFixtures(t)
	t.Run("returned model differs", func(t *testing.T) {
		env := newEnv(t)
		b := bugfixBehaviour()
		b.model = "jev-latest"
		env.jev = newFakeJev(t, env.r, allBehave(b))
		mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev))
		c := classOf(t, readLedgerT(t, env.out), ArmJev, fx.bugfix.BundleID)
		if c.State != StateRequestFailed || !strings.Contains(strings.Join(c.ErrorCodes, " "), "model_mismatch") {
			t.Fatalf("%+v", c)
		}
	})
	t.Run("an accepted alias passes and is recorded", func(t *testing.T) {
		env := newEnv(t)
		b := bugfixBehaviour()
		b.model = "gpt-6-luna-2026-09-01"
		env.dec = newFakeDecisions(t, env.r, allBehave(b))
		cfg := newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmDecisions)
		cfg.Decisions.AcceptedModels = []string{"gpt-6-luna-2026-09-01"}
		mustRun(t, cfg)
		d := readLedgerT(t, env.out)
		c := classOf(t, d, ArmDecisions, fx.bugfix.BundleID)
		if c.State != StateOK || c.ModelReturned != "gpt-6-luna-2026-09-01" || c.ModelRequested != "gpt-6-luna" {
			t.Fatalf("%+v", c)
		}
	})
	t.Run("a 200 that is not JSON", func(t *testing.T) {
		env := newEnv(t)
		env.jev = newFake(t, func([]byte) []byte { return []byte("<html>bad gateway</html>") })
		mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev))
		c := classOf(t, readLedgerT(t, env.out), ArmJev, fx.bugfix.BundleID)
		if c.State != StateRequestFailed || !strings.Contains(strings.Join(c.ErrorCodes, " "), "not_json") {
			t.Fatalf("%+v", c)
		}
	})
}

// A refusal, a missing answer, an invalid value, a duplicate and an unknown id
// are NOT valid mixes and are not repaired: one request each.
func TestPerQuestionFailuresOverTheWire(t *testing.T) {
	fx := makeFixtures(t)
	q := SupportQuestionID("risk.security")
	cases := []struct {
		name  string
		arm   string
		b     behaviour
		state string
		code  string
	}{
		{"decisions refusal", ArmDecisions, behaviour{refuse: map[string]bool{q: true}}, StateQuestionRefused, "question_refused:risk.security"},
		{"decisions missing answer", ArmDecisions, behaviour{drop: map[string]bool{q: true}}, StateAnswerMissing, "answer_missing:risk.security"},
		{"decisions invalid probabilities", ArmDecisions, behaviour{badProbs: map[string]bool{q: true}}, StateAnswerInvalid, "answer_invalid:risk.security:prob_sum"},
		{"decisions wrong type", ArmDecisions, behaviour{wrongType: map[string]bool{q: true}}, StateAnswerInvalid, "answer_invalid:risk.security:type=choice"},
		{"decisions duplicate name", ArmDecisions, behaviour{duplicate: map[string]bool{q: true}}, StateAnswerInvalid, "duplicate_id"},
		{"decisions unknown name", ArmDecisions, behaviour{unknownExtra: true}, StateAnswerInvalid, "unknown_id:surprise"},
		{"jev missing answer", ArmJev, behaviour{drop: map[string]bool{q: true}}, StateAnswerMissing, "answer_missing:risk.security"},
		{"jev invalid probabilities", ArmJev, behaviour{badProbs: map[string]bool{q: true}}, StateAnswerInvalid, "answer_invalid:risk.security:prob_sum"},
		{"jev wrong type", ArmJev, behaviour{wrongType: map[string]bool{q: true}}, StateAnswerInvalid, "type=refusal"},
		{"jev duplicate key", ArmJev, behaviour{duplicate: map[string]bool{q: true}}, StateAnswerInvalid, "duplicate_id"},
		{"jev unknown key", ArmJev, behaviour{unknownExtra: true}, StateAnswerInvalid, "unknown_id:surprise"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newEnv(t)
			b := bugfixBehaviour()
			b.refuse, b.drop, b.badProbs, b.wrongType, b.duplicate, b.unknownExtra = c.b.refuse, c.b.drop, c.b.badProbs, c.b.wrongType, c.b.duplicate, c.b.unknownExtra
			var fake *fakeProvider
			if c.arm == ArmJev {
				env.jev = newFakeJev(t, env.r, allBehave(b))
				fake = env.jev
			} else {
				env.dec = newFakeDecisions(t, env.r, allBehave(b))
				fake = env.dec
			}
			mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, c.arm))
			rec := classOf(t, readLedgerT(t, env.out), c.arm, fx.bugfix.BundleID)
			if rec.State != c.state || rec.Status != categorize.StatusInvalidLLMOutput {
				t.Fatalf("state=%s status=%s codes=%v", rec.State, rec.Status, rec.ErrorCodes)
			}
			if !strings.Contains(strings.Join(rec.ErrorCodes, " "), c.code) {
				t.Fatalf("codes = %v, want %s", rec.ErrorCodes, c.code)
			}
			// GUARD: no repair ask and no second ask of the question.
			if fake.count() != 1 || rec.Status == categorize.StatusRepaired {
				t.Fatalf("requests = %d, status %s", fake.count(), rec.Status)
			}
			// never a valid mix: the persisted mix is the fallback prior
			if math.Abs(rec.Subcategories["risk.security"]-0.2) > 1e-12 || rec.Subcategories["quality.testing"] != 0 || len(rec.Quotes) != 0 {
				t.Fatalf("mix = %v quotes=%d", rec.Subcategories, len(rec.Quotes))
			}
			if rec.ErrorCodes[0] != categorize.StatusInvalidLLMOutput || rec.ErrorCodes[1] != "decision_"+c.state {
				t.Fatalf("error codes = %v", rec.ErrorCodes)
			}
		})
	}
}

func TestZeroSupportIsAnAnswerNotAFailureAndNeverUniform(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.dec = newFakeDecisions(t, env.r, allBehave(behaviour{}))
	mustRun(t, newCfg(t, env, []FixtureRecord{fx.zero}, ArmDecisions))
	rec := classOf(t, readLedgerT(t, env.out), ArmDecisions, fx.zero.BundleID)
	if rec.State != StateZeroSupport || rec.Status != categorize.StatusInsufficientChars {
		t.Fatalf("%+v", rec)
	}
	// the neutral prior, never 1/15 each
	if math.Abs(rec.Subcategories["quality.bugfix"]-0.2) > 1e-12 || rec.Subcategories["maintenance.upgrade"] != 0 {
		t.Fatalf("mix = %v", rec.Subcategories)
	}
	if !strings.Contains(strings.Join(rec.ErrorCodes, " "), "decision_zero_support") || rec.Uncertainty != "Insufficient validated evidence to assign a confident subcategory mix." {
		t.Fatalf("codes=%v uncertainty=%q", rec.ErrorCodes, rec.Uncertainty)
	}
	if env.dec.count() != 1 {
		t.Fatalf("zero support must not trigger a second request: %d", env.dec.count())
	}
}

// ---- repair is never answered ----

type stubSender struct {
	calls int
	body  []byte
}

func (s *stubSender) Send(context.Context, Backend, BuiltRequest) SendResult {
	s.calls++
	return SendResult{Status: 200, Body: s.body}
}

func TestRepairPromptIsNeverAnswered(t *testing.T) {
	r := testRubric(t)
	fx := makeFixtures(t)
	bundle := mustBundle(t, fx.bugfix)
	built, _ := BuildJevRequest(r, DefaultJevModel, bundle)
	exp := ExpectedQuestions(r, built.Spans)
	var parts []string
	for _, q := range exp {
		var ans any
		if q.Kind == "score" {
			lvl := 0
			if q.ID == SupportQuestionID("quality.bugfix") {
				lvl = 3
			}
			ans = jevScore(probsFor(lvl, q.Levels))
		} else {
			ans = jevChoice("E1_1", q.Options)
		}
		raw, _ := json.Marshal(ans)
		parts = append(parts, `"`+q.ID+`":`+string(raw))
	}
	sender := &stubSender{body: []byte(`{"model":"jev-1.13.0","answers":{` + strings.Join(parts, ",") + `},"usage":{"input_tokens":7000,"output_tokens":5}}`)}
	backend := NewJevBackend("http://unused", secrets.NewHidden("x"), "")
	// The hook breaks the payload AFTER the adapter self-check, so the production
	// validator in CategorizeTextBundle rejects it and asks for a repair.
	c, err := DecisionCategorize(context.Background(), bundle, DecisionDeps{Rubric: r, Weights: r.Weights, Backend: backend, Sender: sender,
		postSelfCheck: func([]byte) []byte { return []byte(`{"subcategories":{}}`) }})
	if err != nil {
		t.Fatal(err)
	}
	if c.State != StateAdapterDefect || sender.calls != 1 || c.Outcome.Status != categorize.StatusInvalidLLMOutput {
		t.Fatalf("state=%s calls=%d status=%s", c.State, sender.calls, c.Outcome.Status)
	}
	if !strings.Contains(strings.Join(c.Outcome.Errors, " "), "repair_entered") {
		t.Fatalf("errors = %v", c.Outcome.Errors)
	}
	// Same, called directly: the second Complete sends nothing.
	p := &DecisionProvider{Rubric: r, Weights: r.Weights, Backend: backend, Sender: sender, source: bundle}
	req := categorize.CategorizationRequest(categorize.BuildPrompt(bundle.SourceBlock))
	if _, err := p.Complete(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	before := sender.calls
	if _, err := p.Complete(context.Background(), req); err == nil || sender.calls != before {
		t.Fatalf("the second Complete must fail without a send: err=%v calls %d -> %d", err, before, sender.calls)
	}
}

func TestPromptMismatchAndBadBundleAreAdapterDefects(t *testing.T) {
	r := testRubric(t)
	fx := makeFixtures(t)
	bundle := mustBundle(t, fx.bugfix)
	sender := &stubSender{}
	backend := NewJevBackend("http://unused", secrets.NewHidden("x"), "")
	p := &DecisionProvider{Rubric: r, Weights: r.Weights, Backend: backend, Sender: sender, source: bundle}
	if _, err := p.Complete(context.Background(), categorize.CategorizationRequest("another prompt")); err == nil || sender.calls != 0 {
		t.Fatalf("a prompt for another bundle must not send: %v", err)
	}
	ref := bundle.HandleMap["E1"]
	bundle.SourceTexts[ref.SourceType][ref.SourceID] = "drifted"
	c, err := DecisionCategorize(context.Background(), bundle, DecisionDeps{Rubric: r, Weights: r.Weights, Backend: backend, Sender: sender})
	if err != nil || c.State != StateAdapterDefect || sender.calls != 0 {
		t.Fatalf("state=%s calls=%d err=%v", c.State, sender.calls, err)
	}
}

// ---- gate ----

func TestBelowGateBundleSendsNothing(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	env.dec = newFakeDecisions(t, env.r, allBehave(bugfixBehaviour()))
	env.oai = newFakeResponses(t, func(string) string { return incumbentPayload(t, fx.bugfix, "quality.bugfix") }, "gpt-5-nano")
	s := mustRun(t, newCfg(t, env, []FixtureRecord{fx.short, fx.bugfix}, ArmJev, ArmDecisions, ArmIncumbent))
	// GUARD: if the gate moved or were skipped, each provider would see 2 requests.
	if env.jev.count() != 1 || env.dec.count() != 1 || env.oai.count() != 1 {
		t.Fatalf("requests: jev=%d decisions=%d incumbent=%d", env.jev.count(), env.dec.count(), env.oai.count())
	}
	d := readLedgerT(t, env.out)
	for _, arm := range []string{ArmJev, ArmDecisions, ArmIncumbent} {
		c := classOf(t, d, arm, fx.short.BundleID)
		if c.Gate != categorize.StatusInsufficientChars || c.Status != categorize.StatusInsufficientChars || len(c.AttemptIDs) != 0 {
			t.Fatalf("%s: %+v", arm, c)
		}
		if s.Arms[arm].Gate != 1 {
			t.Fatalf("%s summary: %+v", arm, s.Arms[arm])
		}
	}
}

// ---- spend cap ----

func estTokens(t *testing.T, r *Rubric, f FixtureRecord) int {
	b, err := BuildJevRequest(r, DefaultJevModel, mustBundle(t, f))
	if err != nil {
		t.Fatal(err)
	}
	return b.EstimatedInputTokens
}

func TestSpendCapZeroRefusesToRunLive(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	cfg := newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev)
	cfg.Caps = map[string]float64{ProviderTypeSafe: 0, ProviderOpenAI: 5}
	if _, err := Run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "spend cap") {
		t.Fatalf("err = %v", err)
	}
	cfg.Caps = nil
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("no cap at all must refuse")
	}
	if env.jev.count() != 0 {
		t.Fatalf("%d requests were sent under a zero cap", env.jev.count())
	}
}

func TestLiveModeIsOffByDefault(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	cfg := newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev)
	cfg.Live = false
	if _, err := Run(context.Background(), cfg); err == nil || env.jev.count() != 0 {
		t.Fatalf("a run without Live must not send: %v", err)
	}
}

// GUARD: a cap that does not stop must fail here: with the reservation removed
// all three fixtures would be sent.
func TestSpendCapStopsBeforeTheNextRequest(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	est := estTokens(t, env.r, fx.bugfix)
	b := bugfixBehaviour()
	b.inputTokens = int64(est) // actual spend of one request = its estimate
	env.jev = newFakeJev(t, env.r, allBehave(b))
	perRequest := float64(est) * 0.042 / 1e6
	cfg := newCfg(t, env, []FixtureRecord{fx.bugfix, fx.bugfix, fx.bugfix}, ArmJev)
	cfg.Fixtures[1].FixtureID, cfg.Fixtures[1].BundleID = "x2", "bnd_x2"
	cfg.Fixtures[2].FixtureID, cfg.Fixtures[2].BundleID = "x3", "bnd_x3"
	cfg.Caps[ProviderTypeSafe] = perRequest * 1.5
	s := mustRun(t, cfg)
	if env.jev.count() != 1 {
		t.Fatalf("the cap let %d requests through", env.jev.count())
	}
	a := s.Arms[ArmJev]
	if a.Stopped != "budget" || a.NotRun != 1 {
		t.Fatalf("%+v", a)
	}
	d := readLedgerT(t, env.out)
	if got := d.SpentByProvider()[ProviderTypeSafe]; got > cfg.Caps[ProviderTypeSafe] {
		t.Fatalf("ledger total %v passed the cap %v", got, cfg.Caps[ProviderTypeSafe])
	}
	refused := 0
	for _, at := range d.Attempts {
		if at.AttemptState == AttemptBudgetRefused {
			refused++
			if at.RawResponsePath != "" || at.BilledCostUSD != 0 {
				t.Fatalf("a refused attempt left a response or a cost: %+v", at)
			}
		}
	}
	if refused != 1 {
		t.Fatalf("refused attempts = %d", refused)
	}
}

// In-flight reservations count: with 4 workers and room for 2 requests, at most
// 2 may be sent. GUARD: checking settled spend only would send all 4 at once.
func TestSpendCapCountsInFlightReservations(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	est := estTokens(t, env.r, fx.bugfix)
	b := bugfixBehaviour()
	b.inputTokens = int64(est)
	env.jev = newFakeJev(t, env.r, allBehave(b))
	env.jev.delay = 150 * time.Millisecond
	perRequest := float64(est) * 0.042 / 1e6
	var fixtures []FixtureRecord
	for i := 0; i < 4; i++ {
		f := fx.bugfix
		f.FixtureID, f.BundleID = f.FixtureID+string(rune('a'+i)), f.BundleID+string(rune('a'+i))
		fixtures = append(fixtures, f)
	}
	cfg := newCfg(t, env, fixtures, ArmJev)
	cfg.Concurrency = 4
	cfg.Caps[ProviderTypeSafe] = perRequest * 2.5
	mustRun(t, cfg)
	if env.jev.count() > 2 {
		t.Fatalf("%d requests went out under a cap for 2", env.jev.count())
	}
	if got := readLedgerT(t, env.out).SpentByProvider()[ProviderTypeSafe]; got > cfg.Caps[ProviderTypeSafe] {
		t.Fatalf("spent %v > cap %v", got, cfg.Caps[ProviderTypeSafe])
	}
}

// A response without usage is billed at the reservation: the cap must hold even
// when the provider does not report.
func TestMissingUsageIsBilledAtTheReservation(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	b := bugfixBehaviour()
	b.omitUsage = true
	env.jev = newFakeJev(t, env.r, allBehave(b))
	mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev))
	d := readLedgerT(t, env.out)
	a := d.AttemptsOf(classOf(t, d, ArmJev, fx.bugfix.BundleID))[0]
	if a.CostBasis != "estimated_no_usage" || a.BilledCostUSD <= 0 || a.BilledCostUSD != a.ReservedCostUSD {
		t.Fatalf("%+v", a)
	}
}

func TestBudgetUnit(t *testing.T) {
	b := NewBudget(map[string]float64{"p": 1.0}, map[string]float64{"p": 0.5})
	id, err := b.Reserve("p", 0.4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Reserve("p", 0.2); err == nil {
		t.Fatal("0.5 spent + 0.4 reserved + 0.2 must pass the cap of 1.0")
	}
	b.Settle(id, 0.1)
	if _, err := b.Reserve("p", 0.4); err != nil {
		t.Fatalf("0.6 + 0.4 equals the cap and is allowed: %v", err)
	}
	if _, err := b.Reserve("other", 0.0); err == nil {
		t.Fatal("a provider with no cap must refuse")
	}
}

// ---- resume ----

func TestResumeSkipsAcceptedAndRetriesTransportFailures(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	q := SupportQuestionID("risk.security")
	refuse := bugfixBehaviour()
	refuse.refuse = map[string]bool{q: true}
	env.dec = newFakeDecisions(t, env.r, byBlock(t, map[string]behaviour{
		mustBundle(t, fx.bugfix).SourceBlock:   bugfixBehaviour(),
		mustBundle(t, fx.refactor).SourceBlock: refuse,
		mustBundle(t, fx.vuln).SourceBlock:     bugfixBehaviour(),
	}))
	env.dec.script = []int{200, 200, 422} // third fixture: a transport-level failure
	fixtures := []FixtureRecord{fx.bugfix, fx.refactor, fx.vuln}
	s1 := mustRun(t, newCfg(t, env, fixtures, ArmDecisions))
	if env.dec.count() != 3 || s1.Arms[ArmDecisions].States[StateOK] != 1 || s1.Arms[ArmDecisions].States[StateQuestionRefused] != 1 || s1.Arms[ArmDecisions].States[StateRequestFailed] != 1 {
		t.Fatalf("first run: %+v", s1.Arms[ArmDecisions])
	}
	// Second run, same output dir: only the request_failed fixture is sent again.
	s2 := mustRun(t, newCfg(t, env, fixtures, ArmDecisions))
	a := s2.Arms[ArmDecisions]
	if env.dec.count() != 4 || a.SkippedResume != 2 || a.Sent != 1 {
		t.Fatalf("second run: calls=%d %+v", env.dec.count(), a)
	}
	// The refusal is final: a second ask could hide it.
	d := readLedgerT(t, env.out)
	if got := classOf(t, d, ArmDecisions, fx.refactor.BundleID); got.State != StateQuestionRefused {
		t.Fatal("the refusal record changed")
	}
	// Third run: everything is accepted or final, nothing is sent.
	s3 := mustRun(t, newCfg(t, env, fixtures, ArmDecisions))
	if env.dec.count() != 4 || s3.Arms[ArmDecisions].SkippedResume != 3 {
		t.Fatalf("third run: calls=%d %+v", env.dec.count(), s3.Arms[ArmDecisions])
	}
	// A new rubric version is a new key: the same fixture is sent again.
	other := newCfg(t, env, fixtures[:1], ArmDecisions)
	other.Rubric = withVersion(t, env.r, "decision-support-v1.1")
	mustRun(t, other)
	if env.dec.count() != 5 {
		t.Fatalf("a new rubric version must not resume: %d", env.dec.count())
	}
	// And so is a repeat index.
	rep := newCfg(t, env, fixtures[:1], ArmDecisions)
	rep.Repeat = 1
	mustRun(t, rep)
	if env.dec.count() != 6 {
		t.Fatalf("a repeat index must not resume: %d", env.dec.count())
	}
}

func withVersion(t *testing.T, r *Rubric, version string) *Rubric {
	var m map[string]any
	if err := json.Unmarshal(defaultRubricJSON, &m); err != nil {
		t.Fatal(err)
	}
	m["rubric_version"] = version
	data, _ := json.Marshal(m)
	out, err := ParseRubric(data)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestOrphanReservationBlocksResumeAndCountsAsSpend(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev))
	l, err := OpenLedger(env.out)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(AttemptRecord{Kind: KindAttempt, Phase: PhaseReserved, AttemptID: "orphan/1", Provider: ProviderTypeSafe, ReservedCostUSD: 0.002, Arm: ArmJev, BundleID: "bnd_orphan"}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	cfg := newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev)
	if _, err := Run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "never completed") {
		t.Fatalf("an orphan reservation must stop the run: %v", err)
	}
	d := readLedgerT(t, env.out)
	if len(d.OrphanReservations()) != 1 || d.SpentByProvider()[ProviderTypeSafe] < 0.002 {
		t.Fatalf("orphans=%d spent=%v", len(d.OrphanReservations()), d.SpentByProvider())
	}
	cfg.RetryOrphans = true
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestOutputDirInsideARepoIsRefused(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "sub", "out")
	if _, err := OpenLedger(dir); err == nil {
		t.Fatal("a ledger inside a git repository was opened")
	}
	env := newEnv(t)
	cfg := newCfg(t, env, []FixtureRecord{makeFixtures(t).bugfix}, ArmJev)
	cfg.OutDir = dir
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("Run accepted an output dir inside a repo")
	}
	if _, err := os.Stat(dir); err == nil {
		t.Fatal("the directory was created")
	}
}

// ---- dry run ----

func TestDryRunSendsNothingAndSavesRequests(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	env.jev = newFakeJev(t, env.r, allBehave(bugfixBehaviour()))
	env.dec = newFakeDecisions(t, env.r, allBehave(bugfixBehaviour()))
	env.oai = newFakeResponses(t, func(string) string { return incumbentPayload(t, fx.bugfix, "quality.bugfix") }, "gpt-5-nano")
	cfg := newCfg(t, env, append(fx.gated(), fx.short), ArmJev, ArmDecisions, ArmIncumbent, ArmIncumbentDefs)
	cfg.DryRun, cfg.Live, cfg.Caps = true, false, nil // no cap, no live flag
	cfg.Jev.APIKey, cfg.Decisions.APIKey, cfg.Incumbent.APIKey = secrets.Hidden{}, secrets.Hidden{}, secrets.Hidden{}
	var out strings.Builder
	cfg.Out = &out
	s := mustRun(t, cfg)
	if env.jev.count()+env.dec.count()+env.oai.count() != 0 {
		t.Fatalf("a dry run sent requests: %d %d %d", env.jev.count(), env.dec.count(), env.oai.count())
	}
	if s.DryRun.GateSkips != 4 { // the short fixture, once for each of the 4 arms
		t.Fatalf("gate skips = %d", s.DryRun.GateSkips)
	}
	for _, arm := range []string{ArmJev, ArmDecisions} {
		a := s.DryRun.Arms[arm]
		if a.Requests != 4 || a.EstInputTokensP50 < 5000 || a.EstInputTokensP50 > 12000 || a.EstCostUSD <= 0 {
			t.Fatalf("%s: %+v", arm, a)
		}
		saved, err := os.ReadFile(filepath.Join(a.Dir, fx.bugfix.BundleID+".request.json"))
		if err != nil {
			t.Fatal(err)
		}
		backend := cfg.Jev
		if arm == ArmDecisions {
			backend = cfg.Decisions
		}
		want, _ := backend.BuildRequest(env.r, mustBundle(t, fx.bugfix))
		if string(saved) != string(want.Body) {
			t.Fatalf("%s: saved request differs from the built request", arm)
		}
	}
	if !strings.Contains(out.String(), "dry-run arm=jev requests=4") || !strings.Contains(out.String(), "est_input_tokens") {
		t.Fatalf("output: %s", out.String())
	}
	if _, err := os.Stat(filepath.Join(env.out, LedgerFile)); err == nil {
		t.Fatal("a dry run wrote a ledger")
	}
	// The incumbent dry run renders the exact body a live run sends.
	dryBody, err := os.ReadFile(filepath.Join(s.DryRun.Arms[ArmIncumbent].Dir, fx.bugfix.BundleID+".request.json"))
	if err != nil {
		t.Fatal(err)
	}
	live := newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmIncumbent)
	mustRun(t, live)
	if string(env.oai.bodies()[0]) != string(dryBody) {
		t.Fatal("the incumbent dry-run request differs from the live request")
	}
}

func TestRunConfigFromEnvGuardsEndpoints(t *testing.T) {
	get := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if _, err := RunConfigFromEnv(get(map[string]string{EnvJevEndpoint: "https://evil.example/v1/systemone", EnvJevToken: "t"})); err == nil {
		t.Fatal("a custom Jev endpoint must be refused (the token would leave for another host)")
	}
	if _, err := RunConfigFromEnv(get(map[string]string{EnvOpenAIBase: "https://evil.example/v1"})); err == nil {
		t.Fatal("a custom OpenAI base must be refused")
	}
	cfg, err := RunConfigFromEnv(get(map[string]string{EnvArms: "jev,decisions", EnvCapOpenAI: "1.5", EnvJevToken: "tok", EnvOpenAIKey: "key", EnvConcurrency: "3"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Jev.Endpoint != JevEndpoint || cfg.Decisions.Endpoint != DecisionsEndpoint || cfg.Caps[ProviderOpenAI] != 1.5 || cfg.Caps[ProviderTypeSafe] != 0 || cfg.Concurrency != 3 || cfg.Live {
		t.Fatalf("%+v", cfg)
	}
	if _, err := RunConfigFromEnv(get(map[string]string{EnvCapOpenAI: "abc"})); err == nil {
		t.Fatal("a bad cap was accepted")
	}
	if _, err := RunConfigFromEnv(get(map[string]string{EnvJevEndpoint: "http://127.0.0.1:1", EnvCustomEndpoints: "1"})); err != nil {
		t.Fatalf("explicit override must work: %v", err)
	}
}

// No production package imports the experiment package.
func TestNoProductionPackageImportsTheExperiment(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	self, _ := filepath.Abs(".")
	needle := "internal/jobs/investment/decisioneval"
	checked := 0
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "node_modules", "vendor", "third_party", "worktrees":
				return filepath.SkipDir
			}
			if p == self {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		checked++
		data, _ := os.ReadFile(p)
		if strings.Contains(string(data), `"github.com/full-chaos/dev-health-ops/`+needle+`"`) {
			t.Errorf("%s imports the experiment package", p)
		}
		return nil
	})
	if checked < 100 {
		t.Fatalf("only %d Go files were checked: the import guard looked at nothing", checked)
	}
}
