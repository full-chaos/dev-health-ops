package decisioneval

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// ---- level rules ----

func TestLevelRules(t *testing.T) {
	median := LevelRuleSpec{Name: LevelMedian}
	cond5 := LevelRuleSpec{Name: LevelConditionalMedian, Tau: 0.5}
	cond67 := LevelRuleSpec{Name: LevelConditionalMedian, Tau: 0.67}
	cases := []struct {
		name string
		p    []float64
		want [3]int // median, conditional 0.5, conditional 0.67
	}{
		{"bimodal gives a level the model does not believe under the median", []float64{0.45, 0.05, 0.05, 0.45}, [3]int{1, 3, 0}},
		{"0.51 / 0.49 moves the weight to 0", []float64{0.51, 0, 0, 0.49}, [3]int{0, 0, 0}},
		{"0.49 / 0.51 moves it to 3", []float64{0.49, 0, 0, 0.51}, [3]int{3, 3, 0}},
		{"tie at exactly one half takes the lower level (median)", []float64{0.5, 0.5, 0, 0}, [3]int{0, 1, 0}},
		{"tie inside the conditional distribution takes the lower level", []float64{0.5, 0.25, 0.25, 0}, [3]int{0, 1, 0}},
		{"a tail under the threshold gives no weight", []float64{0.7, 0.1, 0.1, 0.1}, [3]int{0, 0, 0}},
		{"clear primary", []float64{0.05, 0.05, 0.1, 0.8}, [3]int{3, 3, 3}},
		{"clear secondary", []float64{0.1, 0.8, 0.05, 0.05}, [3]int{1, 1, 1}},
	}
	for _, c := range cases {
		got := [3]int{ApplyLevelRule(median, c.p), ApplyLevelRule(cond5, c.p), ApplyLevelRule(cond67, c.p)}
		if got != c.want {
			t.Errorf("%s: %v -> %v, want %v", c.name, c.p, got, c.want)
		}
	}
	if isBimodal([]float64{0.45, 0.05, 0.05, 0.45}, 0.3, 0.3) != true || isBimodal([]float64{0.7, 0.1, 0.1, 0.1}, 0.3, 0.3) != false || isBimodal([]float64{0.2, 0.1, 0.3, 0.4}, 0.3, 0.3) {
		t.Fatal("bimodal flag: p0 >= 0.3 and p2 + p3 >= 0.3")
	}
	r := testRubric(t)
	for _, label := range []string{"median", "conditional-median:0.5", "conditional-median:0.67"} {
		if _, err := r.ParseLevelRule(label); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []string{"mode", "conditional-median:0.8", "conditional-median"} {
		if _, err := r.ParseLevelRule(bad); err == nil {
			t.Fatalf("%q is not a declared candidate", bad)
		}
	}
}

// Renormalisation: a map that sums to 0.99 or 1.01 is valid and renormalised;
// every level rule uses p'.
func TestRenormalisationOfTheLevelDistribution(t *testing.T) {
	qa := scoreAnswerFrom([]*float64{fp(0.495), fp(0.005), fp(0), fp(0.49)}, false, false, fp(1.4), nil, ExpectedQuestion{Levels: 4, Thresholds: []float64{0.5, 1.5, 2.5}})
	if qa.Status != QAOK || qa.Degraded != "" {
		t.Fatalf("%+v", qa)
	}
	sum := 0.0
	for _, p := range qa.Score.Probs {
		sum += p
	}
	if math.Abs(sum-1) > 1e-12 {
		t.Fatalf("p' sums to %v", sum)
	}
	// 0.495 / 0.99 = 0.5 exactly: the median takes the lower level
	if got := MedianLevel(qa.Score.Probs); got != 0 {
		t.Fatalf("level = %d", got)
	}
	// 0.98 is a malformed map
	qa = scoreAnswerFrom([]*float64{fp(0.49), fp(0), fp(0), fp(0.49)}, false, false, fp(1.4), nil, ExpectedQuestion{Levels: 4, Thresholds: []float64{0.5, 1.5, 2.5}})
	if qa.Degraded != "sum:0.98" {
		t.Fatalf("%+v", qa)
	}
}

func fp(v float64) *float64 { return &v }

// The level rule is applied at replay time from the stored responses, with no
// network. The bimodal flag is recorded.
func TestLevelRuleIsAppliedAtReplayTime(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	bug := SupportQuestionID("quality.bugfix")
	b := behaviour{probs: map[string][]float64{bug: {0.45, 0.05, 0.05, 0.45}}, evidence: map[string]string{"quality": "E2_1"}}
	env.jev = newFakeJev(t, env.r, allBehave(b))
	mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev))
	env.jev.Close() // no network from here
	d := readLedgerT(t, env.out)
	c := classOf(t, d, ArmJev, fx.bugfix.BundleID)
	if c.LevelRule != "median" || c.State != StateOK || c.Interpretation.Levels["quality.bugfix"] != 1 {
		t.Fatalf("%+v", c)
	}
	if got := c.Interpretation.BimodalKeys; len(got) != 1 || got[0] != "quality.bugfix" || !contains(c.Warnings, "bimodal:quality.bugfix") {
		t.Fatalf("bimodal flag not recorded: %v %v", got, c.Warnings)
	}
	if c.CompleteStrict != true {
		t.Fatal("a bimodal answer is a recorded flag, not a loss of complete_strict")
	}
	bundle := mustBundle(t, fx.bugfix)
	for label, want := range map[string]struct {
		level int
		state string
	}{
		"median":                  {1, StateOK},
		"conditional-median:0.5":  {3, StateOK},
		"conditional-median:0.67": {0, StateZeroSupport},
	} {
		rule, _ := env.r.ParseLevelRule(label)
		rep, err := ReplayClassification(context.Background(), env.r, env.r.Weights, rule, c, d, bundle)
		if err != nil {
			t.Fatal(err)
		}
		if rep.State != want.state || (rep.Interp != nil && rep.Interp.Levels["quality.bugfix"] != want.level) || rep.Interp.Rule != label {
			t.Fatalf("%s: state=%s level=%d rule=%s", label, rep.State, rep.Interp.Levels["quality.bugfix"], rep.Interp.Rule)
		}
	}
}

// The scorer takes the rule as input. A rule other than the one of the run can
// move a state: that is not a replay mismatch.
func TestScorerAppliesAnotherLevelRule(t *testing.T) {
	s := newScenario(t, func(env *testEnv, fx testFixtures, behave map[string]map[string]behaviour) {
		blk := mustBundle(t, fx.bugfix).SourceBlock
		for _, arm := range []string{ArmJev, ArmDecisions} {
			be := behave[arm][blk]
			be.probs = map[string][]float64{SupportQuestionID("quality.testing"): {0.45, 0.05, 0.05, 0.45}}
			behave[arm][blk] = be
		}
	})
	cfg := s.cfg()
	m, err := Score(context.Background(), cfg)
	if err != nil {
		t.Fatalf("median: %v %v", err, m.Failures)
	}
	cfg.LevelRule = "conditional-median:0.67"
	m2, err := Score(context.Background(), cfg)
	if err != nil {
		t.Fatalf("conditional: %v %v", err, m2.Failures)
	}
	if m2.LevelRule != "conditional-median:0.67" {
		t.Fatalf("rule = %s", m2.LevelRule)
	}
	// testing: median level 1, conditional(0.67) level 0 -> the claimed support set shrinks
	a := m.Arms[ArmJev].Pipelines[PipelineOnly]["all/real/all"]
	b := m2.Arms[ArmJev].Pipelines[PipelineOnly]["all/real/all"]
	if *b.Q1.V >= *a.Q1.V || a.BimodalFixtures != 1 {
		t.Fatalf("Q1 median %v conditional %v, bimodal fixtures %d", *a.Q1.V, *b.Q1.V, a.BimodalFixtures)
	}
	// B1: without the flagged fixture the metric changes
	if a.Q1WithoutFlagged.V == nil || near(*a.Q1WithoutFlagged.V, *a.Q1.V) {
		t.Fatalf("B1 %+v vs %v", a.Q1WithoutFlagged, a.Q1)
	}
}

// ---- degraded answers ----

func TestMalformedMapsAreDegradedNeverCoerced(t *testing.T) {
	fx := makeFixtures(t)
	bug := SupportQuestionID("quality.bugfix")
	shapes := []struct{ shape, want string }{
		{"missing_level", "missing_level"}, {"extra_level", "extra_level"}, {"not_finite", "not_finite"}, {"out_of_range", "out_of_range"},
	}
	for _, arm := range []string{ArmJev, ArmDecisions} {
		for _, sh := range shapes {
			t.Run(arm+"/"+sh.shape, func(t *testing.T) {
				env := newEnv(t)
				be := bugfixBehaviour()
				be.shape = map[string]string{bug: sh.shape}
				if arm == ArmJev {
					env.jev = newFakeJev(t, env.r, allBehave(be))
				} else {
					env.dec = newFakeDecisions(t, env.r, allBehave(be))
				}
				mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, arm))
				c := classOf(t, readLedgerT(t, env.out), arm, fx.bugfix.BundleID)
				// score 2.6 -> level 3 by the declared thresholds (0.5, 1.5, 2.5)
				if c.State != StateOK || c.Interpretation.Levels["quality.bugfix"] != 3 || len(c.Interpretation.DegradedKeys) != 1 {
					t.Fatalf("state=%s levels=%v degraded=%v codes=%v", c.State, c.Interpretation.Levels, c.Interpretation.DegradedKeys, c.ErrorCodes)
				}
				if !contains(c.Warnings, "answer_degraded:quality.bugfix:"+sh.want) {
					t.Fatalf("warnings = %v", c.Warnings)
				}
				if c.CompleteStrict {
					t.Fatal("a degraded classification is not complete_strict")
				}
				if got := c.Interpretation.AnswerValidity[bug]; got != "degraded:"+sh.want {
					t.Fatalf("answer_validity = %q", got)
				}
				if c.Interpretation.AnswerValidity[SupportQuestionID("risk.security")] != "valid" {
					t.Fatal("the other answers are valid")
				}
			})
		}
	}
	t.Run("a sum outside [0.99, 1.01]", func(t *testing.T) {
		env := newEnv(t)
		be := bugfixBehaviour()
		be.badProbs = map[string]bool{bug: true} // sums to 0.5; score is kept
		env.jev = newFakeJev(t, env.r, allBehave(be))
		mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev))
		c := classOf(t, readLedgerT(t, env.out), ArmJev, fx.bugfix.BundleID)
		if c.State != StateOK || !contains(c.Warnings, "answer_degraded:quality.bugfix:sum:0.5") {
			t.Fatalf("%+v", c)
		}
	})
	t.Run("a bad map with a score out of range has no usable score", func(t *testing.T) {
		exp := ExpectedQuestion{ID: "q", Kind: "score", Levels: 4, Thresholds: []float64{0.5, 1.5, 2.5}}
		for _, score := range []*float64{nil, fp(9), fp(-1)} {
			if qa := scoreAnswerFrom([]*float64{fp(0.2), nil, nil, nil}, false, false, score, nil, exp); qa.Status != QAInvalid {
				t.Fatalf("score %v: %+v", score, qa)
			}
		}
		if qa := scoreAnswerFrom([]*float64{fp(0.2), nil, nil, nil}, false, false, fp(1.6), nil, exp); qa.Status != QAOK || *qa.Score.DegradedLevel != 2 {
			t.Fatalf("%+v", qa)
		}
	})
	t.Run("an answer with no probability map at all is degraded", func(t *testing.T) {
		env := newEnv(t)
		be := bugfixBehaviour()
		be.bareProbs = map[string]bool{bug: true}
		env.dec = newFakeDecisions(t, env.r, allBehave(be))
		mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmDecisions))
		c := classOf(t, readLedgerT(t, env.out), ArmDecisions, fx.bugfix.BundleID)
		if c.State != StateOK || !contains(c.Warnings, "answer_degraded:quality.bugfix:missing_level") {
			t.Fatalf("%+v", c)
		}
	})
	t.Run("a malformed evidence map keeps the choice and is recorded", func(t *testing.T) {
		env := newEnv(t)
		be := bugfixBehaviour()
		be.badProbs = map[string]bool{"evidence__quality": true}
		env.jev = newFakeJev(t, env.r, allBehave(be))
		mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmJev))
		c := classOf(t, readLedgerT(t, env.out), ArmJev, fx.bugfix.BundleID)
		if c.State != StateOK || c.CompleteStrict || len(c.Quotes) != 1 || !strings.Contains(strings.Join(c.Warnings, " "), "answer_degraded:evidence__quality:") {
			t.Fatalf("%+v", c)
		}
	})
}

// Per-answer validity is a smoke stop: under 0.997 the arm stops, for the smoke
// set only.
func TestSmokeStopOnPerAnswerValidity(t *testing.T) {
	fx := makeFixtures(t)
	bug := SupportQuestionID("quality.bugfix")
	run := func(set string) (RunSummary, *fakeProvider) {
		env := newEnv(t)
		be := bugfixBehaviour()
		be.shape = map[string]string{bug: "extra_level"}
		env.jev = newFakeJev(t, env.r, allBehave(be))
		cfg := newCfg(t, env, fx.gated(), ArmJev)
		cfg.Set = set
		return mustRun(t, cfg), env.jev
	}
	s, jev := run("smoke")
	a := s.Arms[ArmJev]
	if a.Stopped != "answer_validity_below_0.997" || jev.count() != 1 || a.NotRun != 3 {
		t.Fatalf("smoke: stopped=%q calls=%d %+v", a.Stopped, jev.count(), a)
	}
	if a.AnswerValidity == nil || *a.AnswerValidity >= 0.997 {
		t.Fatalf("validity = %v", a.AnswerValidity)
	}
	s, jev = run("development")
	if s.Arms[ArmJev].Stopped != "" || jev.count() != 4 {
		t.Fatalf("development must not stop: %q calls=%d", s.Arms[ArmJev].Stopped, jev.count())
	}
	if v := s.Arms[ArmJev].AnswerValidity; v == nil || *v >= 1 {
		t.Fatalf("validity must still be reported: %v", v)
	}
}

// ---- tokens on terminal states ----

// Every terminal state that had a provider response carries its tokens in the
// outcome and its cost in the ledger. GUARD: dropping the usage of the terminal
// (the CategorizeTextBundle default) fails here.
func TestTerminalStatesCarryTokensAndCost(t *testing.T) {
	fx := makeFixtures(t)
	q := SupportQuestionID("risk.security")
	cases := map[string]behaviour{
		StateQuestionRefused:    {refuse: map[string]bool{q: true}},
		StateAnswerMissing:      {drop: map[string]bool{q: true}},
		StateAnswerInvalid:      {badNoScore: map[string]bool{q: true}},
		StateEvidenceNone:       {evidence: map[string]string{"quality": "none"}},
		StateEvidenceUnanswered: {refuse: map[string]bool{"evidence__quality": true}},
		StateZeroSupport:        {},
	}
	for state, extra := range cases {
		t.Run(state, func(t *testing.T) {
			env := newEnv(t)
			be := bugfixBehaviour()
			if state == StateZeroSupport {
				be.levels = nil
			}
			be.refuse, be.drop, be.badNoScore = extra.refuse, extra.drop, extra.badNoScore
			if extra.evidence != nil {
				be.evidence = extra.evidence
			}
			env.dec = newFakeDecisions(t, env.r, allBehave(be))
			mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, ArmDecisions))
			d := readLedgerT(t, env.out)
			c := classOf(t, d, ArmDecisions, fx.bugfix.BundleID)
			if c.State != state {
				t.Fatalf("state = %s, want %s (%v)", c.State, state, c.ErrorCodes)
			}
			if c.InputTokens != 7000 || c.BilledCostUSD <= 0 || c.ModelReturned != DefaultLunaModel {
				t.Fatalf("tokens=%d cost=%v model=%q", c.InputTokens, c.BilledCostUSD, c.ModelReturned)
			}
			// and the scorer re-derives the same tokens from the raw response
			bundle := mustBundle(t, fx.bugfix)
			rep, err := ReplayClassification(context.Background(), env.r, env.r.Weights, env.r.SelectedRule, c, d, bundle)
			if err != nil || rep.Outcome.InputTokens != 7000 || rep.Outcome.LLMModel != DefaultLunaModel {
				t.Fatalf("replayed outcome tokens=%d model=%q err=%v", rep.Outcome.InputTokens, rep.Outcome.LLMModel, err)
			}
		})
	}
}

// ---- model id ----

func TestModelPrefixRule(t *testing.T) {
	cases := []struct {
		req, got string
		ok       bool
	}{
		{"gpt-5-nano", "gpt-5-nano", true},
		{"gpt-5-nano", "gpt-5-nano-2025-08-07", true},
		{"jev-1.13.0", "jev-1.13.0", true},
		{"gpt-6-luna", "gpt-6-lunatic", false},
		{"gpt-6-luna", "gpt-6", false},
		{"jev-1.13.0", "jev-latest", false},
		{"gpt-6-luna", "", false},
	}
	for _, c := range cases {
		if got := ModelAccepted(c.req, c.got); got != c.ok {
			t.Errorf("ModelAccepted(%q, %q) = %v", c.req, c.got, got)
		}
	}
}

func TestChangeOfReturnedModelIdInOneRunIsAWarning(t *testing.T) {
	env := newEnv(t)
	fx := makeFixtures(t)
	one, two := bugfixBehaviour(), bugfixBehaviour()
	one.model, two.model = "gpt-6-luna-2026-09-01", "gpt-6-luna-2026-10-01"
	env.dec = newFakeDecisions(t, env.r, byBlock(t, map[string]behaviour{
		mustBundle(t, fx.bugfix).SourceBlock:   one,
		mustBundle(t, fx.refactor).SourceBlock: two,
	}))
	s := mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix, fx.refactor}, ArmDecisions))
	d := readLedgerT(t, env.out)
	if c := classOf(t, d, ArmDecisions, fx.refactor.BundleID); !contains(c.Warnings, "model_id_changed") {
		t.Fatalf("warnings = %v", c.Warnings)
	}
	if c := classOf(t, d, ArmDecisions, fx.bugfix.BundleID); contains(c.Warnings, "model_id_changed") {
		t.Fatal("the first id is the reference")
	}
	if s.Arms[ArmDecisions].ModelIDChanges != 1 {
		t.Fatalf("%+v", s.Arms[ArmDecisions])
	}
}

// ---- v1c end to end ----

func TestCompactRubricEndToEnd(t *testing.T) {
	for _, arm := range []string{ArmJev, ArmDecisions} {
		t.Run(arm, func(t *testing.T) {
			env := newEnv(t)
			env.r = testRubricCompact(t)
			fx := makeFixtures(t)
			be := bugfixBehaviour()
			be.evidence = map[string]string{"evidence": "E2_1"}
			if arm == ArmJev {
				env.jev = newFakeJev(t, env.r, allBehave(be))
			} else {
				env.dec = newFakeDecisions(t, env.r, allBehave(be))
			}
			mustRun(t, newCfg(t, env, []FixtureRecord{fx.bugfix}, arm))
			d := readLedgerT(t, env.out)
			c := classOf(t, d, arm, fx.bugfix.BundleID)
			if c.State != StateOK || !c.CompleteStrict || len(c.Quotes) != 1 || c.Rubric != "decision-support-v1c" {
				t.Fatalf("%+v", c)
			}
			a := d.AttemptsOf(c)[0]
			if a.QuestionCount != 17 || a.RubricSHA256 != env.r.SHA256 {
				t.Fatalf("%+v", a)
			}
			if c.Interpretation.AnswerValidity["evidence"] != "valid" || c.Interpretation.EvidenceChoices["evidence"] != "E2_1" {
				t.Fatalf("%+v", c.Interpretation)
			}
		})
	}
}

// A rubric without the sufficiency question runs: no question, no warning.
func TestRubricWithoutSufficiencyRuns(t *testing.T) {
	r := withEdit(t, func(m map[string]any) { m["sufficiency_question"] = nil })
	if r.HasSufficiency() {
		t.Fatal("sufficiency must be optional")
	}
	b := mustBundle(t, makeFixtures(t).bugfix)
	built, err := BuildJevRequest(r, DefaultJevModel, b)
	if err != nil {
		t.Fatal(err)
	}
	if built.QuestionCount() != 20 || strings.Contains(string(built.Body), `"sufficiency"`) {
		t.Fatalf("questions = %d", built.QuestionCount())
	}
	in := Interpret(r, r.Weights, r.SelectedRule, ProviderTypeSafe, b, built.Spans, jevTyped(t, r, built, bugfixBehaviour()))
	if in.State != StateOK || !in.CompleteStrict || contains(in.Warnings, "sufficiency_unanswered") {
		t.Fatalf("%s %v", in.State, in.Warnings)
	}
}

func TestRubricFormat2IsRequired(t *testing.T) {
	var m map[string]any
	_ = json.Unmarshal(defaultRubricJSON, &m)
	m["rubric_format"] = 1
	data, _ := json.Marshal(m)
	if _, err := ParseRubric(data); err == nil {
		t.Fatal("a rubric file of format 1 (design v1.0) must be refused")
	}
	for _, label := range []string{"median"} {
		r := testRubric(t)
		if r.SelectedRule.Label() != label {
			t.Fatalf("default rule = %s", r.SelectedRule.Label())
		}
	}
}

// Both rubric files load, with their own question counts and evidence modes.
func TestBothRubricFilesLoad(t *testing.T) {
	v1, v1c := testRubric(t), testRubricCompact(t)
	if v1.Evidence.Mode != EvidencePerTheme || len(v1.EvidenceQuestions) != 5 || v1.SharedPreamble != nil {
		t.Fatal("v1")
	}
	if v1c.Evidence.Mode != EvidenceSingle || len(v1c.EvidenceQuestions) != 1 || v1c.SharedPreamble == nil || v1c.RubricVersion != "decision-support-v1c" {
		t.Fatal("v1c")
	}
	for _, r := range []*Rubric{v1, v1c} {
		if len(r.IncumbentDefs.Lines) != 15 || r.IncumbentDefs.InsertBefore == "" || r.AdapterVersion != "decision-adapter-v2" {
			t.Fatalf("%s: incumbent_defs / adapter version", r.RubricVersion)
		}
	}
}

var _ = secrets.Hidden{}
var _ = categorize.StatusOK
