package decisioneval

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	testJevToken = "test-jev-token-NOT-A-SECRET-7f3a"
	testOAIKey   = "test-openai-key-NOT-A-SECRET-91bc"
)

func testRubric(t testing.TB) *Rubric {
	t.Helper()
	r, err := LoadRubric("")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func testRubricCompact(t testing.TB) *Rubric {
	t.Helper()
	r, err := LoadRubric("compact")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// buildBundle builds a bundle with the REAL producer (BuildTextBundle).
func buildBundle(t testing.TB, id string, issues []map[string]any, prs []map[string]any) units.TextBundle {
	t.Helper()
	in := units.BuildTextBundleInput{WorkItemMap: map[string]map[string]any{}, PRMap: map[string]map[string]any{}, CommitMap: map[string]map[string]any{}, WorkUnitID: id}
	for i, it := range issues {
		key := fmt.Sprintf("linear:%s-%d", id, i+1)
		in.IssueIDs = append(in.IssueIDs, key)
		in.WorkItemMap[key] = it
	}
	for i, pr := range prs {
		key := fmt.Sprintf("repo-%s#pr%d", id, i+1)
		in.PRIDs = append(in.PRIDs, key)
		in.PRMap[key] = pr
	}
	b, err := units.BuildTextBundle(in)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fixtureOf renders a bundle as an export line (the shape of local-bundles.jsonl).
func fixtureOf(t testing.TB, id string, b units.TextBundle, set, stratum string) FixtureRecord {
	t.Helper()
	blocks, err := ParseSourceBlock(b.SourceBlock)
	if err != nil {
		t.Fatal(err)
	}
	var handles []map[string]any
	for _, bl := range blocks {
		ref := b.HandleMap[bl.Handle]
		handles = append(handles, map[string]any{"handle": bl.Handle, "source_type": ref.SourceType, "source_id": ref.SourceID, "text_chars": len([]rune(bl.Text))})
	}
	raw, _ := json.Marshal(handles)
	return FixtureRecord{FixtureID: id, BundleID: "bnd_" + id, WorkUnitID: "wu-" + id, InputHash: b.InputHash, SourceBlock: b.SourceBlock,
		TextCharCount: b.TextCharCount, TextSourceCount: b.TextSourceCount, Handles: raw, Set: set, Stratum: stratum}
}

type testFixtures struct {
	bugfix, refactor, vuln, zero, short FixtureRecord
}

func (f testFixtures) gated() []FixtureRecord {
	return []FixtureRecord{f.bugfix, f.refactor, f.vuln, f.zero}
}

func makeFixtures(t testing.TB) testFixtures {
	t.Helper()
	var tf testFixtures
	tf.bugfix = fixtureOf(t, "a1", buildBundle(t, "a1",
		[]map[string]any{{"title": "Export job times out for large workspaces", "description": "Customers with more than 50k rows get a 504 from the CSV export. Acme Corp raised this two times; they need it before their quarter close.", "type": "bug", "labels": []any{"export", "customer-escalation"}}},
		[]map[string]any{{"title": "fix: stream CSV export instead of buffering", "body": "The export buffers the whole result set in memory today. Switch to a streamed writer and add a 30s timeout for each chunk. Adds an integration test for 100k rows. closes ENG-412"}}), "development", "mixed_work")
	tf.refactor = fixtureOf(t, "a2", buildBundle(t, "a2",
		[]map[string]any{{"title": "Split the billing module into smaller packages", "description": "The billing package grew to forty files. Extract invoices, taxes and discounts into their own packages with the same public behaviour and remove the duplicated currency helpers.", "type": "task"}},
		[]map[string]any{{"title": "refactor: extract invoice and tax packages", "body": "Moves code only. No behaviour change. Renames helpers, removes duplication, tidies the interfaces between the three packages and updates imports everywhere."}}), "development", "natural")
	tf.vuln = fixtureOf(t, "a3", buildBundle(t, "a3",
		[]map[string]any{{"title": "CVE-2026-1234 in the yaml parser", "description": "The scanner reports CVE-2026-1234, a denial of service in the yaml parser we ship. Bump the dependency to the patched release and confirm the advisory is closed in the scanner.", "type": "bug"}},
		[]map[string]any{{"title": "security: bump yaml parser to 3.2.1 for CVE-2026-1234", "body": "Upgrades the yaml parser to the patched version named in the advisory. Lockfile updated, the scanner finding now shows as resolved after this change lands."}}), "heldout", "natural")
	tf.zero = fixtureOf(t, "a4", buildBundle(t, "a4",
		[]map[string]any{{"title": "Weekly team sync notes for the third week of the month", "description": "Attendees met on Tuesday to talk about holidays, the office move, the snacks budget, the next offsite venue, birthday cards and the seating plan for the new floor. No decisions were taken today.", "type": "task"},
			{"title": "Team lunch planning for the end of the quarter", "description": "Collecting food preferences for the lunch on Friday. Vote for the restaurant by Wednesday, tell the organiser about allergies and remember to bring the voucher you received from the office manager.", "type": "task"}},
		nil), "development", "zero_support")
	tf.short = fixtureOf(t, "a5", buildBundle(t, "a5", []map[string]any{{"title": "Fix typo", "description": "teh", "type": "bug"}}, nil), "development", "below_gate")
	if g := GateStatus(mustBundle(t, tf.short)); g == "" {
		t.Fatal("the short fixture must be below the gate")
	}
	for _, f := range tf.gated() {
		if g := GateStatus(mustBundle(t, f)); g != "" {
			t.Fatalf("fixture %s must pass the gate, got %s (chars %d)", f.ID(), g, f.TextCharCount)
		}
	}
	return tf
}

func mustBundle(t testing.TB, f FixtureRecord) units.TextBundle {
	t.Helper()
	b, err := f.Bundle()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// --- wire builders (REAL shapes from api-facts.md and the old captures) ---

func probsFor(level, n int) []float64 {
	p := make([]float64, n)
	rest := 0.2 / float64(n-1)
	for i := range p {
		p[i] = rest
	}
	p[level] = 0.8
	return p
}

func expScore(p []float64) float64 {
	s := 0.0
	for l, x := range p {
		s += float64(l) * x
	}
	return s
}

// behaviour tells a fake decision server how to answer.
type behaviour struct {
	levels       map[string]int       // support key -> level (default 0)
	evidence     map[string]string    // theme -> span id (default "E1_1"); "none" allowed
	refuse       map[string]bool      // question ids answered with type refusal (decisions)
	drop         map[string]bool      // question ids left out of answers
	badProbs     map[string]bool      // question ids with probabilities that sum to 0.5 (score kept: a degraded answer)
	badNoScore   map[string]bool      // malformed probabilities AND no score: an invalid answer
	shape        map[string]string    // question id -> malformed map shape: missing_level, extra_level, not_finite, out_of_range
	bareProbs    map[string]bool      // question ids with a score and NO probability map
	probs        map[string][]float64 // question id -> exact level probabilities
	wrongType    map[string]bool      // question ids answered with the wrong type
	duplicate    map[string]bool      // question ids answered two times
	unknownExtra bool                 // add an answer with an unknown id
	model        string               // returned model (default: requested)
	inputTokens  int64
	outputTokens int64
	omitUsage    bool
}

func (b behaviour) level(key string) int { return b.levels[key] }

func (b behaviour) usage() (int64, int64) {
	in, out := b.inputTokens, b.outputTokens
	if in == 0 {
		in = 7000
	}
	if out == 0 {
		out = 50
	}
	return in, out
}

// jevAnswer builds one Jev answer object in the real wire shape.
func jevScore(p []float64) map[string]any {
	legend, probs := map[string]string{}, map[string]float64{}
	for i, x := range p {
		legend[fmt.Sprint(i)] = "level " + fmt.Sprint(i)
		probs[fmt.Sprint(i)] = x
	}
	return map[string]any{"type": "score", "score": expScore(p), "legend": legend, "probabilities": probs, "confidence": 0.8}
}

func jevChoice(choice string, options []string) map[string]any {
	probs := map[string]float64{}
	for _, o := range options {
		probs[o] = 0
	}
	probs[choice] = 0.9
	left := 0.1 / float64(len(options)-1)
	for _, o := range options {
		if o != choice {
			probs[o] = left
		}
	}
	return map[string]any{"type": "choice", "choice": choice, "probabilities": probs, "confidence": 0.7}
}

func decisionsScore(name string, p []float64, labels []string) map[string]any {
	list := make([]map[string]any, len(p))
	for i, x := range p {
		list[i] = map[string]any{"value": i, "label": labels[i], "probability": x}
	}
	return map[string]any{"type": "score", "name": name, "score": expScore(p), "probabilities": list, "confidence": 0.8}
}

func decisionsChoice(name, choice string, options []string) map[string]any {
	list := make([]map[string]any, len(options))
	left := 0.1 / float64(len(options)-1)
	for i, o := range options {
		pr := left
		if o == choice {
			pr = 0.9
		}
		list[i] = map[string]any{"value": o, "probability": pr}
	}
	return map[string]any{"type": "choice", "name": name, "choice": choice, "probabilities": list, "confidence": 0.7}
}

func badProbs(p []float64) []float64 {
	out := make([]float64, len(p))
	for i := range out {
		out[i] = 0.5 / float64(len(p))
	}
	return out
}

// fakeProvider is a recording httptest server for one provider.
type fakeProvider struct {
	*httptest.Server
	mu       sync.Mutex
	requests [][]byte
	auth     []string
	script   []int // status codes consumed in order (then 200)
	headers  map[int]http.Header
	behave   func(fixtureKey string) behaviour
	delay    time.Duration
	t        testing.TB
}

func (f *fakeProvider) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *fakeProvider) bodies() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.requests...)
}

func newFake(t testing.TB, respond func(body []byte) []byte) *fakeProvider {
	f := &fakeProvider{t: t, headers: map[int]http.Header{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, body)
		f.auth = append(f.auth, r.Header.Get("Authorization"))
		status := 200
		if len(f.script) > 0 {
			status, f.script = f.script[0], f.script[1:]
		}
		hdr := f.headers[status]
		delay := f.delay
		f.mu.Unlock()
		time.Sleep(delay)
		for k, v := range hdr {
			w.Header()[k] = v
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("x-typesafe-request-id", "req_test_"+fmt.Sprint(f.count()))
		switch status {
		case 200:
			w.WriteHeader(200)
			_, _ = w.Write(respond(body))
		default:
			w.WriteHeader(status)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"error":{"message":"planted %d"}}`, status)))
		}
	}))
	t.Cleanup(f.Server.Close)
	return f
}

var spanLine = regexp.MustCompile(`(?m)^\[(E[0-9]+_[0-9]+)\] `)

// jevAnswerFor builds the Jev answer object of one question for a behaviour.
// ok is false when the answer is left out.
func jevAnswerFor(b behaviour, id, kind string, levels int, options []string) (ans map[string]any, ok bool) {
	if b.drop[id] {
		return nil, false
	}
	if kind == "score" {
		level := 2
		if strings.HasPrefix(id, "support__") {
			level = b.level(strings.Replace(strings.TrimPrefix(id, "support__"), "__", ".", 1))
		}
		p := probsFor(level, levels)
		if cp, ok := b.probs[id]; ok {
			p = cp
		}
		if b.badProbs[id] || b.badNoScore[id] {
			p = badProbs(p)
		}
		ans = jevScore(p)
		if b.wrongType[id] {
			ans["type"] = "refusal"
		}
		if b.refuse[id] {
			return map[string]any{"type": "refusal"}, true
		}
		if b.badNoScore[id] {
			delete(ans, "score")
		}
		if b.bareProbs[id] {
			delete(ans, "probabilities")
		}
		switch b.shape[id] {
		case "missing_level":
			delete(ans["probabilities"].(map[string]float64), fmt.Sprint(levels-1))
		case "extra_level":
			ans["probabilities"].(map[string]float64)["9"] = 0.0
		case "not_finite":
			ans["probabilities"] = map[string]any{"0": "high", "1": 0.1, "2": 0.1, "3": 0.1}
		case "out_of_range":
			ans["probabilities"] = map[string]any{"0": 1.5, "1": -0.25, "2": -0.25, "3": 0.0}
		}
		return ans, true
	}
	theme := strings.TrimPrefix(id, "evidence__")
	choice := "E1_1"
	if c, found := b.evidence[theme]; found {
		choice = c
	}
	ans = jevChoice(choice, options)
	if b.refuse[id] {
		ans = map[string]any{"type": "refusal"}
	}
	if b.badProbs[id] {
		ans["probabilities"] = map[string]float64{choice: 0.4}
	}
	return ans, true
}

// jevBody renders a Jev response body (duplicate keys possible).
func jevBody(b behaviour, model string, answers map[string]map[string]any) []byte {
	ids := make([]string, 0, len(answers))
	for id := range answers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var parts []string
	for _, id := range ids {
		raw, _ := json.Marshal(answers[id])
		pair := fmt.Sprintf("%q:%s", id, raw)
		parts = append(parts, pair)
		if b.duplicate[id] {
			parts = append(parts, pair) // the JSON object repeats a key
		}
	}
	if b.unknownExtra {
		raw, _ := json.Marshal(jevScore(probsFor(1, 4)))
		parts = append(parts, `"surprise":`+string(raw))
	}
	in, out := b.usage()
	usage := ""
	if !b.omitUsage {
		usage = fmt.Sprintf(`,"usage":{"input_tokens":%d,"output_tokens":%d}`, in, out)
	}
	return []byte(fmt.Sprintf(`{"model":%q,"answers":{%s}%s}`, model, strings.Join(parts, ","), usage))
}

// newFakeJev answers the TypeSafe wire.
func newFakeJev(t testing.TB, r *Rubric, behave func(blockKey string) behaviour) *fakeProvider {
	return newFake(t, func(body []byte) []byte {
		var req struct {
			Model string `json:"model"`
			State struct {
				SourceBlock string `json:"source_block"`
			} `json:"state"`
			Questions map[string]json.RawMessage `json:"questions"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("fake jev: bad request: %v", err)
		}
		b := behave(req.State.SourceBlock)
		answers := map[string]map[string]any{}
		for id, raw := range req.Questions {
			var q struct {
				Type     string          `json:"type"`
				Criteria json.RawMessage `json:"criteria"`
			}
			_ = json.Unmarshal(raw, &q)
			levels := 0
			var options []string
			if q.Type == "score" {
				var crit []string
				_ = json.Unmarshal(q.Criteria, &crit)
				levels = len(crit)
			} else {
				var crit map[string]json.RawMessage
				_ = json.Unmarshal(q.Criteria, &crit)
				for o := range crit {
					options = append(options, o)
				}
				sort.Strings(options)
			}
			if ans, ok := jevAnswerFor(b, id, q.Type, levels, options); ok {
				answers[id] = ans
			}
		}
		model := b.model
		if model == "" {
			model = req.Model
		}
		return jevBody(b, model, answers)
	})
}

// newFakeDecisions answers the OpenAI Decisions wire.
func newFakeDecisions(t testing.TB, r *Rubric, behave func(blockKey string) behaviour) *fakeProvider {
	return newFake(t, func(body []byte) []byte {
		var req struct {
			Model     string `json:"model"`
			Input     string `json:"input"`
			Questions []struct {
				Type    string                   `json:"type"`
				Name    string                   `json:"name"`
				Levels  []struct{ Label string } `json:"levels"`
				Choices []struct{ Value string } `json:"choices"`
			} `json:"questions"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("fake decisions: bad request: %v", err)
		}
		start := strings.Index(req.Input, "SOURCE_BLOCK\n") + len("SOURCE_BLOCK\n")
		end := strings.Index(req.Input, "\nEND_SOURCE_BLOCK")
		b := behave(req.Input[start:end])
		var answers []any
		for _, q := range req.Questions {
			if b.drop[q.Name] {
				continue
			}
			if b.refuse[q.Name] {
				answers = append(answers, map[string]any{"type": "refusal", "name": q.Name})
				continue
			}
			var ans map[string]any
			if q.Type == "score" {
				labels := make([]string, len(q.Levels))
				for i, l := range q.Levels {
					labels[i] = l.Label
				}
				level := 2
				if strings.HasPrefix(q.Name, "support__") {
					level = b.level(strings.Replace(strings.TrimPrefix(q.Name, "support__"), "__", ".", 1))
				}
				p := probsFor(level, len(labels))
				if cp, ok := b.probs[q.Name]; ok {
					p = cp
				}
				if b.badProbs[q.Name] || b.badNoScore[q.Name] {
					p = badProbs(p)
				}
				ans = decisionsScore(q.Name, p, labels)
				if b.wrongType[q.Name] {
					ans["type"] = "choice"
				}
				if b.badNoScore[q.Name] {
					delete(ans, "score")
				}
				if b.bareProbs[q.Name] {
					delete(ans, "probabilities")
				}
				switch b.shape[q.Name] {
				case "missing_level":
					ans["probabilities"] = ans["probabilities"].([]map[string]any)[:3]
				case "extra_level":
					ans["probabilities"] = append(ans["probabilities"].([]map[string]any), map[string]any{"value": 9, "label": "x", "probability": 0.0})
				case "not_finite":
					ans["probabilities"].([]map[string]any)[0]["probability"] = "high"
				case "out_of_range":
					ans["probabilities"].([]map[string]any)[0]["probability"] = 1.5
				}
			} else {
				options := make([]string, len(q.Choices))
				for i, c := range q.Choices {
					options[i] = c.Value
				}
				theme := strings.TrimPrefix(q.Name, "evidence__")
				choice := "E1_1"
				if c, ok := b.evidence[theme]; ok {
					choice = c
				}
				ans = decisionsChoice(q.Name, choice, options)
			}
			answers = append(answers, ans)
			if b.duplicate[q.Name] {
				answers = append(answers, ans)
			}
		}
		if b.unknownExtra {
			answers = append(answers, decisionsScore("surprise", probsFor(1, 4), []string{"a", "b", "c", "d"}))
		}
		model := b.model
		if model == "" {
			model = req.Model
		}
		in, out := b.usage()
		resp := map[string]any{"model": model, "answers": answers}
		if !b.omitUsage {
			resp["usage"] = map[string]any{"input_tokens": in, "input_tokens_details": map[string]any{"cached_tokens": 0, "cache_write_tokens": 0},
				"output_tokens": out, "output_tokens_details": map[string]any{"reasoning_tokens": 0}, "total_tokens": in + out}
		}
		data, _ := json.Marshal(resp)
		return data
	})
}

// newFakeResponses answers the OpenAI Responses API for the unchanged
// incumbent provider. payload builds the generative JSON for a prompt.
func newFakeResponses(t testing.TB, payload func(prompt string) string, returnedModel string) *fakeProvider {
	return newFake(t, func(body []byte) []byte {
		var req struct {
			Input string `json:"input"`
		}
		_ = json.Unmarshal(body, &req)
		resp := map[string]any{"id": "resp_test", "status": "completed", "model": returnedModel, "output_text": payload(req.Input),
			"usage": map[string]any{"input_tokens": 1165, "output_tokens": 802, "input_tokens_details": map[string]any{"cached_tokens": 100}}}
		data, _ := json.Marshal(resp)
		return data
	})
}

// incumbentPayload returns a valid generative payload for a bundle: weight 1.0
// on one key and one quote that is a real substring of the first handle.
func incumbentPayload(t testing.TB, f FixtureRecord, key string) string {
	t.Helper()
	b := mustBundle(t, f)
	blocks, _ := ParseSourceBlock(b.SourceBlock)
	words := strings.Fields(blocks[0].Text)
	if len(words) > 6 {
		words = words[:6]
	}
	subs := map[string]float64{}
	for _, k := range SortedKeys() {
		subs[k] = 0
	}
	subs[key] = 1
	out, _ := json.Marshal(map[string]any{"subcategories": subs,
		"evidence_quotes": []any{map[string]any{"quote": strings.Join(words, " "), "source": blocks[0].SourceType, "id": blocks[0].Handle}},
		"uncertainty":     "incumbent test payload"})
	return string(out)
}

// byBlock maps a source block to a behaviour. Unknown blocks fail the test.
func byBlock(t testing.TB, m map[string]behaviour) func(string) behaviour {
	return func(block string) behaviour {
		if b, ok := m[block]; ok {
			return b
		}
		t.Errorf("fake server: no behaviour for source block %.60q", block)
		return behaviour{}
	}
}

func allBehave(b behaviour) func(string) behaviour { return func(string) behaviour { return b } }

// testEnv wires a run configuration over fake servers.
type testEnv struct {
	t       testing.TB
	r       *Rubric
	out     string
	sleeps  []time.Duration
	jev     *fakeProvider
	dec     *fakeProvider
	oai     *fakeProvider
	oaiDefs func(prompt string) string
}

func newCfg(t testing.TB, env *testEnv, fixtures []FixtureRecord, arms ...string) RunConfig {
	t.Helper()
	cfg := RunConfig{
		OutDir: env.out, Fixtures: fixtures, Arms: arms, Concurrency: 1, Live: true, Set: "development",
		Caps:   map[string]float64{ProviderTypeSafe: 5, ProviderOpenAI: 5},
		Rubric: env.r, Timeout: 5 * time.Second, IncumbentTimeout: 5 * time.Second, RunID: "run-test-" + fmt.Sprint(time.Now().UnixNano()),
		Sleep: func(_ context.Context, d time.Duration) bool { env.sleeps = append(env.sleeps, d); return true },
	}
	rates, err := LoadRates("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Rates = rates
	if env.jev != nil {
		cfg.Jev = NewJevBackend(env.jev.URL, secrets.NewHidden(testJevToken), "")
	} else {
		cfg.Jev = NewJevBackend(JevEndpoint, secrets.NewHidden(testJevToken), "")
	}
	if env.dec != nil {
		cfg.Decisions = NewDecisionsBackend(env.dec.URL, secrets.NewHidden(testOAIKey), "")
	} else {
		cfg.Decisions = NewDecisionsBackend(DecisionsEndpoint, secrets.NewHidden(testOAIKey), "")
	}
	base := defaultOpenAIBase
	if env.oai != nil {
		base = env.oai.URL + "/v1"
	}
	cfg.Incumbent = IncumbentConfig{BaseURL: base, APIKey: secrets.NewHidden(testOAIKey), Model: "gpt-5-nano", MaxOutputTokens: 2048, Rubric: env.r}
	return cfg
}

func newEnv(t testing.TB) *testEnv {
	t.Helper()
	return &testEnv{t: t, r: testRubric(t), out: filepath.Join(t.TempDir(), "out")}
}

func readLedgerT(t testing.TB, dir string) *LedgerData {
	t.Helper()
	d, err := ReadLedger(dir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func classOf(t testing.TB, d *LedgerData, arm, bundleID string) ClassificationRecord {
	t.Helper()
	var found *ClassificationRecord
	for i := range d.Classifications {
		c := d.Classifications[i]
		if c.Arm == arm && c.BundleID == bundleID {
			found = &c
		}
	}
	if found == nil {
		t.Fatalf("no classification for %s/%s", arm, bundleID)
	}
	return *found
}

func mustRun(t testing.TB, cfg RunConfig) RunSummary {
	t.Helper()
	s, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return s
}

// assertNoSecret fails when any file under dir holds the secret.
func assertNoSecret(t testing.TB, dir string, secrets ...string) {
	t.Helper()
	n := 0
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		data, _ := os.ReadFile(p)
		n++
		for _, s := range secrets {
			if strings.Contains(string(data), s) {
				t.Errorf("secret value found in %s", p)
			}
		}
		return nil
	})
	if n == 0 {
		t.Errorf("no files under %s: the secret check did not look at anything", dir)
	}
}
