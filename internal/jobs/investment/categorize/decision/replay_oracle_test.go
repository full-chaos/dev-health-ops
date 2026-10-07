package decision

// The replay oracle: the differential check that the shipped adapter IS the
// evaluated adapter.
//
// The other implementation is the experiment package decisioneval at commit
// cf881a356 (branch experiment/decision-categorization-eval). Each case holds a
// bundle, the raw /v1/systemone response body, the digest of the request the
// experiment sent, and the outcome that the EVALUATED adapter derives from
// that response under presence-floor:0.4, written with type-tagged leaves by
// oraclecompare.TypedEncode. The test sends the stored body through a fake
// transport into Completer.Classify (so categorize.CategorizeBundleOnce and
// the shared validation run) and compares EVERY field of Classification, found
// by reflection.
//
// Two sets:
//
//   - committed (testdata/replay_cases.jsonl): a synthetic bundle, built by
//     units.BuildTextBundle from testdata/synthetic_bundle_input.json, against
//     one real scrubbed provider response and planted variants of it, one or
//     more for each reachable state and validity row; and a large synthetic
//     bundle (more span candidates than the cap, text that HTML escaping
//     would change) against the real response. It runs in a plain `go test`.
//   - real (DECISION_REPLAY_ORACLE_DIR): every stored response of the frozen
//     evaluation (1,253 full-run + 2 x 94 held-out). It holds real organization
//     text, so it lives outside this open-source repository. With the variable
//     unset the test is skipped; with it set, a missing file, a digest or count
//     that differs from the manifest, or zero cases is a FAILURE.
//
// How the expected side is produced (exporter sources, not in this repo):
// .remember/jev-cats/runs/replay-oracle-v1d/zz_export_*.go, run in a worktree
// of cf881a356.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
)

// replayOracleDirEnv names the directory of the real replay set.
const replayOracleDirEnv = "DECISION_REPLAY_ORACLE_DIR"

type replayFixture struct {
	BundleID        string `json:"bundle_id"`
	InputHash       string `json:"input_hash"`
	SourceBlock     string `json:"source_block"`
	TextCharCount   int    `json:"text_char_count"`
	TextSourceCount int    `json:"text_source_count"`
	Handles         []struct {
		Handle     string `json:"handle"`
		SourceType string `json:"source_type"`
		SourceID   string `json:"source_id"`
	} `json:"handles"`
}

// bundle rebuilds the units.TextBundle of an exported fixture. SourceTexts is
// parsed from SourceBlock: the texts in the block are the SourceTexts of the
// handles by construction of units.BuildTextBundle.
func (f replayFixture) bundle() (units.TextBundle, error) {
	blocks, err := ParseSourceBlock(f.SourceBlock)
	if err != nil {
		return units.TextBundle{}, err
	}
	if len(blocks) != len(f.Handles) {
		return units.TextBundle{}, fmt.Errorf("%d handles, %d blocks", len(f.Handles), len(blocks))
	}
	b := units.TextBundle{
		SourceBlock: f.SourceBlock, InputHash: f.InputHash, TextCharCount: f.TextCharCount, TextSourceCount: f.TextSourceCount,
		SourceTexts: map[string]map[string]string{"issue": {}, "pr": {}, "commit": {}},
		SourceOrder: map[string][]string{"issue": nil, "pr": nil, "commit": nil},
		HandleMap:   map[string]units.SourceRef{},
	}
	byHandle := map[string]int{}
	for i, h := range f.Handles {
		byHandle[h.Handle] = i
	}
	for _, blk := range blocks {
		i, ok := byHandle[blk.Handle]
		if !ok || f.Handles[i].SourceType != blk.SourceType || f.Handles[i].SourceID == "" {
			return units.TextBundle{}, fmt.Errorf("block %s does not match its handle record", blk.Handle)
		}
		id := f.Handles[i].SourceID
		b.SourceTexts[blk.SourceType][id] = blk.Text
		b.SourceOrder[blk.SourceType] = append(b.SourceOrder[blk.SourceType], id)
		b.HandleMap[blk.Handle] = units.SourceRef{SourceType: blk.SourceType, SourceID: id}
	}
	return b, nil
}

type replayCase struct {
	CaseID         string         `json:"case_id"`
	BundleInput    string         `json:"bundle_input"`
	ModelRequested string         `json:"model_requested"`
	RequestSHA256  string         `json:"request_sha256"`
	Fixture        replayFixture  `json:"fixture"`
	ResponseFile   string         `json:"response_file"`
	ResponseB64    string         `json:"response_b64"`
	Expected       map[string]any `json:"expected"`
}

func readReplayCases(path string) ([]replayCase, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []replayCase
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var c replayCase
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			return nil, fmt.Errorf("line %d: %w", len(out)+1, err)
		}
		out = append(out, c)
	}
	return out, sc.Err()
}

// bodyTransport serves one stored response body and records what was sent.
type bodyTransport struct {
	body  []byte
	err   error
	sent  [][]byte
	calls int
}

func (b *bodyTransport) PostSystemOne(_ context.Context, body []byte) ([]byte, http.Header, error) {
	b.calls++
	b.sent = append(b.sent, append([]byte(nil), body...))
	if b.err != nil {
		return nil, nil, b.err
	}
	return b.body, http.Header{}, nil
}

// replayOne runs one case through the production path and returns the
// divergences from the evaluated outcome.
func replayOne(t *testing.T, c replayCase) (state string, messages []string) {
	t.Helper()
	bundle, err := c.Fixture.bundle()
	if err != nil {
		return "", []string{fmt.Sprintf("case %q: fixture: %v", c.CaseID, err)}
	}
	body, err := base64.StdEncoding.DecodeString(c.ResponseB64)
	if err != nil {
		return "", []string{fmt.Sprintf("case %q: response: %v", c.CaseID, err)}
	}
	transport := &bodyTransport{body: body}
	completer, err := NewCompleter(transport, c.ModelRequested)
	if err != nil {
		t.Fatal(err)
	}
	got, err := completer.Classify(context.Background(), bundle)
	if err != nil {
		return "", []string{fmt.Sprintf("case %q: classify: %v", c.CaseID, err)}
	}
	// Request oracle: the production builder gives the bytes the experiment sent.
	if transport.calls != 1 {
		messages = append(messages, fmt.Sprintf("case %q: %d requests were sent, want 1", c.CaseID, transport.calls))
	} else {
		sum := sha256.Sum256(transport.sent[0])
		if hex.EncodeToString(sum[:]) != c.RequestSHA256 {
			messages = append(messages, fmt.Sprintf("case %q: request digest %s, the evaluated request has %s", c.CaseID, hex.EncodeToString(sum[:]), c.RequestSHA256))
		}
	}
	// Both sides in the one wire shape: the production row encoded and passed
	// through JSON, like the stored expected row.
	encoded, err := json.Marshal(oraclecompare.TypedEncode(t, reflect.ValueOf(got)))
	if err != nil {
		t.Fatal(err)
	}
	var row map[string]any
	if err := json.Unmarshal(encoded, &row); err != nil {
		t.Fatal(err)
	}
	messages = append(messages, oraclecompare.DiffRows(c.CaseID, c.Expected, row, nil, nil)...)
	return got.State, messages
}

// runReplaySet replays every case and fails on any divergence. It returns the
// count of cases for each state.
func runReplaySet(t *testing.T, cases []replayCase) map[string]int {
	t.Helper()
	if len(cases) == 0 {
		t.Fatal("the replay set holds zero cases: nothing was compared, which is a failure, not a pass")
	}
	// The compared field set is the production type's, by reflection: a stored
	// row that lacks a field of Classification (or holds one more) fails here
	// for every case, before any value is compared.
	want := oraclecompare.EncodedFieldNames(reflect.TypeOf(Classification{}))
	sort.Strings(want)
	states := map[string]int{}
	mismatched := 0
	for _, c := range cases {
		have := make([]string, 0, len(c.Expected))
		for k := range c.Expected {
			have = append(have, k)
		}
		sort.Strings(have)
		if !reflect.DeepEqual(have, want) {
			t.Fatalf("case %q: the stored outcome has fields %v, Classification has %v; export the set again", c.CaseID, have, want)
		}
		state, messages := replayOne(t, c)
		states[state]++
		if len(messages) > 0 {
			mismatched++
			if mismatched <= 10 {
				for _, m := range messages {
					t.Error(m)
				}
			}
		}
	}
	if mismatched > 0 {
		t.Errorf("%d of %d cases differ from the evaluated outcome", mismatched, len(cases))
	}
	t.Logf("replay oracle: %d cases, %d mismatches, states %v", len(cases), mismatched, states)
	return states
}

// committedReplayStates is what the committed set must cover. A planted
// response that stops giving its declared state fails the per-case compare; a
// state that loses its last case fails here.
var committedReplayStates = map[string]int{
	StateOK: 21, StateZeroSupport: 1, StateEvidenceNone: 1, StateEvidenceUnanswered: 3,
	StateAnswerMissing: 1, StateAnswerInvalid: 6, StateRequestFailed: 4,
}

func TestReplayOracleCommittedSet(t *testing.T) {
	cases, err := readReplayCases(filepath.Join("testdata", "replay_cases.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	states := runReplaySet(t, cases)
	if !reflect.DeepEqual(states, committedReplayStates) {
		t.Errorf("states of the committed set = %v, want %v", states, committedReplayStates)
	}

	// The inputs are what they claim to be: every response file is a case and
	// every case's body is its file; the bundle is the producer's output for
	// the synthetic input.
	files, err := filepath.Glob(filepath.Join("testdata", "responses", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	// Every response file is a case of the small bundle; the large bundle adds
	// one case.
	small, large := "synthetic_bundle_input.json", "synthetic_large_bundle_input.json"
	perInput := map[string]int{}
	for _, c := range cases {
		perInput[c.BundleInput]++
	}
	if perInput[small] != len(files) || perInput[large] != 1 || len(cases) != len(files)+1 {
		t.Errorf("%d response files; cases for each bundle input: %v", len(files), perInput)
	}
	if spans, dropped := spanCount(t, syntheticBundleFrom(t, large)); spans != 48 || dropped == 0 {
		t.Errorf("the large bundle has %d spans and %d dropped; it must go over the span cap", spans, dropped)
	}
	for _, c := range cases {
		produced := syntheticBundleFrom(t, c.BundleInput)
		onDisk, err := os.ReadFile(filepath.Join("testdata", filepath.FromSlash(c.ResponseFile)))
		if err != nil {
			t.Fatal(err)
		}
		stored, _ := base64.StdEncoding.DecodeString(c.ResponseB64)
		if !bytes.Equal(onDisk, stored) {
			t.Errorf("case %q: %s differs from the body stored in the case", c.CaseID, c.ResponseFile)
		}
		rebuilt, err := c.Fixture.bundle()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rebuilt, produced) {
			t.Errorf("case %q: the fixture bundle is not units.BuildTextBundle of testdata/%s", c.CaseID, c.BundleInput)
		}
	}
}

func spanCount(t *testing.T, bundle units.TextBundle) (kept, dropped int) {
	t.Helper()
	r, err := LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	spans, dropped, err := BuildSpans(bundle, r.Evidence.MaxSpanRunes, r.Evidence.MaxSpans)
	if err != nil {
		t.Fatal(err)
	}
	return len(spans), dropped
}

// syntheticBundle builds the committed set's small bundle with the real
// producer.
func syntheticBundle(t *testing.T) units.TextBundle {
	t.Helper()
	return syntheticBundleFrom(t, "synthetic_bundle_input.json")
}

// syntheticBundleFrom builds a bundle with units.BuildTextBundle from an input
// file of testdata.
func syntheticBundleFrom(t *testing.T, name string) units.TextBundle {
	t.Helper()
	if name == "" {
		t.Fatal("the case names no bundle input file")
	}
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var in struct {
		WorkUnitID  string                    `json:"work_unit_id"`
		IssueIDs    []string                  `json:"issue_ids"`
		PRIDs       []string                  `json:"pr_ids"`
		CommitIDs   []string                  `json:"commit_ids"`
		WorkItemMap map[string]map[string]any `json:"work_item_map"`
		PRMap       map[string]map[string]any `json:"pr_map"`
		CommitMap   map[string]map[string]any `json:"commit_map"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		t.Fatal(err)
	}
	bundle, err := units.BuildTextBundle(units.BuildTextBundleInput{
		IssueIDs: in.IssueIDs, PRIDs: in.PRIDs, CommitIDs: in.CommitIDs,
		WorkItemMap: in.WorkItemMap, PRMap: in.PRMap, CommitMap: in.CommitMap, WorkUnitID: in.WorkUnitID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

// TestReplayOracleRealSet replays the full real set. It is skipped ONLY when
// the variable is unset. With the variable set, everything that would make the
// measurement not happen is a failure.
func TestReplayOracleRealSet(t *testing.T) {
	dir := os.Getenv(replayOracleDirEnv)
	if dir == "" {
		t.Skipf("%s is not set: the real replay set (real organization text, outside this repository) was NOT replayed", replayOracleDirEnv)
	}
	var manifest struct {
		Cases        int    `json:"cases"`
		CasesSHA256  string `json:"cases_sha256"`
		RubricSHA256 string `json:"rubric_sha256"`
		LevelRule    string `json:"level_rule"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "replay_oracle_manifest.json"))
	if err != nil {
		t.Fatalf("%s is set but its manifest cannot be read: %v", replayOracleDirEnv, err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "cases.jsonl"))
	if err != nil {
		t.Fatalf("%s is set but the cases cannot be read: %v", replayOracleDirEnv, err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != manifest.CasesSHA256 {
		t.Fatalf("cases.jsonl has sha256 %s, the manifest says %s", got, manifest.CasesSHA256)
	}
	if manifest.RubricSHA256 != RubricSHA256 || manifest.LevelRule != LevelRule {
		t.Fatalf("the set was exported for rubric %s and rule %q; this package is %s and %q", manifest.RubricSHA256, manifest.LevelRule, RubricSHA256, LevelRule)
	}
	cases, err := readReplayCases(filepath.Join(dir, "cases.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Cases == 0 || len(cases) != manifest.Cases {
		t.Fatalf("%d cases were read, the manifest says %d", len(cases), manifest.Cases)
	}
	runReplaySet(t, cases)
}
