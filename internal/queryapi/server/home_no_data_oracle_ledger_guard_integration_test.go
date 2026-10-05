//go:build integration

package server

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The Go response was observed in the failed b6 venue-oracles job.  It is a
// capture, not a frozen Python golden: D4834 keeps dict-order.json unchanged.
const (
	chaos8169HomeNoDataCapturedGoBuild   = "b6eb5af44fa393d748298f48786ce30160c40daa"
	chaos8169HomeNoDataCapturedGoRun     = "37332155037"
	chaos8169HomeNoDataCapturedGoJob     = "111837687259"
	chaos8169HomeNoDataCapturedGoSHA256  = "a09b10b437af2280224d96494819572bd9a3ad38687b209ae00e5a6a2abc88f8"
	chaos8169HomeNoDataNegativeChildEnv  = "DHO_CHAOS8169_LEDGER_NEGATIVE_CHILD"
	chaos8169GraphQLEdgeNegativeChildEnv = "DHO_CHAOS8169_EDGE_LEDGER_NEGATIVE_CHILD"
	chaos8169HomeNoDataFrozenProducerSHA = "6e1dc97e68624398015f60caee160cceee6ae601bd434d01b8b7d5975267e137"
)

type chaos8169DictOrderGolden struct {
	Header struct {
		PythonBuild    string `json:"python_build"`
		ProducerDigest string `json:"producer_digest"`
	} `json:"header"`
	Requests []struct {
		Name string `json:"name"`
		Body string `json:"body"`
	} `json:"requests"`
}

type chaos8169DictOrderAnswer struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
}

type chaos8169GraphQLEdgeCapture struct {
	Source struct {
		WorkflowRun string `json:"workflow_run"`
		Job         string `json:"job"`
		Head        string `json:"head"`
		PythonBuild string `json:"python_build"`
	} `json:"source"`
	Responses []struct {
		Case   string `json:"case"`
		Python string `json:"python"`
		Go     string `json:"go"`
	} `json:"responses"`
}

// chaos8169HomeNoDataCapturedBodies obtains the old plane from the unchanged
// a484 golden and the observed Go body from the exact b6 hosted failure. The
// live handler/database comparison remains a hosted venue-oracle obligation.
func chaos8169HomeNoDataCapturedBodies(t *testing.T) (string, string) {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatalf("find module root: %v", err)
	}

	goldenRaw, err := os.ReadFile(filepath.Join(root, "internal/queryapi/server/testdata/venue/dict-order.json"))
	if err != nil {
		t.Fatalf("read frozen dict-order golden: %v", err)
	}
	var golden chaos8169DictOrderGolden
	if err := json.Unmarshal(goldenRaw, &golden); err != nil {
		t.Fatalf("decode frozen dict-order golden: %v", err)
	}
	if golden.Header.PythonBuild != venuePythonBuild {
		t.Fatalf("frozen Home producer = %q, want %q", golden.Header.PythonBuild, venuePythonBuild)
	}
	if golden.Header.ProducerDigest != chaos8169HomeNoDataFrozenProducerSHA {
		t.Fatalf("frozen Home producer digest = %q, want %q", golden.Header.ProducerDigest, chaos8169HomeNoDataFrozenProducerSHA)
	}
	if len(golden.Requests) != 1 || golden.Requests[0].Name != "dict order services" {
		t.Fatalf("frozen dict-order requests = %d/%q, want one dict order services request", len(golden.Requests), golden.Requests[0].Name)
	}
	result, ok := strings.CutPrefix(golden.Requests[0].Body, "RESULT ")
	if !ok {
		t.Fatal("frozen dict-order producer result has no RESULT prefix")
	}
	var answers []chaos8169DictOrderAnswer
	if err := json.Unmarshal([]byte(result), &answers); err != nil {
		t.Fatalf("decode frozen dict-order producer result: %v", err)
	}
	// dictOrderCase declares the Home request second and the producer result
	// must stay one-for-one with that list.
	if len(answers) != 8 || answers[1].Status != 200 {
		t.Fatalf("frozen Home answer = result[%d] status %d of %d, want result[1] status 200 of 8", 1, answers[1].Status, len(answers))
	}

	goRaw, err := os.ReadFile(filepath.Join(root, "internal/queryapi/server/testdata/chaos-8169/home-no-data-go-b6.json"))
	if err != nil {
		t.Fatalf("read observed Go Home capture from run %s job %s at %s: %v", chaos8169HomeNoDataCapturedGoRun, chaos8169HomeNoDataCapturedGoJob, chaos8169HomeNoDataCapturedGoBuild, err)
	}
	if got := sha256.Sum256(goRaw); fmtSHA256(got) != chaos8169HomeNoDataCapturedGoSHA256 {
		t.Fatalf("observed Go Home capture digest = %s, want %s", fmtSHA256(got), chaos8169HomeNoDataCapturedGoSHA256)
	}
	if !json.Valid([]byte(answers[1].Body)) || !json.Valid(goRaw) {
		t.Fatal("recorded Home body is not valid JSON")
	}
	return answers[1].Body, string(goRaw)
}

func fmtSHA256(sum [sha256.Size]byte) string {
	const hex = "0123456789abcdef"
	bytes := make([]byte, 0, sha256.Size*2)
	for _, value := range sum {
		bytes = append(bytes, hex[value>>4], hex[value&0x0f])
	}
	return string(bytes)
}

func TestCHAOS8169HomeNoDataLedgerRecordedPair(t *testing.T) {
	pythonBody, goBody := chaos8169HomeNoDataCapturedBodies(t)
	assertCHAOS8169HomeNoDataLedger(t, chaos8169DictOrderHomeLedgerKey, pythonBody, goBody)
}

func TestCHAOS8169HomeNoDataLedgerNegativeGuards(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{name: "outside_leaf", want: "CHAOS-8169 /tiles changed outside the ledger"},
		{name: "ledger_leaf_disappears", want: "CHAOS-8169 ledger Go /summary ="},
		{name: "frozen_value_drifts", want: "CHAOS-8169 ledger Python /summary ="},
		{name: "go_value_drifts", want: "CHAOS-8169 ledger Go /health_state ="},
		{name: "ledger_path_missing", want: "CHAOS-8169 ledger path /limiting_factor is absent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestCHAOS8169HomeNoDataLedgerNegativeChild$")
			command.Env = append(os.Environ(), chaos8169HomeNoDataNegativeChildEnv+"="+tc.name)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("negative %s passed; expected its ledger guard to fail\n%s", tc.name, output)
			}
			if !strings.Contains(string(output), tc.want) {
				t.Fatalf("negative %s failed without the expected guard %q:\n%s", tc.name, tc.want, output)
			}
			t.Logf("negative %s rejected by %q", tc.name, tc.want)
		})
	}
}

func TestCHAOS8169HomeNoDataLedgerNegativeChild(t *testing.T) {
	negative := os.Getenv(chaos8169HomeNoDataNegativeChildEnv)
	if negative == "" {
		t.Skip("runs only as an expected-failure child of TestCHAOS8169HomeNoDataLedgerNegativeGuards")
	}
	pythonBody, goBody := chaos8169HomeNoDataCapturedBodies(t)
	ledger := chaos8169HomeLedgerFor(t, chaos8169DictOrderHomeLedgerKey)
	switch negative {
	case "outside_leaf":
		goBody = chaos8169ReplaceOnce(t, goBody, `"title":"Understand"`, `"title":"Changed"`)
	case "ledger_leaf_disappears":
		goBody = chaos8169ReplaceOnce(t, goBody, `"summary":[]`, `"summary":`+ledger.Entries[0].Python)
	case "frozen_value_drifts":
		pythonBody = chaos8169ReplaceOnce(t, pythonBody, `Cycle Time held steady 0%.`, `Cycle Time no longer holds.`)
	case "go_value_drifts":
		goBody = chaos8169ReplaceOnce(t, goBody, `"status":"no_data"`, `"status":"watch"`)
	case "ledger_path_missing":
		needle := `"limiting_factor":` + ledger.Entries[4].Go + `,`
		goBody = chaos8169ReplaceOnce(t, goBody, needle, "")
	default:
		t.Fatalf("unknown negative ledger case %q", negative)
	}
	assertCHAOS8169HomeNoDataLedger(t, chaos8169DictOrderHomeLedgerKey, pythonBody, goBody)
}

// chaos8169GraphQLEdgeHomeCapturedBodies reads the exact POST or GET pair
// captured from #3820 head 073's failed hosted venue job. This is a response
// model rendering capture; the real handler/database oracle stays hosted.
func chaos8169GraphQLEdgeHomeCapturedBodies(t *testing.T, name string) (string, string) {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatalf("find module root: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal/queryapi/server/testdata/chaos-8169/home-no-data-edge-073cc.json"))
	if err != nil {
		t.Fatalf("read hosted GraphQL Home capture: %v", err)
	}
	if got := sha256.Sum256(raw); fmtSHA256(got) != chaos8169GraphQLEdgeCaptureSHA256 {
		t.Fatalf("hosted GraphQL Home capture digest = %s, want %s", fmtSHA256(got), chaos8169GraphQLEdgeCaptureSHA256)
	}
	var captured chaos8169GraphQLEdgeCapture
	if err := json.Unmarshal(raw, &captured); err != nil {
		t.Fatalf("decode hosted GraphQL Home capture: %v", err)
	}
	if captured.Source.WorkflowRun != "37343463320" || captured.Source.Job != "111876064807" ||
		captured.Source.Head != "073cc339d4cc8a63d0a458c13a84e08a033f9e0c" || captured.Source.PythonBuild != venuePythonBuild {
		t.Fatalf("hosted GraphQL Home capture source = %+v, want job 111876064807 at 073cc from %s", captured.Source, venuePythonBuild)
	}
	for _, response := range captured.Responses {
		if response.Case != name {
			continue
		}
		if !json.Valid([]byte(response.Python)) || !json.Valid([]byte(response.Go)) {
			t.Fatalf("hosted GraphQL %s capture is not valid JSON", name)
		}
		return chaos8169GraphQLHomeBody(t, response.Python), chaos8169GraphQLHomeBody(t, response.Go)
	}
	t.Fatalf("hosted GraphQL Home capture has no %s pair", name)
	return "", ""
}

func TestCHAOS8169GraphQLEdgeHomeLedgerGenerated(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatalf("find module root: %v", err)
	}
	command := exec.Command("go", "run", "-trimpath", "./internal/queryapi/server/testdata/chaos-8169/home_no_data_ledger_generate.go", "-root", ".", "-check")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated Home ledger is stale: %v\n%s", err, output)
	}
}

func TestCHAOS8169GraphQLEdgeHomeLedgerRecordedPairs(t *testing.T) {
	for _, name := range []string{"POST home", "GET home"} {
		t.Run(name, func(t *testing.T) {
			pythonBody, goBody := chaos8169GraphQLEdgeHomeCapturedBodies(t, name)
			key, ok := chaos8169GraphQLEdgeHomeLedgerKey(name)
			if !ok {
				t.Fatalf("no GraphQL Home ledger key for %s", name)
			}
			assertCHAOS8169HomeNoDataLedger(t, key, pythonBody, goBody)
		})
	}
}

func TestCHAOS8169GraphQLEdgeHomeLedgerNegativeGuards(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{name: "outside_leaf", want: "CHAOS-8169 /tiles changed outside the ledger"},
		{name: "ledger_leaf_disappears", want: "CHAOS-8169 ledger Go /summary ="},
		{name: "frozen_value_drifts", want: "CHAOS-8169 ledger Python /summary ="},
		{name: "go_value_drifts", want: "CHAOS-8169 ledger Go /healthState ="},
		{name: "ledger_path_missing", want: "CHAOS-8169 ledger path /limitingFactor is absent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestCHAOS8169GraphQLEdgeHomeLedgerNegativeChild$")
			command.Env = append(os.Environ(), chaos8169GraphQLEdgeNegativeChildEnv+"="+tc.name)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("negative %s passed; expected its ledger guard to fail\n%s", tc.name, output)
			}
			if !strings.Contains(string(output), tc.want) {
				t.Fatalf("negative %s failed without the expected guard %q:\n%s", tc.name, tc.want, output)
			}
			t.Logf("negative %s rejected by %q", tc.name, tc.want)
		})
	}
}

func TestCHAOS8169GraphQLEdgeHomeLedgerNegativeChild(t *testing.T) {
	negative := os.Getenv(chaos8169GraphQLEdgeNegativeChildEnv)
	if negative == "" {
		t.Skip("runs only as an expected-failure child of TestCHAOS8169GraphQLEdgeHomeLedgerNegativeGuards")
	}
	pythonBody, goBody := chaos8169GraphQLEdgeHomeCapturedBodies(t, "POST home")
	key, ok := chaos8169GraphQLEdgeHomeLedgerKey("POST home")
	if !ok {
		t.Fatal("no GraphQL Home ledger key for POST home")
	}
	ledger := chaos8169HomeLedgerFor(t, key)
	switch negative {
	case "outside_leaf":
		goBody = chaos8169ReplaceOnce(t, goBody, `"title":"Understand"`, `"title":"Changed"`)
	case "ledger_leaf_disappears":
		goBody = chaos8169ReplaceOnce(t, goBody, `"summary":[]`, `"summary":`+ledger.Entries[0].Python)
	case "frozen_value_drifts":
		pythonBody = chaos8169ReplaceOnce(t, pythonBody, `"text":"Cycle Time held steady 0%."`, `"text":"Cycle Time no longer holds."`)
	case "go_value_drifts":
		goBody = chaos8169ReplaceOnce(t, goBody, `"status":"no_data"`, `"status":"watch"`)
	case "ledger_path_missing":
		needle := `"limitingFactor":` + ledger.Entries[4].Go + `,`
		goBody = chaos8169ReplaceOnce(t, goBody, needle, "")
	default:
		t.Fatalf("unknown negative ledger case %q", negative)
	}
	assertCHAOS8169HomeNoDataLedger(t, key, pythonBody, goBody)
}

func chaos8169ReplaceOnce(t *testing.T, body, old, replacement string) string {
	t.Helper()
	if count := strings.Count(body, old); count != 1 {
		t.Fatalf("negative fixture occurrence count for %q = %d, want 1", old, count)
	}
	return strings.Replace(body, old, replacement, 1)
}
