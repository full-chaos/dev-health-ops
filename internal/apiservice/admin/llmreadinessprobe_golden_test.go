package admin

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// readinessProbePythonBuild is a build whose readiness.py and
// openai_compatible.py answered the frozen scenarios.
const readinessProbePythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// readinessOracleProgram is the Python producer: readiness.py's real
// AgentReadinessService.certify over the real OpenAICompatibleAgentProvider
// (see the file's own comment for why it calls certify directly).
//
//go:embed testdata/llmreadinessoracle/certify_oracle.py
var readinessOracleProgram string

// readinessStubSource is the text of the scripted provider both planes call.
//
//go:embed llmreadinessprobe_stub_test.go
var readinessStubSource string

// readinessScenarios are the scripted provider's behaviours, selected by the
// wire "model" field. They cover "ready" and every safe_error_code the route
// can persist EXCEPT "timeout": a real client-side deadline is exercised by
// TestReadinessProbeTimeoutClassification (Go only).
var readinessScenarios = []struct {
	name  string
	model string
}{
	{"ready", "scripted-ready"},
	{"unauthorized -> provider_not_configured", "scripted-unauthorized"},
	{"rate limited -> rate_limited", "scripted-ratelimited"},
	{"bad request -> invalid_request", "scripted-badrequest"},
	{"model not found -> model_not_supported", "scripted-modelnotfound"},
	{"server error -> provider_unavailable", "scripted-servererror"},
	{"two tool calls -> provider_contract_violation", "scripted-contractviolation"},
	{"finish_reason length -> output_exhausted", "scripted-outputexhausted"},
	{"malformed final answer -> invalid_response", "scripted-invalidresponse"},
	{"extra envelope field -> invalid_response", "scripted-extrafield"},
	{"quota exhaustion (429) -> provider_not_configured, not rate_limited", "scripted-quota"},
	{"one transient 500 then success -> ready", "scripted-transient"},
	{"redirect is not followed -> provider_unavailable", "scripted-redirect"},
}

// readinessAnswer is what a plane's probe of one scenario ends in.
type readinessAnswer struct {
	Outcome       string  `json:"outcome"`
	SafeErrorCode *string `json:"safe_error_code"`
}

// TestReadinessProbeMatchesTheFrozenPythonProbe compares this port's
// readinessProber with readiness.py's real, unmodified
// AgentReadinessService.certify over OpenAICompatibleAgentProvider, scenario by
// scenario, on outcome and safe_error_code. Both planes call ONE scripted
// provider (llmreadinessprobe_stub_test.go), which also refuses a request whose
// wire shape is not the contract's.
//
// The Python answers were executed on readinessProbePythonBuild against that
// stub and are frozen in testdata/admin/llm_readiness_probe.json. The
// producer's text, each scenario's model and the stub's text are in the
// golden's key: a changed producer, scenario or stub cannot replay the answers
// of another, it must be recorded again.
//
// NOT pinned: the stub checks each request against the schema constants of
// llmreadinessprobe.go. A change of those constants changes what the stub
// accepts without changing its text, so the frozen Python answers would then be
// answers to an older contract; the Go plane's "ready" scenarios still fail if
// the Go request and the constants disagree.
func TestReadinessProbeMatchesTheFrozenPythonProbe(t *testing.T) {
	_, currentFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/admin/llm_readiness_probe.json",
		PythonBuild: readinessProbePythonBuild,
		SHA256:      "13d5a6bd6dc2de7543cf6322b7d2f8d3679ab9ba0958f8c0b8d3bd21104143bc",
		Recipe: "git worktree add --detach $DIR " + readinessProbePythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/apiservice/admin/ -test '^TestReadinessProbeMatchesTheFrozenPythonProbe$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)

	server := httptest.NewServer(http.HandlerFunc(scriptedReadinessStub))
	defer server.Close()

	stubDigest := sha256.Sum256([]byte(readinessStubSource))
	requests := make([]venueoracle.Request, len(readinessScenarios))
	for index, scenario := range readinessScenarios {
		input, err := json.Marshal(map[string]string{"model": scenario.model, "stub_sha256": hex.EncodeToString(stubDigest[:])})
		if err != nil {
			t.Fatal(err)
		}
		requests[index] = venueoracle.ProgramRequest(scenario.name, readinessOracleProgram, input, nil)
	}
	scriptPath := filepath.Join(repoRoot, "internal", "apiservice", "admin", "testdata", "llmreadinessoracle", "certify_oracle.py")
	answers := golden.Produce(t, root, requests, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		ctx := context.Background()
		version, err := producer.Command(ctx, nil, nil, pyoracle.VersionProbeArgs...)
		if err != nil {
			t.Fatal(err)
		}
		probe, probeErr := version.Output()
		pyoracle.RequireDeployed(t, version.Path, probe, probeErr)
		out := make([]venueoracle.Response, len(readinessScenarios))
		for index, scenario := range readinessScenarios {
			// The "transient" scenario is stateful (round 1 fails once, then
			// succeeds): each plane starts it from the same fresh state.
			if scenario.model == "scripted-transient" {
				atomic.StoreInt32(&transientRound1Attempts, 0)
			}
			// The stub's address is another one in every run: it is the
			// program's argument, never part of its answer.
			command, err := producer.Command(ctx, nil, nil, scriptPath, server.URL, scenario.model)
			if err != nil {
				t.Fatal(err)
			}
			output, err := command.Output()
			if err != nil {
				var stderr []byte
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					stderr = exitErr.Stderr
				}
				t.Fatalf("%s: python producer: %v", scenario.name, pyoracle.RunError(command.Path, err, stderr))
			}
			out[index] = venueoracle.Response{Status: 0, Body: string(output)}
		}
		return out
	})
	golden.Consumed(t, answers...)
	if len(answers) != len(readinessScenarios) {
		t.Fatalf("python answered %d of %d scenarios", len(answers), len(readinessScenarios))
	}

	outcomes := map[string]int{}
	for index, scenario := range readinessScenarios {
		python, err := decodeReadinessAnswer(answers[index].Body)
		if err != nil {
			t.Fatalf("%s: decode python answer %q: %v", scenario.name, answers[index].Body, err)
		}
		if scenario.model == "scripted-transient" {
			atomic.StoreInt32(&transientRound1Attempts, 0)
		}
		prober := newOpenAICompatibleReadinessProber(nil)
		goOutcome, goSafeErrorCode := prober.probe(t.Context(), "openai", scenario.model, server.URL, "go-oracle-key")
		outcomes[python.Outcome]++
		if goOutcome != python.Outcome {
			t.Errorf("%s: outcome: go=%q python=%q", scenario.name, goOutcome, python.Outcome)
		}
		goCode, pythonCode := "<none>", "<none>"
		if goSafeErrorCode != nil {
			goCode = *goSafeErrorCode
		}
		if python.SafeErrorCode != nil {
			pythonCode = *python.SafeErrorCode
		}
		if goCode != pythonCode {
			t.Errorf("%s: safe_error_code: go=%q python=%q", scenario.name, goCode, pythonCode)
		}
	}
	// Answers that are all one outcome compare nothing about the other.
	if outcomes["ready"] < 2 || outcomes["failed"] < 10 {
		t.Fatalf("the scenarios did not reach both outcomes: %v", outcomes)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// decodeReadinessAnswer decodes the producer's stdout: exactly one JSON
// object with exactly the two fields. Anything else is output the comparison
// would not see, so it is refused.
func decodeReadinessAnswer(body string) (readinessAnswer, error) {
	var answer readinessAnswer
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&answer); err != nil {
		return answer, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return answer, fmt.Errorf("the producer's output holds more than one JSON value (next token: %v)", err)
	}
	if answer.Outcome == "" {
		return answer, fmt.Errorf("the producer's output holds no outcome")
	}
	return answer, nil
}

func TestTheReadinessAnswerIsExactlyOneJSONObject(t *testing.T) {
	if got, err := decodeReadinessAnswer(`{"outcome": "failed", "safe_error_code": "rate_limited"}` + "\n"); err != nil || got.Outcome != "failed" || got.SafeErrorCode == nil || *got.SafeErrorCode != "rate_limited" {
		t.Fatalf("one object with a trailing newline: %+v %v", got, err)
	}
	for name, body := range map[string]string{
		"trailing text":    `{"outcome": "ready", "safe_error_code": null} WARNING`,
		"a second value":   `{"outcome": "ready", "safe_error_code": null}{"outcome": "failed"}`,
		"an unknown field": `{"outcome": "ready", "safe_error_code": null, "detail": "x"}`,
		"no outcome":       `{"safe_error_code": null}`,
		"a log line first": "INFO probing\n" + `{"outcome": "ready", "safe_error_code": null}`,
		"nothing":          ``,
	} {
		if got, err := decodeReadinessAnswer(body); err == nil {
			t.Errorf("%s: decoded %+v", name, got)
		}
	}
}
