package mail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// resendOracleProgram drives the REAL Python Resend path
// (ResendEmailProvider in src/dev_health_ops/api/services/email.py, over the
// installed `resend` SDK). It prints whether the provider raised.
const resendOracleProgram = `
import asyncio, json, sys
from dev_health_ops.api.services.email import ResendEmailProvider

spec = json.load(sys.stdin)
provider = ResendEmailProvider(api_key=spec["api_key"])
try:
    asyncio.run(provider.send_email(
        from_address=spec["from"], to_address=spec["to"],
        subject=spec["subject"], html_content=spec["html"]))
    print(json.dumps({"sent": True}))
except Exception as error:
    print(json.dumps({"sent": False, "error": type(error).__name__}))
`

type resendRequest struct {
	Method        string
	Path          string
	Authorization string
	Accept        string
	UserAgent     string
	ContentType   string
	Body          map[string]any
}

// resendStub records requests and answers each with a canned response.
type resendStub struct {
	server *httptest.Server
	mu     sync.Mutex
	last   *resendRequest
	status int
	header string // Content-Type of the reply; "" means application/json
	body   string
}

func newResendStub(t *testing.T) *resendStub {
	t.Helper()
	stub := &resendStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var decoded map[string]any
		_ = json.Unmarshal(raw, &decoded)
		stub.mu.Lock()
		stub.last = &resendRequest{
			Method: r.Method, Path: r.URL.Path,
			Authorization: r.Header.Get("Authorization"),
			Accept:        r.Header.Get("Accept"),
			UserAgent:     r.Header.Get("User-Agent"),
			ContentType:   r.Header.Get("Content-Type"),
			Body:          decoded,
		}
		status, contentType, body := stub.status, stub.header, stub.body
		stub.mu.Unlock()
		if contentType == "" {
			contentType = "application/json"
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(stub.server.Close)
	return stub
}

func (stub *resendStub) respond(status int, contentType, body string) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.status, stub.header, stub.body, stub.last = status, contentType, body, nil
}

func (stub *resendStub) take(t *testing.T) resendRequest {
	t.Helper()
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.last == nil {
		t.Fatal("the Resend stub received no request")
	}
	return *stub.last
}

func runPythonResend(t *testing.T, interpreter, root, stubURL string, spec map[string]any) (sent bool, errorType string) {
	t.Helper()
	input, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(interpreter, "-c", resendOracleProgram)
	command.Dir = root
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "RESEND_API_URL="+stubURL)
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("python resend: %v\n%s", err, output)
	}
	var result struct {
		Sent  bool   `json:"sent"`
		Error string `json:"error"`
	}
	// The SDK may log; the result is the last line.
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil {
		t.Fatalf("decode python result: %v\n%s", err, output)
	}
	return result.Sent, result.Error
}

func runGoResend(t *testing.T, stubURL string, spec map[string]any) error {
	t.Helper()
	for _, name := range []string{"EMAIL_API_KEY", "RESEND_API_KEY", "SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_USE_TLS", "SMTP_TLS_CA_FILE", "SMTP_TLS_SERVER_NAME"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("EMAIL_PROVIDER", "resend")
	t.Setenv("EMAIL_FROM_ADDRESS", spec["from"].(string))
	t.Setenv("EMAIL_API_KEY", spec["api_key"].(string))
	t.Setenv("RESEND_API_BASE_URL", stubURL)
	sender, err := NewSenderFromEnv(&http.Client{})
	if err != nil {
		t.Fatalf("NewSenderFromEnv: %v", err)
	}
	return sender.Send(context.Background(), Message{
		To: spec["to"].(string), Subject: spec["subject"].(string), HTML: spec["html"].(string),
	})
}

// TestResendSenderMatchesLivePythonResendProvider is the cross-runtime proof
// for the Resend transport, in two halves against one stub server:
//
//   - the REQUEST both planes make: method, path, Authorization, Accept, the
//     JSON media type, and the JSON body (parsed -- key order and HTML-safe
//     escaping are serialization details, not content);
//   - the OUTCOME class each canned reply produces: sent, or failed. A reply
//     where the two planes deliberately disagree is named in
//     resendKnownOutcomeDivergences with its reason; the test fails if a
//     listed divergence stops diverging or an unlisted one appears.
func TestResendSenderMatchesLivePythonResendProvider(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve mail package path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	interpreter := pyoracle.Resolve(t, root)

	spec := func(subject, html string) map[string]any {
		return map[string]any{
			"api_key": "re_test_key", "from": "Dev Health <dev-health@example.com>",
			"to": "owner@example.test", "subject": subject, "html": html,
		}
	}

	t.Run("request", func(t *testing.T) {
		stub := newResendStub(t)
		for _, c := range []struct {
			name string
			spec map[string]any
		}{
			{"ascii", spec("Hello", "<p>Hi there</p>")},
			{"non-ascii", spec("Café ☃ \U0001F389", "<p>Café 日本語</p>\n<p>tail</p>")},
			{"markup and quotes", spec(`He said "hi" & <left>`, `<a href="x?a=1&b=2">'q'</a>`)},
			{"empty subject and body", spec("", "")},
		} {
			t.Run(c.name, func(t *testing.T) {
				stub.respond(200, "", `{"id":"49a3999c-0ce1-4ea6-ab68-afcd6dc2e794"}`)
				if sent, errorType := runPythonResend(t, interpreter, root, stub.server.URL, c.spec); !sent {
					t.Fatalf("Python failed against a 200 stub: %s", errorType)
				}
				python := stub.take(t)
				stub.respond(200, "", `{"id":"49a3999c-0ce1-4ea6-ab68-afcd6dc2e794"}`)
				if err := runGoResend(t, stub.server.URL, c.spec); err != nil {
					t.Fatalf("Go failed against a 200 stub: %v", err)
				}
				goRequest := stub.take(t)

				if python.Method != goRequest.Method || python.Path != goRequest.Path {
					t.Errorf("request line: python %s %s, go %s %s", python.Method, python.Path, goRequest.Method, goRequest.Path)
				}
				if python.Authorization != goRequest.Authorization {
					t.Errorf("Authorization: python %q, go %q", python.Authorization, goRequest.Authorization)
				}
				if python.Accept != goRequest.Accept {
					t.Errorf("Accept: python %q, go %q", python.Accept, goRequest.Accept)
				}
				if python.UserAgent != goRequest.UserAgent {
					t.Errorf("User-Agent: python %q, go %q", python.UserAgent, goRequest.UserAgent)
				}
				pythonType, _, _ := mime.ParseMediaType(python.ContentType)
				goType, _, _ := mime.ParseMediaType(goRequest.ContentType)
				if pythonType != goType {
					t.Errorf("Content-Type: python %q, go %q", python.ContentType, goRequest.ContentType)
				}
				if !reflect.DeepEqual(python.Body, goRequest.Body) {
					t.Errorf("JSON body differs:\n python: %#v\n go:     %#v", python.Body, goRequest.Body)
				}
			})
		}
	})

	replies := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{"accepted", 200, "", `{"id":"49a3999c-0ce1-4ea6-ab68-afcd6dc2e794"}`},
		{"validation error", 422, "", `{"statusCode":422,"name":"validation_error","message":"Invalid from field."}`},
		{"invalid api key", 401, "", `{"statusCode":401,"name":"invalid_api_key","message":"API key is invalid"}`},
		{"forbidden", 403, "", `{"statusCode":403,"name":"invalid_from_address","message":"domain not verified"}`},
		{"rate limited", 429, "", `{"statusCode":429,"name":"rate_limit_exceeded","message":"Too many requests"}`},
		{"server error", 500, "", `{"statusCode":500,"name":"application_error","message":"boom"}`},
		{"200 wrapping an error object", 200, "", `{"error":"something"}`},
		{"200 with a null error key", 200, "", `{"id":"x","error":null}`},
		{"200 wrapping an error status", 200, "", `{"statusCode":422,"name":"validation_error","message":"nope"}`},
		{"200 with a non-JSON content type", 200, "text/plain", `{"id":"x"}`},
		{"200 with an undecodable body", 200, "", `not json`},
		{"200 with an empty body", 200, "", ``},
		{"non-JSON 500", 500, "text/html", `<html>bad gateway</html>`},
		{"JSON 500 with no statusCode field", 500, "", `{"message":"upstream exploded"}`},
	}
	// Where the planes deliberately disagree: Python's SDK never consults the
	// HTTP status, only a `statusCode` field in the body, so a non-2xx reply
	// whose JSON carries none is a "sent" to it. Go treats every non-2xx as a
	// failure. (Resend's own error bodies always carry statusCode, so real
	// traffic never reaches this shape; Go is the safe side of it.)
	resendKnownOutcomeDivergences := map[string]string{
		"JSON 500 with no statusCode field": "python: sent (never checks the HTTP status); go: failed (5xx)",
	}

	// Outcome CLASS. Python has one failure kind (it raises); Go splits it in
	// two: a definite rejection, and an ambiguous outcome (the request may have
	// been processed, so a caller must not blindly resend). Go is deliberately
	// more cautious wherever the reply cannot prove Resend refused the message.
	// Every reply where Go answers "ambiguous" while Python raises is listed
	// here, and a listed one that stops being ambiguous fails the test.
	resendKnownAmbiguousWherePythonRaises := map[string]bool{
		"server error":                      true,
		"200 with a non-JSON content type":  true,
		"200 with an undecodable body":      true,
		"200 with an empty body":            true,
		"non-JSON 500":                      true,
		"JSON 500 with no statusCode field": true,
	}
	classSeen := map[string]bool{}

	t.Run("outcome", func(t *testing.T) {
		stub := newResendStub(t)
		diverged := map[string]bool{}
		for _, reply := range replies {
			t.Run(reply.name, func(t *testing.T) {
				stub.respond(reply.status, reply.contentType, reply.body)
				pythonSent, pythonError := runPythonResend(t, interpreter, root, stub.server.URL, spec("Hello", "<p>x</p>"))
				stub.respond(reply.status, reply.contentType, reply.body)
				goErr := runGoResend(t, stub.server.URL, spec("Hello", "<p>x</p>"))
				goSent := goErr == nil
				var ambiguous *AmbiguousSendError
				goAmbiguous := errors.As(goErr, &ambiguous)
				if goAmbiguous && !pythonSent {
					classSeen[reply.name] = true
					if !resendKnownAmbiguousWherePythonRaises[reply.name] {
						t.Errorf("UNLISTED class divergence: python raised (%s), go answered ambiguous", pythonError)
					}
				} else if resendKnownAmbiguousWherePythonRaises[reply.name] && !(pythonSent != goSent) {
					t.Errorf("%q is listed as ambiguous-where-python-raises but Go now answers %v", reply.name, goErr)
				}
				if pythonSent != goSent {
					diverged[reply.name] = true
					reason, known := resendKnownOutcomeDivergences[reply.name]
					if !known {
						t.Fatalf("UNLISTED divergence: python sent=%v (%s), go sent=%v (%v)", pythonSent, pythonError, goSent, goErr)
					}
					t.Logf("known divergence (%s)", reason)
				}
			})
		}
		for name := range resendKnownAmbiguousWherePythonRaises {
			if !classSeen[name] && !diverged[name] {
				t.Errorf("%q is listed as ambiguous-where-python-raises but the planes now agree on the class", name)
			}
		}
		for name := range resendKnownOutcomeDivergences {
			if !diverged[name] {
				t.Errorf("%q is listed as a known divergence but the planes now agree -- delete it from resendKnownOutcomeDivergences", name)
			}
		}
	})

	if err := os.WriteFile(filepath.Join(proofDir, "mail-resend-oracle"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = fmt.Sprint
}
