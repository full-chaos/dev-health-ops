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
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// resendOracleProgram drives the REAL Python Resend path
// (ResendEmailProvider in src/dev_health_ops/api/services/email.py, over the
// installed `resend` SDK). It prints whether the provider raised. Its input
// is {"spec": the message, "stub_reply": what the stand-in answers}; the
// address of the stand-in is RESEND_API_URL in its environment.
const resendOracleProgram = `
import asyncio, json, sys
from dev_health_ops.api.services.email import ResendEmailProvider

spec = json.load(sys.stdin)["spec"]
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

// resendAnswer is what one run of the Python provider gave: whether it sent,
// the class of the error it raised, and the request the stand-in received from
// it (none when the provider made none). The Authorization value is held as
// its digest.
type resendAnswer struct {
	Sent    bool                   `json:"sent"`
	Error   string                 `json:"error,omitempty"`
	Request *resendRecordedRequest `json:"request"`
}

type resendRecordedRequest struct {
	Method        string         `json:"method"`
	Path          string         `json:"path"`
	Authorization textDigest     `json:"authorization"`
	Accept        string         `json:"accept"`
	UserAgent     string         `json:"user_agent"`
	ContentType   string         `json:"content_type"`
	Body          map[string]any `json:"body"`
}

// resendReply is the reply the stand-in gives to one run. It is in the
// program's input: the answer of the provider depends on it.
type resendReply struct {
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Body        string `json:"body"`
}

// resendProgram is one run of the Python provider: spec is the message, reply
// what the stand-in answers. When the golden is recorded, the stand-in is
// started once for all runs, each run gets its address by name (it is new in
// every run and is not part of the request), and the recorded answer is the
// provider's outcome with the request the stand-in received.
func resendProgram(t *testing.T, name string, spec map[string]any, reply resendReply, stub func() *resendStub) programoracle.Program {
	t.Helper()
	input, err := json.Marshal(map[string]any{"spec": spec, "stub_reply": reply})
	if err != nil {
		t.Fatal(err)
	}
	return programoracle.Program{
		Name: name, Text: resendOracleProgram, Stdin: input,
		PerRun: func() map[string]string {
			server := stub()
			server.respond(reply.Status, reply.ContentType, reply.Body)
			return map[string]string{"RESEND_API_URL": server.server.URL}
		},
		Answer: func(stdout []byte) ([]byte, error) {
			// The SDK may log; the outcome is the last line.
			lines := strings.Split(strings.TrimSpace(string(stdout)), "\n")
			var answer resendAnswer
			if err := json.Unmarshal([]byte(lines[len(lines)-1]), &answer); err != nil {
				return nil, fmt.Errorf("decode the provider's outcome: %w", err)
			}
			if request := stub().last; request != nil {
				answer.Request = &resendRecordedRequest{
					Method: request.Method, Path: request.Path, Authorization: digestOf(request.Authorization),
					Accept: request.Accept, UserAgent: request.UserAgent, ContentType: request.ContentType, Body: request.Body,
				}
			}
			return json.Marshal(answer)
		},
	}
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

// TestResendSenderMatchesFrozenPythonResendProvider is the cross-runtime proof
// for the Resend transport, in two halves. The Python half is frozen: what the
// real provider sent to a stand-in server, and how it ended, executed once on
// the pinned build.
//
//   - the REQUEST both planes make: method, path, Authorization (by its
//     digest), Accept, User-Agent, the JSON media type, and the JSON body
//     (parsed -- key order and HTML-safe escaping are serialization details,
//     not content);
//   - the OUTCOME class each canned reply produces: sent, or failed. A reply
//     where the two planes deliberately disagree is named in
//     resendKnownOutcomeDivergences with its reason; the test fails if a
//     listed divergence stops diverging or an unlisted one appears.
func TestResendSenderMatchesFrozenPythonResendProvider(t *testing.T) {
	spec := func(subject, html string) map[string]any {
		return map[string]any{
			"api_key": "re_test_key", "from": "Dev Health <dev-health@example.com>",
			"to": "owner@example.test", "subject": subject, "html": html,
		}
	}
	requests := []struct {
		name string
		spec map[string]any
	}{
		{"ascii", spec("Hello", "<p>Hi there</p>")},
		{"non-ascii", spec("Café ☃ \U0001F389", "<p>Café 日本語</p>\n<p>tail</p>")},
		{"markup and quotes", spec(`He said "hi" & <left>`, `<a href="x?a=1&b=2">'q'</a>`)},
		{"empty subject and body", spec("", "")},
	}
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

	// The stand-in of a recording: one server for every Python run, started
	// only when the golden is recorded.
	var recording *resendStub
	recordingStub := func() *resendStub {
		if recording == nil {
			recording = newResendStub(t)
		}
		return recording
	}
	accepted := resendReply{Status: 200, Body: `{"id":"49a3999c-0ce1-4ea6-ab68-afcd6dc2e794"}`}
	var programs []programoracle.Program
	for _, c := range requests {
		programs = append(programs, resendProgram(t, "request: "+c.name, c.spec, accepted, recordingStub))
	}
	for _, reply := range replies {
		programs = append(programs, resendProgram(t, "outcome: "+reply.name, spec("Hello", "<p>x</p>"),
			resendReply{Status: reply.status, ContentType: reply.contentType, Body: reply.body}, recordingStub))
	}
	outputs := frozenPython(t, "resend.golden.json", programs...)
	answers := make([]resendAnswer, len(outputs))
	for index, output := range outputs {
		if err := json.Unmarshal([]byte(output), &answers[index]); err != nil {
			t.Fatalf("decode answer %d: %v", index, err)
		}
		if answers[index].Request == nil {
			t.Fatalf("answer %d (%s): the Python provider made no request when it was recorded", index, programs[index].Name)
		}
	}

	t.Run("request", func(t *testing.T) {
		stub := newResendStub(t)
		for index, c := range requests {
			t.Run(c.name, func(t *testing.T) {
				python := answers[index]
				if !python.Sent {
					t.Fatalf("Python failed against a 200 stand-in when it was recorded: %s", python.Error)
				}
				stub.respond(accepted.Status, accepted.ContentType, accepted.Body)
				if err := runGoResend(t, stub.server.URL, c.spec); err != nil {
					t.Fatalf("Go failed against a 200 stub: %v", err)
				}
				goRequest := stub.take(t)

				if python.Request.Method != goRequest.Method || python.Request.Path != goRequest.Path {
					t.Errorf("request line: python %s %s, go %s %s", python.Request.Method, python.Request.Path, goRequest.Method, goRequest.Path)
				}
				if python.Request.Authorization != digestOf(goRequest.Authorization) {
					t.Errorf("Authorization: the Go value is not the Python value: python %+v, go %+v", python.Request.Authorization, digestOf(goRequest.Authorization))
				}
				if python.Request.Accept != goRequest.Accept {
					t.Errorf("Accept: python %q, go %q", python.Request.Accept, goRequest.Accept)
				}
				if python.Request.UserAgent != goRequest.UserAgent {
					t.Errorf("User-Agent: python %q, go %q", python.Request.UserAgent, goRequest.UserAgent)
				}
				pythonType, _, _ := mime.ParseMediaType(python.Request.ContentType)
				goType, _, _ := mime.ParseMediaType(goRequest.ContentType)
				if pythonType != goType {
					t.Errorf("Content-Type: python %q, go %q", python.Request.ContentType, goRequest.ContentType)
				}
				if !reflect.DeepEqual(python.Request.Body, goRequest.Body) {
					t.Errorf("JSON body differs:\n python: %#v\n go:     %#v", python.Request.Body, goRequest.Body)
				}
			})
		}
	})

	// Where the planes deliberately disagree: nowhere for the deployed
	// resend 2.47.0. Up to 2.30.0 the Python SDK never consulted the HTTP
	// status (only a `statusCode` in the body), so a non-2xx JSON reply with
	// none was a "sent" there; 2.47.0's Request.perform raises on any status
	// >= 400, so the planes agree. A new divergence is listed here again.
	resendKnownOutcomeDivergences := map[string]string{}

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
		for index, reply := range replies {
			t.Run(reply.name, func(t *testing.T) {
				python := answers[len(requests)+index]
				pythonSent, pythonError := python.Sent, python.Error
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

}
