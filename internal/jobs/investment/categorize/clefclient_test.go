package categorize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// A planted, token-shaped value. It is a marker, never a real credential.
const (
	clefToken   = "cfut_PLANTED0123456789abcdefTOKENmark"
	clefAccount = "0123456789abcdef0123456789abcdef"
	clefBase    = "https://api.cloudflare.com/client/v4/accounts/" + clefAccount + "/ai"
	clefPost    = "https://api.cloudflare.com/client/v4/accounts/" + clefAccount + "/ai/run/@cf/cloudflare/clef"
)

// clefTransport is the fake network of these tests: it records each request and
// answers from a script. No test of this file opens a socket.
type clefTransport struct {
	requests []*http.Request
	bodies   [][]byte
	answer   func(call int, req *http.Request) (*http.Response, error)
}

func (transport *clefTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	transport.requests = append(transport.requests, req)
	transport.bodies = append(transport.bodies, body)
	return transport.answer(len(transport.requests), req)
}

func clefAnswer(status int, body string) (*http.Response, error) {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func newTestClef(t *testing.T, answer func(int, *http.Request) (*http.Response, error), mutate ...func(*ClefClientConfig)) (*TypeSafeClient, *clefTransport, *bytes.Buffer) {
	t.Helper()
	transport := &clefTransport{answer: answer}
	logs := &bytes.Buffer{}
	cfg := ClefClientConfig{
		Token: secrets.NewHidden(clefToken), BaseURL: clefBase, Model: "clef",
		HTTPClient: &http.Client{Transport: transport},
		Logger:     slog.New(slog.NewJSONHandler(&syncWriter{w: logs}, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	for _, m := range mutate {
		m(&cfg)
	}
	client, err := NewClefClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client.sleep = func(context.Context, time.Duration) bool { return true }
	return client, transport, logs
}

func clefOKBody(model string) string {
	return `{"model":"` + model + `","answers":{},"usage":{"input_tokens":2644,"output_tokens":358}}`
}

// The request goes to the one URL the account and the closed model map give,
// with the token as the bearer and the SystemOne body byte for byte.
func TestClefClientPostsTheBodyToTheAccountURLWithTheBearerOnly(t *testing.T) {
	client, transport, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) {
		return clefAnswer(200, clefOKBody("clef"))
	})
	body := []byte(`{"model":"clef","state":{"source_block":"s"},"questions":{"q":{"type":"noul","instructions":"i"}}}`)
	result, err := client.PostSystemOneDetailed(context.Background(), body)
	if err != nil {
		t.Fatal(err)
	}
	req := transport.requests[0]
	if req.Method != http.MethodPost || req.URL.String() != clefPost {
		t.Fatalf("request = %s %s, want POST %s", req.Method, req.URL, clefPost)
	}
	if req.Header.Get("Authorization") != "Bearer "+clefToken {
		t.Fatalf("Authorization = %q", req.Header.Get("Authorization"))
	}
	for name, values := range req.Header {
		if name != "Authorization" && strings.Contains(strings.Join(values, ","), clefToken) {
			t.Fatalf("the token is in header %s", name)
		}
	}
	if strings.Contains(req.URL.String(), clefToken) || !bytes.Equal(transport.bodies[0], body) {
		t.Fatal("the token is in the URL, or the body was changed")
	}
	if result.Model != "clef" || !result.Usage.Reported || result.Usage.InputTokens != 2644 || result.Usage.OutputTokens != 358 {
		t.Fatalf("result = %+v", result)
	}
	flash, _, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) { return clefAnswer(200, clefOKBody("clef-flash")) },
		func(cfg *ClefClientConfig) { cfg.Model = "clef-flash" })
	if flash.target.url != strings.TrimSuffix(clefPost, "clef")+"clef-flash" || flash.Model() != "clef-flash" {
		t.Fatalf("clef-flash target = %q model %q", flash.target.url, flash.Model())
	}
}

// Every refused target is refused before a client exists, with one error that
// holds no part of the configured value.
func TestClefClientRefusesEveryTargetOutsideTheExactAccountURL(t *testing.T) {
	good := clefBase
	for name, tc := range map[string]struct{ base, model string }{
		"http":                {"http://api.cloudflare.com/client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"other host":          {"https://api.typesafe.ai/client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"jev base":            {"https://api.typesafe.ai", "clef"},
		"subdomain":           {"https://x.api.cloudflare.com/client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"uppercase host":      {"https://API.cloudflare.com/client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"trailing dot host":   {"https://api.cloudflare.com./client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"port":                {"https://api.cloudflare.com:443/client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"user info":           {"https://user:pw@api.cloudflare.com/client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"ip literal":          {"https://104.16.0.1/client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"idn lookalike":       {"https://api.cloudfаre.com/client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"escaped slash":       {"https://api.cloudflare.com/client/v4/accounts/" + clefAccount + "%2F../ai", "clef"},
		"escaped letter":      {"https://api.cloudflare.com/client/v4/accounts/" + clefAccount + "/%61i", "clef"},
		"double slash":        {"https://api.cloudflare.com//client/v4/accounts/" + clefAccount + "/ai", "clef"},
		"double slash inside": {"https://api.cloudflare.com/client/v4/accounts//" + clefAccount + "/ai", "clef"},
		"two trailing slash":  {good + "//", "clef"},
		"query":               {good + "?x=1", "clef"},
		"empty query":         {good + "?", "clef"},
		"fragment":            {good + "#f", "clef"},
		"short account":       {"https://api.cloudflare.com/client/v4/accounts/abc/ai", "clef"},
		"uppercase account":   {"https://api.cloudflare.com/client/v4/accounts/" + strings.ToUpper(clefAccount) + "/ai", "clef"},
		"non hex account":     {"https://api.cloudflare.com/client/v4/accounts/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz/ai", "clef"},
		"other path":          {"https://api.cloudflare.com/client/v4/accounts/" + clefAccount + "/ai/v1", "clef"},
		"run path":            {good + "/run/@cf/cloudflare/clef", "clef"},
		"no path":             {"https://api.cloudflare.com", "clef"},
		"empty":               {"", "clef"},
		"model traversal":     {good, "clef/../x"},
		"model query":         {good, "clef?x=1"},
		"model flash suffix":  {good, "clef-flash/x"},
		"model uppercase":     {good, "CLEF"},
		"model qwen":          {good, "@cf/qwen/qwq-32b"},
		"model empty":         {good, ""},
	} {
		t.Run(name, func(t *testing.T) {
			client, err := NewClefClient(ClefClientConfig{
				Token: secrets.NewHidden(clefToken), BaseURL: tc.base, Model: tc.model,
				HTTPClient: &http.Client{Transport: &clefTransport{answer: func(int, *http.Request) (*http.Response, error) {
					t.Fatal("a request was sent for a refused target")
					return nil, nil
				}}},
			})
			if client != nil || !errors.Is(err, ErrClefTarget) {
				t.Fatalf("NewClefClient = %v, %v; want ErrClefTarget", client, err)
			}
			if tc.base != "" && strings.Contains(err.Error(), "cloudflare") && strings.Contains(err.Error(), clefAccount) {
				t.Fatalf("the error carries the configured value: %v", err)
			}
		})
	}
	if _, err := NewClefClient(ClefClientConfig{Token: secrets.NewHidden(""), BaseURL: good, Model: "clef"}); err == nil {
		t.Fatal("an empty token was accepted")
	}
	if _, err := NewClefClient(ClefClientConfig{Token: secrets.NewHidden("short"), BaseURL: good, Model: "clef"}); err == nil {
		t.Fatal("a token too short to be protected was accepted")
	}
}

// The two backends cannot be pointed at each other: the jev client takes only
// api.typesafe.ai, the clef client only the account URL, and each client holds
// its own bearer.
func TestTheJevAndClefTargetsCannotReachEachOther(t *testing.T) {
	for _, base := range []string{clefBase, clefPost, "https://api.cloudflare.com"} {
		if _, err := NewTypeSafeClient(TypeSafeClientConfig{APIKey: secrets.NewHidden(tsKey), BaseURL: base}); err == nil {
			t.Fatalf("the jev client accepted %q", base)
		}
	}
	if _, err := NewClefClient(ClefClientConfig{Token: secrets.NewHidden(clefToken), BaseURL: "https://api.typesafe.ai", Model: "clef"}); !errors.Is(err, ErrClefTarget) {
		t.Fatalf("the clef client accepted the jev host: %v", err)
	}
	jev, err := NewTypeSafeClient(TypeSafeClientConfig{APIKey: secrets.NewHidden(tsKey)})
	if err != nil {
		t.Fatal(err)
	}
	if jev.target.url != "https://api.typesafe.ai/v1/systemone" || jev.target.provider != ProviderKindTypeSafe || jev.target.bearer.Reveal() != tsKey {
		t.Fatalf("jev target = %+v", jev.target)
	}
	clef, _, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) { return clefAnswer(200, clefOKBody("clef")) })
	if clef.target.url != clefPost || clef.target.provider != ClefProviderName || clef.target.bearer.Reveal() != clefToken {
		t.Fatalf("clef target = %+v", clef.target)
	}
	if clef.target.bearer.Reveal() == jev.target.bearer.Reveal() {
		t.Fatal("the two clients share a bearer")
	}
}

// One retry on a rate limit and then the answer; a 404 stops (model not found);
// a 403 is an auth failure that stops: the same classes as the jev client.
func TestClefClientRetriesOnceAndClassifiesLikeTheJevClient(t *testing.T) {
	client, transport, _ := newTestClef(t, func(call int, _ *http.Request) (*http.Response, error) {
		if call == 1 {
			return clefAnswer(429, "")
		}
		return clefAnswer(200, clefOKBody("clef"))
	})
	result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{}`))
	if err != nil || len(transport.requests) != 2 || result.AttemptCount() != 2 {
		t.Fatalf("retry: err=%v requests=%d attempts=%d", err, len(transport.requests), result.AttemptCount())
	}
	for status, class := range map[int]SystemOneClass{404: SystemOneClassModelNotFound, 403: SystemOneClassAuth, 401: SystemOneClassAuth, 422: SystemOneClassInvalid} {
		failing, ft, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) { return clefAnswer(status, "") })
		_, err := failing.PostSystemOneDetailed(context.Background(), []byte(`{}`))
		var one *SystemOneError
		if !errors.As(err, &one) || one.Class != class || len(ft.requests) != 1 {
			t.Fatalf("status %d: %v (requests %d), want class %s once", status, err, len(ft.requests), class)
		}
	}
}

// A planted token-shaped value never reaches an error text, a log line or the
// returned result, on a failure, on a success and when the peer echoes it.
func TestClefTokenIsAbsentFromEveryErrorLogAndResult(t *testing.T) {
	collect := func(texts ...string) {
		t.Helper()
		for _, text := range texts {
			for i := 0; i+12 <= len(clefToken); i++ {
				if strings.Contains(strings.ToLower(text), strings.ToLower(clefToken[i:i+12])) {
					t.Fatalf("a piece of the token is in %q", text)
				}
			}
		}
	}
	for name, answer := range map[string]func(int, *http.Request) (*http.Response, error){
		"server error": func(int, *http.Request) (*http.Response, error) { return clefAnswer(500, clefToken) },
		"auth":         func(int, *http.Request) (*http.Response, error) { return clefAnswer(403, "denied "+clefToken) },
		"not json":     func(int, *http.Request) (*http.Response, error) { return clefAnswer(200, clefToken) },
		"echo in model": func(int, *http.Request) (*http.Response, error) {
			return clefAnswer(200, `{"model":"`+clefToken+`","answers":{},"usage":{"input_tokens":1,"output_tokens":1}}`)
		},
		"transport": func(int, *http.Request) (*http.Response, error) {
			return nil, errors.New("dial " + clefToken + " failed")
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, _, logs := newTestClef(t, answer)
			result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{"model":"clef"}`))
			texts := []string{logs.String(), result.Model, string(result.Header.Get("x-typesafe-request-id"))}
			if err != nil {
				// The top-level text only: the client's contract forbids printing an unwrapped inner transport layer.
				texts = append(texts, err.Error())
			}
			collect(texts...)
			if name == "echo in model" && (!result.ModelRejected || result.Model != "") {
				t.Fatalf("an echoed token was kept as the model: %+v", result)
			}
		})
	}
}

// Without a supplied client, the clef client dials through the hardened,
// SSRF-guarded client (no redirects, no ambient proxy, guarded transport), the
// one production builds.
func TestClefClientDefaultsToTheHardenedGuardedClient(t *testing.T) {
	client, err := NewClefClient(ClefClientConfig{Token: secrets.NewHidden(clefToken), BaseURL: clefBase, Model: "clef"})
	if err != nil {
		t.Fatal(err)
	}
	if client.client.CheckRedirect == nil || client.client.Transport == nil || client.client.Transport == http.DefaultTransport {
		t.Fatalf("clef client is not the hardened guarded client: %+v", client.client)
	}
	if client.client.Timeout != hardenedHTTPTimeout {
		t.Fatalf("timeout = %v", client.client.Timeout)
	}
}

const clefWrappedBody = `{"result":{"model":"clef","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":2644,"output_tokens":358}},"success":true,"errors":[],"messages":[]}`

// The REST envelope: model, usage and answers sit under "result". They are read
// from there, and the body handed to the decision adapter is the inner object.
func TestClefClientReadsAnAnswerWrappedInAResultEnvelope(t *testing.T) {
	client, _, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) { return clefAnswer(200, clefWrappedBody) })
	result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Model != "clef" || !result.Usage.Reported || result.Usage.InputTokens != 2644 || result.Usage.OutputTokens != 358 {
		t.Fatalf("result = model %q usage %+v", result.Model, result.Usage)
	}
	var inner map[string]any
	if err := json.Unmarshal(result.Body, &inner); err != nil || inner["answers"] == nil || inner["result"] != nil {
		t.Fatalf("body = %s, want the inner object with answers and no result key", result.Body)
	}
}

// A body with "answers" at the top is the plain shape even if it has a "result"
// key: it is left as it is.
func TestClefClientDoesNotUnwrapABodyThatHasAnswers(t *testing.T) {
	plain := `{"model":"clef","answers":{},"usage":{"input_tokens":1,"output_tokens":2},"result":{"model":"other"}}`
	client, _, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) { return clefAnswer(200, plain) })
	result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{}`))
	if err != nil || result.Model != "clef" || string(result.Body) != plain {
		t.Fatalf("result = %q %s %v", result.Model, result.Body, err)
	}
}

// The model is taken from the request only when the answer omits the field.
func TestClefClientTakesTheModelFromTheRequestOnlyWhenTheAnswerOmitsIt(t *testing.T) {
	omitted, _, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) {
		return clefAnswer(200, `{"answers":{},"usage":{"input_tokens":1,"output_tokens":2}}`)
	})
	result, err := omitted.PostSystemOneDetailed(context.Background(), []byte(`{}`))
	if err != nil || result.Model != "clef" || !strings.Contains(string(result.Body), `"model":"clef"`) {
		t.Fatalf("omitted model: %q %s %v", result.Model, result.Body, err)
	}
	for name, body := range map[string]string{
		"other model": `{"model":"clef-flash","answers":{}}`,
		"empty model": `{"model":"","answers":{}}`,
	} {
		client, _, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) { return clefAnswer(200, body) })
		got, err := client.PostSystemOneDetailed(context.Background(), []byte(`{}`))
		if err != nil || strings.Contains(string(got.Body), `"model":"clef"`) || got.Model == "clef" {
			t.Fatalf("%s: model %q body %s err %v: a present model must not be replaced", name, got.Model, got.Body, err)
		}
	}
}
