package pushcli

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func recordingClient() (*ingestClient, *[]float64) {
	client := newIngestClient()
	var waits []float64
	client.sleep = func(seconds float64) { waits = append(waits, seconds) }
	return client, &waits
}

func serverAnswering(t *testing.T, answer func(call int32, w http.ResponseWriter)) (*httptest.Server, *int32) {
	t.Helper()
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answer(atomic.AddInt32(&calls, 1), w)
	}))
	t.Cleanup(server.Close)
	return server, &calls
}

// A 503 or 429 is retried four times with the waits 1, 2, 4, 8 seconds, five
// attempts in all, and the last failure is the answer.
func TestRetryWaitsAreOneTwoFourEight(t *testing.T) {
	server, calls := serverAnswering(t, func(_ int32, w http.ResponseWriter) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":"stream_unavailable","message":"later"}}`))
	})
	client, waits := recordingClient()
	_, err := client.getBatchStatus(context.Background(), clientConfig{apiURL: server.URL, token: "t", orgID: "o"}, "id")
	var transient *transientError
	if !errors.As(err, &transient) || transient.message != "503 stream_unavailable: later" {
		t.Fatalf("error = %v, want the 503 as a transient error", err)
	}
	if *calls != 5 || !reflect.DeepEqual(*waits, []float64{1, 2, 4, 8}) {
		t.Fatalf("%d attempts, waits %v, want 5 attempts and waits 1, 2, 4, 8", *calls, *waits)
	}
}

// Retry-After replaces the wait, clamped to 30 seconds, and does not advance the
// backoff; a value that is not a non-negative finite number is ignored.
func TestRetryAfterReplacesAndClampsTheWait(t *testing.T) {
	values := []string{"100000", "0", "abc", "-1", "nan", "inf", "2.5"}
	server, _ := serverAnswering(t, func(call int32, w http.ResponseWriter) {
		if int(call) <= len(values) {
			w.Header().Set("Retry-After", values[call-1])
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	client, waits := recordingClient()
	// Five attempts: the first four failures are waited on, the fifth is raised.
	_, err := client.getBatchStatus(context.Background(), clientConfig{apiURL: server.URL, token: "t", orgID: "o"}, "id")
	if err == nil {
		t.Fatal("five 429 answers must fail")
	}
	if want := []float64{30, 0, 1, 2}; !reflect.DeepEqual(*waits, want) {
		t.Fatalf("waits %v, want %v (clamped 30, zero, then the 1 and 2 second backoff for the two ignored values)", *waits, want)
	}
}

func TestOtherFailuresAreNotRetried(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 409, 413, 422, 500, 502} {
		server, calls := serverAnswering(t, func(_ int32, w http.ResponseWriter) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"code":"c","message":"m"}}`))
		})
		client, waits := recordingClient()
		_, err := client.getBatchStatus(context.Background(), clientConfig{apiURL: server.URL, token: "t", orgID: "o"}, "id")
		var api *apiError
		if !errors.As(err, &api) || api.status != status || *calls != 1 || len(*waits) != 0 {
			t.Errorf("status %d: err %v, %d attempts, waits %v; want one attempt and an API error", status, err, *calls, *waits)
		}
	}
}

func TestRedaction(t *testing.T) {
	for text, want := range map[string]string{
		"token fcpush_abc-DEF_123 here":    "token fcpush_[REDACTED] here",
		"Authorization: Bearer secret123":  "Authorization: Bearer [REDACTED]",
		"BEARER nbsp-secret and more":      "Bearer [REDACTED] and more",
		"bearer em-secret":                 "Bearer [REDACTED]",
		"bearer\tx":                        "Bearer [REDACTED]",
		"no secret":                        "no secret",
		"fcpush_ then":                     "fcpush_ then",
		"unbearable Bearer xyz":            "unbearable Bearer [REDACTED]",
		"fcpush_aaa fcpush_bbb Bearer c d": "fcpush_[REDACTED] fcpush_[REDACTED] Bearer [REDACTED] d",
	} {
		if got := redactSecrets(text); got != want {
			t.Errorf("redactSecrets(%q) = %q, want %q", text, got, want)
		}
	}
}

// The token a server echoes back never reaches stdout or stderr, in either form.
func TestAServerEchoNeverPrintsTheToken(t *testing.T) {
	const token = "fcpush_super-secret-token_1"
	server, _ := serverAnswering(t, func(_ int32, w http.ResponseWriter) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"echo ` + token + `","message":"Authorization: Bearer ` + token + `","errors":[{"message":"` + token + `","` + token + `":"x"}]}}`))
	})
	for _, extra := range [][]string{nil, {"--json"}} {
		var stdout, stderr bytes.Buffer
		args := append([]string{"push", "status", "id", "--api-url", server.URL, "--token", token, "--org", "o"}, extra...)
		code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{Args: args, Stdout: &stdout, Stderr: &stderr})
		if code != exitTransport {
			t.Errorf("%v: exit %d, want 3", extra, code)
		}
		if strings.Contains(stdout.String()+stderr.String(), token) {
			t.Errorf("%v: the token is in the output:\nstdout %q\nstderr %q", extra, stdout.String(), stderr.String())
		}
	}
}

// The token never appears in a request URL: it is only in the Authorization header.
func TestTheTokenIsOnlyInTheAuthorizationHeader(t *testing.T) {
	const token = "fcpush_header-only"
	var target string
	var auth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target, auth = r.RequestURI, r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"ingestionId":"i","status":"queued"}`))
	}))
	t.Cleanup(server.Close)
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args: []string{"push", "status", "i", "--api-url", server.URL, "--token", token, "--org", "o"}, Stdout: &stdout, Stderr: &stderr,
	})
	if code != 0 || strings.Contains(target, token) || auth != "Bearer "+token {
		t.Fatalf("exit %d, target %q, auth %q", code, target, auth)
	}
}

// A request goes to the path httpx would send: dot segments removed, a space and
// non-ASCII text escaped, an existing escape kept, the fragment dropped.
func TestRequestTargets(t *testing.T) {
	for id, want := range map[string]string{
		"ing-1":    "/api/v1/external-ingest/batches/ing-1",
		"../x":     "/api/v1/external-ingest/x",
		"a b?c d":  "/api/v1/external-ingest/batches/a%20b?c%20d",
		"é":        "/api/v1/external-ingest/batches/%C3%A9",
		"%41":      "/api/v1/external-ingest/batches/%41",
		"a#b":      "/api/v1/external-ingest/batches/a",
		"x/./y/..": "/api/v1/external-ingest/batches/x",
		"a\\b":     "/api/v1/external-ingest/batches/a\\b",
		"a{b}":     "/api/v1/external-ingest/batches/a%7Bb%7D",
	} {
		var target string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			target = r.RequestURI
			_, _ = w.Write([]byte(`{}`))
		}))
		client, _ := recordingClient()
		if _, err := client.getBatchStatus(context.Background(), clientConfig{apiURL: server.URL, token: "t", orgID: "o"}, id); err != nil {
			t.Errorf("%q: %v", id, err)
		}
		server.Close()
		if target != want {
			t.Errorf("id %q: request target %q, want %q", id, target, want)
		}
	}
}

// A URL's userinfo is httpx's BasicAuth: the header replaces the Bearer one, on
// every request (the limits pre-flight included), percent-decoded, and only when
// the login or the password is not empty.
func TestURLUserinfoIsBasicAuth(t *testing.T) {
	for userinfo, want := range map[string]string{
		"ann:s%40cret": "Basic " + base64.StdEncoding.EncodeToString([]byte("ann:s@cret")),
		"ann":          "Basic " + base64.StdEncoding.EncodeToString([]byte("ann:")),
		":pw":          "Basic " + base64.StdEncoding.EncodeToString([]byte(":pw")),
		"%C3%A9:x":     "Basic " + base64.StdEncoding.EncodeToString([]byte("é:x")),
		"a%zz:b":       "Basic " + base64.StdEncoding.EncodeToString([]byte("a%zz:b")),
		"":             "Bearer tok",
		":":            "Bearer tok",
	} {
		var auths []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auths = append(auths, r.Header.Get("Authorization"))
			_, _ = w.Write([]byte(`{}`))
		}))
		base := strings.Replace(server.URL, "://", "://"+userinfo+"@", 1)
		client, _ := recordingClient()
		client.schemaDocument(context.Background(), base)
		if _, err := client.getBatchStatus(context.Background(), clientConfig{apiURL: base, token: "tok", orgID: "o"}, "id"); err != nil {
			t.Errorf("%q: %v", userinfo, err)
		}
		server.Close()
		schemasWant := want
		if want == "Bearer tok" {
			schemasWant = "" // the limits pre-flight sends no token of its own
		}
		if len(auths) != 2 || auths[0] != schemasWant || auths[1] != want {
			t.Errorf("userinfo %q: Authorization %q, want the schemas call and the request to carry %q", userinfo, auths, want)
		}
	}
}

// A response that goes silent for the read timeout is a retryable network error
// with no text after "transport error: ", whether the headers or the body stall.
func TestSilentResponseIsARetryableReadTimeout(t *testing.T) {
	for name, stallBody := range map[string]bool{"headers": false, "body": true} {
		server, calls := serverAnswering(t, func(call int32, w http.ResponseWriter) {
			if call == 1 {
				if stallBody {
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
				}
				time.Sleep(400 * time.Millisecond)
			}
			_, _ = w.Write([]byte(`{"status":"queued"}`))
		})
		client := newIngestClientWithReadTimeout(150 * time.Millisecond)
		var waits []float64
		client.sleep = func(seconds float64) { waits = append(waits, seconds) }
		body, err := client.getBatchStatus(context.Background(), clientConfig{apiURL: server.URL, token: "t", orgID: "o"}, "id")
		if err != nil || body == nil {
			t.Errorf("%s stall: err %v body %v, want the retry to succeed", name, err, body)
		}
		if *calls != 2 || !reflect.DeepEqual(waits, []float64{1}) {
			t.Errorf("%s stall: %d attempts, waits %v, want 2 attempts and one 1 s wait", name, *calls, waits)
		}
	}
	// Always silent: five attempts, then the error text is empty.
	server, calls := serverAnswering(t, func(_ int32, w http.ResponseWriter) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond)
	})
	client := newIngestClientWithReadTimeout(100 * time.Millisecond)
	client.sleep = func(float64) {}
	_, err := client.getBatchStatus(context.Background(), clientConfig{apiURL: server.URL, token: "t", orgID: "o"}, "id")
	var transient *transientError
	if !errors.As(err, &transient) || transient.message != "transport error: " || *calls != 5 {
		t.Errorf("always silent: err %v after %d attempts, want \"transport error: \" after 5", err, *calls)
	}
}

// The credential in an API URL's userinfo never reaches stdout, stderr or a log,
// whatever goes wrong with the URL or the connection (and never as its Basic
// header value either).
func TestURLUserinfoNeverReachesOutputOrLogs(t *testing.T) {
	const password = "PWSECRET-5x9"
	basic := base64.StdEncoding.EncodeToString([]byte("ann:" + password))
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	waited := pause
	pause = func(float64) {}
	t.Cleanup(func() { pause = waited })
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"bad","message":"refused"}}`))
	}))
	t.Cleanup(refusing.Close)
	urls := map[string]string{
		"connection refused": "http://ann:" + password + "@127.0.0.1:1",
		"invalid port":       "http://ann:" + password + "@127.0.0.1:notaport",
		"invalid ipv4":       "http://ann:" + password + "@999.1.1.1",
		"non-ascii host":     "http://ann:" + password + "@hôst.example",
		"unsupported scheme": "ftp://ann:" + password + "@127.0.0.1",
		"control character":  "http://ann:" + password + "@127.0.0.1/\x01",
		"a 400 answer":       strings.Replace(refusing.URL, "://", "://ann:"+password+"@", 1),
	}
	for name, apiURL := range urls {
		for _, args := range [][]string{
			{"push", "status", "id", "--api-url", apiURL, "--token", "t", "--org", "o", "--json"},
			{"push", "status", "id", "--api-url", apiURL, "--token", "t", "--org", "o"},
		} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{Args: args, Stdout: &stdout, Stderr: &stderr})
			text := stdout.String() + stderr.String() + logs.String()
			if strings.Contains(text, password) || strings.Contains(text, basic) {
				t.Errorf("%s (exit %d): the credential is in the output or logs:\n%s", name, code, text)
			}
			if code == exitOK {
				t.Errorf("%s: exit 0", name)
			}
		}
	}
}
