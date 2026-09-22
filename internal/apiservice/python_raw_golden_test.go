package apiservice

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// pythonRawGolden is testdata/python_raw_http_golden.json: the REAL Python
// api middleware stack on a route-less FastAPI app, served by the REAL
// uvicorn the api runs on, driven with raw HTTP/1.1 request bytes over TCP so
// no client library normalised the target, the method or a header. It covers
// what a handler-level matrix cannot: request targets the server or mux could
// rewrite (dot segments, repeated slashes, encoded dots and slashes, "*",
// absolute and authority forms), every method spelling, and request-id
// values. It was produced by running that Python code; the command is in
// generated_by.
type pythonRawGolden struct {
	GeneratedBy string `json:"generated_by"`
	Cases       []struct {
		Request string              `json:"request"`
		Method  string              `json:"method"`
		Target  string              `json:"target"`
		Status  int                 `json:"status"`
		Body    string              `json:"body"`
		Headers map[string][]string `json:"headers"`
	} `json:"cases"`
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// transportOnly are headers each HTTP server writes for itself (the date,
// the server's own name, connection management); they are not part of the
// api's contract with a client and are left out of the comparison.
var transportOnly = map[string]bool{"date": true, "server": true, "connection": true}

// TestServerMatchesThePythonAPIOverRawHTTP replays every raw golden request
// against a real `dho api` listener and requires the same status, body and
// response headers. A generated X-Request-ID is compared as "a fresh UUID"
// on both sides; an echoed one is compared byte for byte.
func TestServerMatchesThePythonAPIOverRawHTTP(t *testing.T) {
	raw, err := os.ReadFile("testdata/python_raw_http_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden pythonRawGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("golden has no cases")
	}

	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
		if key == "DEV_HEALTH_API_ADDR" {
			return "127.0.0.1:0", true
		}
		return "", false
	}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(cfg, quietLogger(), Routes())
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(t.Context()) })

	mismatches, invalid := 0, 0
	for _, c := range golden.Cases {
		request, err := base64.StdEncoding.DecodeString(c.Request)
		if err != nil {
			t.Fatal(err)
		}
		status, body, headers := rawRoundTrip(t, server.Address(), request, c.Method)
		want := normalizeRaw(latin1Bytes(c.Headers))
		got := normalizeRaw(headers)
		if invalidEscape(c.Target) {
			// Go's HTTP parser refuses a request target that is not a valid
			// URI before any handler runs (url.ParseRequestURI), with a
			// plain-text 400; uvicorn passes it on and the Python api answers
			// its JSON 404. This is the one known server-level difference;
			// it is pinned here so it cannot widen silently.
			if status != http.StatusBadRequest || body != "400 Bad Request" && !(c.Method == "HEAD" && body == "") {
				t.Errorf("%s %s: invalid-escape target gave %d %q, want the parser's 400", c.Method, c.Target, status, body)
			}
			invalid++
			continue
		}
		if status != c.Status || body != c.Body || !reflect.DeepEqual(got, want) {
			mismatches++
			if mismatches <= 8 {
				t.Errorf("%s %s:\n go     %d %q %v\n python %d %q %v", c.Method, c.Target, status, body, got, c.Status, c.Body, want)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d raw requests differ from the Python api", mismatches, len(golden.Cases))
	}
	// The golden holds exactly one invalid-escape target (one per method);
	// if that count changes, the pinned difference changed with it.
	if invalid != 6 {
		t.Fatalf("%d invalid-escape cases, want 6", invalid)
	}
}

// invalidEscape reports a request target url.ParseRequestURI refuses for a
// malformed percent-escape.
func invalidEscape(target string) bool {
	_, err := url.ParseRequestURI(target)
	return err != nil && strings.Contains(err.Error(), "invalid URL escape")
}

// latin1Bytes turns the golden's header values back into the bytes on the
// wire: the oracle decoded every header as latin-1, one rune per byte.
func latin1Bytes(headers map[string][]string) map[string][]string {
	out := make(map[string][]string, len(headers))
	for name, values := range headers {
		for _, value := range values {
			var raw []byte
			for _, r := range value {
				raw = append(raw, byte(r))
			}
			out[name] = append(out[name], string(raw))
		}
	}
	return out
}

func rawRoundTrip(t *testing.T, address string, request []byte, method string) (int, string, map[string][]string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(request); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: strings.ToUpper(method)})
	if err != nil {
		t.Fatalf("%q: read response: %v", request, err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	headers := map[string][]string{}
	for name, values := range response.Header {
		headers[strings.ToLower(name)] = values
	}
	return response.StatusCode, string(bytes.ToValidUTF8(body, []byte("?"))), headers
}

func normalizeRaw(headers map[string][]string) map[string][]string {
	out := map[string][]string{}
	for name, values := range headers {
		key := strings.ToLower(name)
		if transportOnly[key] {
			continue
		}
		copied := append([]string(nil), values...)
		if key == "x-request-id" {
			for index, value := range copied {
				if uuidPattern.MatchString(value) {
					copied[index] = "<fresh uuid>"
				}
			}
		}
		sort.Strings(copied)
		out[key] = copied
	}
	return out
}
