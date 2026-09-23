package apiservice

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// pythonRawGolden is testdata/python_raw_http_golden.json.gz (gzip of JSON): the REAL Python
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
		Kind    string              `json:"kind"`
		GoCode  int                 `json:"go_status"`
		Method  string              `json:"method"`
		Target  string              `json:"target"`
		Status  int                 `json:"status"`
		Line    string              `json:"status_line"`
		Body    string              `json:"body"`
		Headers map[string][]string `json:"headers"`
	} `json:"cases"`
	Scenarios []struct {
		ID     string   `json:"id"`
		Steps  [][2]any `json:"steps"`
		Python []string `json:"python"`
	} `json:"scenarios"`
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Header normalisation. Header NAMES are compared case-insensitively
// (RFC 9110; uvicorn sends Starlette's lower-case names, net/http its
// canonical ones). Date is compared for presence (its value is the clock).
// Server is the one decided header difference: uvicorn names itself, the Go
// api sends none; the replay asserts exactly that instead of skipping it.

// TestServerMatchesThePythonAPIOverRawHTTP replays every raw golden request
// against a real `dho api` listener and requires the same status, body and
// response headers. A generated X-Request-ID is compared as "a fresh UUID"
// on both sides; an echoed one is compared byte for byte.
func TestServerMatchesThePythonAPIOverRawHTTP(t *testing.T) {
	golden := loadRawGolden(t)
	if len(golden.Cases) == 0 {
		t.Fatal("golden has no cases")
	}
	address := startRawServer(t)

	counts := map[string]int{}
	mismatches := 0
	for _, c := range golden.Cases {
		request, err := base64.StdEncoding.DecodeString(c.Request)
		if err != nil {
			t.Fatal(err)
		}
		counts[c.Kind]++
		line, status, body, headers := rawRoundTrip(t, address, request, c.Method)
		switch c.Kind {
		case "same":
			// The api's own answer: status, body and headers equal.
			want, got := normalizeRaw(latin1Bytes(c.Headers)), normalizeRaw(headers)
			wantLine := c.Line
			if bytes.Contains(firstLine(request), []byte(" HTTP/1.0")) {
				// Decided: net/http answers an HTTP/1.0 request with an
				// HTTP/1.0 status line; uvicorn answers HTTP/1.1. Both are
				// valid (RFC 9110 section 6.2); nothing else may differ.
				wantLine = strings.Replace(wantLine, "HTTP/1.1 ", "HTTP/1.0 ", 1)
			}
			if line != wantLine || status != c.Status || body != c.Body || !reflect.DeepEqual(got, want) || !serverHeaderDecided(headers, c.Headers) {
				mismatches++
				if mismatches <= 8 {
					t.Errorf("%s %.40s:\n go     %s %q %v %v\n python %s %q %v %v", c.Method, c.Target, line, body, got, headers["server"], c.Line, c.Body, want, c.Headers["server"])
				}
			}
		case "status":
			// Both HTTP parsers refuse the request with the same status
			// before any handler runs; each writes its own error text.
			if status != c.Status {
				mismatches++
				t.Errorf("%s %.40s: parser refusal %d, python %d", c.Method, c.Target, status, c.Status)
			}
		case "differs":
			// A decided, pinned difference in the HTTP parser layer: the Go
			// status is exactly the one recorded beside the case.
			if status != c.GoCode {
				mismatches++
				t.Errorf("%s %.40s: go %d, pinned %d (python %d)", c.Method, c.Target, status, c.GoCode, c.Status)
			}
		default:
			t.Fatalf("case %s %.40s has unknown kind %q", c.Method, c.Target, c.Kind)
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d raw requests break their decided outcome", mismatches, len(golden.Cases))
	}
	// The decided outcomes are pinned by count, so a case cannot move
	// between kinds without this line changing with it.
	if want := map[string]int{"same": 83, "status": 8, "differs": 12}; !reflect.DeepEqual(counts, want) {
		t.Fatalf("outcome counts %v, want %v", counts, want)
	}
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

func rawRoundTrip(t *testing.T, address string, request []byte, _ string) (string, int, string, map[string][]string) {
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
	// Every golden request asks for Connection: close, so the response is
	// everything up to EOF. It is parsed from the raw bytes, exactly as the
	// oracle parsed uvicorn's, so no header (Connection included) is lost to
	// a client library.
	raw, _ := io.ReadAll(conn)
	return parseRawResponse(t, raw)
}

func parseRawResponse(t *testing.T, raw []byte) (string, int, string, map[string][]string) {
	t.Helper()
	head, body, found := bytes.Cut(raw, []byte("\r\n\r\n"))
	if !found {
		t.Fatalf("no response head in %q", raw)
	}
	lines := strings.Split(string(head), "\r\n")
	fields := strings.SplitN(lines[0], " ", 3)
	if len(fields) < 2 {
		t.Fatalf("bad status line %q", lines[0])
	}
	status, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatalf("bad status line %q", lines[0])
	}
	headers := map[string][]string{}
	for _, line := range lines[1:] {
		name, value, _ := strings.Cut(line, ":")
		key := strings.ToLower(strings.TrimSpace(name))
		headers[key] = append(headers[key], strings.TrimSpace(value))
	}
	return lines[0], status, string(bytes.ToValidUTF8(body, []byte("?"))), headers
}

func firstLine(request []byte) []byte {
	line, _, _ := bytes.Cut(request, []byte("\r\n"))
	return line
}

// serverHeaderDecided: uvicorn sends exactly "server: uvicorn"; the Go api
// sends no Server header.
func serverHeaderDecided(goHeaders, pythonHeaders map[string][]string) bool {
	python := pythonHeaders["server"]
	return len(goHeaders["server"]) == 0 && len(python) == 1 && python[0] == "uvicorn"
}

func normalizeRaw(headers map[string][]string) map[string][]string {
	out := map[string][]string{}
	for name, values := range headers {
		key := strings.ToLower(name)
		if key == "server" {
			continue // asserted separately, see serverHeaderDecided
		}
		copied := append([]string(nil), values...)
		if key == "date" {
			copied = []string{"<present>"}
		}
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

// scenarioDecided holds the one connection scenario whose Go outcome is a
// decided difference: uvicorn on h11 refuses a request head that is still
// incomplete past 16 KiB, so a 32 KiB header delivered in slow fragments is
// refused there (its send fails, then 400), while the same bytes in one read
// are accepted. The Go api accepts the head either way, keeping "the Go api
// accepts every request the Python api accepts" true for both deliveries.
// net/http also answers an HTTP/1.0 request with an HTTP/1.0 status line
// (the pinned status-line difference of the single-request cases); the
// close after it matches h11.
var scenarioDecided = map[string][]string{
	"fragmented_32k_header": {"HTTP/1.1 404 Not Found"},
	"http10_keepalive":      {"HTTP/1.0 404 Not Found", "EOF"},
}

// TestConnectionScenariosMatchThePythonAPI replays the golden's multi-step
// connection scenarios (keep-alive reuse inside and past the idle timeout,
// pipelining, HTTP/1.0 keep-alive, fragmented header delivery) byte for
// byte, with the same pauses, against a live dho api listener.
func TestConnectionScenariosMatchThePythonAPI(t *testing.T) {
	golden := loadRawGolden(t)
	if len(golden.Scenarios) != 6 {
		t.Fatalf("%d scenarios, want 6", len(golden.Scenarios))
	}
	address := startRawServer(t)
	for _, scenario := range golden.Scenarios {
		t.Run(scenario.ID, func(t *testing.T) {
			t.Parallel()
			got := runScenario(t, address, scenario.Steps)
			want := scenario.Python
			if decided, ok := scenarioDecided[scenario.ID]; ok {
				want = decided
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("go %q, want %q (python %q)", got, want, scenario.Python)
			}
		})
	}
}

func runScenario(t *testing.T, address string, steps [][2]any) []string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := &scenarioReader{conn: conn}
	var results []string
	for _, step := range steps {
		switch step[0] {
		case "send":
			payload, err := base64.StdEncoding.DecodeString(step[1].(string))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write(payload); err != nil {
				results = append(results, "SEND-FAILED")
			}
		case "sleep":
			time.Sleep(time.Duration(step[1].(float64) * float64(time.Second)))
		case "read":
			results = append(results, reader.readResponse())
		default:
			t.Fatalf("unknown step %v", step[0])
		}
	}
	return results
}

// scenarioReader reads one Content-Length-framed response at a time, the
// way the oracle read uvicorn's, reporting EOF when the server closed first.
type scenarioReader struct {
	conn net.Conn
	buf  []byte
}

func (r *scenarioReader) readResponse() string {
	_ = r.conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	chunk := make([]byte, 65536)
	for !bytes.Contains(r.buf, []byte("\r\n\r\n")) {
		n, err := r.conn.Read(chunk)
		r.buf = append(r.buf, chunk[:n]...)
		if err != nil && !bytes.Contains(r.buf, []byte("\r\n\r\n")) {
			if len(r.buf) == 0 {
				return "EOF"
			}
			return "PARTIAL"
		}
	}
	head, rest, _ := bytes.Cut(r.buf, []byte("\r\n\r\n"))
	lines := strings.Split(string(head), "\r\n")
	length := 0
	for _, line := range lines[1:] {
		name, value, _ := strings.Cut(line, ":")
		if strings.EqualFold(strings.TrimSpace(name), "content-length") {
			length, _ = strconv.Atoi(strings.TrimSpace(value))
		}
	}
	for len(rest) < length {
		n, err := r.conn.Read(chunk)
		rest = append(rest, chunk[:n]...)
		if err != nil {
			break
		}
	}
	r.buf = append([]byte(nil), rest[min(length, len(rest)):]...)
	return lines[0]
}

func loadRawGolden(t *testing.T) pythonRawGolden {
	t.Helper()
	file, err := os.Open("testdata/python_raw_http_golden.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	unzipped, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(unzipped)
	if err != nil {
		t.Fatal(err)
	}
	var golden pythonRawGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	return golden
}

// startRawServer starts the real api listener, configured through the
// production config.Load, on a free port.
func startRawServer(t *testing.T) string {
	t.Helper()
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
		if key == "DEV_HEALTH_API_ADDR" {
			return "127.0.0.1:0", true
		}
		return "", false
	}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(cfg, quietLogger(), Routes(Deps{}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	return server.Address()
}
