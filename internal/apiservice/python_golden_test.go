package apiservice

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// pythonGolden is testdata/python_transport_golden.json: every response the
// REAL Python api middleware stack (register_middleware +
// register_exception_handlers on a route-less FastAPI app, starlette 1.3.1)
// gave for the request matrix in Axes. It was produced by running that Python
// code, never written by hand; the command is in the file's generated_by field.
type pythonGolden struct {
	GeneratedBy string             `json:"generated_by"`
	Configs     map[string]*string `json:"configs"`
	Axes        struct {
		Config         []string  `json:"config"`
		Method         []string  `json:"method"`
		Origin         []*string `json:"origin"`
		RequestMethod  []*string `json:"access_control_request_method"`
		RequestHeaders []*string `json:"access_control_request_headers"`
		PrivateNetwork []*string `json:"access_control_request_private_network"`
	} `json:"axes"`
	RowLayout []string `json:"row_layout"`
	Responses []struct {
		Status  int                 `json:"status"`
		Body    string              `json:"body"`
		Headers map[string][]string `json:"headers"`
	} `json:"responses"`
	Cases [][7]int `json:"cases"`
}

func loadPythonGolden(t *testing.T) pythonGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/python_transport_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden pythonGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return golden
}

// serverFor builds the api handler for one golden config through the real
// config.Load, so the origin list is parsed by the production parser.
func serverFor(t *testing.T, origins *string) http.Handler {
	t.Helper()
	env := map[string]string{}
	if origins != nil {
		env["CORS_ALLOWED_ORIGINS"] = *origins
	}
	cfg, err := config.Load(config.Spec{
		Service:   config.APIServiceName,
		LookupEnv: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
	})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	server, err := NewServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), Routes(Deps{}, nil))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return server.Handler()
}

// TestTransportMatchesThePythonAPI replays every golden request against the Go
// api and requires the same status, body and response headers. The only
// header whose VALUE is not compared is X-Request-ID, which both planes
// generate fresh when the request carries none; its presence is compared.
func TestTransportMatchesThePythonAPI(t *testing.T) {
	golden := loadPythonGolden(t)

	// The matrix is complete: every combination of the axes appears exactly
	// once, so a trimmed golden cannot pass by covering less.
	want := len(golden.Axes.Config) * len(golden.Axes.Method) * len(golden.Axes.Origin) *
		len(golden.Axes.RequestMethod) * len(golden.Axes.RequestHeaders) * len(golden.Axes.PrivateNetwork)
	if len(golden.Cases) != want || want == 0 {
		t.Fatalf("golden has %d cases, the axes span %d", len(golden.Cases), want)
	}
	seen := make(map[[6]int]bool, len(golden.Cases))
	for _, row := range golden.Cases {
		key := [6]int{row[0], row[1], row[2], row[3], row[4], row[5]}
		if seen[key] {
			t.Fatalf("golden repeats case %v", key)
		}
		seen[key] = true
	}

	handlers := map[string]http.Handler{}
	for _, name := range golden.Axes.Config {
		origins, declared := golden.Configs[name]
		if !declared {
			t.Fatalf("golden config %q has no origin value", name)
		}
		handlers[name] = serverFor(t, origins)
	}

	mismatches := 0
	for _, row := range golden.Cases {
		configName := golden.Axes.Config[row[0]]
		request := httptest.NewRequest(golden.Axes.Method[row[1]], "/api/v1/nope", nil)
		setIf(request, "Origin", golden.Axes.Origin[row[2]])
		setIf(request, "Access-Control-Request-Method", golden.Axes.RequestMethod[row[3]])
		setIf(request, "Access-Control-Request-Headers", golden.Axes.RequestHeaders[row[4]])
		setIf(request, "Access-Control-Request-Private-Network", golden.Axes.PrivateNetwork[row[5]])

		recorder := httptest.NewRecorder()
		handlers[configName].ServeHTTP(recorder, request)

		expected := golden.Responses[row[6]]
		got := normalizedHeaders(recorder.Header())
		if recorder.Code != expected.Status || recorder.Body.String() != expected.Body ||
			!reflect.DeepEqual(got, normalizedGolden(expected.Headers)) {
			mismatches++
			if mismatches <= 5 {
				t.Errorf("config=%s %s origin=%v acrm=%v acrh=%v pna=%v:\n go     %d %q %v\n python %d %q %v",
					configName, request.Method, deref(golden.Axes.Origin[row[2]]),
					deref(golden.Axes.RequestMethod[row[3]]), deref(golden.Axes.RequestHeaders[row[4]]),
					deref(golden.Axes.PrivateNetwork[row[5]]),
					recorder.Code, recorder.Body.String(), got,
					expected.Status, expected.Body, normalizedGolden(expected.Headers))
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ from the Python api", mismatches, len(golden.Cases))
	}
}

func setIf(request *http.Request, name string, value *string) {
	if value != nil {
		request.Header.Set(name, *value)
	}
}

func deref(value *string) string {
	if value == nil {
		return "<absent>"
	}
	return *value
}

func normalizedHeaders(header http.Header) map[string][]string {
	out := make(map[string][]string, len(header))
	for name, values := range header {
		key := strings.ToLower(name)
		if key == "date" || key == "server" || key == "x-dev-health-plane" || key == "x-dev-health-build" {
			continue // the Go api's own plane/build stamp; asserted by TestServerMatchesThePythonAPIOverRawHTTP
		}
		if key == "x-request-id" {
			values = []string{"<generated>"}
		}
		sorted := append([]string(nil), values...)
		sort.Strings(sorted)
		out[key] = sorted
	}
	return out
}

func normalizedGolden(headers map[string][]string) map[string][]string {
	out := make(map[string][]string, len(headers))
	for name, values := range headers {
		sorted := append([]string(nil), values...)
		sort.Strings(sorted)
		out[strings.ToLower(name)] = sorted
	}
	return out
}
