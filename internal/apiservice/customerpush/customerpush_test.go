package customerpush

import (
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestScopesValueIsTheStoredListOrEmpty(t *testing.T) {
	for _, tc := range []struct {
		raw, want string
		fails     bool
	}{
		{"", "[]", false}, {"null", "[]", false}, {`["b", "a"]`, `["b","a"]`, false}, {"[]", "[]", false},
		{`{"a": 1}`, "", true}, {`[1]`, "", true}, {`[`, "", true},
	} {
		value, err := scopesValue([]byte(tc.raw))
		if (err != nil) != tc.fails {
			t.Fatalf("%q: err %v", tc.raw, err)
		}
		if !tc.fails {
			got, _ := pyjson.Marshal(value)
			if string(got) != tc.want {
				t.Fatalf("%q: got %s want %s", tc.raw, got, tc.want)
			}
		}
	}
}

func TestJSONObjectColumnKeepsStoredKeyOrder(t *testing.T) {
	value, err := jsonObjectColumn([]byte(`{"zeta": 1, "alpha": {"b": 2, "a": 1}}`))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := pyjson.Marshal(value)
	if string(got) != `{"zeta":1,"alpha":{"b":2,"a":1}}` {
		t.Fatalf("got %s", got)
	}
	for _, raw := range []string{"", "null"} {
		if value, err := jsonObjectColumn([]byte(raw)); err != nil || value != nil {
			t.Fatalf("%q: %v %v", raw, value, err)
		}
	}
	if _, err := jsonObjectColumn([]byte(`[1]`)); err == nil {
		t.Fatal("a list is not a dict")
	}
}

func TestLastQueryIsStarlettesLastValue(t *testing.T) {
	values, _ := url.ParseQuery("limit=1&limit=2&empty=")
	if got := pybody.LastQuery(values, "limit"); got == nil || *got != "2" {
		t.Fatalf("limit: %v", got)
	}
	if got := pybody.LastQuery(values, "empty"); got == nil || *got != "" {
		t.Fatalf("empty: %v", got)
	}
	if got := pybody.LastQuery(values, "absent"); got != nil {
		t.Fatalf("absent: %v", *got)
	}
}

func TestLimitsAreIntOfTheEnvironment(t *testing.T) {
	for _, tc := range []struct {
		env   map[string]string
		want  string
		fails bool
	}{
		{nil, `{"maxRecordsPerBatch":1000,"maxBodyBytes":10000000}`, false},
		{map[string]string{"EXTERNAL_INGEST_MAX_RECORDS": " 2_500 "}, `{"maxRecordsPerBatch":2500,"maxBodyBytes":10000000}`, false},
		{map[string]string{"EXTERNAL_INGEST_MAX_BODY_BYTES": "-7"}, `{"maxRecordsPerBatch":1000,"maxBodyBytes":-7}`, false},
		{map[string]string{"EXTERNAL_INGEST_MAX_RECORDS": ""}, "", true},
		{map[string]string{"EXTERNAL_INGEST_MAX_RECORDS": "1.0"}, "", true},
	} {
		h := &handlers{getenv: func(name string) (string, bool) { value, ok := tc.env[name]; return value, ok }}
		out, err := h.limits()
		if (err != nil) != tc.fails {
			t.Fatalf("%v: err %v", tc.env, err)
		}
		if !tc.fails {
			got, _ := pyjson.Marshal(out)
			if string(got) != tc.want {
				t.Fatalf("%v: got %s", tc.env, got)
			}
		}
	}
}

// TestReadBodyLimitedAtTheMaximumLimit: EXTERNAL_INGEST_MAX_BODY_BYTES at
// int64's maximum (or past it) still reads the whole body; limit+1 would
// wrap negative and read nothing.
func TestReadBodyLimitedAtTheMaximumLimit(t *testing.T) {
	for _, limit := range []string{"9223372036854775807", "99999999999999999999"} {
		maxBytes, _ := new(big.Int).SetString(limit, 10)
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"a":1}`))
		request.ContentLength = -1
		body, err := readBodyLimited(request, maxBytes)
		if err != nil || string(body) != `{"a":1}` {
			t.Errorf("limit %s: body %q err %v, want the whole body", limit, body, err)
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"a":1}`))
	request.ContentLength = -1
	if _, err := readBodyLimited(request, big.NewInt(3)); err != errTooLarge {
		t.Errorf("limit 3: err %v, want errTooLarge", err)
	}
}
