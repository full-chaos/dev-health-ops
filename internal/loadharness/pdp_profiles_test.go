package loadharness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	pdpAuthorize      = "/v1/authorize"
	pdpAuthorizeBatch = "/v1/authorize:batch"
)

var pdpProfileNames = []string{
	"authorize-batch-10", "authorize-batch-100", "authorize-deny", "authorize-high",
	"authorize-knee", "authorize-low", "authorize-low-hot", "authorize-mix",
}

func pdpGetenv(name string) string { return "x-" + name }

func loadPDPProfile(t *testing.T, name string) Config {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "pdp", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "eyJ") {
		t.Fatalf("%s holds a JWT-looking literal", name)
	}
	cfg, err := ParseConfig(raw, pdpGetenv)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return cfg
}

func TestPDPProfileSet(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join("testdata", "pdp", "*.json"))
	var got []string
	for _, f := range files {
		got = append(got, strings.TrimSuffix(filepath.Base(f), ".json"))
	}
	slices.Sort(got)
	if !slices.Equal(got, pdpProfileNames) {
		t.Fatalf("profiles %v, want %v", got, pdpProfileNames)
	}
}

// A run that gates the PDP must fail on any status but 200 and on any 5xx: a 401 from an expired workload token or a 429
// from the route limiter must never pass as a fast answer. The knee profile is the one recorded run that expects 429.
func TestPDPProfilesFailOnAnythingButAnAnswer(t *testing.T) {
	for _, name := range pdpProfileNames {
		cfg := loadPDPProfile(t, name)
		if err := CheckHost(cfg.BaseURL, nil); err != nil {
			t.Fatalf("%s: default base url is not a local host: %v", name, err)
		}
		for _, tg := range cfg.Targets {
			if tg.Method != "POST" || (tg.Path != pdpAuthorize && tg.Path != pdpAuthorizeBatch) {
				t.Fatalf("%s/%s: %s %s is not a PDP route", name, tg.Name, tg.Method, tg.Path)
			}
			if (tg.Path == pdpAuthorizeBatch) != strings.HasPrefix(tg.Name, "authorize-batch") {
				t.Fatalf("%s/%s: the name and the route disagree (%s)", name, tg.Name, tg.Path)
			}
			if !tg.Forbid5xx {
				t.Fatalf("%s/%s: forbid_5xx is off", name, tg.Name)
			}
			bearer, ok := tg.HeaderEnv["Authorization"]
			if !ok || bearer.Env != "AUTH_LOADTEST_WORKLOAD_TOKEN" || bearer.Prefix != "Bearer " || len(tg.Headers) != 1 {
				t.Fatalf("%s/%s: the bearer must come from AUTH_LOADTEST_WORKLOAD_TOKEN only: %+v %+v", name, tg.Name, tg.HeaderEnv, tg.Headers)
			}
			if name == "authorize-knee" {
				if !slices.Equal(tg.ExpectStatus, []int{200, 429}) || tg.MaxErrorRate != nil {
					t.Fatalf("%s/%s: the knee run records 200 and 429: %+v", name, tg.Name, tg)
				}
				continue
			}
			if !slices.Equal(tg.ExpectStatus, []int{200}) || tg.MaxErrorRate == nil || *tg.MaxErrorRate != 0 {
				t.Fatalf("%s/%s: a gated run expects exactly 200 and max_error_rate 0: %+v", name, tg.Name, tg)
			}
		}
	}
}

func pdpBody(t *testing.T, tg Target, seq int) []byte {
	t.Helper()
	body, err := expand(tg.Body, seq, pdpGetenv)
	if err != nil {
		t.Fatalf("%s: %v", tg.Name, err)
	}
	return []byte(body)
}

type pdpQuestion struct {
	Action   string `json:"action"`
	Resource struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	} `json:"resource"`
	Context struct {
		RequestID string `json:"request_id"`
	} `json:"context"`
	Reference string `json:"principal_reference"`
}

var pdpRequestID = regexp.MustCompile(`^req_[A-Za-z0-9_-]{26}$`)

func TestPDPProfileBodiesCarryTheClassTheyNameAndTheCacheShape(t *testing.T) {
	classes := map[string]struct {
		action, rtype string
		keyVaries     bool
	}{
		"authorize-low":     {"analytics.read", "organization", true},
		"authorize-low-hot": {"analytics.read", "organization", false},
		"authorize-high":    {"settings.write", "organization", true},
		"authorize-deny":    {"admin.users", "platform", false},
		"authorize-knee":    {"analytics.read", "organization", true},
	}
	for _, name := range pdpProfileNames {
		cfg := loadPDPProfile(t, name)
		for _, tg := range cfg.Targets {
			if strings.HasPrefix(tg.Name, "authorize-batch") {
				continue
			}
			want, ok := classes[tg.Name]
			if !ok {
				t.Fatalf("%s: target %q has no class", name, tg.Name)
			}
			var q0, q1 pdpQuestion
			if err := json.Unmarshal(pdpBody(t, tg, 0), &q0); err != nil {
				t.Fatalf("%s/%s: %v", name, tg.Name, err)
			}
			if err := json.Unmarshal(pdpBody(t, tg, 1), &q1); err != nil {
				t.Fatal(err)
			}
			if q0.Action != want.action || q0.Resource.Type != want.rtype {
				t.Fatalf("%s/%s: %s on %s, want %s on %s", name, tg.Name, q0.Action, q0.Resource.Type, want.action, want.rtype)
			}
			if varies := q0.Resource.Key != q1.Resource.Key; varies != want.keyVaries {
				t.Fatalf("%s/%s: key varies per request = %v, want %v", name, tg.Name, varies, want.keyVaries)
			}
			if !pdpRequestID.MatchString(q0.Context.RequestID) || q0.Reference != "x-AUTH_LOADTEST_PRINCIPAL_REFERENCE" {
				t.Fatalf("%s/%s: request id %q reference %q", name, tg.Name, q0.Context.RequestID, q0.Reference)
			}
		}
	}
	mix := loadPDPProfile(t, "authorize-mix")
	var weights []int
	for _, tg := range mix.Targets {
		weights = append(weights, tg.Weight)
	}
	if !slices.Equal(weights, []int{70, 10, 10, 10}) {
		t.Fatalf("mix weights %v, want 70/10/10/10", weights)
	}
}

func TestPDPBatchProfilesHoldTheirItemCounts(t *testing.T) {
	for name, want := range map[string]int{"authorize-batch-10": 10, "authorize-batch-100": 100} {
		tg := loadPDPProfile(t, name).Targets[0]
		body := pdpBody(t, tg, 0)
		var doc struct {
			Schema   string        `json:"schema_version"`
			Requests []pdpQuestion `json:"requests"`
		}
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatal(err)
		}
		if doc.Schema != "authorization_batch_request.v1" || len(doc.Requests) != want {
			t.Fatalf("%s: schema %q with %d items, want %d", name, doc.Schema, len(doc.Requests), want)
		}
		if len(body) > 256<<10 {
			t.Fatalf("%s: body is %d bytes, the route takes at most 256 KiB", name, len(body))
		}
	}
}

// The ticket line: p95 < 25 ms and p99 < 75 ms. ApplySLOs holds a limit as "at most", so the ticket numbers pass at the
// limit and fail one tick above it.
func TestPDPSLOWiring(t *testing.T) {
	slos, err := ParseSLOs([]string{"authorize-low:p95=25ms,p99=75ms,err=0"})
	if err != nil {
		t.Fatal(err)
	}
	report := func(samples ...time.Duration) Report {
		return BuildReport(ReportInput{Profile: "normal", Duration: time.Second, Rate: 1, Concurrency: 1,
			Paths: []PathResult{{Name: "authorize-low", Latencies: samples}}})
	}
	fill := func(n int, d time.Duration) []time.Duration {
		out := make([]time.Duration, n)
		for i := range out {
			out[i] = d
		}
		return out
	}
	if r := ApplySLOs(report(fill(100, 24*time.Millisecond)...), slos); !r.SLOOK {
		t.Fatalf("24 ms must pass: %+v", r.SLOChecks)
	}
	if r := ApplySLOs(report(fill(100, 26*time.Millisecond)...), slos); r.SLOOK {
		t.Fatalf("26 ms must fail the p95 limit: %+v", r.SLOChecks)
	}
	tail := append(fill(97, 10*time.Millisecond), fill(3, 80*time.Millisecond)...)
	if r := ApplySLOs(report(tail...), slos); r.SLOOK {
		t.Fatalf("a p99 of 80 ms must fail: %+v", r.SLOChecks)
	}
}
