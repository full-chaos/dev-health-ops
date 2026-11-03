package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/loadharness"
)

// The PDP load profiles (internal/loadharness/testdata/pdp) are run here against the real authorize handlers over a real
// socket, on the handler fixture's fakes, so a profile that the decoder refuses, a bearer the caller gate refuses or a
// class that decides differently than its name says fails here and not on a cluster run.

type pdpTally struct {
	mu      sync.Mutex
	allowed map[string]int
	denied  map[string]int
	reasons map[string]map[string]int
}

func (p *pdpTally) add(action string, d wireDecision) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d.Allowed {
		p.allowed[action]++
	} else {
		p.denied[action]++
	}
	if p.reasons[action] == nil {
		p.reasons[action] = map[string]int{}
	}
	p.reasons[action][d.ReasonCode]++
}

// pdpRecorder serves the fixture's handler one request at a time (the fixture's fakes keep plain counters) and tallies each
// decision by the action that was asked.
func pdpRecorder(t *testing.T, h *authorizeHandlerFixture, tally *pdpTally, delay time.Duration) http.Handler {
	t.Helper()
	var mu sync.Mutex
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		time.Sleep(delay)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.routes[r.URL.Path].ServeHTTP(rec, r)
		if rec.Code == http.StatusOK {
			tally.record(t, r.URL.Path, body, rec.Body.Bytes())
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	})
}

func (p *pdpTally) record(t *testing.T, path string, request, response []byte) {
	t.Helper()
	switch path {
	case PatternAuthorize:
		var q struct {
			Action string `json:"action"`
		}
		var d wireDecision
		if json.Unmarshal(request, &q) != nil || json.Unmarshal(response, &d) != nil {
			t.Errorf("undecodable exchange: %s -> %s", request, response)
			return
		}
		p.add(q.Action, d)
	case PatternAuthorizeBatch:
		var q struct {
			Requests []struct {
				Action string `json:"action"`
			} `json:"requests"`
		}
		var out struct {
			Decisions []struct {
				Index    int          `json:"index"`
				Decision wireDecision `json:"decision"`
			} `json:"decisions"`
		}
		if json.Unmarshal(request, &q) != nil || json.Unmarshal(response, &out) != nil || len(q.Requests) != len(out.Decisions) {
			t.Errorf("undecodable batch exchange: %s -> %s", request, response)
			return
		}
		for i, d := range out.Decisions {
			p.add(q.Requests[i].Action, d.Decision)
		}
	}
}

func (p *pdpTally) total(m map[string]int) int {
	n := 0
	for _, c := range m {
		n += c
	}
	return n
}

type pdpWant struct {
	allowed, denied []string
	auditPerCall    int
	noAudit         bool
	rate            float64
}

var pdpWants = map[string]pdpWant{
	"authorize-low":       {allowed: []string{"analytics.read"}, noAudit: true},
	"authorize-low-hot":   {allowed: []string{"analytics.read"}, noAudit: true},
	"authorize-knee":      {allowed: []string{"analytics.read"}, noAudit: true},
	"authorize-high":      {allowed: []string{"settings.write"}, auditPerCall: 1},
	"authorize-deny":      {denied: []string{"admin.users"}},
	"authorize-mix":       {rate: 80, allowed: []string{"analytics.read", "settings.write"}, denied: []string{"admin.users"}},
	"authorize-batch-10":  {rate: 8, allowed: []string{"analytics.read", "settings.write"}, denied: []string{"admin.users"}},
	"authorize-batch-100": {rate: 2, allowed: []string{"analytics.read", "settings.write"}, denied: []string{"admin.users"}},
}

// runPDPProfile runs one profile and checks its shape. The handler delay stands in for a slow host: no check here may
// depend on how long a request took (the latency bounds live in ci/pdp_load_k3s.sh).
func runPDPProfile(t *testing.T, file, name string, exp pdpWant, delay time.Duration) {
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	h := newAuthorizeHandlerFixture(t)
	env := map[string]string{
		"AUTH_LOADTEST_WORKLOAD_TOKEN":      h.workloadToken(nil),
		"AUTH_LOADTEST_PRINCIPAL_REFERENCE": h.reference,
		"AUTH_LOADTEST_ORG_ID":              h.org,
	}
	rate := exp.rate
	if rate == 0 {
		rate = 40
	}
	getenv := func(k string) string { return env[k] }
	cfg, err := loadharness.ParseConfig(raw, getenv)
	if err != nil {
		t.Fatal(err)
	}
	tally := &pdpTally{allowed: map[string]int{}, denied: map[string]int{}, reasons: map[string]map[string]int{}}
	srv := httptest.NewServer(pdpRecorder(t, h, tally, delay))
	t.Cleanup(srv.Close)
	cfg.BaseURL = srv.URL
	// The runner drops a request when its queue is full, and counts a drop as an error: one worker per scheduled request
	// means nothing is dropped however slow the host is, so the error-rate check below never reads the clock.
	duration := 2 * time.Second
	workers := int(rate * duration.Seconds())
	rep, err := loadharness.Run(context.Background(), cfg, loadharness.Options{
		Profile: loadharness.Profile{Name: name, Rate: rate, Duration: duration, Concurrency: workers},
		Getenv:  getenv,
		Client:  &http.Client{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.AssertOK {
		t.Fatalf("assertions failed: %+v %+v", rep.Assertions, rep.Paths)
	}
	var sent int
	for _, p := range rep.Paths {
		if p.Count == 0 || p.Dropped != 0 || p.Errors != 0 || p.Unexpected5xx != 0 || p.Forbidden != 0 || len(p.Statuses) != 1 || p.Statuses["200"] != p.Count {
			t.Fatalf("%s: count %d dropped %d errors %d 5xx %d forbidden %d statuses %v, want only 200", p.Name, p.Count, p.Dropped, p.Errors, p.Unexpected5xx, p.Forbidden, p.Statuses)
		}
		sent += p.Count
	}
	for _, a := range exp.allowed {
		if tally.allowed[a] == 0 || tally.denied[a] != 0 {
			t.Fatalf("action %s: allowed %d denied %d (%v), want allows only", a, tally.allowed[a], tally.denied[a], tally.reasons[a])
		}
	}
	for _, a := range exp.denied {
		if tally.denied[a] == 0 || tally.allowed[a] != 0 {
			t.Fatalf("action %s: allowed %d denied %d (%v), want denies only", a, tally.allowed[a], tally.denied[a], tally.reasons[a])
		}
	}
	if got := tally.total(tally.allowed) + tally.total(tally.denied); got < sent {
		t.Fatalf("%d decisions for %d calls", got, sent)
	}
	rows := len(h.audit.rows())
	switch {
	case exp.auditPerCall > 0 && rows != exp.auditPerCall*sent:
		t.Fatalf("%d audit rows for %d high-risk calls, want %d each", rows, sent, exp.auditPerCall)
	case exp.noAudit && rows != 0:
		t.Fatalf("%d audit rows for a low-risk profile, want 0", rows)
	}
}

func pdpProfileFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "loadharness", "testdata", "pdp", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no PDP profiles: %v", err)
	}
	if len(files) != len(pdpWants) {
		t.Fatalf("%d profiles on disk, %d with an expectation", len(files), len(pdpWants))
	}
	return files
}

func TestPDPLoadProfilesAgainstRealHandlers(t *testing.T) {
	for _, file := range pdpProfileFiles(t) {
		name := strings.TrimSuffix(filepath.Base(file), ".json")
		exp, ok := pdpWants[name]
		if !ok {
			t.Fatalf("profile %s has no expectation", name)
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runPDPProfile(t, file, name, exp, 0)
		})
	}
}

// A slow host (here: a fixed delay per request) must not fail a profile whose shape is right.
func TestPDPLoadProfileShapeDoesNotDependOnLatency(t *testing.T) {
	t.Parallel()
	file := filepath.Join("..", "..", "loadharness", "testdata", "pdp", "authorize-mix.json")
	runPDPProfile(t, file, "authorize-mix", pdpWants["authorize-mix"], 50*time.Millisecond)
}
