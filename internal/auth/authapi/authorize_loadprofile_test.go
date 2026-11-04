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
func pdpRecorder(t *testing.T, h *authorizeHandlerFixture, tally *pdpTally) http.Handler {
	t.Helper()
	var mu sync.Mutex
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
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

func TestPDPLoadProfilesAgainstRealHandlers(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "loadharness", "testdata", "pdp", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no PDP profiles: %v", err)
	}
	type want struct {
		allowed, denied []string
		auditPerCall    int
		noAudit         bool
		rate            float64
	}
	wants := map[string]want{
		"authorize-low":       {allowed: []string{"analytics.read"}, noAudit: true},
		"authorize-low-hot":   {allowed: []string{"analytics.read"}, noAudit: true},
		"authorize-knee":      {allowed: []string{"analytics.read"}, noAudit: true},
		"authorize-high":      {allowed: []string{"settings.write"}, auditPerCall: 1},
		"authorize-deny":      {denied: []string{"admin.users"}},
		"authorize-mix":       {rate: 80, allowed: []string{"analytics.read", "settings.write"}, denied: []string{"admin.users"}},
		"authorize-batch-10":  {rate: 8, allowed: []string{"analytics.read", "settings.write"}, denied: []string{"admin.users"}},
		"authorize-batch-100": {rate: 2, allowed: []string{"analytics.read", "settings.write"}, denied: []string{"admin.users"}},
	}
	if len(files) != len(wants) {
		t.Fatalf("%d profiles on disk, %d with an expectation", len(files), len(wants))
	}
	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".json")
		exp, ok := wants[name]
		if !ok {
			t.Fatalf("profile %s has no expectation", name)
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
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
			relaxDropLimits(&cfg, raceEnabled)
			tally := &pdpTally{allowed: map[string]int{}, denied: map[string]int{}, reasons: map[string]map[string]int{}}
			srv := httptest.NewServer(pdpRecorder(t, h, tally))
			t.Cleanup(srv.Close)
			cfg.BaseURL = srv.URL
			rep, err := loadharness.Run(context.Background(), cfg, loadharness.Options{
				Profile: loadharness.Profile{Name: name, Rate: rate, Duration: 2 * time.Second, Concurrency: 4},
				Getenv:  getenv,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !rep.AssertOK {
				t.Fatalf("assertions failed: %+v", rep.Paths)
			}
			var sent int
			for _, p := range rep.Paths {
				if p.Count == 0 || p.Errors != 0 || p.Unexpected5xx != 0 || p.Forbidden != 0 || len(p.Statuses) != 1 || p.Statuses["200"] != p.Count {
					t.Fatalf("%s: count %d errors %d 5xx %d forbidden %d statuses %v, want only 200", p.Name, p.Count, p.Errors, p.Unexpected5xx, p.Forbidden, p.Statuses)
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
		})
	}
}

// relaxDropLimits clears each target's max_error_rate when race is true. The harness counts a job it had to drop (its
// queue was full because the handler answered slowly) as an error, so under -race on a shared runner the limit fails on a
// slow answer and not on a wrong one. The cell still fails on a failed request (Errors), a 5xx, a forbidden status and an
// unexpected status; the latency and drop SLO stays with the k3s run (ci/pdp_load_k3s.sh).
func relaxDropLimits(cfg *loadharness.Config, race bool) {
	if !race {
		return
	}
	for i := range cfg.Targets {
		cfg.Targets[i].MaxErrorRate = nil
	}
}

func TestPDPLoadRaceRelaxesOnlyTheDropLimit(t *testing.T) {
	run := func(t *testing.T, status int, race bool) loadharness.Report {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(30 * time.Millisecond)
			w.WriteHeader(status)
		}))
		t.Cleanup(srv.Close)
		raw := `{"base_url":"` + srv.URL + `","targets":[{"name":"slow","method":"GET","path":"/x","weight":1,` +
			`"expect_status":[200],"forbid_5xx":true,"max_error_rate":0}]}`
		cfg, err := loadharness.ParseConfig([]byte(raw), func(string) string { return "" })
		if err != nil {
			t.Fatal(err)
		}
		relaxDropLimits(&cfg, race)
		rep, err := loadharness.Run(context.Background(), cfg, loadharness.Options{
			Profile: loadharness.Profile{Name: "slow", Rate: 200, Duration: time.Second, Concurrency: 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		return rep
	}
	t.Run("race-slow-answers-pass", func(t *testing.T) {
		rep := run(t, http.StatusOK, true)
		if p := rep.Paths[0]; p.Dropped == 0 || p.Errors != 0 {
			t.Fatalf("precondition: dropped %d errors %d, want drops and no errors", p.Dropped, p.Errors)
		}
		if !rep.AssertOK {
			t.Fatalf("race build: assertions failed on drops: %+v", rep.Assertions)
		}
	})
	t.Run("race-5xx-still-fails", func(t *testing.T) {
		if rep := run(t, http.StatusInternalServerError, true); rep.AssertOK {
			t.Fatalf("race build: a 5xx answer passed: %+v", rep.Assertions)
		}
	})
	t.Run("norace-drops-fail", func(t *testing.T) {
		if rep := run(t, http.StatusOK, false); rep.AssertOK {
			t.Fatalf("non-race build: drops passed the limit: %+v", rep.Assertions)
		}
	})
}
