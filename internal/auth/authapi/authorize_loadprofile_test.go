package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
			sent, failure := pdpReportFailure(rep, raceEnabled)
			if failure != "" {
				t.Fatal(failure)
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

// pdpReportFailure judges one load report. Under the race detector a request takes about a second, so the harness drops
// requests and the max_error_rate assertion (errors plus drops) fails on speed alone; the real load budget is the k3s run.
// Under race only that assertion is skipped; every other assertion and the per-path checks (errors, 5xx, status) stay.
func pdpReportFailure(rep loadharness.Report, race bool) (int, string) {
	for _, c := range rep.Assertions {
		if !c.OK && !(race && c.Name == "max_error_rate") {
			return 0, fmt.Sprintf("assertion %s/%s failed: %s", c.Path, c.Name, c.Detail)
		}
	}
	var sent int
	for _, p := range rep.Paths {
		if p.Count == 0 || p.Errors != 0 || p.Unexpected5xx != 0 || p.Forbidden != 0 || len(p.Statuses) != 1 || p.Statuses["200"] != p.Count {
			return 0, fmt.Sprintf("%s: count %d errors %d 5xx %d forbidden %d statuses %v, want only 200", p.Name, p.Count, p.Errors, p.Unexpected5xx, p.Forbidden, p.Statuses)
		}
		sent += p.Count
	}
	return sent, ""
}

func pdpGoodReport() loadharness.Report {
	return loadharness.Report{
		AssertOK: true,
		Paths:    []loadharness.PathReport{{Name: "authorize-low", Count: 10, Statuses: map[string]int{"200": 10}}},
		Assertions: []loadharness.AssertionCheck{
			{Path: "authorize-low", Name: "forbid_5xx", OK: true},
			{Path: "authorize-low", Name: "max_error_rate", OK: true},
		},
	}
}

func pdpFailCheck(rep loadharness.Report, name string) loadharness.Report {
	for i := range rep.Assertions {
		if rep.Assertions[i].Name == name {
			rep.Assertions[i].OK = false
		}
	}
	rep.AssertOK = false
	return rep
}

func TestPDPAssertionGate(t *testing.T) {
	dropped := func() loadharness.Report {
		r := pdpFailCheck(pdpGoodReport(), "max_error_rate")
		r.Paths[0].Dropped = 8
		return r
	}
	t.Run("good_report_passes_in_both", func(t *testing.T) {
		for _, race := range []bool{false, true} {
			if _, f := pdpReportFailure(pdpGoodReport(), race); f != "" {
				t.Fatalf("race=%v: %s", race, f)
			}
		}
	})
	t.Run("dropped_fails_without_race", func(t *testing.T) {
		if _, f := pdpReportFailure(dropped(), false); f == "" {
			t.Fatal("a report with dropped requests passed without the race detector")
		}
	})
	t.Run("dropped_passes_with_race", func(t *testing.T) {
		if _, f := pdpReportFailure(dropped(), true); f != "" {
			t.Fatalf("dropped requests failed under race: %s", f)
		}
	})
	t.Run("5xx_assertion_fails_with_race", func(t *testing.T) {
		if _, f := pdpReportFailure(pdpFailCheck(pdpGoodReport(), "forbid_5xx"), true); f == "" {
			t.Fatal("a failed forbid_5xx assertion passed under race")
		}
	})
	t.Run("forbid_status_assertion_fails_with_race", func(t *testing.T) {
		r := pdpGoodReport()
		r.Assertions = append(r.Assertions, loadharness.AssertionCheck{Path: "authorize-low", Name: "forbid_status"})
		if _, f := pdpReportFailure(r, true); f == "" {
			t.Fatal("a failed forbid_status assertion passed under race")
		}
	})
	t.Run("errors_fail_with_race_through_path_loop", func(t *testing.T) {
		r := dropped()
		r.Paths[0].Errors = 1
		if _, f := pdpReportFailure(r, true); f == "" {
			t.Fatal("a path with Errors>0 passed under race")
		}
	})
	t.Run("unexpected_5xx_count_fails_with_race", func(t *testing.T) {
		r := dropped()
		r.Paths[0].Unexpected5xx = 1
		if _, f := pdpReportFailure(r, true); f == "" {
			t.Fatal("a path with Unexpected5xx>0 passed under race")
		}
	})
	t.Run("build_tag_wiring", func(t *testing.T) {
		_, f := pdpReportFailure(dropped(), raceEnabled)
		if raceEnabled && f != "" {
			t.Fatalf("race build failed on drops: %s", f)
		}
		if !raceEnabled && f == "" {
			t.Fatal("non-race build passed a dropped report")
		}
	})
}
