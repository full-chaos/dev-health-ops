package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// This file holds the whole input domain of one decision: after a bounded
// candidate search sends a candidate, does the search take that candidate
// as its result (win or refusal) or move on to the next one? An operator
// relies on it so that a repository/person with no rows is skipped, and
// so that nothing else -- a difference, a plane failure, a malformed body
// -- is ever replaced by a later clean match.

// legReply is one leg's answer for the first candidate of a cell.
type legReply struct {
	status int
	body   string
	stall  bool
}

func ok(body string) legReply { return legReply{status: http.StatusOK, body: body} }

// domainServer answers path "/things/{id}". The first candidate (p-1)
// answers the cell's reply; every other candidate answers second.
func domainServer(t *testing.T, build string, first, second legReply) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if build != "" {
			w.Header().Set("x-dev-health-build", build)
		} else {
			// The baseline role: the Python app's positive identity.
			w.Header().Set("Server", goapiproof.ReferencePlaneServer)
		}
		id := strings.TrimPrefix(r.URL.Path, "/things/")
		mu.Lock()
		seen = append(seen, id)
		mu.Unlock()
		reply := second
		if id == "p-1" {
			reply = first
		}
		if reply.stall {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(reply.status)
		_, _ = w.Write([]byte(reply.body))
	}))
	t.Cleanup(srv.Close)
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
	return srv, snapshot
}

type domainCell struct {
	name                string
	candidate, baseline legReply
	// want is one of: "win-match", "win-mismatch", "next", "stop:<refusal>".
	want string
}

func runDomainCell(t *testing.T, statusOnly bool, cell domainCell, second legReply, secondBaseline legReply) (string, int, []string) {
	t.Helper()
	const build = "abc123def456"
	candidate, seen := domainServer(t, build, cell.candidate, second)
	baseline, _ := domainServer(t, "", cell.baseline, secondBaseline)
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 200 * time.Millisecond}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/things/{thing_id}"}
	binding := goapiproof.RESTIDBinding{Producer: "thing_id", PathParam: "thing_id", Candidates: 10, ExposeAs: "chosen_thing_id"}
	request := goapiproof.RESTRequest{
		Name: "things_default", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode:   goapiproof.RESTBodyModeJSON,
		IDBindings: []goapiproof.RESTIDBinding{binding},
		Produces:   []goapiproof.RESTIDProducer{{Name: "item_id", ListPath: "items", IDField: "id"}},
		Parity: goapiproof.Options{BaselineDefects: []goapiproof.BaselineDefect{
			{Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.items.unused_field"}},
		}},
	}
	if statusOnly {
		request.WantBaselineStatus = 503
		request.StatusDivergenceReason = "test fixture"
		request.BodyMode = goapiproof.RESTBodyModeStatusOnly
		request.Parity = goapiproof.Options{}
	}
	writer := &fakeReceiptWriter{}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/things/{thing_id}",
		spec, request, binding, map[string]string{}, map[string][]string{"thing_id": {"p-1", "p-2"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	got := ""
	switch {
	case attempt.out.Admitted && attempt.out.producedIDs["chosen_thing_id"] == "p-1":
		got = "win-" + attempt.out.TerminalState
	case attempt.out.Admitted && attempt.out.producedIDs["chosen_thing_id"] == "p-2":
		got = "next"
	case !attempt.out.Admitted:
		got = "stop:" + attempt.out.Refusal
	default:
		got = "unexpected"
	}
	return got, len(writer.receipts), seen()
}

const (
	goodJSON = `{"label":"x","items":[{"id":"ABC-123"}]}`
)

// TestIterationDecision_JSONBodyModeInputDomain runs every JSON-body cell
// in one pass and prints the executed table.
func TestIterationDecision_JSONBodyModeInputDomain(t *testing.T) {
	cells := []domainCell{
		{"match, id present", ok(goodJSON), ok(goodJSON), "win-match"},
		{"mismatch, id present", ok(`{"label":"y","items":[{"id":"ABC-123"}]}`), ok(goodJSON), "win-mismatch"},
		{"empty list both legs, clean match", ok(`{"label":"x","items":[]}`), ok(`{"label":"x","items":[]}`), "next"},
		{"empty list both legs, other field differs", ok(`{"label":"y","items":[]}`), ok(`{"label":"x","items":[]}`), "win-mismatch"},
		{"empty on baseline only", ok(goodJSON), ok(`{"label":"x","items":[]}`), "win-mismatch"},
		{"empty on candidate only", ok(`{"label":"x","items":[]}`), ok(goodJSON), "win-mismatch"},
		{"vacuous both legs", ok(`{"items":[]}`), ok(`{"items":[]}`), "next"},
		{"vacuous both legs, list null on the candidate", ok(`{"items":null}`), ok(`{"items":[]}`), "win-mismatch"},
		{"vacuous both legs, list null on the baseline", ok(`{"items":[]}`), ok(`{"items":null}`), "win-mismatch"},
		{"vacuous both legs, list absent on the candidate", ok(`{"other":[]}`), ok(`{"items":[]}`), "win-mismatch"},
		{"vacuous both legs, lists empty, other zero-leaf field differs", ok(`{"items":[],"meta":[]}`), ok(`{"items":[],"meta":{}}`), "win-mismatch"},
		{"vacuous both legs, identical, non-empty but leafless", ok(`{"items":[],"meta":{"tags":[]}}`), ok(`{"items":[],"meta":{"tags":[]}}`), "next"},
		{"vacuous both legs, identical, list null", ok(`{"items":null}`), ok(`{"items":null}`), "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"candidate 404", legReply{status: 404, body: `{"detail":"x"}`}, ok(goodJSON), "stop:" + goapiproof.RESTRefusalUnexpectedStatus},
		{"candidate 500", legReply{status: 500, body: `{"detail":"x"}`}, ok(goodJSON), "stop:" + goapiproof.RESTRefusalUnexpectedStatus},
		{"baseline 404", ok(goodJSON), legReply{status: 404, body: `{"detail":"x"}`}, "stop:" + goapiproof.RESTRefusalUnexpectedStatus},
		{"baseline 500", ok(goodJSON), legReply{status: 500, body: `{"detail":"x"}`}, "stop:" + goapiproof.RESTRefusalUnexpectedStatus},
		{"candidate timeout", legReply{stall: true}, ok(goodJSON), "stop:" + goapiproof.RESTRefusalCandidateLegTimedOut},
		{"baseline timeout", ok(goodJSON), legReply{stall: true}, "stop:" + goapiproof.RESTRefusalBaselineLegTimedOut},
		{"candidate not JSON", ok(`not-json`), ok(goodJSON), "stop:" + goapiproof.RESTRefusalBodyNotJSON},
		{"baseline not JSON", ok(goodJSON), ok(`not-json`), "stop:" + goapiproof.RESTRefusalBodyNotJSON},
		{"candidate trailing bytes", ok(`{"label":"x","items":[]}garbage`), ok(`{"label":"x","items":[]}`), "stop:" + goapiproof.RESTRefusalTrailingBytes},
		{"baseline trailing bytes", ok(`{"label":"x","items":[]}`), ok(`{"label":"x","items":[]}garbage`), "stop:" + goapiproof.RESTRefusalTrailingBytes},
		{"elements without the id, both legs", ok(`{"label":"x","items":[{"other":"ABC-123"}]}`), ok(`{"label":"x","items":[{"other":"ABC-123"}]}`), "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"empty id, both legs", ok(`{"label":"x","items":[{"id":""}]}`), ok(`{"label":"x","items":[{"id":""}]}`), "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"list wrong type, both legs", ok(`{"label":"x","items":"x"}`), ok(`{"label":"x","items":"x"}`), "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"list null, both legs", ok(`{"label":"x","items":null}`), ok(`{"label":"x","items":null}`), "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"list absent, both legs", ok(`{"label":"x"}`), ok(`{"label":"x"}`), "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"error object, both legs", ok(`{"detail":"backend failed"}`), ok(`{"detail":"backend failed"}`), "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
	}
	var table []string
	for _, cell := range cells {
		got, receipts, seen := runDomainCell(t, false, cell, ok(goodJSON), ok(goodJSON))
		table = append(table, cell.name+" | "+got)
		if got != cell.want {
			t.Errorf("%s: got %s, want %s", cell.name, got, cell.want)
			continue
		}
		assertCellConsistent(t, cell.name, got, receipts, seen)
	}
	t.Logf("JSON body mode:\n%s", strings.Join(table, "\n"))
}

// TestIterationDecision_StatusOnlyInputDomain is the status-only twin:
// the baseline plane is declared failing (503), the declared id is read
// from the candidate leg.
func TestIterationDecision_StatusOnlyInputDomain(t *testing.T) {
	down := legReply{status: http.StatusServiceUnavailable, body: `{"detail":"Data unavailable"}`}
	cells := []domainCell{
		{"candidate 200 with id", ok(goodJSON), down, "win-match"},
		{"candidate empty list", ok(`{"items":[]}`), down, "next"},
		{"candidate 404", legReply{status: 404, body: `{"detail":"x"}`}, down, "stop:" + goapiproof.RESTRefusalUnexpectedStatus},
		{"candidate 500", legReply{status: 500, body: `{"detail":"x"}`}, down, "stop:" + goapiproof.RESTRefusalUnexpectedStatus},
		{"baseline answers 200 instead of 503", ok(goodJSON), ok(goodJSON), "stop:" + goapiproof.RESTRefusalUnexpectedStatus},
		{"candidate timeout", legReply{stall: true}, down, "stop:" + goapiproof.RESTRefusalCandidateLegTimedOut},
		{"baseline timeout", ok(goodJSON), legReply{stall: true}, "stop:" + goapiproof.RESTRefusalBaselineLegTimedOut},
		{"candidate not JSON", ok(`not-json`), down, "stop:" + goapiproof.RESTRefusalCandidateBodyUndecodable},
		{"candidate trailing bytes", ok(`{"items":[]}garbage`), down, "stop:" + goapiproof.RESTRefusalTrailingBytes},
		{"elements without the id", ok(`{"items":[{"other":"x"}]}`), down, "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"list wrong type", ok(`{"items":"x"}`), down, "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"list null", ok(`{"items":null}`), down, "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
		{"list absent", ok(`{"detail":"x"}`), down, "stop:" + goapiproof.RESTRefusalDeclaredIDListUnrecognised},
	}
	var table []string
	for _, cell := range cells {
		got, receipts, seen := runDomainCell(t, true, cell, ok(goodJSON), down)
		table = append(table, cell.name+" | "+got)
		if got != cell.want {
			t.Errorf("%s: got %s, want %s", cell.name, got, cell.want)
			continue
		}
		assertCellConsistent(t, cell.name, got, receipts, seen)
	}
	t.Logf("status-only mode:\n%s", strings.Join(table, "\n"))
}

// TestIterationDecision_ExhaustionIsNamedWithEveryAttempt covers the last
// outcome of the domain in both modes: every candidate has no data.
func TestIterationDecision_ExhaustionIsNamedWithEveryAttempt(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusOnly bool
		empty      legReply
		baseline   legReply
	}{
		{"JSON body mode", false, ok(`{"label":"x","items":[]}`), ok(`{"label":"x","items":[]}`)},
		{"status-only", true, ok(`{"items":[]}`), legReply{status: http.StatusServiceUnavailable, body: `{"detail":"x"}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, receipts, seen := runDomainCell(t, tc.statusOnly, domainCell{candidate: tc.empty, baseline: tc.baseline}, tc.empty, tc.baseline)
			if got != "stop:"+goapiproof.RESTRefusalCandidateIterationExhausted || receipts != 0 || len(seen) != 2 {
				t.Fatalf("got %s with %d receipts, candidates tried %v; want exhaustion, 0 receipts, both tried", got, receipts, seen)
			}
		})
	}
}

func assertCellConsistent(t *testing.T, name, got string, receipts int, seen []string) {
	t.Helper()
	switch {
	case got == "next":
		if receipts != 1 || len(seen) != 2 {
			t.Errorf("%s: next -> %d receipts, tried %v; want the second candidate's one receipt, both tried", name, receipts, seen)
		}
	case strings.HasPrefix(got, "win-"):
		if receipts != 1 || len(seen) != 1 {
			t.Errorf("%s: win -> %d receipts, tried %v; want 1 receipt, only p-1 tried", name, receipts, seen)
		}
	default:
		if receipts != 0 || len(seen) != 1 {
			t.Errorf("%s: stop -> %d receipts, tried %v; want 0 receipts, only p-1 tried", name, receipts, seen)
		}
	}
}

// TestRunMeasurement_RepositoryScopedCasesBindARepositoryWithData drives
// runMeasurement over the REAL, full corpus the way an operator runs it:
// filters/options lists an empty repository first and one with data
// second. The flow repo_scoped search skips the empty repository, the
// sunburst repo_scoped case binds the one with data, the work-units
// repo_scoped search does the same, and the explain case explains that
// search's unit inside that same repository.
func TestRunMeasurement_RepositoryScopedCasesBindARepositoryWithData(t *testing.T) {
	const build = "build123"
	flowEmptyBaseline := readProveFixture(t, "investmentflow_reposcoped_emptyrepo_baseline_acc5f56b.json")
	flowEmptyCandidate := readProveFixture(t, "investmentflow_reposcoped_emptyrepo_candidate_2a7a073a.json")
	flowData := readProveFixture(t, "investmentflow_dynamicorg_baseline_1988f415.json")
	handler := func(candidate bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if candidate {
				w.Header().Set("x-dev-health-build", build)
			}
			raw, _ := io.ReadAll(r.Body)
			body := string(raw)
			switch {
			case r.URL.Path == "/api/v1/filters/options":
				_, _ = w.Write([]byte(`{"teams":["team-a"],"repos":["repo-empty","repo-data"]}`))
			case r.URL.Path == "/api/v1/investment/flow" && strings.Contains(body, `"level":"repo"`):
				switch {
				case strings.Contains(body, "repo-empty") && candidate:
					_, _ = w.Write([]byte(flowEmptyCandidate))
				case strings.Contains(body, "repo-empty"):
					_, _ = w.Write([]byte(flowEmptyBaseline))
				default:
					_, _ = w.Write([]byte(flowData))
				}
			case r.URL.Path == "/api/v1/work-units" && r.URL.Query().Get("scope_type") == "repo":
				if r.URL.Query().Get("scope_id") == "repo-data" {
					_, _ = w.Write([]byte(`[{"work_unit_id":"ABC-123"}]`))
					return
				}
				_, _ = w.Write([]byte(`[]`))
			default:
				_, _ = w.Write([]byte(`{}`))
			}
		}
	}
	candidate := httptest.NewServer(handler(true))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(handler(false)))
	defer baseline.Close()

	dir := t.TempDir()
	reportPath := dir + "/report.json"
	artifacts, err := goapiproof.NewArtifactStore(dir + "/artifacts")
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	f := flags{
		queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL,
		org: "org-1", recordedBy: "chris", reviewEvidence: "test",
		timeout: 5 * time.Second, dryRun: true, reportPath: reportPath,
	}
	_ = captureStdout(t, func() {
		_ = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	find := func(op, name string) *outcome {
		for i := range report.Outcomes {
			if report.Outcomes[i].Operation == op && report.Outcomes[i].Request == name {
				return &report.Outcomes[i]
			}
		}
		t.Fatalf("report has no outcome for %s/%s", op, name)
		return nil
	}
	flow := find("REST:POST:/api/v1/investment/flow", "repo_scoped")
	if flow.BoundIDs["repo_id"] != "repo-data" || len(flow.Attempts) != 1 || flow.Attempts[0].CandidateID != "repo-empty" {
		t.Fatalf("flow repo_scoped = %+v, want repo-data bound after skipping repo-empty", flow)
	}
	sunburst := find("REST:GET:/api/v1/investment/sunburst", "repo_scoped")
	if sunburst.BoundIDs["investment_repo_id"] != "repo-data" {
		t.Fatalf("sunburst repo_scoped bound = %v, want investment_repo_id=repo-data", sunburst.BoundIDs)
	}
	units := find("REST:GET:/api/v1/work-units", "repo_scoped")
	if units.BoundIDs["repo_id"] != "repo-data" || len(units.Attempts) != 1 || units.Attempts[0].CandidateID != "repo-empty" {
		t.Fatalf("work-units repo_scoped = %+v, want repo-data bound after skipping repo-empty", units)
	}
	explain := find("REST:POST:/api/v1/work-units/{work_unit_id}/explain", "repo_scoped_live_work_unit")
	if explain.Refusal == goapiproof.RESTRefusalIDBindingUnresolved || explain.BoundIDs["work_units_repo_id"] != "repo-data" || explain.BoundIDs["work_unit_id_repo_scoped"] != "ABC-123" {
		t.Fatalf("explain repo_scoped_live_work_unit = %+v, want ABC-123 explained inside repo-data", explain)
	}
	t.Logf("flow=%v attempts=%v | sunburst=%v | work-units=%v attempts=%v | explain=%v refusal=%q",
		flow.BoundIDs, flow.Attempts, sunburst.BoundIDs, units.BoundIDs, units.Attempts, explain.BoundIDs, explain.Refusal)
}

// bodyWithList builds a JSON body carrying, at each producer's ListPath
// ("" means the body IS the list), either an empty list (withElement
// false) or one element carrying every declared IDField/JoinField of the
// producers that share that ListPath.
func bodyWithList(producers []goapiproof.RESTIDProducer, withElement bool) string {
	byPath := map[string]map[string]any{}
	var order []string
	bareString := false
	for _, p := range producers {
		if _, seen := byPath[p.ListPath]; !seen {
			byPath[p.ListPath] = map[string]any{}
			order = append(order, p.ListPath)
		}
		if p.IDField == "" {
			bareString = true
			continue
		}
		byPath[p.ListPath][p.IDField] = "ABC-123"
		if p.JoinField != "" {
			byPath[p.ListPath][p.JoinField] = 7
		}
	}
	listFor := func(path string) []any {
		if !withElement {
			return []any{}
		}
		if bareString && len(byPath[path]) == 0 {
			return []any{"ABC-123"}
		}
		return []any{byPath[path]}
	}
	root := map[string]any{}
	var out any = root
	for _, path := range order {
		if path == "" {
			out = listFor(path)
			continue
		}
		segments := strings.Split(path, ".")
		node := root
		for _, seg := range segments[:len(segments)-1] {
			next, ok := node[seg].(map[string]any)
			if !ok {
				next = map[string]any{}
				node[seg] = next
			}
			node = next
		}
		node[segments[len(segments)-1]] = listFor(path)
	}
	raw, _ := json.Marshal(out)
	return string(raw)
}

// TestIterationDecision_EveryCorpusSearchSkipsNoDataAndStopsOnFailure is
// the class sweep: it finds EVERY bounded-candidate binding in the real
// corpus at run time (so a new one is covered without editing this test)
// and executes these cells on each real request: a first candidate that
// is the same empty answer on both legs ([] vs [], or identical and
// leafless) moves the search to the second candidate; a candidate-leg
// HTTP 500, a declared list null or absent on the candidate leg only,
// end the search on the first candidate.
func TestIterationDecision_EveryCorpusSearchSkipsNoDataAndStopsOnFailure(t *testing.T) {
	const build = "abc123def456"
	swept := 0
	for _, operation := range goapiproof.RESTRunOrder() {
		spec, err := goapiproof.SpecForREST(operation)
		if err != nil {
			t.Fatalf("SpecForREST(%s): %v", operation, err)
		}
		for _, request := range spec.Requests {
			binding, iterating := findIteratingBinding(request.IDBindings)
			if !iterating {
				continue
			}
			swept++
			emptyBody := bodyWithList(request.Produces, false)
			dataBody := bodyWithList(request.Produces, true)
			leafless := emptyBody
			if strings.HasPrefix(emptyBody, "{") {
				leafless = strings.TrimSuffix(emptyBody, "}") + `,"meta":{"tags":[]}}`
			}
			for _, cell := range []struct {
				name      string
				firstCand legReply
				firstBase string
				want      string
			}{
				{"no data ([] vs [])", ok(emptyBody), emptyBody, "next"},
				{"identical, non-empty but leafless", ok(leafless), leafless, "next"},
				{"candidate 500", legReply{status: 500, body: `{"detail":"x"}`}, emptyBody, "stop:" + goapiproof.RESTRefusalUnexpectedStatus},
				{"declared list null on the candidate (null vs [])", ok(strings.Replace(emptyBody, "[]", "null", 1)), emptyBody, "stop"},
				{"declared list absent on the candidate (absent vs [])", ok(`{}`), emptyBody, "stop"},
			} {
				t.Run(operation+"/"+request.Name+"/"+cell.name, func(t *testing.T) {
					whichCandidate := func(r *http.Request) string {
						raw, _ := io.ReadAll(r.Body)
						if strings.Contains(r.URL.String()+string(raw), "cand-1") {
							return "cand-1"
						}
						return "cand-2"
					}
					var seen []string
					var mu sync.Mutex
					candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("x-dev-health-build", build)
						id := whichCandidate(r)
						mu.Lock()
						seen = append(seen, id)
						mu.Unlock()
						reply := ok(dataBody)
						if id == "cand-1" {
							reply = cell.firstCand
						}
						w.WriteHeader(reply.status)
						_, _ = w.Write([]byte(reply.body))
					}))
					defer candidate.Close()
					baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if request.WantBaselineStatus != http.StatusOK {
							w.WriteHeader(request.WantBaselineStatus)
							_, _ = w.Write([]byte(`{"detail":"x"}`))
							return
						}
						body := dataBody
						if whichCandidate(r) == "cand-1" {
							body = cell.firstBase
						}
						_, _ = w.Write([]byte(body))
					})))
					defer baseline.Close()
					f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
					produced := map[string]string{}
					for _, b := range request.IDBindings {
						if b.Candidates == 0 {
							produced[b.Producer] = "ABC-999"
						}
					}
					writer := &fakeReceiptWriter{}
					attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, operation,
						spec, request, binding, produced, map[string][]string{binding.Producer: {"cand-1", "cand-2"}},
						staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
					if err != nil {
						t.Fatalf("resolveIteratingRequest: %v", err)
					}
					mu.Lock()
					tried := append([]string(nil), seen...)
					mu.Unlock()
					got := "stop:" + attempt.out.Refusal
					if attempt.out.Admitted {
						got = "win:" + attempt.out.producedIDs[binding.ExposeAs]
						if attempt.out.producedIDs[binding.ExposeAs] == "cand-2" {
							got = "next"
						}
					}
					t.Logf("%s/%s %s -> %s (tried %v, receipts %d)", operation, request.Name, cell.name, got, tried, len(writer.receipts))
					if cell.want == "stop" {
						// Any result that ends the search on cand-1 (a
						// named refusal, or a mismatch won by cand-1).
						if got == "next" {
							t.Fatalf("got next, want the search to end on cand-1 (attempt %+v)", attempt.out)
						}
						if len(tried) != 1 {
							t.Fatalf("tried %v, want only cand-1", tried)
						}
						return
					}
					if got != cell.want {
						t.Fatalf("got %s, want %s (attempt %+v)", got, cell.want, attempt.out)
					}
					if cell.want == "next" && len(writer.receipts) != 1 {
						t.Fatalf("receipts = %d, want 1 (the second candidate's)", len(writer.receipts))
					}
					if cell.want != "next" && (len(writer.receipts) != 0 || len(tried) != 1) {
						t.Fatalf("receipts = %d, tried %v; want 0 and only cand-1", len(writer.receipts), tried)
					}
				})
			}
		}
	}
	if swept < 4 {
		t.Fatalf("swept %d bounded-candidate corpus entries, want every one (at least the 4 declared today)", swept)
	}
	t.Logf("swept %d bounded-candidate corpus entries", swept)
}

// TestRunMeasurement_NullVersusEmptyPullRequestsIsReportedAsAMismatch
// runs the full corpus the way an operator does. The first person listed
// answers {"items":[]} on the baseline and {"items":null} on the
// candidate: zero non-null leaves on both legs under a parity that arms
// the vacuity check, and the legs differ. The report carries that
// person's drilldown_prs_default outcome as a mismatch; the search does
// not move on to the person with pull requests.
func TestRunMeasurement_NullVersusEmptyPullRequestsIsReportedAsAMismatch(t *testing.T) {
	const build = "build123"
	prs := `{"items":[{"repo_id":"ABC-123","number":7,"title":"ABC-123","created_at":"2026-01-01T00:00:00"}]}`
	handler := func(candidate bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if candidate {
				w.Header().Set("x-dev-health-build", build)
			}
			switch {
			case r.URL.Path == "/api/v1/people":
				_, _ = w.Write([]byte(`[{"person_id":"p-first"},{"person_id":"p-data"}]`))
			case r.URL.Path == "/api/v1/people/p-first/drilldown/prs" && candidate:
				_, _ = w.Write([]byte(`{"items":null}`))
			case r.URL.Path == "/api/v1/people/p-first/drilldown/prs":
				_, _ = w.Write([]byte(`{"items":[]}`))
			case r.URL.Path == "/api/v1/people/p-data/drilldown/prs":
				_, _ = w.Write([]byte(prs))
			default:
				_, _ = w.Write([]byte(`{}`))
			}
		}
	}
	candidate := httptest.NewServer(handler(true))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(handler(false)))
	defer baseline.Close()
	dir := t.TempDir()
	reportPath := dir + "/report.json"
	artifacts, err := goapiproof.NewArtifactStore(dir + "/artifacts")
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	f := flags{
		queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL,
		org: "org-1", recordedBy: "chris", reviewEvidence: "test",
		timeout: 5 * time.Second, dryRun: true, reportPath: reportPath,
	}
	_ = captureStdout(t, func() {
		_ = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
			staticCredentialForTest(), staticCredentialForTest(), build, nil, artifacts)
	})
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	for _, o := range report.Outcomes {
		if o.Operation == "REST:GET:/api/v1/people/{person_id}/drilldown/prs" && o.Request == "drilldown_prs_default" {
			t.Logf("drilldown_prs_default -> admitted=%v terminal=%q refusal=%q bound=%v attempts=%v", o.Admitted, o.TerminalState, o.Refusal, o.BoundIDs, o.Attempts)
			if !o.Admitted || o.TerminalState != goapiproof.TerminalStateMismatch || o.BoundIDs["person_id"] != "p-first" || len(o.Attempts) != 0 {
				t.Fatalf("drilldown_prs_default = %+v, want a mismatch on p-first with no skipped attempt", o)
			}
			return
		}
	}
	t.Fatal("report has no drilldown_prs_default outcome")
}

// TestProveOneRESTRequest_AllNullRowsUnderDedupAreNeverAMatch sweeps every
// real corpus request that injects synthetic dedup keys before Compare.
// Both legs answer the declared list with one row whose every field is
// null: the planes' own bodies carry no evidence, so the request must not
// be admitted as a match (the synthetic key built from those nulls is not
// a leaf of either answer).
func TestProveOneRESTRequest_AllNullRowsUnderDedupAreNeverAMatch(t *testing.T) {
	const build = "abc123def456"
	swept := 0
	for _, operation := range goapiproof.RESTRunOrder() {
		spec, err := goapiproof.SpecForREST(operation)
		if err != nil {
			t.Fatalf("SpecForREST(%s): %v", operation, err)
		}
		for _, request := range spec.Requests {
			if request.DedupListPath == "" || request.BodyMode != goapiproof.RESTBodyModeJSON {
				continue
			}
			swept++
			row := map[string]any{}
			for _, field := range request.DedupKeyFields {
				row[field] = nil
			}
			body := bodyAtPath(request.DedupListPath, []any{row})
			t.Run(operation+"/"+request.Name, func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("x-dev-health-build", build)
					_, _ = w.Write([]byte(body))
				}))
				defer srv.Close()
				f := flags{queryAPIURL: srv.URL, pythonAPIURL: srv.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
				resolved := spec
				resolved.Path = strings.NewReplacer("{person_id}", "ABC-123", "{team_id}", "ABC-123", "{work_unit_id}", "ABC-123").Replace(spec.Path)
				out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, operation, resolved, request,
					staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
				if err != nil {
					t.Fatalf("proveOneRESTRequest: %v", err)
				}
				t.Logf("%s/%s all-null rows -> admitted=%v terminal=%q refusal=%q", operation, request.Name, out.Admitted, out.TerminalState, out.Refusal)
				if out.Admitted && out.TerminalState == goapiproof.TerminalStateMatch {
					t.Fatalf("all-null rows admitted as a match")
				}
			})
		}
	}
	if swept == 0 {
		t.Fatal("no corpus request injects dedup keys; the sweep covered nothing")
	}
	t.Logf("swept %d dedup-injecting corpus requests", swept)
}

func bodyAtPath(path string, list []any) string {
	var out any = list
	segments := strings.Split(path, ".")
	for i := len(segments) - 1; i >= 0; i-- {
		out = map[string]any{segments[i]: out}
	}
	raw, _ := json.Marshal(out)
	return string(raw)
}
