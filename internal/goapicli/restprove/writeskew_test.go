package restprove

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// skewServers serves the candidate body on every candidate call and, on
// the baseline plane, baselineReads[n] on the n-th call (the last one on
// every later call). A reply of "" answers HTTP 500.
func skewServers(t *testing.T, build, candidateBody string, baselineReads []string) (candidateURL, baselineURL string, candidateCalls, baselineCalls func() int) {
	t.Helper()
	var mu sync.Mutex
	var cCalls, bCalls int
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cCalls++
		mu.Unlock()
		w.Header().Set("x-dev-health-build", build)
		_, _ = w.Write([]byte(candidateBody))
	}))
	t.Cleanup(candidate.Close)
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n := bCalls
		bCalls++
		mu.Unlock()
		body := baselineReads[len(baselineReads)-1]
		if n < len(baselineReads) {
			body = baselineReads[n]
		}
		if body == "" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"detail":"x"}`))
			return
		}
		_, _ = w.Write([]byte(body))
	})))
	t.Cleanup(baseline.Close)
	count := func(p *int) func() int {
		return func() int { mu.Lock(); defer mu.Unlock(); return *p }
	}
	return candidate.URL, baseline.URL, count(&cCalls), count(&bCalls)
}

// withSunburstValue returns body (a sunburst list) with the element at
// (theme, subcategory, scope) carrying value.
func withSunburstValue(t *testing.T, body, theme, subcategory, scope, value string) string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	var rows []map[string]any
	if err := dec.Decode(&rows); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, row := range rows {
		if row["theme"] == theme && row["subcategory"] == subcategory && row["scope"] == scope {
			row["value"] = json.Number(value)
			found = true
		}
	}
	if !found {
		t.Fatalf("no row %s/%s/%s", theme, subcategory, scope)
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

// TestProveOneRESTRequest_BracketedRereadOnCapturedSunburst drives the
// real team_scoped sunburst request through proveOneRESTRequest on the
// first production capture: the candidate carries 167758.7945792228 at
// quality / quality.bugfix / full-chaos/dev-health-ops, the first
// baseline read 167682.7945792228. What the second baseline read carries
// decides.
func TestProveOneRESTRequest_BracketedRereadOnCapturedSunburst(t *testing.T) {
	const build = "abc123def456"
	firstBaseline := readProveFixture(t, "investmentsunburst_teamscoped_skew_baseline_3cf72260.json")
	candidateBody := readProveFixture(t, "investmentsunburst_teamscoped_skew_candidate_a12379ac.json")
	moved := withSunburstValue(t, firstBaseline, "quality", "quality.bugfix", "full-chaos/dev-health-ops", "167700")
	caughtUp := withSunburstValue(t, firstBaseline, "quality", "quality.bugfix", "full-chaos/dev-health-ops", "167758.7945792228")
	spec, err := goapiproof.SpecForREST("REST:GET:/api/v1/investment/sunburst")
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}
	var request goapiproof.RESTRequest
	for _, r := range spec.Requests {
		if r.Name == "team_scoped" {
			request = r
		}
	}
	// GET /api/v1/investment/sunburst is a deleted-Python-body route
	// (CHAOS-6241, goapiproof/restdeletedbody.go): the committed corpus
	// now declares team_scoped's baseline as the fixed sentinel, never
	// 200. This test is about the bracketed-reread MECHANISM, exercised
	// against a real captured production write-skew case that predates
	// the deletion -- restored to its pre-deletion shape locally, on this
	// copy only; every other field (Parity, IDBindings, Produces, ...) is
	// left exactly as the live corpus declares it.
	request.WantBaselineStatus = 200
	request.BodyMode = goapiproof.RESTBodyModeJSON
	request.StatusDivergenceReason = ""
	for _, cell := range []struct {
		name          string
		baselineReads []string
		wantAdmitted  bool
		wantOutside   int
		wantRefusal   string
		wantVerdict   goapiproof.WriteSkewVerdict
		wantCalls     int
		wantReceipts  int
	}{
		{"second baseline read carries the candidate's generation", []string{firstBaseline, caughtUp}, true, 0, "", goapiproof.WriteSkewAdmitted, 2, 1},
		{"second baseline read equals the first (reference unchanged)", []string{firstBaseline, firstBaseline}, true, 1, "", goapiproof.WriteSkewStands, 2, 1},
		{"second baseline read moved to a third value", []string{firstBaseline, moved}, false, 0, goapiproof.RESTRefusalLeafMovedBetweenReads, goapiproof.WriteSkewRefused, 2, 0},
		{"second baseline read answers HTTP 500", []string{firstBaseline, ""}, false, 0, goapiproof.RESTRefusalUnexpectedStatus, goapiproof.WriteSkewRefused, 2, 0},
		{"first read already agrees: no re-read", []string{caughtUp}, true, 0, "", "", 1, 1},
	} {
		t.Run(cell.name, func(t *testing.T) {
			candidateURL, baselineURL, candidateCalls, baselineCalls := skewServers(t, build, candidateBody, cell.baselineReads)
			f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
			writer := &fakeReceiptWriter{}
			out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/investment/sunburst", spec, request,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
			if err != nil {
				t.Fatalf("proveOneRESTRequest: %v", err)
			}
			t.Logf("%s -> %s", cell.name, out.line())
			if out.Admitted != cell.wantAdmitted || out.Refusal != cell.wantRefusal || baselineCalls() != cell.wantCalls || candidateCalls() != 1 || len(writer.receipts) != cell.wantReceipts {
				t.Fatalf("admitted=%v refusal=%q baseline calls=%d candidate calls=%d receipts=%d; want %v %q %d 1 %d", out.Admitted, out.Refusal, baselineCalls(), candidateCalls(), len(writer.receipts), cell.wantAdmitted, cell.wantRefusal, cell.wantCalls, cell.wantReceipts)
			}
			if cell.wantVerdict == "" {
				if out.WriteSkew != nil {
					t.Fatalf("write skew record %+v, want none", out.WriteSkew)
				}
				return
			}
			if out.WriteSkew == nil || out.WriteSkew.Verdict != cell.wantVerdict {
				t.Fatalf("write skew = %+v, want verdict %s", out.WriteSkew, cell.wantVerdict)
			}
			if !strings.Contains(out.line(), "write_skew="+string(cell.wantVerdict)) {
				t.Fatalf("line %q does not name the verdict", out.line())
			}
			if cell.wantAdmitted {
				if out.TerminalState != goapiproof.TerminalStateMismatch || out.DifferencesOutsideBaselineDefect != cell.wantOutside {
					t.Fatalf("terminal=%q outside=%d, want mismatch %d", out.TerminalState, out.DifferencesOutsideBaselineDefect, cell.wantOutside)
				}
				cited := false
				for _, d := range out.BaselineDefectsMatched {
					if d == goapiproof.WriteSkewCitation {
						cited = true
					}
				}
				if cited != (cell.wantVerdict == goapiproof.WriteSkewAdmitted) {
					t.Fatalf("baseline_defect=%v: write-skew citation present=%v, want %v", out.BaselineDefectsMatched, cited, cell.wantVerdict == goapiproof.WriteSkewAdmitted)
				}
				if cell.wantVerdict == goapiproof.WriteSkewAdmitted && writer.receipts[0].DifferencesOutsideBaselineDefect != 0 {
					t.Fatalf("receipt outside = %d, want 0", writer.receipts[0].DifferencesOutsideBaselineDefect)
				}
			}
			if cell.wantVerdict == goapiproof.WriteSkewAdmitted {
				leaf := out.WriteSkew.Leaves[0]
				if len(out.WriteSkew.Leaves) != 1 || leaf.FirstBaseline.(json.Number) != "167682.7945792228" || leaf.Candidate.(json.Number) != "167758.7945792228" || leaf.SecondBaseline.(json.Number) != "167758.7945792228" {
					t.Fatalf("leaves = %+v, want the one leaf with B1, C1 and B2", out.WriteSkew.Leaves)
				}
			}
		})
	}
}

// reread is one baseline reply in a domain cell: a body (status 200), a
// status with body, or a stall (timeout).
type reread struct {
	body   string
	status int
	stall  bool
}

func domainSkewServer(t *testing.T, reads []reread) (string, func() int) {
	t.Helper()
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n := calls
		calls++
		mu.Unlock()
		reply := reads[len(reads)-1]
		if n < len(reads) {
			reply = reads[n]
		}
		if reply.stall {
			<-r.Context().Done()
			return
		}
		// The baseline role: the Python app's positive identity.
		w.Header().Set("Server", goapiproof.ReferencePlaneServer)
		if reply.status != 0 {
			w.WriteHeader(reply.status)
		}
		_, _ = w.Write([]byte(reply.body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() int { mu.Lock(); defer mu.Unlock(); return calls }
}

// TestBracketedRereadInputDomain runs every cell of the decision in one
// pass through proveOneRESTRequest: the first comparison's outside shape
// x what the second baseline read answers. The candidate answers
// {"a":1,"b":2,"s":"x"}; a numeric "b" leaf is the usual outside value.
func TestBracketedRereadInputDomain(t *testing.T) {
	const build = "abc123def456"
	const candidateBody = `{"a":1,"b":2,"s":"x"}`
	ok := func(body string) reread { return reread{body: body} }
	cells := []struct {
		name  string
		reads []reread
		want  string // "<verdict or ->|<admitted or refusal>|<outside>|<baseline calls>"
	}{
		// First read has no outside difference: never re-read.
		{"match", []reread{ok(candidateBody)}, "-|admitted|0|1"},
		{"match, other number spelling", []reread{ok(`{"a":1.0,"b":2e0,"s":"x"}`)}, "-|admitted|0|1"},
		// Outside differences that are not value leaves: never re-read.
		{"presence outside", []reread{ok(`{"a":1,"b":2,"s":"x","extra":3}`)}, "-|admitted|1|1"},
		{"type outside", []reread{ok(`{"a":1,"b":"2","s":"x"}`)}, "-|admitted|1|1"},
		{"null outside", []reread{ok(`{"a":1,"b":null,"s":"x"}`)}, "-|admitted|1|1"},
		{"value and presence outside", []reread{ok(`{"a":1,"b":3,"s":"x","extra":3}`)}, "-|admitted|2|1"},
		// A value leaf outside: re-read the baseline once.
		{"second baseline read = candidate (skew)", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(candidateBody)}, "skew_admitted|admitted|0|2"},
		{"second baseline read = candidate, other number spelling", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`{"a":1,"b":2.0,"s":"x"}`)}, "skew_admitted|admitted|0|2"},
		{"string leaf skew", []reread{ok(`{"a":1,"b":2,"s":"y"}`), ok(candidateBody)}, "skew_admitted|admitted|0|2"},
		// Accepted limit: a baseline that flaps (B1 wrong, B2 right and
		// equal to the candidate) reads exactly like a write. The
		// baseline is the reference plane, and its second read agreeing
		// exactly with Go on the whole case is the evidence admitted.
		{"baseline flaps: B1 wrong, B2 equals the candidate (accepted limit)", []reread{ok(`{"a":1,"b":9,"s":"x"}`), ok(candidateBody)}, "skew_admitted|admitted|0|2"},
		{"reference unchanged, candidate below (undercount)", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`{"a":1,"b":3,"s":"x"}`)}, "stands|admitted|1|2"},
		{"reference unchanged, candidate above (overcount)", []reread{ok(`{"a":1,"b":1,"s":"x"}`), ok(`{"a":1,"b":1,"s":"x"}`)}, "stands|admitted|1|2"},
		{"second baseline read: third value", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`{"a":1,"b":4,"s":"x"}`)}, "refused|" + goapiproof.RESTRefusalLeafMovedBetweenReads + "|0|2"},
		{"second baseline read: leaf missing", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`{"a":1,"s":"x"}`)}, "refused|" + goapiproof.RESTRefusalLeafMovedBetweenReads + "|0|2"},
		{"second baseline read: leaf null", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`{"a":1,"b":null,"s":"x"}`)}, "refused|" + goapiproof.RESTRefusalLeafMovedBetweenReads + "|0|2"},
		{"second baseline read: leaf agrees, another leaf differs", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`{"a":5,"b":2,"s":"x"}`)}, "stands|admitted|1|2"},
		{"second baseline read: leaf agrees, extra field", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`{"a":1,"b":2,"s":"x","extra":1}`)}, "stands|admitted|1|2"},
		{"two leaves outside, both moved to the candidate", []reread{ok(`{"a":5,"b":3,"s":"x"}`), ok(candidateBody)}, "skew_admitted|admitted|0|2"},
		{"two leaves outside, one moved one unchanged", []reread{ok(`{"a":5,"b":3,"s":"x"}`), ok(`{"a":5,"b":2,"s":"x"}`)}, "stands|admitted|2|2"},
		{"second baseline read: HTTP 500", []reread{ok(`{"a":1,"b":3,"s":"x"}`), {body: `{"detail":"x"}`, status: 500}}, "refused|" + goapiproof.RESTRefusalUnexpectedStatus + "|0|2"},
		{"second baseline read: HTTP 404", []reread{ok(`{"a":1,"b":3,"s":"x"}`), {body: `{"detail":"x"}`, status: 404}}, "refused|" + goapiproof.RESTRefusalUnexpectedStatus + "|0|2"},
		{"second baseline read: timeout", []reread{ok(`{"a":1,"b":3,"s":"x"}`), {stall: true}}, "refused|" + goapiproof.RESTRefusalBaselineLegTimedOut + "|0|2"},
		{"second baseline read: not JSON", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`not-json`)}, "refused|" + goapiproof.RESTRefusalBodyNotJSON + "|0|2"},
		{"second baseline read: trailing bytes", []reread{ok(`{"a":1,"b":3,"s":"x"}`), ok(`{"a":1,"b":2,"s":"x"}garbage`)}, "refused|" + goapiproof.RESTRefusalTrailingBytes + "|0|2"},
	}
	var rows []string
	for _, cell := range cells {
		baselineURL, calls := domainSkewServer(t, cell.reads)
		candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("x-dev-health-build", build)
			_, _ = w.Write([]byte(candidateBody))
		}))
		f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 300 * time.Millisecond}
		spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/thing"}
		request := goapiproof.RESTRequest{Name: "thing", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
		writer := &fakeReceiptWriter{}
		out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/thing", spec, request,
			staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
		candidate.Close()
		if err != nil {
			t.Fatalf("%s: proveOneRESTRequest: %v", cell.name, err)
		}
		verdict := "-"
		if out.WriteSkew != nil {
			verdict = string(out.WriteSkew.Verdict)
		}
		status := "admitted"
		if !out.Admitted {
			status = out.Refusal
		}
		got := verdict + "|" + status + "|" + itoa(out.DifferencesOutsideBaselineDefect) + "|" + itoa(calls())
		rows = append(rows, cell.name+" -> "+got)
		if got != cell.want {
			t.Errorf("%s: got %s, want %s", cell.name, got, cell.want)
		}
		if out.Admitted != (len(writer.receipts) == 1) {
			t.Errorf("%s: admitted=%v with %d receipts", cell.name, out.Admitted, len(writer.receipts))
		}
	}
	t.Logf("bracketed re-read domain:\n%s", strings.Join(rows, "\n"))
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// TestRunMeasurement_SunburstTeamScopedAdmitsUnderTheDeletedBodyOverride
// runs the full real corpus the way an operator does. GET
// /api/v1/investment/sunburst and GET /api/v1/filters/options (team_scoped's
// own team_id producer) are both deleted-Python-body routes (CHAOS-6241,
// goapiproof/restdeletedbody.go): their baseline now always answers the
// fixed sentinel, never the real capture team_scoped used to write-skew
// against (that scenario, and this test's own bracketed-reread coverage of
// it, moved to TestProveOneRESTRequest_BracketedRereadOnCapturedSunburst,
// which restores the pre-deletion shape on a local, unexported RESTRequest
// copy -- not reachable from here, since restEndpointSpecs is unexported).
// What THIS test proves instead, end to end through the real report:
// team_scoped is ADMITTED (never refused) with a real candidate_shape
// liveness pass, and its own team_id producer resolves from the CANDIDATE
// leg of filters/options (also admitted, also candidate_shape) -- the
// exact mechanism restidbind.go's own doc comment describes.
func TestRunMeasurement_SunburstTeamScopedAdmitsUnderTheDeletedBodyOverride(t *testing.T) {
	const build = "build123"
	candidateBody := readProveFixture(t, "investmentsunburst_teamscoped_skew_candidate_a12379ac.json")
	sentinelBody := `{"detail":"GET /api/v1/investment/sunburst is served by query-api and has no Python implementation"}`
	isTeamSunburst := func(r *http.Request) bool {
		return r.URL.Path == "/api/v1/investment/sunburst" && r.URL.Query().Get("scope_type") == "team"
	}
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		switch {
		case r.URL.Path == "/api/v1/filters/options":
			_, _ = w.Write([]byte(`{"teams":["ABC-123"],"repos":[]}`))
		case isTeamSunburst(r):
			_, _ = w.Write([]byte(candidateBody))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/filters/options":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"detail":"GET /api/v1/filters/options is served by query-api and has no Python implementation"}`))
		case "/api/v1/investment/sunburst":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(sentinelBody))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	})))
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
			staticCredentialForTest(), staticCredentialForTest(), sameProverBuildForTest(build), nil, artifacts)
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
		if o.Operation != "REST:GET:/api/v1/investment/sunburst" || o.Request != "team_scoped" {
			continue
		}
		if !o.Admitted || o.Refusal != "" {
			t.Fatalf("team_scoped = %+v, want admitted with no refusal", o)
		}
		if o.TerminalState != goapiproof.TerminalStateUnsupported {
			t.Fatalf("team_scoped TerminalState = %q, want unsupported -- candidate_shape never compares bodies and must not satisfy EnablementProofClause", o.TerminalState)
		}
		if o.WriteSkew != nil {
			t.Fatalf("team_scoped WriteSkew = %+v, want none -- the deleted-body baseline never times out or mismatches", o.WriteSkew)
		}
		return
	}
	t.Fatal("report has no team_scoped sunburst outcome")
}

// TestResolveIteratingRequest_WriteSkewOnTheFirstCandidateWins pairs the
// re-read with a bounded candidate search: the first candidate's value
// difference is write skew, the re-read admits it, and that candidate
// wins -- a skew-admitted attempt is an admitted mismatch, never "no data".
func TestResolveIteratingRequest_WriteSkewOnTheFirstCandidateWins(t *testing.T) {
	const build = "abc123def456"
	var mu sync.Mutex
	reads := map[string]int{}
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-dev-health-build", build)
		mu.Lock()
		reads["candidate "+r.URL.Path]++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"items":[{"id":"ABC-123","v":2}]}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reads["baseline "+r.URL.Path]++
		n := reads["baseline "+r.URL.Path]
		mu.Unlock()
		if r.URL.Path == "/things/p-1" && n == 1 {
			_, _ = w.Write([]byte(`{"items":[{"id":"ABC-123","v":3}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"items":[{"id":"ABC-123","v":2}]}`))
	})))
	defer baseline.Close()
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/things/{thing_id}"}
	binding := goapiproof.RESTIDBinding{Producer: "thing_id", PathParam: "thing_id", Candidates: 10, ExposeAs: "chosen_thing_id"}
	request := goapiproof.RESTRequest{
		Name: "things_default", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode:   goapiproof.RESTBodyModeJSON,
		IDBindings: []goapiproof.RESTIDBinding{binding},
		Produces:   []goapiproof.RESTIDProducer{{Name: "item_id", ListPath: "items", IDField: "id"}},
	}
	writer := &fakeReceiptWriter{}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/things/{thing_id}",
		spec, request, binding, map[string]string{}, map[string][]string{"thing_id": {"p-1", "p-2"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	t.Logf("-> %s", attempt.out.line())
	if !attempt.out.Admitted || attempt.out.producedIDs["chosen_thing_id"] != "p-1" || attempt.out.WriteSkew == nil || attempt.out.WriteSkew.Verdict != goapiproof.WriteSkewAdmitted || len(writer.receipts) != 1 || reads["candidate /things/p-2"] != 0 {
		t.Fatalf("attempt = %+v, reads = %v, want p-1 won skew-admitted with its one receipt and p-2 never read", attempt.out, reads)
	}
}

// TestProveOneRESTRequest_BracketedRereadInjectsDedupKeysOnTheSecondRead
// runs the re-read on a real corpus request that injects synthetic dedup
// keys before Compare (drilldown/prs default_window): the second baseline
// read carries the candidate's title, and it is admitted as write skew
// only because it is compared with the same injected keys the first two
// reads carry.
func TestProveOneRESTRequest_BracketedRereadInjectsDedupKeysOnTheSecondRead(t *testing.T) {
	const build = "abc123def456"
	spec, err := goapiproof.SpecForREST("REST:GET:/api/v1/drilldown/prs")
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}
	var request goapiproof.RESTRequest
	for _, r := range spec.Requests {
		if r.Name == "default_window" {
			request = r
		}
	}
	if request.DedupListPath == "" {
		t.Fatal("drilldown/prs default_window injects no dedup keys")
	}
	// GET /api/v1/drilldown/prs is a deleted-Python-body route
	// (CHAOS-6241, goapiproof/restdeletedbody.go): the committed corpus
	// now declares default_window's baseline as the fixed sentinel, never
	// 200. This test is about the bracketed-reread's dedup-key injection,
	// restored to its pre-deletion shape locally, on this copy only.
	request.WantBaselineStatus = 200
	request.BodyMode = goapiproof.RESTBodyModeJSON
	request.StatusDivergenceReason = ""
	item := func(title string) string {
		return `{"items":[{"repo_id":"ABC-123","number":7,"title":"` + title + `"}]}`
	}
	candidateURL, baselineURL, _, baselineCalls := skewServers(t, build, item("new"), []string{item("old"), item("new")})
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/drilldown/prs", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	t.Logf("-> %s", out.line())
	if out.WriteSkew == nil || out.WriteSkew.Verdict != goapiproof.WriteSkewAdmitted || baselineCalls() != 2 {
		t.Fatalf("out = %+v (write skew %+v), baseline calls %d; want skew-admitted after one re-read", out, out.WriteSkew, baselineCalls())
	}
}

// TestProveOneRESTRequest_LegOrderIsBaselineCandidateBaseline pins the
// invariant the bracket rests on: the baseline (reference) is read FIRST,
// the candidate second, and on an outside value leaf the baseline again,
// after the candidate. A write between the legs then shows as a change on
// the reference plane itself; if the prover's leg order flips, that
// witness is gone, and this test goes red until the bracket re-reads
// whichever leg is read first.
func TestProveOneRESTRequest_LegOrderIsBaselineCandidateBaseline(t *testing.T) {
	const build = "abc123def456"
	var mu sync.Mutex
	var order []string
	baselineReads := 0
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, "candidate")
		mu.Unlock()
		w.Header().Set("x-dev-health-build", build)
		_, _ = w.Write([]byte(`{"a":1,"b":2}`))
	}))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, "baseline")
		baselineReads++
		n := baselineReads
		mu.Unlock()
		if n == 1 {
			_, _ = w.Write([]byte(`{"a":1,"b":3}`))
			return
		}
		_, _ = w.Write([]byte(`{"a":1,"b":2}`))
	})))
	defer baseline.Close()
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/thing"}
	request := goapiproof.RESTRequest{Name: "thing", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/thing", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	t.Logf("leg order: %v -> %s", order, out.line())
	if strings.Join(order, ",") != "baseline,candidate,baseline" {
		t.Fatalf("leg order = %v, want baseline, candidate, baseline", order)
	}
	leaf := out.WriteSkew.Leaves[0]
	if leaf.FirstBaseline.(json.Number) != "3" || leaf.Candidate.(json.Number) != "2" || leaf.SecondBaseline.(json.Number) != "2" {
		t.Fatalf("leaf = %+v, want B1=3 C1=2 B2=2 recorded", leaf)
	}
}

// TestProveOneRESTRequest_LegOrderSweep executes one cell for every
// behaviour that depends on which leg is read first: transport failure
// and timeout on each leg (refusal naming, and whether the other leg was
// read at all), the candidate plane's build binding, and an id producer
// reading the baseline leg. The GraphQL prover does not share doREST
// (its runner posts through its own client), and no report field records
// leg order, so neither has a cell here.
func TestProveOneRESTRequest_LegOrderSweep(t *testing.T) {
	const build = "abc123def456"
	type legServer struct {
		stall  bool
		closed bool
		build  string
		body   string
	}
	// start returns the server's URL and, for a "closed" leg, a close function
	// the caller runs only AFTER every server of the cell is up: closing one
	// before the other starts lets the kernel hand its just-released port to
	// the next server, so both legs would reach the same server (CHAOS-6623).
	start := func(t *testing.T, s legServer, calls *int, mu *sync.Mutex) (string, func()) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			*calls++
			mu.Unlock()
			if s.stall {
				<-r.Context().Done()
				return
			}
			if s.build != "" {
				w.Header().Set("x-dev-health-build", s.build)
			} else {
				// The baseline role: the Python app's positive identity.
				w.Header().Set("Server", goapiproof.ReferencePlaneServer)
			}
			_, _ = w.Write([]byte(s.body))
		}))
		if s.closed {
			return srv.URL, srv.Close
		}
		t.Cleanup(srv.Close)
		return srv.URL, func() {}
	}
	good := `{"items":[{"id":"ABC-123"}]}`
	for _, cell := range []struct {
		name                string
		baseline, candidate legServer
		wantRefusal         string
		wantBaselineCalls   int
		wantCandidateCalls  int
		wantProduced        string
	}{
		{"baseline timeout", legServer{stall: true}, legServer{build: build, body: good}, goapiproof.RESTRefusalBaselineLegTimedOut, 1, 0, ""},
		{"baseline transport error", legServer{closed: true}, legServer{build: build, body: good}, goapiproof.RESTRefusalBaselineLegTransportError, 0, 0, ""},
		{"candidate timeout", legServer{body: good}, legServer{stall: true}, goapiproof.RESTRefusalCandidateLegTimedOut, 1, 1, ""},
		{"candidate transport error", legServer{body: good}, legServer{closed: true}, goapiproof.RESTRefusalCandidateLegTransportError, 1, 0, ""},
		{"candidate build header wrong", legServer{body: good}, legServer{build: "some-other-build", body: good}, goapiproof.RESTRefusalBuildUnbound, 1, 1, ""},
		{"id producer reads the baseline leg", legServer{body: good}, legServer{build: build, body: good}, "", 1, 1, "ABC-123"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var mu sync.Mutex
			var bCalls, cCalls int
			baselineURL, closeBaseline := start(t, cell.baseline, &bCalls, &mu)
			candidateURL, closeCandidate := start(t, cell.candidate, &cCalls, &mu)
			closeBaseline()
			closeCandidate()
			f := flags{
				pythonAPIURL: baselineURL,
				queryAPIURL:  candidateURL,
				org:          "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 200 * time.Millisecond,
			}
			spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/things"}
			request := goapiproof.RESTRequest{
				Name: "things", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON,
				Produces: []goapiproof.RESTIDProducer{{Name: "thing_id", ListPath: "items", IDField: "id"}},
			}
			out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/things", spec, request,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
			if err != nil {
				t.Fatalf("proveOneRESTRequest: %v", err)
			}
			mu.Lock()
			b, c := bCalls, cCalls
			mu.Unlock()
			t.Logf("%s -> refusal=%q baseline calls=%d candidate calls=%d produced=%v", cell.name, out.Refusal, b, c, out.producedIDs)
			if out.Refusal != cell.wantRefusal || b != cell.wantBaselineCalls || c != cell.wantCandidateCalls {
				t.Fatalf("refusal=%q baseline=%d candidate=%d; want %q %d %d", out.Refusal, b, c, cell.wantRefusal, cell.wantBaselineCalls, cell.wantCandidateCalls)
			}
			if cell.wantProduced != "" && out.producedIDs["thing_id"] != cell.wantProduced {
				t.Fatalf("producedIDs = %v, want thing_id=%s", out.producedIDs, cell.wantProduced)
			}
		})
	}
}

// TestProveOneRESTRequest_UnmovedLeafUnderAShapedDeclarationStands runs
// the case through the prover: two value leaves outside, the second
// baseline read moves c to the candidate's value but leaves b where it was
// (B1=1, C1=2, B2=1), and a scalar-direction declaration on b would cover
// it when evaluated against the second read. No write is witnessed at b,
// so the case stands with b outside.
func TestProveOneRESTRequest_UnmovedLeafUnderAShapedDeclarationStands(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, _, baselineCalls := skewServers(t, build, `{"b":2,"c":2}`, []string{`{"b":1,"c":1}`, `{"b":1,"c":2}`})
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/thing"}
	request := goapiproof.RESTRequest{
		Name: "thing", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON,
		Parity: goapiproof.Options{BaselineDefects: []goapiproof.BaselineDefect{{
			Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.b"},
			ScalarDirectionShape: &goapiproof.ScalarDirectionShape{Path: "data.b", ContestedPaths: []string{"data.b"}},
		}}},
	}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/thing", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	t.Logf("-> %s", out.line())
	if out.WriteSkew == nil || out.WriteSkew.Verdict == goapiproof.WriteSkewAdmitted || out.DifferencesOutsideBaselineDefect == 0 || baselineCalls() != 2 {
		t.Fatalf("out = %+v (write skew %+v), baseline calls %d; want the case not admitted, difference still outside", out, out.WriteSkew, baselineCalls())
	}
}

// TestBracketedRereadProverEnumeration runs the prover itself over every
// one-leaf case: B1 and C1 over {1, 2, 3, null, missing}, each leaf under
// every declaration coverage. For a case whose first comparison leaves no
// outside value-only difference, the baseline is read exactly once and the
// outcome is the first comparison's own. For every case that is re-read,
// each way the second baseline read can fail -- HTTP 500, HTTP 404, a
// timeout, a dropped connection, a body that is not JSON, trailing bytes
// -- refuses the case by that read's own named reason with no receipt.
func TestBracketedRereadProverEnumeration(t *testing.T) {
	const build = "abc123def456"
	symbols := []string{"1", "2", "3", "null", "missing"}
	body := func(symbol string) string {
		switch symbol {
		case "missing":
			return `{"other":7}`
		default:
			return `{"other":7,"b":` + symbol + `}`
		}
	}
	coverages := []goapiproof.Options{
		{},
		{BaselineDefects: []goapiproof.BaselineDefect{{Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.b"}}}},
		{BaselineDefects: []goapiproof.BaselineDefect{{Ticket: "ABC-124", Reason: "test fixture", Paths: []string{"data.b"},
			ScalarDirectionShape: &goapiproof.ScalarDirectionShape{Path: "data.b", BaselineMustBeGreater: true}}}},
	}
	type failure struct {
		name   string
		reply  func(w http.ResponseWriter, r *http.Request)
		reason string
	}
	failures := []failure{
		{"HTTP 500", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"detail":"x"}`))
		}, goapiproof.RESTRefusalUnexpectedStatus},
		{"HTTP 404", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"detail":"x"}`))
		}, goapiproof.RESTRefusalUnexpectedStatus},
		{"timeout", func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }, goapiproof.RESTRefusalBaselineLegTimedOut},
		{"dropped connection", func(w http.ResponseWriter, r *http.Request) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}, goapiproof.RESTRefusalBaselineLegTransportError},
		{"not JSON", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`not-json`)) }, goapiproof.RESTRefusalBodyNotJSON},
		{"trailing bytes", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"other":7}garbage`)) }, goapiproof.RESTRefusalTrailingBytes},
	}
	var noReread, rereadFailures, rereadStood int
	run := func(t *testing.T, b1, c1 string, opts goapiproof.Options, second func(w http.ResponseWriter, r *http.Request)) (outcome, int, int) {
		var mu sync.Mutex
		calls := 0
		baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n == 1 {
				_, _ = w.Write([]byte(body(b1)))
				return
			}
			second(w, r)
		})))
		defer baseline.Close()
		candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("x-dev-health-build", build)
			_, _ = w.Write([]byte(body(c1)))
		}))
		defer candidate.Close()
		f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 300 * time.Millisecond}
		spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/thing"}
		request := goapiproof.RESTRequest{Name: "thing", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON, Parity: opts}
		writer := &fakeReceiptWriter{}
		out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/thing", spec, request,
			staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
		if err != nil {
			t.Fatalf("proveOneRESTRequest: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		return out, calls, len(writer.receipts)
	}
	for ci, opts := range coverages {
		for _, b1 := range symbols {
			for _, c1 := range symbols {
				first := goapiproof.Compare(decodeREST(t, body(b1)), decodeREST(t, body(c1)), opts)
				if !goapiproof.WriteSkewRereadNeeded(first) {
					out, calls, _ := run(t, b1, c1, opts, func(w http.ResponseWriter, r *http.Request) { t.Errorf("unexpected second baseline read") })
					noReread++
					if calls != 1 || out.WriteSkew != nil || out.DifferencesOutsideBaselineDefect != first.DifferencesOutsideBaselineDefect || (out.Admitted && out.TerminalState != first.TerminalState) {
						t.Fatalf("coverage %d, B1=%s C1=%s: calls=%d write_skew=%+v outside=%d terminal=%q; want the first comparison's own outcome (%q outside %d) and one baseline read",
							ci, b1, c1, calls, out.WriteSkew, out.DifferencesOutsideBaselineDefect, out.TerminalState, first.TerminalState, first.DifferencesOutsideBaselineDefect)
					}
					continue
				}
				out, calls, _ := run(t, b1, c1, opts, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body(b1))) })
				rereadStood++
				if calls != 2 || out.WriteSkew == nil || out.WriteSkew.Verdict != goapiproof.WriteSkewStands {
					t.Fatalf("coverage %d, B1=%s C1=%s, B2=B1: calls=%d write_skew=%+v; want stands after one re-read", ci, b1, c1, calls, out.WriteSkew)
				}
				for _, fail := range failures {
					out, calls, receipts := run(t, b1, c1, opts, fail.reply)
					rereadFailures++
					// CHAOS-6580: every baseline request (this reread's included)
					// now closes its own connection rather than pooling it
					// (doREST(baseline=true) sets req.Close), so the reread never
					// reuses call 1's connection. Go's http.Transport only
					// silently retries a request whose connection came from the
					// idle pool (a fresh dial failing is a real failure, not a
					// race with the server's own idle timeout) -- so a dropped
					// connection on the reread is no longer masked by one hidden
					// retry, and reports on exactly the same call count as every
					// other failure shape.
					const wantCalls = 2
					if out.Admitted || out.Refusal != fail.reason || receipts != 0 || calls != wantCalls || out.WriteSkew == nil || out.WriteSkew.Verdict != goapiproof.WriteSkewRefused {
						t.Fatalf("coverage %d, B1=%s C1=%s, second read %s: admitted=%v refusal=%q receipts=%d calls=%d write_skew=%+v; want refused %q, no receipt",
							ci, b1, c1, fail.name, out.Admitted, out.Refusal, receipts, calls, out.WriteSkew, fail.reason)
					}
				}
			}
		}
	}
	t.Logf("prover enumeration: %d cases never re-read (outcome = first comparison), %d re-read with an unchanged reference (stands), %d re-read failures (each refused by name, no receipt)", noReread, rereadStood, rereadFailures)
}

func decodeREST(t *testing.T, body string) goapiproof.Snapshot {
	t.Helper()
	s, err := goapiproof.DecodeRESTSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return s
}
