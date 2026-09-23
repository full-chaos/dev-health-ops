package restprove

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

// These tests drive real corpus requests through resolveIteratingRequest
// against captured production bodies: a repository with no investment
// rows answers a flow body with empty nodes/links and a work-units body
// of `[]`, and neither may win a bounded candidate search whose request
// declares an id that such a body cannot yield.

func readProveFixture(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(raw)
}

// scopedFixtureServers answers every request whose scope id (read by
// scopeOf) is a key of candidateBodies/baselineBodies with that body, on
// the matching leg. The candidate leg names build, as a live Go plane does.
func scopedFixtureServers(t *testing.T, build string, scopeOf func(*http.Request) string, candidateBodies, baselineBodies map[string]string) (candidateURL, baselineURL string, candidateScopes *[]string) {
	t.Helper()
	var scopes []string
	var mu sync.Mutex
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := scopeOf(r)
		mu.Lock()
		scopes = append(scopes, scope)
		mu.Unlock()
		w.Header().Set("x-dev-health-build", build)
		body, ok := candidateBodies[scope]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(candidate.Close)
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := baselineBodies[scopeOf(r)]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})))
	t.Cleanup(baseline.Close)
	return candidate.URL, baseline.URL, &scopes
}

func flowBodyScopeID(r *http.Request) string {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return ""
	}
	var body struct {
		Filters struct {
			Scope struct {
				IDs []string `json:"ids"`
			} `json:"scope"`
		} `json:"filters"`
	}
	if json.Unmarshal(raw, &body) != nil || len(body.Filters.Scope.IDs) == 0 {
		return ""
	}
	return body.Filters.Scope.IDs[0]
}

func queryScopeID(r *http.Request) string {
	return r.URL.Query().Get("scope_id")
}

func corpusRequest(t *testing.T, operation, name string) (goapiproof.RESTEndpointSpec, goapiproof.RESTRequest, goapiproof.RESTIDBinding) {
	t.Helper()
	spec, err := goapiproof.SpecForREST(operation)
	if err != nil {
		t.Fatalf("SpecForREST(%s): %v", operation, err)
	}
	for _, request := range spec.Requests {
		if request.Name != name {
			continue
		}
		binding, ok := findIteratingBinding(request.IDBindings)
		if !ok {
			t.Fatalf("%s request %q declares no bounded-candidate binding", operation, name)
		}
		return spec, request, binding
	}
	t.Fatalf("%s declares no request %q", operation, name)
	return goapiproof.RESTEndpointSpec{}, goapiproof.RESTRequest{}, goapiproof.RESTIDBinding{}
}

// TestResolveIteratingRequest_FlowRepoScopedSkipsARepositoryWithNoInvestmentRows
// replays the captured flow bodies for a repository with no investment
// rows (empty nodes/links, zeroed coverage, non-null labels) as the first
// candidate. That body is not vacuous -- its labels and coverage figures
// are non-null leaves -- so only the request's own declared node id keeps
// it from winning. The second candidate answers a body with nodes and
// wins; exactly one receipt is written, for the winner.
func TestResolveIteratingRequest_FlowRepoScopedSkipsARepositoryWithNoInvestmentRows(t *testing.T) {
	const build = "abc123def456"
	withData := readProveFixture(t, "investmentflow_dynamicorg_baseline_1988f415.json")
	candidateURL, baselineURL, scopes := scopedFixtureServers(t, build, flowBodyScopeID,
		map[string]string{
			"repo-empty": readProveFixture(t, "investmentflow_reposcoped_emptyrepo_candidate_2a7a073a.json"),
			"repo-data":  withData,
		},
		map[string]string{
			"repo-empty": readProveFixture(t, "investmentflow_reposcoped_emptyrepo_baseline_acc5f56b.json"),
			"repo-data":  withData,
		},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec, request, binding := corpusRequest(t, "REST:POST:/api/v1/investment/flow", "repo_scoped")

	writer := &fakeReceiptWriter{}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:POST:/api/v1/investment/flow",
		spec, request, binding, map[string]string{}, map[string][]string{binding.Producer: {"repo-empty", "repo-data"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if !attempt.out.Admitted || !attempt.legsSent {
		t.Fatalf("attempt = %+v, want an admitted win", attempt.out)
	}
	if got := attempt.out.producedIDs[binding.ExposeAs]; got != "repo-data" {
		t.Fatalf("producedIDs[%s] = %q, want repo-data (the repository with investment rows)", binding.ExposeAs, got)
	}
	if len(attempt.out.Attempts) != 1 || attempt.out.Attempts[0].CandidateID != "repo-empty" {
		t.Fatalf("Attempts = %+v, want exactly one losing attempt naming repo-empty", attempt.out.Attempts)
	}
	if attempt.out.Attempts[0].Refusal != goapiproof.RESTRefusalNoLegProducedTheDeclaredID {
		t.Fatalf("Attempts[0].Refusal = %q, want %q", attempt.out.Attempts[0].Refusal, goapiproof.RESTRefusalNoLegProducedTheDeclaredID)
	}
	if len(writer.receipts) != 1 {
		t.Fatalf("wrote %d receipts, want exactly 1 -- for the winner only", len(writer.receipts))
	}
	if len(*scopes) != 2 {
		t.Fatalf("candidate scopes = %v, want exactly repo-empty then repo-data", *scopes)
	}
}

// TestResolveIteratingRequest_WorkUnitsRepoScopedSkipsARepositoryWithNoWorkUnits
// replays the captured `[]` body both planes answered for a repository
// with no work units as the first candidate: the request declares the
// work_unit_id its explain sibling consumes, so a candidate that cannot
// yield one loses and the next candidate, with one work unit, wins and
// is exposed to the siblings.
func TestResolveIteratingRequest_WorkUnitsRepoScopedSkipsARepositoryWithNoWorkUnits(t *testing.T) {
	const build = "abc123def456"
	empty := readProveFixture(t, "workunits_reposcoped_emptyrepo_baseline.json")
	oneUnit := `[{"work_unit_id":"ABC-123"}]`
	candidateURL, baselineURL, _ := scopedFixtureServers(t, build, queryScopeID,
		map[string]string{"repo-empty": empty, "repo-data": oneUnit},
		map[string]string{"repo-empty": empty, "repo-data": oneUnit},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec, request, binding := corpusRequest(t, "REST:GET:/api/v1/work-units", "repo_scoped")

	writer := &fakeReceiptWriter{}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/work-units",
		spec, request, binding, map[string]string{}, map[string][]string{binding.Producer: {"repo-empty", "repo-data"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if !attempt.out.Admitted {
		t.Fatalf("attempt = %+v, want an admitted win", attempt.out)
	}
	if got := attempt.out.producedIDs[binding.ExposeAs]; got != "repo-data" {
		t.Fatalf("producedIDs[%s] = %q, want repo-data", binding.ExposeAs, got)
	}
	if got := attempt.out.producedIDs["work_unit_id_repo_scoped"]; got != "ABC-123" {
		t.Fatalf("producedIDs[work_unit_id_repo_scoped] = %q, want ABC-123", got)
	}
	if len(attempt.out.Attempts) != 1 || attempt.out.Attempts[0].Refusal != goapiproof.RESTRefusalNoLegProducedTheDeclaredID {
		t.Fatalf("Attempts = %+v, want exactly one losing attempt refused as %q", attempt.out.Attempts, goapiproof.RESTRefusalNoLegProducedTheDeclaredID)
	}
	if len(writer.receipts) != 1 {
		t.Fatalf("wrote %d receipts, want exactly 1", len(writer.receipts))
	}
}

// TestProveOneRESTRequest_SingleShotJSONRequestStillAdmittedWithoutItsDeclaredID
// pins that the declared-id win condition applies to bounded-candidate
// requests only: a single-shot JSON request whose baseline body yields
// no id is still compared and admitted, and its consumers refuse by
// name through the unresolved-binding path.
func TestProveOneRESTRequest_SingleShotJSONRequestStillAdmittedWithoutItsDeclaredID(t *testing.T) {
	const build = "abc123def456"
	empty := readProveFixture(t, "workunits_reposcoped_emptyrepo_baseline.json")
	candidateURL, baselineURL, _ := scopedFixtureServers(t, build, queryScopeID,
		map[string]string{"repo-empty": empty}, map[string]string{"repo-empty": empty})
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/work-units"}
	request := goapiproof.RESTRequest{
		Name: "repo_scoped", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: goapiproof.RESTBodyModeJSON,
		Query:    map[string][]string{"scope_id": {"repo-empty"}},
		Produces: []goapiproof.RESTIDProducer{{Name: "work_unit_id_repo_scoped", IDField: "work_unit_id"}},
	}
	out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/work-units", spec, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
	if err != nil {
		t.Fatalf("proveOneRESTRequest: %v", err)
	}
	if !out.Admitted || out.TerminalState != goapiproof.TerminalStateMatch {
		t.Fatalf("out = %+v, want an admitted match", out)
	}
	if _, ok := out.producedIDs["work_unit_id_repo_scoped"]; ok {
		t.Fatalf("producedIDs = %v, want work_unit_id_repo_scoped absent", out.producedIDs)
	}
}

// TestResolveIteratingRequest_FlowRepoScopedComparesACandidateOnlyNodeInsteadOfSkippingIt
// pins the other side of the win condition: when the baseline leg answers
// the captured empty flow body but the candidate leg answers nodes, the
// declared node id is yielded by one leg, so the attempt is compared --
// and wins as a mismatch -- rather than being skipped for a later
// candidate where both planes agree.
func TestResolveIteratingRequest_FlowRepoScopedComparesACandidateOnlyNodeInsteadOfSkippingIt(t *testing.T) {
	const build = "abc123def456"
	withData := readProveFixture(t, "investmentflow_dynamicorg_baseline_1988f415.json")
	emptyCandidate := readProveFixture(t, "investmentflow_reposcoped_emptyrepo_candidate_2a7a073a.json")
	oneNode := strings.Replace(emptyCandidate, `"nodes":[]`, `"nodes":[{"name":"ABC-123","group":"subcategory","value":1}]`, 1)
	if oneNode == emptyCandidate {
		t.Fatal("captured empty candidate flow body carries no empty nodes list to replace")
	}
	candidateURL, baselineURL, scopes := scopedFixtureServers(t, build, flowBodyScopeID,
		map[string]string{"repo-diverges": oneNode, "repo-data": withData},
		map[string]string{
			"repo-diverges": readProveFixture(t, "investmentflow_reposcoped_emptyrepo_baseline_acc5f56b.json"),
			"repo-data":     withData,
		},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec, request, binding := corpusRequest(t, "REST:POST:/api/v1/investment/flow", "repo_scoped")

	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:POST:/api/v1/investment/flow",
		spec, request, binding, map[string]string{}, map[string][]string{binding.Producer: {"repo-diverges", "repo-data"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if !attempt.out.Admitted || attempt.out.TerminalState != goapiproof.TerminalStateMismatch {
		t.Fatalf("attempt = %+v, want an admitted mismatch on the first candidate", attempt.out)
	}
	if got := attempt.out.producedIDs[binding.ExposeAs]; got != "repo-diverges" {
		t.Fatalf("producedIDs[%s] = %q, want repo-diverges", binding.ExposeAs, got)
	}
	if len(attempt.out.Attempts) != 0 || len(*scopes) != 1 {
		t.Fatalf("Attempts = %+v, candidate scopes = %v, want the first candidate compared and no other tried", attempt.out.Attempts, *scopes)
	}
}

// TestResolveIteratingRequest_WorkUnitsRepoScopedGoOnlyUnitIsComparedAndNotExplained
// pins the work-units side of a candidate-only result: when only the Go
// plane returns a work unit for the first repository, that repository is
// compared (a mismatch) and wins, but work_unit_id_repo_scoped is read
// from the baseline leg only, so it stays unproduced and the explain
// sibling refuses by name (rest_request_id_binding_unresolved). A unit
// the baseline plane does not list cannot be explained on both planes.
func TestResolveIteratingRequest_WorkUnitsRepoScopedGoOnlyUnitIsComparedAndNotExplained(t *testing.T) {
	const build = "abc123def456"
	empty := readProveFixture(t, "workunits_reposcoped_emptyrepo_baseline.json")
	oneUnit := `[{"work_unit_id":"ABC-123"}]`
	candidateURL, baselineURL, _ := scopedFixtureServers(t, build, queryScopeID,
		map[string]string{"repo-go-only": oneUnit, "repo-data": oneUnit},
		map[string]string{"repo-go-only": empty, "repo-data": oneUnit},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec, request, binding := corpusRequest(t, "REST:GET:/api/v1/work-units", "repo_scoped")

	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/work-units",
		spec, request, binding, map[string]string{}, map[string][]string{binding.Producer: {"repo-go-only", "repo-data"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if !attempt.out.Admitted || attempt.out.TerminalState != goapiproof.TerminalStateMismatch {
		t.Fatalf("attempt = %+v, want an admitted mismatch on repo-go-only", attempt.out)
	}
	if got := attempt.out.producedIDs[binding.ExposeAs]; got != "repo-go-only" {
		t.Fatalf("producedIDs[%s] = %q, want repo-go-only", binding.ExposeAs, got)
	}
	if _, ok := attempt.out.producedIDs["work_unit_id_repo_scoped"]; ok {
		t.Fatalf("producedIDs = %v, want work_unit_id_repo_scoped absent (the baseline listed no unit)", attempt.out.producedIDs)
	}

	explain, err := goapiproof.SpecForREST("REST:POST:/api/v1/work-units/{work_unit_id}/explain")
	if err != nil {
		t.Fatalf("SpecForREST(explain): %v", err)
	}
	for _, req := range explain.Requests {
		if req.Name != "repo_scoped_live_work_unit" {
			continue
		}
		_, _, _, unresolved := goapiproof.ResolveRESTIDBindings(explain.Path, req, attempt.out.producedIDs)
		if len(unresolved) != 1 || unresolved[0] != "work_unit_id_repo_scoped" {
			t.Fatalf("explain unresolved = %v, want [work_unit_id_repo_scoped]", unresolved)
		}
		return
	}
	t.Fatal("explain declares no repo_scoped_live_work_unit request")
}

// TestResolveIteratingRequest_ARealFailureStopsTheSearch pins that only a
// candidate with no data moves the search on. The first repository
// answers HTTP 500 on the candidate leg: that is a failure of the plane,
// not an empty repository, so the request is refused with that reason,
// no later candidate is tried, nothing is exposed to the siblings, and no
// receipt is written -- a later matching repository can never hide it.
func TestResolveIteratingRequest_ARealFailureStopsTheSearch(t *testing.T) {
	const build = "abc123def456"
	withData := readProveFixture(t, "investmentflow_dynamicorg_baseline_1988f415.json")
	var scopes []string
	var mu sync.Mutex
	candidate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := flowBodyScopeID(r)
		mu.Lock()
		scopes = append(scopes, scope)
		mu.Unlock()
		w.Header().Set("x-dev-health-build", build)
		if scope == "repo-fails" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"detail":"boom"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(withData))
	}))
	t.Cleanup(candidate.Close)
	baseline := httptest.NewServer(referencePlane(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(withData))
	})))
	t.Cleanup(baseline.Close)
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec, request, binding := corpusRequest(t, "REST:POST:/api/v1/investment/flow", "repo_scoped")

	writer := &fakeReceiptWriter{}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:POST:/api/v1/investment/flow",
		spec, request, binding, map[string]string{}, map[string][]string{binding.Producer: {"repo-fails", "repo-data"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if attempt.out.Admitted || attempt.out.Refusal != goapiproof.RESTRefusalUnexpectedStatus {
		t.Fatalf("attempt = %+v, want refused as %q", attempt.out, goapiproof.RESTRefusalUnexpectedStatus)
	}
	if _, ok := attempt.out.producedIDs[binding.ExposeAs]; ok {
		t.Fatalf("producedIDs = %v, want %s absent", attempt.out.producedIDs, binding.ExposeAs)
	}
	if len(scopes) != 1 || scopes[0] != "repo-fails" {
		t.Fatalf("candidate scopes = %v, want only repo-fails tried", scopes)
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0", len(writer.receipts))
	}
}

// TestResolveIteratingRequest_UndecodableCandidateBodyStopsTheSearch pins
// that a status-only candidate leg answering HTTP 200 with a body that
// does not decode is a failure of the plane, not an empty candidate: the
// search ends on it with a decode refusal, the next candidate is never
// tried, and no receipt is written.
func TestResolveIteratingRequest_UndecodableCandidateBodyStopsTheSearch(t *testing.T) {
	const build = "abc123def456"
	candidateURL, baselineURL, paths := iteratingFixtureServers(t, build,
		[]string{"p-1", "p-2"},
		map[string]string{"/people/p-1/issues": "not-json", "/people/p-2/issues": `[{"status":"open"}]`},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	produced, producedCandidates := runIteratingProducer(t, f, build, &fakeReceiptWriter{})

	writer := &fakeReceiptWriter{}
	spec, request, binding := iteratingConsumerRequest()
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/issues",
		spec, request, binding, produced, producedCandidates,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if attempt.out.Admitted || attempt.out.Refusal != goapiproof.RESTRefusalCandidateBodyUndecodable {
		t.Fatalf("attempt = %+v, want refused as %q", attempt.out, goapiproof.RESTRefusalCandidateBodyUndecodable)
	}
	if _, ok := attempt.out.producedIDs[binding.ExposeAs]; ok {
		t.Fatalf("producedIDs = %v, want %s absent", attempt.out.producedIDs, binding.ExposeAs)
	}
	for _, p := range *paths {
		if p == "/people/p-2/issues" {
			t.Fatalf("candidate paths = %v, want p-2 never tried", *paths)
		}
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("wrote %d receipts, want 0", len(writer.receipts))
	}
}

// TestResolveIteratingRequest_OnlyAnEmptyDeclaredListIsNoData runs the
// real work-units repo_scoped request over every shape a first candidate
// can answer without yielding the declared work_unit_id. Only a clean
// match with the declared list an empty array on both legs is "no data"
// and moves the search to the second candidate. Every other shape -- the
// list missing, null, not an array, or elements without the id, on
// either leg -- ends the search on the first candidate: as a mismatch
// (that candidate wins with its one receipt) when the legs differ, or
// refused with no receipt when they match. A later match never hides it.
func TestResolveIteratingRequest_OnlyAnEmptyDeclaredListIsNoData(t *testing.T) {
	const build = "abc123def456"
	oneUnit := `[{"work_unit_id":"ABC-123"}]`
	for _, tc := range []struct {
		name                string
		candidate, baseline string
		wantNextCandidate   bool
	}{
		{"empty array on both legs", `[]`, `[]`, true},
		{"error object on the candidate leg", `{"detail":"backend failed"}`, `[]`, false},
		{"error object on the baseline leg", `[]`, `{"detail":"backend failed"}`, false},
		{"error object on both legs", `{"detail":"backend failed"}`, `{"detail":"backend failed"}`, false},
		{"null on both legs", `null`, `null`, false},
		{"elements without the id field on both legs", `[{"other":"ABC-123"}]`, `[{"other":"ABC-123"}]`, false},
		{"empty id on both legs", `[{"work_unit_id":""}]`, `[{"work_unit_id":""}]`, false},
		{"empty on the baseline, a unit on the candidate", `[{"work_unit_id":"ABC-123"}]`, `[]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidateURL, baselineURL, scopes := scopedFixtureServers(t, build, queryScopeID,
				map[string]string{"repo-first": tc.candidate, "repo-data": oneUnit},
				map[string]string{"repo-first": tc.baseline, "repo-data": oneUnit},
			)
			f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
			spec, request, binding := corpusRequest(t, "REST:GET:/api/v1/work-units", "repo_scoped")
			writer := &fakeReceiptWriter{}
			attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/work-units",
				spec, request, binding, map[string]string{}, map[string][]string{binding.Producer: {"repo-first", "repo-data"}},
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
			if err != nil {
				t.Fatalf("resolveIteratingRequest: %v", err)
			}
			if tc.wantNextCandidate {
				if !attempt.out.Admitted || attempt.out.producedIDs[binding.ExposeAs] != "repo-data" {
					t.Fatalf("attempt = %+v, want repo-data to win after repo-first had no data", attempt.out)
				}
				return
			}
			assertSearchEndedOnFirst(t, attempt, binding, *scopes, "repo-first", len(writer.receipts))
		})
	}
}

// TestResolveIteratingRequest_StatusOnlyOnlyAnEmptyDeclaredListIsNoData is
// the status-only twin: the declared id is read from the candidate leg,
// and only an empty items array there moves the search on.
func TestResolveIteratingRequest_StatusOnlyOnlyAnEmptyDeclaredListIsNoData(t *testing.T) {
	const build = "abc123def456"
	for _, tc := range []struct {
		name              string
		items             string
		wantNextCandidate bool
	}{
		{"empty items", `[]`, true},
		{"items not an array", `"backend failed"`, false},
		{"items null", `null`, false},
		{"items without the id field", `[{"other":"open"}]`, false},
		{"empty items followed by trailing bytes", `[]}garbage{"x":1`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidateURL, baselineURL, paths := iteratingFixtureServers(t, build,
				[]string{"p-1", "p-2"},
				map[string]string{"/people/p-1/issues": tc.items, "/people/p-2/issues": `[{"status":"open"}]`},
			)
			f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
			produced, producedCandidates := runIteratingProducer(t, f, build, &fakeReceiptWriter{})
			writer := &fakeReceiptWriter{}
			spec, request, binding := iteratingConsumerRequest()
			attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/issues",
				spec, request, binding, produced, producedCandidates,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
			if err != nil {
				t.Fatalf("resolveIteratingRequest: %v", err)
			}
			triedP2 := false
			for _, p := range *paths {
				if p == "/people/p-2/issues" {
					triedP2 = true
				}
			}
			if tc.wantNextCandidate {
				if !attempt.out.Admitted || attempt.out.producedIDs["issues_person_id"] != "p-2" {
					t.Fatalf("attempt = %+v, want p-2 to win after p-1 had no data", attempt.out)
				}
				return
			}
			if attempt.out.Admitted || triedP2 || len(writer.receipts) != 0 {
				t.Fatalf("attempt = %+v, triedP2 = %v, receipts = %d, want the search to end refused on p-1", attempt.out, triedP2, len(writer.receipts))
			}
		})
	}
}

// assertSearchEndedOnFirst checks that a bounded candidate search ended
// on its first candidate: no other candidate was tried, the result is not
// a clean match, and the outcome is consistent -- an admitted mismatch
// exposes the first candidate with exactly its one receipt; a refusal
// exposes nothing and writes none.
func assertSearchEndedOnFirst(t *testing.T, attempt resolvedAttempt, binding goapiproof.RESTIDBinding, scopes []string, first string, receipts int) {
	t.Helper()
	if len(scopes) != 1 || scopes[0] != first {
		t.Fatalf("candidate scopes = %v, want only %s tried", scopes, first)
	}
	if attempt.out.Admitted && attempt.out.TerminalState == goapiproof.TerminalStateMatch {
		t.Fatalf("attempt = %+v, want the first candidate's difference or refusal, not a clean match", attempt.out)
	}
	exposed, ok := attempt.out.producedIDs[binding.ExposeAs]
	if attempt.out.Admitted {
		if exposed != first || receipts != 1 {
			t.Fatalf("admitted mismatch exposes %q with %d receipts, want %s with exactly 1", exposed, receipts, first)
		}
		return
	}
	if ok || receipts != 0 {
		t.Fatalf("refused attempt exposes %q (present=%v) with %d receipts, want nothing exposed and no receipt", exposed, ok, receipts)
	}
}

// TestResolveIteratingRequest_FlowEmptyNodesWithOtherDifferencesWins pins
// that an empty declared list on both legs is not "no data" by itself:
// the captured empty-repository flow bodies are served with one extra
// link on the candidate leg only, so the comparison finds a difference,
// and that first repository wins as a mismatch with its one receipt
// instead of being skipped for a later matching repository.
func TestResolveIteratingRequest_FlowEmptyNodesWithOtherDifferencesWins(t *testing.T) {
	const build = "abc123def456"
	emptyCandidate := readProveFixture(t, "investmentflow_reposcoped_emptyrepo_candidate_2a7a073a.json")
	extraLink := strings.Replace(emptyCandidate, `"links":[]`, `"links":[{"source":"ABC-123","target":"ABC-124","value":1}]`, 1)
	if extraLink == emptyCandidate {
		t.Fatal("captured empty candidate flow body carries no empty links list to replace")
	}
	withData := readProveFixture(t, "investmentflow_dynamicorg_baseline_1988f415.json")
	candidateURL, baselineURL, scopes := scopedFixtureServers(t, build, flowBodyScopeID,
		map[string]string{"repo-first": extraLink, "repo-data": withData},
		map[string]string{
			"repo-first": readProveFixture(t, "investmentflow_reposcoped_emptyrepo_baseline_acc5f56b.json"),
			"repo-data":  withData,
		},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec, request, binding := corpusRequest(t, "REST:POST:/api/v1/investment/flow", "repo_scoped")
	writer := &fakeReceiptWriter{}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:POST:/api/v1/investment/flow",
		spec, request, binding, map[string]string{}, map[string][]string{binding.Producer: {"repo-first", "repo-data"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if !attempt.out.Admitted || attempt.out.TerminalState != goapiproof.TerminalStateMismatch {
		t.Fatalf("attempt = %+v, want an admitted mismatch on repo-first", attempt.out)
	}
	assertSearchEndedOnFirst(t, attempt, binding, *scopes, "repo-first", len(writer.receipts))
}

// TestResolveIteratingRequest_JSONExhaustionIsANamedRefusalWithEveryAttempt
// pins exhaustion for a JSON-body search: when every candidate is a clean
// match with the declared list empty on both legs, the request refuses as
// RESTRefusalCandidateIterationExhausted with every attempt attached,
// exposes nothing, and writes no receipt.
func TestResolveIteratingRequest_JSONExhaustionIsANamedRefusalWithEveryAttempt(t *testing.T) {
	const build = "abc123def456"
	empty := readProveFixture(t, "workunits_reposcoped_emptyrepo_baseline.json")
	candidateURL, baselineURL, _ := scopedFixtureServers(t, build, queryScopeID,
		map[string]string{"repo-a": empty, "repo-b": empty},
		map[string]string{"repo-a": empty, "repo-b": empty},
	)
	f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	spec, request, binding := corpusRequest(t, "REST:GET:/api/v1/work-units", "repo_scoped")
	writer := &fakeReceiptWriter{}
	attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/work-units",
		spec, request, binding, map[string]string{}, map[string][]string{binding.Producer: {"repo-a", "repo-b"}},
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
	if err != nil {
		t.Fatalf("resolveIteratingRequest: %v", err)
	}
	if attempt.out.Admitted || attempt.out.Refusal != goapiproof.RESTRefusalCandidateIterationExhausted {
		t.Fatalf("attempt = %+v, want refused as %q", attempt.out, goapiproof.RESTRefusalCandidateIterationExhausted)
	}
	if len(attempt.out.Attempts) != 2 {
		t.Fatalf("Attempts = %+v, want both candidates attached", attempt.out.Attempts)
	}
	for _, a := range attempt.out.Attempts {
		if a.Refusal != goapiproof.RESTRefusalNoLegProducedTheDeclaredID {
			t.Fatalf("attempt %s refused as %q, want %q", a.CandidateID, a.Refusal, goapiproof.RESTRefusalNoLegProducedTheDeclaredID)
		}
	}
	if _, ok := attempt.out.producedIDs[binding.ExposeAs]; ok || len(writer.receipts) != 0 {
		t.Fatalf("producedIDs = %v, receipts = %d, want nothing exposed and no receipt", attempt.out.producedIDs, len(writer.receipts))
	}
}

// TestResolveIteratingRequest_EachLegsDeclaredListIsCheckedUnderAVolatileList
// pins that "empty on both legs" reads each leg's own body: with the
// declared list declared volatile, the comparison matches even when one
// leg carries an error object in its place, and that leg's shape alone
// must end the search.
func TestResolveIteratingRequest_EachLegsDeclaredListIsCheckedUnderAVolatileList(t *testing.T) {
	const build = "abc123def456"
	for _, tc := range []struct{ name, candidate, baseline string }{
		{"error object on the candidate leg", `{"items":{"detail":"backend failed"}}`, `{"items":[]}`},
		{"error object on the baseline leg", `{"items":[]}`, `{"items":{"detail":"backend failed"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidateURL, baselineURL, paths := jsonBodyModeIteratingFixtureServers(t, build,
				[]string{"p-1", "p-2"},
				map[string]string{"/people/p-1/prs": tc.candidate, "/people/p-2/prs": `{"items":[{"title":"ABC-123"}]}`},
				map[string]string{"/people/p-1/prs": tc.baseline, "/people/p-2/prs": `{"items":[{"title":"ABC-123"}]}`},
			)
			f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
			produced, producedCandidates := runIteratingProducer(t, f, build, &fakeReceiptWriter{})
			spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/people/{person_id}/prs"}
			binding := goapiproof.RESTIDBinding{Producer: "person_id", PathParam: "person_id", Candidates: 10, ExposeAs: "prs_person_id"}
			request := goapiproof.RESTRequest{
				Name: "prs_default", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   goapiproof.RESTBodyModeJSON,
				IDBindings: []goapiproof.RESTIDBinding{binding},
				Produces:   []goapiproof.RESTIDProducer{{Name: "prs_title", ListPath: "items", IDField: "title"}},
				Parity:     goapiproof.Options{VolatileFields: map[string]string{"data.items": "test fixture"}},
			}
			writer := &fakeReceiptWriter{}
			attempt, err := resolveIteratingRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/people/{person_id}/prs",
				spec, request, binding, produced, producedCandidates,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil)
			if err != nil {
				t.Fatalf("resolveIteratingRequest: %v", err)
			}
			var scopes []string
			for _, p := range *paths {
				if strings.HasSuffix(p, "/prs") {
					scopes = append(scopes, strings.TrimSuffix(strings.TrimPrefix(p, "/people/"), "/prs"))
				}
			}
			if attempt.out.Admitted || attempt.out.Refusal != goapiproof.RESTRefusalDeclaredIDListUnrecognised {
				t.Fatalf("attempt = %+v, want refused as %q", attempt.out, goapiproof.RESTRefusalDeclaredIDListUnrecognised)
			}
			assertSearchEndedOnFirst(t, attempt, binding, scopes, "p-1", len(writer.receipts))
		})
	}
}
