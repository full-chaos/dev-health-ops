package goapiproof

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// Go-edge mode, against fake edges. One fake is query-api answering alone;
// every other test plants ONE way an edge can be something else, or a
// candidate can lack a fact this mode needs, and requires the named refusal.

const (
	goEdgeBuild = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	goEdgeOrg   = "70d529e0-0000-4000-8000-000000000001"
	// unregisteredBody is query-api's refusal of an unregistered document.
	unregisteredBody = `{"errors":[{"message":"This GraphQL document is not registered.","extensions":{"code":"UNREGISTERED_DOCUMENT"}}],"data":null}`
	goEdgeAnswer     = `{"data":{"featureFlags":[{"key":"a"}]}}`
)

// goEdgeLeg is one leg's answer from the fake edge. A "-" plane or build
// means the header is absent.
type goEdgeLeg struct {
	status        int
	plane         string
	build         string
	contentType   string
	body          string
	impersonating bool
}

func (l goEdgeLeg) write(w http.ResponseWriter) {
	if l.plane != "-" {
		w.Header().Set(planeHeader, l.plane)
	}
	if l.build != "-" {
		w.Header().Set(buildHeader, l.build)
	}
	if l.impersonating {
		w.Header().Set(impersonationHeader, "true")
	}
	w.Header().Set("Content-Type", l.contentType)
	w.WriteHeader(l.status)
	_, _ = io.WriteString(w, l.body)
}

// fakeGoEdge answers the pre-run probe, each case's control document and the
// candidate, each with its own leg, and records the documents it saw.
type fakeGoEdge struct {
	probe, control, candidate goEdgeLeg
	seen                      []string
}

// queryAPIAlone is the edge Go-edge mode proves: both control documents are
// refused as query-api refuses them, and the candidate is served by plane go
// from the named build.
func queryAPIAlone() *fakeGoEdge {
	refusal := goEdgeLeg{status: http.StatusNotFound, plane: "go", build: goEdgeBuild, contentType: "application/json", body: unregisteredBody}
	return &fakeGoEdge{
		probe:     refusal,
		control:   refusal,
		candidate: goEdgeLeg{status: http.StatusOK, plane: "go", build: goEdgeBuild, contentType: "application/json", body: goEdgeAnswer},
	}
}

func (e *fakeGoEdge) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		e.seen = append(e.seen, parsed.Query)
		switch {
		case parsed.Query == goEdgeControlProbe:
			e.probe.write(w)
		case strings.HasSuffix(parsed.Query, baselineComment):
			e.control.write(w)
		default:
			e.candidate.write(w)
		}
	}
}

// orgToken is an unsigned JWT-shaped value naming org: the shape BindOrg
// reads. The fake edge checks no signature.
func orgToken(org string) string {
	segment := func(v any) string {
		encoded, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(encoded)
	}
	return "Bearer " + segment(map[string]string{"alg": "HS256"}) + "." + segment(map[string]string{"org_id": org}) + ".sig"
}

func newGoEdgeRunner(t *testing.T, edge *fakeGoEdge) *Runner {
	t.Helper()
	server := httptest.NewServer(edge.handler())
	t.Cleanup(server.Close)
	store, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := DefaultGoServedLedger()
	if err != nil {
		t.Fatal(err)
	}
	return &Runner{
		Client:    NewLegClient(0),
		GoServed:  ledger,
		Documents: map[string]string{"featureFlags": "query FeatureFlags { featureFlags { key } }"},
		Registry: RegistryView{
			SchemaDigest:   "sha256:29d509cd",
			BuildIdentity:  goEdgeBuild,
			DocumentDigest: map[string]string{"featureFlags": "06ca28a0"},
		},
		Routing:   map[string]RoutingRow{"featureFlags": {Mode: "canary", CandidateBuild: goEdgeBuild}},
		Artifacts: store,
		Config: Config{
			OrgID:           goEdgeOrg,
			Window:          DefaultWindow(),
			PythonEdgeURL:   server.URL,
			GoEdge:          true,
			Auth:            AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "local-dev-20260906"},
			EdgeCredential:  StaticCredential("Authorization", "edge access token", orgToken(goEdgeOrg)).BindOrg(goEdgeOrg),
			ProofCredential: StaticCredential("Authorization", "envelope", "Bearer envelope"),
		},
	}
}

// TestGoEdgeProvesAnOperationByItsCandidateAlone: against query-api alone the
// operation is proven by the go-only class, the outcome, the summary and the
// receipt all say the mode, and the receipt is the ledger's own citation.
func TestGoEdgeProvesAnOperationByItsCandidateAlone(t *testing.T) {
	edge := queryAPIAlone()
	runner := newGoEdgeRunner(t, edge)
	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.EdgeMode != EdgeModeGo || summary.Attempted != 1 || summary.Executed != 1 || summary.Refused != 0 || summary.ProvenGoOnly != 1 {
		t.Fatalf("summary %+v, want one go-edge go-only proof", summary)
	}
	outcome := outcomes[0]
	if outcome.EdgeMode != EdgeModeGo || outcome.ProvenUnder != ProvenUnderGoOnly || outcome.TerminalState != TerminalStateMismatch ||
		outcome.DifferencesOutsideBaselineDefect != 0 || outcome.EdgeBuildBinding != EdgeBuildPresent {
		t.Fatalf("outcome %+v, want a go-only proof bound to the build, with nothing outside its citation", outcome)
	}
	ledger, _ := DefaultGoServedLedger()
	want, _ := NewGoOnlyCitation(ledger, "featureFlags")
	if len(outcome.BaselineDefects) != 1 || outcome.BaselineDefects[0] != want {
		t.Fatalf("citations %v, want the ledger's own %q", outcome.BaselineDefects, want)
	}
	// The probe first, then the candidate and the control document of the
	// case: the registered text, and that text plus the inert comment.
	document := runner.Documents["featureFlags"]
	if len(edge.seen) != 3 || edge.seen[0] != goEdgeControlProbe || edge.seen[1] != document || edge.seen[2] != document+baselineComment {
		t.Fatalf("the edge saw %q", edge.seen)
	}
	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil || len(receipts) != 1 {
		t.Fatalf("ReceiptsFor: %v, %d receipts", err, len(receipts))
	}
	var provenance ReceiptProvenance
	if err := json.Unmarshal([]byte(receipts[0].ReviewEvidence), &provenance); err != nil || provenance.EdgeMode != EdgeModeGo {
		t.Fatalf("receipt provenance %q (err %v), want edge_mode=go", receipts[0].ReviewEvidence, err)
	}
}

// TestGoEdgeRefusesAnEdgeThatIsNotQueryAPIAlone plants each control answer
// that is not query-api refusing an unregistered document from the named
// build. Planted on the pre-run probe, the run does not start; planted on a
// case's control document, the case is refused. Each by its own name.
func TestGoEdgeRefusesAnEdgeThatIsNotQueryAPIAlone(t *testing.T) {
	refusal := queryAPIAlone().control
	with := func(change func(*goEdgeLeg)) goEdgeLeg {
		leg := refusal
		change(&leg)
		return leg
	}
	for name, tc := range map[string]struct {
		leg  goEdgeLeg
		want string
	}{
		"answered by plane python (a Python plane behind the edge)": {with(func(l *goEdgeLeg) { l.plane, l.status, l.body = "python", 200, `{"data":{"__typename":"Query"}}` }), RefusalGoEdgeControlOtherPlane},
		"python plane refusing it":                                  {with(func(l *goEdgeLeg) { l.plane = "python" }), RefusalGoEdgeControlOtherPlane},
		"no plane header":                                           {with(func(l *goEdgeLeg) { l.plane = "-" }), RefusalGoEdgeControlPlaneUnidentified},
		"served with a 200 by plane go":                             {with(func(l *goEdgeLeg) { l.status, l.body = 200, goEdgeAnswer }), RefusalGoEdgeControlServed},
		"a 404 that is not the unregistered refusal":                {with(func(l *goEdgeLeg) { l.body = `{"errors":[{"message":"not found"}],"data":null}` }), RefusalGoEdgeControlNotRefused},
		"the unregistered body under another status":                {with(func(l *goEdgeLeg) { l.status = 500 }), RefusalGoEdgeControlNotRefused},
		"the unregistered error beside data": {with(func(l *goEdgeLeg) {
			l.body = `{"errors":[{"message":"x","extensions":{"code":"UNREGISTERED_DOCUMENT"}}],"data":{"a":1}}`
		}), RefusalGoEdgeControlNotRefused},
		"a 404 whose body is not JSON":                    {with(func(l *goEdgeLeg) { l.body = "<html>not found</html>" }), RefusalGoEdgeControlNotRefused},
		"the unregistered refusal followed by more bytes": {with(func(l *goEdgeLeg) { l.body = unregisteredBody + `{"x":1}` }), RefusalGoEdgeControlNotRefused},
		"a proxy page with no plane header and no JSON":   {with(func(l *goEdgeLeg) { l.plane, l.body = "-", "<html>bad gateway</html>" }), RefusalGoEdgeControlPlaneUnidentified},
		"no build header":                                 {with(func(l *goEdgeLeg) { l.build = "-" }), RefusalGoEdgeControlBuildUnbound},
		"another build":                                   {with(func(l *goEdgeLeg) { l.build = "ffffffffffffffffffffffffffffffffffffffff" }), RefusalBuildMismatch},
		"an impersonation stamp":                          {with(func(l *goEdgeLeg) { l.impersonating = true }), RefusalServedUnderImpersonation},
	} {
		// On the pre-run probe: nothing is attempted.
		edge := queryAPIAlone()
		edge.probe = tc.leg
		_, summary, err := newGoEdgeRunner(t, edge).Run(context.Background())
		if !errors.Is(err, ErrGoEdge) || !strings.Contains(err.Error(), tc.want) || summary.Attempted != 0 || len(edge.seen) != 1 {
			t.Errorf("%s, on the probe: err %v attempted %d seen %d; want %s and no case sent", name, err, summary.Attempted, len(edge.seen), tc.want)
		}
		// On a case's control document: that case is refused by name.
		edge = queryAPIAlone()
		edge.control = tc.leg
		outcomes, _, _ := newGoEdgeRunner(t, edge).Run(context.Background())
		if len(outcomes) != 1 || outcomes[0].Executed || outcomes[0].RefusalReason != tc.want {
			t.Errorf("%s, on a case: outcomes %+v; want refused %s", name, outcomes, tc.want)
		}
	}
}

// TestGoEdgeRefusesACandidateItCannotTieToQueryAPI plants each candidate
// answer that lacks a fact this mode needs.
func TestGoEdgeRefusesACandidateItCannotTieToQueryAPI(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*goEdgeLeg)
		want   string
	}{
		"no plane header":        {func(l *goEdgeLeg) { l.plane = "-" }, RefusalPlaneUnidentified},
		"served by plane python": {func(l *goEdgeLeg) { l.plane = "python" }, RefusalWrongPlane},
		"no build header":        {func(l *goEdgeLeg) { l.build = "-" }, RefusalGoEdgeCandidateBuildUnbound},
		"another build":          {func(l *goEdgeLeg) { l.build = "ffffffffffffffffffffffffffffffffffffffff" }, RefusalBuildMismatch},
		"an impersonation stamp": {func(l *goEdgeLeg) { l.impersonating = true }, RefusalServedUnderImpersonation},
		"a 500":                  {func(l *goEdgeLeg) { l.status = 500 }, RefusalNonSuccessStatus},
		"an HTML content type":   {func(l *goEdgeLeg) { l.contentType = "text/html" }, RefusalGoEdgeCandidateContentType},
		"a GraphQL error":        {func(l *goEdgeLeg) { l.body = `{"errors":[{"message":"boom"}],"data":null}` }, RefusalErroredResponse},
		"a null root":            {func(l *goEdgeLeg) { l.body = `{"data":{"featureFlags":null}}` }, RefusalEmptyResponseRoot},
		"bytes after the value":  {func(l *goEdgeLeg) { l.body = goEdgeAnswer + `{"x":1}` }, RefusalTrailingBytes},
	} {
		edge := queryAPIAlone()
		tc.change(&edge.candidate)
		outcomes, summary, _ := newGoEdgeRunner(t, edge).Run(context.Background())
		if len(outcomes) != 1 || outcomes[0].Executed || outcomes[0].RefusalReason != tc.want || summary.ProvenGoOnly != 0 {
			t.Errorf("%s: outcomes %+v; want refused %s and nothing proven", name, outcomes, tc.want)
		}
	}
}

// TestGoEdgeRefusesAPrincipalItCannotBindToTheOrg: the org the edge serves
// the credential for is the credential's own org claim, so a credential that
// is not bound to the run's org, or that names another org, stops the run.
func TestGoEdgeRefusesAPrincipalItCannotBindToTheOrg(t *testing.T) {
	for name, credential := range map[string]*Credential{
		"not bound":                 StaticCredential("Authorization", "edge access token", orgToken(goEdgeOrg)),
		"bound to another org":      StaticCredential("Authorization", "edge access token", orgToken(goEdgeOrg)).BindOrg("another-org"),
		"bound, naming another org": StaticCredential("Authorization", "edge access token", orgToken("another-org")).BindOrg(goEdgeOrg),
		"bound, not a readable JWT": StaticCredential("Authorization", "edge access token", "Bearer opaque").BindOrg(goEdgeOrg),
	} {
		edge := queryAPIAlone()
		runner := newGoEdgeRunner(t, edge)
		runner.Config.EdgeCredential = credential
		_, summary, err := runner.Run(context.Background())
		if !errors.Is(err, ErrGoEdge) || summary.Attempted != 0 {
			t.Errorf("%s: err %v attempted %d; want the run refused before any case", name, err, summary.Attempted)
		}
	}
	// The admin credential, when the run has one, is held to the same rule.
	runner := newGoEdgeRunner(t, queryAPIAlone())
	runner.Config.AdminEdgeCredential = StaticCredential("Authorization", "admin edge access token", orgToken(goEdgeOrg))
	if _, _, err := runner.Run(context.Background()); !errors.Is(err, ErrGoEdge) {
		t.Errorf("an unbound admin credential: err %v, want the run refused", err)
	}
}

// TestGoEdgeProvesOnlyAnOperationTheLedgerNames: with no Python plane to
// compare against, the ledger's citation is what stands where a comparison
// would; an operation it does not name is refused, never proven alone.
func TestGoEdgeProvesOnlyAnOperationTheLedgerNames(t *testing.T) {
	for name, ledger := range map[string]*GoServedLedger{"no ledger": nil, "a ledger without the operation": ledgerWithout(t, "featureFlags")} {
		runner := newGoEdgeRunner(t, queryAPIAlone())
		runner.GoServed = ledger
		outcomes, summary, _ := runner.Run(context.Background())
		if len(outcomes) != 1 || outcomes[0].Executed || outcomes[0].RefusalReason != RefusalGoEdgeNotGoServed || summary.ProvenGoOnly != 0 {
			t.Errorf("%s: outcomes %+v; want refused %s", name, outcomes, RefusalGoEdgeNotGoServed)
		}
	}
}

// ledgerWithout is the embedded ledger with one operation removed.
func ledgerWithout(t *testing.T, operation string) *GoServedLedger {
	t.Helper()
	var file map[string]any
	if err := json.Unmarshal(goServedLedgerJSON, &file); err != nil {
		t.Fatal(err)
	}
	entries, _ := file["entries"].([]any)
	var kept []any
	for _, entry := range entries {
		if named, _ := entry.(map[string]any); named["operation"] != operation {
			kept = append(kept, entry)
		}
	}
	if len(kept) == len(entries) {
		t.Fatalf("the ledger does not name %s", operation)
	}
	file["entries"] = kept
	raw, _ := json.Marshal(file)
	ledger, err := ParseGoServedLedger(raw)
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}

// TestTheModesAreNeverMistakenForEachOther: the mode is the flag, never a
// guess. The Python-reference mode refuses an edge that is query-api alone
// (its control must be Python), and Go-edge mode refuses the Python edge.
func TestTheModesAreNeverMistakenForEachOther(t *testing.T) {
	// Python-reference mode against query-api alone.
	runner := newGoEdgeRunner(t, queryAPIAlone())
	runner.Config.GoEdge = false
	outcomes, summary, _ := runner.Run(context.Background())
	if summary.EdgeMode != EdgeModePython || len(outcomes) != 1 || outcomes[0].Executed || outcomes[0].RefusalReason != RefusalWrongPlane || outcomes[0].EdgeMode != "" {
		t.Errorf("python-reference mode against query-api alone: %+v; want refused %s", outcomes, RefusalWrongPlane)
	}
	// Go-edge mode against the Python edge (fakeEdge: the control document
	// is answered 200 by plane python).
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	python := newRunner(t, &fakeEdge{goBody: body, pythonBody: body, goBuild: goEdgeBuild}, "canary")
	python.Config.GoEdge = true
	python.Config.OrgID = goEdgeOrg
	python.Config.EdgeCredential = StaticCredential("Authorization", "edge access token", orgToken(goEdgeOrg)).BindOrg(goEdgeOrg)
	if _, _, err := python.Run(context.Background()); !errors.Is(err, ErrGoEdge) || !strings.Contains(err.Error(), RefusalGoEdgeControlOtherPlane) {
		t.Errorf("go-edge mode against the Python edge: err %v; want %s", err, RefusalGoEdgeControlOtherPlane)
	}
}

// TestTheDocNamesEveryGoEdgeRefusal: the architecture note lists what Go-edge
// mode refuses, by the names an operator will read on a refused run. A
// refusal the note does not name, or names differently, fails here.
func TestTheDocNamesEveryGoEdgeRefusal(t *testing.T) {
	raw, err := os.ReadFile("../../docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md")
	if err != nil {
		t.Fatal(err)
	}
	note := string(raw)
	start := strings.Index(note, "### The two edge modes of `dho goapi prove`")
	if start < 0 {
		t.Fatal("the architecture note has no section on the two edge modes")
	}
	note = note[start:]
	for _, name := range []string{
		RefusalGoEdgeControlOtherPlane, RefusalGoEdgeControlPlaneUnidentified, RefusalGoEdgeControlServed,
		RefusalGoEdgeControlNotRefused, RefusalGoEdgeControlBuildUnbound, RefusalGoEdgeCandidateBuildUnbound,
		RefusalGoEdgeCandidateContentType, RefusalGoEdgeNotGoServed, RefusalBuildMismatch, RefusalPlaneUnidentified,
		RefusalWrongPlane, RefusalServedUnderImpersonation, "go_edge_mode_refused", UnregisteredDocumentCode,
		VerdictGoOnly + " (go-edge mode", "-go-edge",
	} {
		if !strings.Contains(note, name) {
			t.Errorf("the note on the two edge modes does not name %q", name)
		}
	}
	if !strings.Contains(ErrGoEdge.Error(), "go_edge_mode_refused") {
		t.Errorf("ErrGoEdge reads %q, and the note names go_edge_mode_refused", ErrGoEdge)
	}
}
