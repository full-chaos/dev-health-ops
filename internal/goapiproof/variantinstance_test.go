package goapiproof

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// RequireNonEmpty refuses a request whose named collection is empty or
// absent on BOTH legs, and leaves a one-sided empty to the comparison.
func TestRequireNonEmpty_Cells(t *testing.T) {
	empty := `{"data":{"busFactor":{"repos":[],"value":0}}}`
	full := `{"data":{"busFactor":{"repos":[{"repoId":"r1"}],"value":1}}}`
	absent := `{"data":{"busFactor":{"value":0}}}`
	nullList := `{"data":{"busFactor":{"repos":null,"value":0}}}`
	wrongType := `{"data":{"busFactor":{"repos":{"a":1},"value":0}}}`
	opts := Options{RequireNonEmpty: []string{"data.busFactor.repos"}}
	cases := []struct {
		name         string
		base, cand   string
		wantVacuous  bool
		wantTerminal string
	}{
		{"both empty", empty, empty, true, ""},
		{"both absent", absent, absent, true, ""},
		{"both null", nullList, nullList, true, ""},
		{"both wrong container", wrongType, wrongType, true, ""},
		{"empty vs absent", empty, absent, true, ""},
		{"one side empty is a difference, not vacuity", empty, full, false, ""},
		{"other side empty", full, empty, false, ""},
		{"both non-empty and equal", full, full, false, "match"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := Compare(snapshotFromJSON(t, c.base), snapshotFromJSON(t, c.cand), opts)
			if (res.StructuralRefusal == RefusalVacuousEmptyLegs) != c.wantVacuous {
				t.Fatalf("StructuralRefusal=%q detail=%q", res.StructuralRefusal, res.StructuralDetail)
			}
			if c.wantVacuous && !strings.Contains(res.StructuralDetail, "data.busFactor.repos") {
				t.Errorf("the refusal must name the collection: %q", res.StructuralDetail)
			}
			if !c.wantVacuous && res.StructuralRefusal != "" {
				t.Errorf("unexpected structural refusal %q", res.StructuralRefusal)
			}
		})
	}
	// With nothing declared the same empty pair keeps today's behaviour.
	res := Compare(snapshotFromJSON(t, empty), snapshotFromJSON(t, empty), Options{})
	if res.StructuralRefusal != "" {
		t.Errorf("an undeclared request must be unchanged, got %q", res.StructuralRefusal)
	}
}

func TestInstanceKey_Form(t *testing.T) {
	if got := InstanceKey("busFactor", "REPO_VALID"); got != "busFactor.REPO_VALID" {
		t.Fatalf("key %q", got)
	}
}

// variantRunner builds a Runner over a fake edge whose two legs both answer
// answer, recording the variables each request carried.
func variantRunner(t *testing.T, answer string, instanceIDs map[string]string) (*Runner, *[]map[string]any) {
	t.Helper()
	var mu sync.Mutex
	sent := &[]map[string]any{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		_ = json.Unmarshal(raw, &parsed)
		mu.Lock()
		*sent = append(*sent, parsed.Variables)
		mu.Unlock()
		w.Header().Set(buildHeader, "b18e56fa79cfe20ce0f75df148144b832d92be36")
		if strings.Contains(parsed.Query, "python-plane control") {
			w.Header().Set(planeHeader, "python")
		} else {
			w.Header().Set(planeHeader, "go")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(answer))
	}))
	t.Cleanup(server.Close)
	store, err := NewArtifactStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &Runner{
		Client:    NewLegClient(0),
		Documents: map[string]string{"busFactor": "query BusFactor($orgId: String!, $scope: BusFactorScopeInput = null) { busFactor(orgId: $orgId, scope: $scope) { value } }"},
		Registry: RegistryView{SchemaDigest: "sha256:29d509cd", BuildIdentity: "b18e56fa79cfe20ce0f75df148144b832d92be36",
			DocumentDigest: map[string]string{"busFactor": "06ca28a0"}},
		Routing:   map[string]RoutingRow{"busFactor": {Mode: "canary", CandidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}},
		Artifacts: store,
		Config: Config{OrgID: "70d529e0", Window: DefaultWindow(), PythonEdgeURL: server.URL,
			Auth:            AuthContext{PrincipalKind: "stored_account", Audience: "query-api", KeyID: "local-dev-20260906"},
			EdgeCredential:  StaticCredential("Authorization", "edge access token", "Bearer edge"),
			ProofCredential: StaticCredential("Authorization", "envelope", "Bearer envelope"),
			InstanceIDs:     instanceIDs},
	}, sent
}

func findVariant(t *testing.T, outcomes []Outcome, name string) Outcome {
	t.Helper()
	for _, o := range outcomes {
		if o.Variant == name {
			return o
		}
	}
	t.Fatalf("no outcome for variant %s", name)
	return Outcome{}
}

// A variant that needs an identifier is refused by name when the run
// supplies none, and nothing reaches the wire for it.
func TestVariantNeedingAnInstanceIsRefusedWithoutOne(t *testing.T) {
	runner, sent := variantRunner(t, `{"data":{"busFactor":{"value":1,"repos":[{"repoId":"r1"}]}}}`, nil)
	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	o := findVariant(t, outcomes, "REPO_VALID")
	if o.RefusalReason != RefusalNeedsInstanceID || !strings.Contains(o.RefusalDetail, "-instance-id busFactor.REPO_VALID=") {
		t.Fatalf("outcome %s / %s", o.RefusalReason, o.RefusalDetail)
	}
	for _, vars := range *sent {
		if scope, ok := vars["scope"].(map[string]any); ok && scope["repoId"] != nil && scope["repoId"] != "" {
			if id := scope["repoId"]; id != "not-a-uuid" && id != "00000000-0000-0000-0000-000000000001" {
				t.Fatalf("a guessed identifier reached the wire: %v", id)
			}
		}
	}
}

// With an identifier the variant is measured and the supplied value is what
// goes on the wire.
func TestVariantWithASuppliedInstanceSendsIt(t *testing.T) {
	const id = "9f5c2e6a-1111-2222-3333-444455556666"
	runner, sent := variantRunner(t, `{"data":{"busFactor":{"scope":{"repoId":"`+id+`"},"value":1,"repos":[{"repoId":"`+strings.ToUpper(id)+`"}]}}}`, map[string]string{"busFactor.REPO_VALID": id})
	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	o := findVariant(t, outcomes, "REPO_VALID")
	if o.RefusalReason != "" || !o.Admitted {
		t.Fatalf("outcome refused=%s admitted=%v", o.RefusalReason, o.Admitted)
	}
	seen := 0
	for _, vars := range *sent {
		if scope, ok := vars["scope"].(map[string]any); ok && scope["repoId"] == id {
			seen++
		}
	}
	if seen != 2 {
		t.Fatalf("expected the supplied id on both legs, saw %d", seen)
	}
}

// A supplied identifier that yields an empty collection on both legs is
// refused as vacuous, never recorded as a match.
func TestVariantWithAnEmptyAnswerOnBothLegsIsRefused(t *testing.T) {
	runner, _ := variantRunner(t, `{"data":{"busFactor":{"value":0,"repos":[]}}}`, map[string]string{"busFactor.REPO_VALID": "9f5c2e6a-1111-2222-3333-444455556666"})
	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	o := findVariant(t, outcomes, "REPO_VALID")
	if o.TerminalState == "match" || o.Executed {
		t.Fatalf("an empty answer on both legs was recorded as measured: terminal=%s executed=%v", o.TerminalState, o.Executed)
	}
	if o.RefusalReason != RefusalVacuousEmptyLegs {
		t.Fatalf("refusal %q / %q", o.RefusalReason, o.RefusalDetail)
	}
}

// A non-empty answer that does not show the supplied scope is refused: both
// planes returning an org-wide repository list for a one-repository request
// must never be recorded as a match.
func TestVariantWhoseAnswerIgnoresTheSuppliedScopeIsRefused(t *testing.T) {
	runner, _ := variantRunner(t, `{"data":{"busFactor":{"value":1,"repos":[{"repoId":"org-wide"}]}}}`, map[string]string{"busFactor.REPO_VALID": "9f5c2e6a-1111-2222-3333-444455556666"})
	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	o := findVariant(t, outcomes, "REPO_VALID")
	if o.Executed || o.RefusalReason != RefusalScopeNotReflected {
		t.Fatalf("executed=%v refusal=%q", o.Executed, o.RefusalReason)
	}
}

func TestScopeEcho_Cells(t *testing.T) {
	list := func(body string) string { return `{"data":{"securityAlerts":{"edges":` + body + `}}}` }
	echo := []ScopeEcho{{List: "data.securityAlerts.edges", Fields: []string{"node.repoId"}, Value: "R1"}}
	contains := []ScopeEcho{{List: "data.securityAlerts.edges", Fields: []string{"node.title", "node.packageName", "node.cveId"}, Contains: true, Value: "LoDash"}}
	first := []ScopeEcho{{List: "data.securityAlerts.edges", Fields: []string{"cursor"}, FirstOnly: true, Value: "6"}}
	match := list(`[{"node":{"repoId":"r1","title":"Lodash flaw"},"cursor":"6"},{"node":{"repoId":"R1","title":null,"packageName":"x-lodash-y"},"cursor":"7"}]`)
	stray := list(`[{"node":{"repoId":"r1"},"cursor":"6"},{"node":{"repoId":"r2"},"cursor":"7"}]`)
	cases := []struct {
		name       string
		base, cand string
		echo       []ScopeEcho
		refused    bool
	}{
		{"every element carries the value (case-insensitive)", match, match, echo, false},
		{"one stray element on the baseline", stray, match, echo, true},
		{"one stray element on the candidate", match, stray, echo, true},
		{"empty on one leg is not a scope failure", list(`[]`), match, echo, false},
		{"absent list is not a scope failure", `{"data":{"securityAlerts":{}}}`, match, echo, false},
		{"substring in any of the fields", match, match, contains, false},
		{"substring absent from every field", stray, stray, contains, true},
		{"first cursor as required", match, match, first, false},
		{"first cursor wrong", list(`[{"cursor":"1"}]`), list(`[{"cursor":"6"}]`), first, true},
		{"only the first element is checked when asked", list(`[{"cursor":"6"},{"cursor":"9"}]`), list(`[{"cursor":"6"}]`), first, false},
		{"first-only on an empty leg is not a scope failure", list(`[]`), list(`[]`), first, false},
		{"scalar echo equal", `{"data":{"securityAlerts":{"scope":{"repoId":"r1"}}}}`, `{"data":{"securityAlerts":{"scope":{"repoId":"R1"}}}}`, []ScopeEcho{{List: "data.securityAlerts.scope.repoId", Scalar: true, Value: "r1"}}, false},
		{"scalar echo null", `{"data":{"securityAlerts":{"scope":{"repoId":null}}}}`, `{"data":{"securityAlerts":{"scope":{"repoId":"r1"}}}}`, []ScopeEcho{{List: "data.securityAlerts.scope.repoId", Scalar: true, Value: "r1"}}, true},
		{"scalar echo other", `{"data":{"securityAlerts":{"scope":{"repoId":"r1"}}}}`, `{"data":{"securityAlerts":{"scope":{"repoId":"r2"}}}}`, []ScopeEcho{{List: "data.securityAlerts.scope.repoId", Scalar: true, Value: "r1"}}, true},
		{"field missing on an element", list(`[{"node":{}}]`), match, echo, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := Compare(snapshotFromJSON(t, c.base), snapshotFromJSON(t, c.cand), Options{ScopeEcho: c.echo})
			if (res.StructuralRefusal == RefusalScopeNotReflected) != c.refused {
				t.Fatalf("StructuralRefusal=%q detail=%q", res.StructuralRefusal, res.StructuralDetail)
			}
		})
	}
}

// An org-wide answer that happens to list only the requested repository
// still fails to echo the scope: the answer's own scope must name it.
func TestBusFactorVariantRequiresTheEchoedScope(t *testing.T) {
	const id = "9f5c2e6a-1111-2222-3333-444455556666"
	for name, scope := range map[string]string{"null scope repo": `"scope":{"repoId":null}`, "absent scope": `"scope":null`, "other repo": `"scope":{"repoId":"other"}`} {
		runner, _ := variantRunner(t, `{"data":{"busFactor":{`+scope+`,"value":1,"repos":[{"repoId":"`+id+`"}]}}}`, map[string]string{"busFactor.REPO_VALID": id})
		outcomes, _, err := runner.Run(context.Background())
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		o := findVariant(t, outcomes, "REPO_VALID")
		if o.Executed || o.RefusalReason != RefusalScopeNotReflected {
			t.Errorf("%s: executed=%v refusal=%q", name, o.Executed, o.RefusalReason)
		}
	}
}

// The second-page variant compares cursors like any other field: a cursor
// difference between the planes stays a mismatch, never a scope refusal.
func TestSecondPageVariantDoesNotTurnACursorDifferenceIntoARefusal(t *testing.T) {
	spec, err := SpecFor("securityAlerts")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range spec.Variants {
		if v.Name != "PAGE_AFTER" {
			continue
		}
		base := `{"data":{"securityAlerts":{"edges":[{"cursor":"6"}]}}}`
		cand := `{"data":{"securityAlerts":{"edges":[{"cursor":"7"}]}}}`
		res := Compare(snapshotFromJSON(t, base), snapshotFromJSON(t, cand), v.Parity)
		if res.StructuralRefusal != "" {
			t.Fatalf("a cursor difference was refused as %q instead of compared", res.StructuralRefusal)
		}
		if res.TerminalState != "mismatch" {
			t.Fatalf("a cursor difference must stay a mismatch, got %q", res.TerminalState)
		}
		return
	}
	t.Fatal("PAGE_AFTER variant missing")
}

// Every compoundingRisk case that can return rows requires them and shows the
// breakout asked for; the cases that name nothing declare no row timestamp
// difference (it would match nothing and refuse them on every run).
func TestCompoundingRiskVariants_RequireRowsOrDeclareNoRowLeaf(t *testing.T) {
	spec, err := SpecFor("compoundingRisk")
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]string{"REPO_BREAKOUT": "REPO", "TEAM_BREAKOUT": "TEAM", "DAY": "REPO", "TEAM_DAY": "TEAM",
		"TREND_ONE_DAY": "REPO", "TREND_CLAMPED_HIGH": "REPO", "TREND_CLAMPED_LOW": "REPO", "REPO_VALID": "", "TEAM_STORED": ""}
	empty := map[string]bool{"REPO_IDS_UNKNOWN": true, "REPO_IDS_EMPTY": true, "TEAM_IDS_UNKNOWN": true, "TEAM_IDS_EMPTY": true, "TEAM_AND_REPO_UNKNOWN": true}
	if got := spec.Parity.ScopeEcho; len(got) != 1 || got[0].Value != "REPO" || len(spec.Parity.RequireNonEmpty) != 1 {
		t.Errorf("base: %#v", spec.Parity)
	}
	seen := 0
	for _, v := range spec.Variants {
		seen++
		if breakout, ok := rows[v.Name]; ok {
			if len(v.Parity.RequireNonEmpty) != 1 || v.Parity.RequireNonEmpty[0] != "data.compoundingRisk.rows" {
				t.Errorf("%s must require rows: %#v", v.Name, v.Parity.RequireNonEmpty)
			}
			if breakout != "" && (len(v.Parity.ScopeEcho) != 1 || v.Parity.ScopeEcho[0].Value != breakout || v.Parity.ScopeEcho[0].Fields[0] != "scope") {
				t.Errorf("%s must echo breakout %s: %#v", v.Name, breakout, v.Parity.ScopeEcho)
			}
			continue
		}
		if !empty[v.Name] {
			t.Errorf("unclassified compoundingRisk variant %s", v.Name)
			continue
		}
		if len(v.Parity.BaselineDefects) != 0 || len(v.Parity.RequireNonEmpty) != 0 || len(v.Parity.VolatileFields) != 1 {
			t.Errorf("%s: %#v", v.Name, v.Parity)
		}
	}
	if seen != len(rows)+len(empty) {
		t.Errorf("%d variants, classified %d", seen, len(rows)+len(empty))
	}
}
