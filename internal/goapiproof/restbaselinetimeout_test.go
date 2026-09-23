package goapiproof

import (
	"strings"
	"testing"
	"time"
)

func validDeclaration() (BaselineTimeoutDeclaration, RESTRequest) {
	decl := BaselineTimeoutDeclaration{
		Ticket: "ABC-123", Reason: "the reference plane does not answer",
		MinTimeout: BaselineTimeoutFloor, NonEmptyPaths: []string{"data"},
	}
	req := RESTRequest{
		Name: "r", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: RESTBodyModeJSON, Timeout: BaselineTimeoutFloor,
		BaselineTimeoutDeclared: &decl,
	}
	return decl, req
}

// Every clause of Validate has one cell that reddens only it.
func TestBaselineTimeoutDeclarationValidate_OneCellPerClause(t *testing.T) {
	if decl, req := validDeclaration(); decl.Validate(req) != nil {
		t.Fatalf("the canonical declaration must validate: %v", decl.Validate(req))
	}
	for _, cell := range []struct {
		name   string
		mutate func(*BaselineTimeoutDeclaration, *RESTRequest)
		want   string
	}{
		{"blank ticket", func(d *BaselineTimeoutDeclaration, _ *RESTRequest) { d.Ticket = "  " }, "blank Ticket"},
		{"go-only ticket", func(d *BaselineTimeoutDeclaration, _ *RESTRequest) { d.Ticket = "GO-ONLY:op=x" }, "no REST citation may carry"},
		{"blank reason", func(d *BaselineTimeoutDeclaration, _ *RESTRequest) { d.Reason = "" }, "blank Reason"},
		{"min timeout below floor", func(d *BaselineTimeoutDeclaration, r *RESTRequest) {
			d.MinTimeout = BaselineTimeoutFloor - time.Second
		}, "below the"},
		{"request timeout below min", func(_ *BaselineTimeoutDeclaration, r *RESTRequest) { r.Timeout = BaselineTimeoutFloor - time.Second }, "at least its MinTimeout"},
		{"request timeout unset", func(_ *BaselineTimeoutDeclaration, r *RESTRequest) { r.Timeout = 0 }, "at least its MinTimeout"},
		{"no paths", func(d *BaselineTimeoutDeclaration, _ *RESTRequest) { d.NonEmptyPaths = nil }, "no NonEmptyPaths"},
		{"path not rooted at data", func(d *BaselineTimeoutDeclaration, _ *RESTRequest) { d.NonEmptyPaths = []string{"deltas"} }, "does not start at"},
		{"path with blank segment", func(d *BaselineTimeoutDeclaration, _ *RESTRequest) { d.NonEmptyPaths = []string{"data..x"} }, "blank segment"},
		{"bounded-candidate binding", func(_ *BaselineTimeoutDeclaration, r *RESTRequest) {
			r.IDBindings = []RESTIDBinding{{Producer: "p", QueryParam: "q", Candidates: 3, ExposeAs: "e"}}
		}, "bounded-candidate binding"},
		{"candidate status not 200", func(_ *BaselineTimeoutDeclaration, r *RESTRequest) { r.WantCandidateStatus = 503 }, "WantCandidateStatus 200"},
		{"status-only body", func(_ *BaselineTimeoutDeclaration, r *RESTRequest) { r.BodyMode = RESTBodyModeStatusOnly }, "BodyMode"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			decl, req := validDeclaration()
			cell.mutate(&decl, &req)
			err := decl.Validate(req)
			if err == nil || !strings.Contains(err.Error(), cell.want) {
				t.Fatalf("Validate = %v, want an error containing %q", err, cell.want)
			}
		})
	}
}

func TestSnapshotHoldsNonEmpty_Domain(t *testing.T) {
	decode := func(body string) Snapshot {
		s, err := DecodeRESTSnapshot([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	for _, cell := range []struct {
		name, body, path string
		want             bool
	}{
		{"root array with rows", `[{"a":1}]`, "data", true},
		{"root empty array", `[]`, "data", false},
		{"root null", `null`, "data", false},
		{"root empty object", `{}`, "data", false},
		{"root empty string", `""`, "data", false},
		{"root zero number", `0`, "data", true},
		{"root false", `false`, "data", true},
		{"child array with rows", `{"k":[1]}`, "data.k", true},
		{"child empty array", `{"k":[]}`, "data.k", false},
		{"child null", `{"k":null}`, "data.k", false},
		{"child missing", `{"other":[1]}`, "data.k", false},
		{"child under a list is not a map", `[{"k":[1]}]`, "data.k", false},
		{"grandchild under a scalar", `{"k":3}`, "data.k.j", false},
		{"nested object with a key", `{"k":{"j":1}}`, "data.k", true},
		{"path not rooted at data", `{"k":[1]}`, "k", false},
	} {
		if got := SnapshotHoldsNonEmpty(decode(cell.body), cell.path); got != cell.want {
			t.Errorf("%s: SnapshotHoldsNonEmpty(%s, %q) = %v, want %v", cell.name, cell.body, cell.path, got, cell.want)
		}
	}
	if SnapshotHoldsNonEmpty(Snapshot{}, "data") {
		t.Error("a snapshot with no decoded body holds nothing")
	}
}

func TestRESTAdmitCandidateAlone_Cells(t *testing.T) {
	const build = "abc123"
	decl := BaselineTimeoutDeclaration{NonEmptyPaths: []string{"data.items"}}
	ok := RESTLeg{StatusCode: 200, Build: build, Body: []byte(`{"items":[1]}`)}
	for _, cell := range []struct {
		name string
		leg  func(RESTLeg) RESTLeg
		want string
	}{
		{"canonical", func(l RESTLeg) RESTLeg { return l }, ""},
		{"impersonated", func(l RESTLeg) RESTLeg { l.Impersonating = true; return l }, RESTRefusalServedUnderImpersonation},
		{"status 503", func(l RESTLeg) RESTLeg { l.StatusCode = 503; return l }, RESTRefusalUnexpectedStatus},
		{"status 404", func(l RESTLeg) RESTLeg { l.StatusCode = 404; return l }, RESTRefusalUnexpectedStatus},
		{"no build header", func(l RESTLeg) RESTLeg { l.Build = ""; return l }, RESTRefusalBuildUnbound},
		{"other build", func(l RESTLeg) RESTLeg { l.Build = "zzz"; return l }, RESTRefusalBuildUnbound},
		{"not json", func(l RESTLeg) RESTLeg { l.Body = []byte(`nope`); return l }, RESTRefusalBodyNotJSON},
		{"empty body", func(l RESTLeg) RESTLeg { l.Body = nil; return l }, RESTRefusalBodyNotJSON},
		{"trailing bytes", func(l RESTLeg) RESTLeg { l.Body = []byte(`{"items":[1]}x`); return l }, RESTRefusalTrailingBytes},
		{"empty list", func(l RESTLeg) RESTLeg { l.Body = []byte(`{"items":[]}`); return l }, RESTRefusalCandidateEmptyUnderBaselineTimeout},
		{"null list", func(l RESTLeg) RESTLeg { l.Body = []byte(`{"items":null}`); return l }, RESTRefusalCandidateEmptyUnderBaselineTimeout},
		{"missing list", func(l RESTLeg) RESTLeg { l.Body = []byte(`{}`); return l }, RESTRefusalCandidateEmptyUnderBaselineTimeout},
	} {
		t.Run(cell.name, func(t *testing.T) {
			got := RESTAdmitCandidateAlone(RESTAdmissionInput{NamedBuild: build, WantCandidateStatus: 200, Candidate: cell.leg(ok)}, decl)
			if cell.want == "" {
				if !got.Admitted {
					t.Fatalf("refused %q: %s", got.Reason, got.Detail)
				}
				return
			}
			if got.Admitted || got.Reason != cell.want {
				t.Fatalf("admitted=%v reason=%q, want a refusal %q", got.Admitted, got.Reason, cell.want)
			}
			// The absent-header and wrong-header clauses share one refusal
			// reason; the detail says which clause spoke.
			switch cell.name {
			case "no build header":
				if !strings.Contains(got.Detail, "carried no x-dev-health-build header") {
					t.Fatalf("detail = %q, want the absent-header clause", got.Detail)
				}
			case "other build":
				if !strings.Contains(got.Detail, "reports build") {
					t.Fatalf("detail = %q, want the mismatched-build clause", got.Detail)
				}
			}
		})
	}
}

// Every candidate-side cell RESTAdmit refuses is refused by
// RESTAdmitCandidateAlone with the same reason: the timeout arm never admits a
// candidate the ordinary path would have refused.
func TestRESTAdmitCandidateAlone_RefusesWhatRESTAdmitRefusesOnTheCandidate(t *testing.T) {
	const build = "abc123"
	base := RESTLeg{StatusCode: 200, Server: ReferencePlaneServer, Body: []byte(`{"items":[1]}`)}
	for name, leg := range map[string]RESTLeg{
		"impersonated": {StatusCode: 200, Build: build, Impersonating: true, Body: []byte(`{"items":[1]}`)},
		"status":       {StatusCode: 500, Build: build, Body: []byte(`{"items":[1]}`)},
		"no build":     {StatusCode: 200, Body: []byte(`{"items":[1]}`)},
		"wrong build":  {StatusCode: 200, Build: "z", Body: []byte(`{"items":[1]}`)},
		"not json":     {StatusCode: 200, Build: build, Body: []byte(`x`)},
		"trailing":     {StatusCode: 200, Build: build, Body: []byte(`{"items":[1]} x`)},
	} {
		in := RESTAdmissionInput{NamedBuild: build, WantCandidateStatus: 200, WantBaselineStatus: 200, Candidate: leg, Baseline: base}
		ordinary := RESTAdmit(in, true)
		alone := RESTAdmitCandidateAlone(in, BaselineTimeoutDeclaration{NonEmptyPaths: []string{"data.items"}})
		if ordinary.Admitted {
			t.Fatalf("%s: fixture must be a candidate-side refusal under RESTAdmit", name)
		}
		if alone.Admitted || alone.Reason != ordinary.Reason {
			t.Errorf("%s: alone admitted=%v reason=%q, RESTAdmit refused with %q", name, alone.Admitted, alone.Reason, ordinary.Reason)
		}
	}
}

// Every team-scoped request on home and work-units (GET and POST) is bound to a
// live team id and carries the declaration; no other request carries one.
//
// GET/POST /api/v1/home and GET/POST /api/v1/work-units are all in
// DeletedPythonBodyOperations (CHAOS-6241, restdeletedbody.go): their
// baseline answers the fixed sentinel immediately in the overwhelming
// case, so a real timeout is rare now, not the common path it once was --
// but the declaration is left in place, not cleared: its NonEmptyPaths is
// read independently of the timeout mechanism (cmd/query-api's own
// team_scope_routes_integration_test.go cross-checks it against the real
// candidate handler's response, regardless of BodyMode), and the rare
// genuine-timeout case still has somewhere to land instead of going
// unproven.
func TestBaselineTimeoutDeclaredExactlyOnTheTeamScopedRequests(t *testing.T) {
	routes := []string{
		"REST:GET:/api/v1/home", "REST:POST:/api/v1/home",
		"REST:GET:/api/v1/work-units", "REST:POST:/api/v1/work-units",
	}
	team := map[string]bool{}
	for _, operation := range KnownRESTOperations() {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		for _, req := range spec.Requests {
			bindsTeam := false
			for _, binding := range req.IDBindings {
				if binding.Producer == "team_id" {
					bindsTeam = true
				}
			}
			isRoute := false
			for _, route := range routes {
				isRoute = isRoute || route == operation
			}
			key := operation + "/" + req.Name
			switch {
			case isRoute && bindsTeam:
				team[key] = true
				if req.BaselineTimeoutDeclared == nil {
					t.Errorf("%s binds a team id and carries no BaselineTimeoutDeclared", key)
				}
			case req.BaselineTimeoutDeclared != nil:
				t.Errorf("%s carries a BaselineTimeoutDeclared but is not a team-scoped request on the four routes", key)
			}
		}
	}
	if len(team) != 4 {
		t.Fatalf("team-scoped requests on the four routes = %v, want exactly 4 (GET+POST home home_team_scoped, GET+POST work-units team_scoped)", team)
	}
}

// ValidateRESTCorpus runs every declaration's own validation: a corpus entry
// whose declaration is invalid does not load. Rather than mutating one of
// the 4 real declared entries in place (fragile: it stops proving anything
// the day their own shape changes), this test temporarily swaps an
// existing operation's own Requests for a synthetic one carrying a
// valid-then-broken declaration -- restRunOrder requires every
// restEndpointSpecs KEY to appear exactly once (ValidateRESTCorpus
// checks the two counts agree), so this reuses an existing key rather than
// adding a new one. Which key does not matter: restdeletedbody.go's init()
// already ran once at package load, before this test's own temporary swap,
// so it never re-fires on this test's synthetic Requests.
func TestValidateRESTCorpus_RefusesAnInvalidBaselineTimeoutDeclaration(t *testing.T) {
	const operation = "REST:GET:/api/v1/quadrant"
	original := restEndpointSpecs[operation]
	t.Cleanup(func() { restEndpointSpecs[operation] = original })
	if err := ValidateRESTCorpus(); err != nil {
		t.Fatalf("the committed corpus must validate first: %v", err)
	}

	valid := &BaselineTimeoutDeclaration{
		Ticket:        "TEST-1",
		Reason:        "constructed for this test",
		MinTimeout:    BaselineTimeoutFloor,
		NonEmptyPaths: []string{"data.items"},
	}
	spec := original
	spec.Requests = []RESTRequest{{
		Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: RESTBodyModeJSON, Timeout: BaselineTimeoutFloor,
		BaselineTimeoutDeclared: valid,
	}}
	restEndpointSpecs[operation] = spec
	if err := ValidateRESTCorpus(); err != nil {
		t.Fatalf("a well-formed declaration must validate: %v", err)
	}

	broken := *valid
	broken.Ticket = ""
	spec.Requests = []RESTRequest{{
		Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: RESTBodyModeJSON, Timeout: BaselineTimeoutFloor,
		BaselineTimeoutDeclared: &broken,
	}}
	restEndpointSpecs[operation] = spec
	err := ValidateRESTCorpus()
	if err == nil || !strings.Contains(err.Error(), "blank Ticket") {
		t.Fatalf("ValidateRESTCorpus = %v, want the declaration's own refusal", err)
	}
}
