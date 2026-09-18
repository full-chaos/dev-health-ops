package goapiproof

import (
	"net/url"
	"testing"
)

func TestExtractRESTID_RootArrayOfObjects(t *testing.T) {
	body := []any{
		map[string]any{"person_id": "", "display_name": "empty first"},
		map[string]any{"person_id": "p-123", "display_name": "second"},
	}
	got, ok := ExtractRESTID(body, RESTIDProducer{Name: "person_id", IDField: "person_id"})
	if !ok {
		t.Fatal("want an id, got none")
	}
	// The FIRST element's person_id is empty -- ExtractRESTID must skip it
	// and bind the first NON-EMPTY id, not simply element zero.
	if got != "p-123" {
		t.Fatalf("got %q, want p-123 (the first non-empty person_id)", got)
	}
}

func TestExtractRESTID_NestedArrayOfBareStrings(t *testing.T) {
	body := map[string]any{"teams": []any{"team-a", "team-b"}}
	got, ok := ExtractRESTID(body, RESTIDProducer{Name: "team_id", ListPath: "teams"})
	if !ok || got != "team-a" {
		t.Fatalf("got (%q, %v), want (team-a, true)", got, ok)
	}
}

// TestExtractRESTID_MalformedProducerResponse pins the ruling's own
// "malformed producer response" case: a producer's declared ListPath
// resolving to something that is not a JSON array (wrong type, key
// absent, or the response decoded to something else entirely) must fail
// the extraction cleanly, never panic, so a downstream consumer refuses
// by name instead of this tool crashing on bad live data.
func TestExtractRESTID_MalformedProducerResponse(t *testing.T) {
	cases := []struct {
		name string
		body any
		prod RESTIDProducer
	}{
		{"list path absent", map[string]any{"other": []any{"x"}}, RESTIDProducer{Name: "team_id", ListPath: "teams"}},
		{"list path is not an object", []any{"unexpected"}, RESTIDProducer{Name: "team_id", ListPath: "teams"}},
		{"addressed value is not an array", map[string]any{"teams": "not-a-list"}, RESTIDProducer{Name: "team_id", ListPath: "teams"}},
		{"every element is empty", []any{"", ""}, RESTIDProducer{Name: "person_id"}},
		{"every element misses the id field", []any{map[string]any{"display_name": "x"}}, RESTIDProducer{Name: "person_id", IDField: "person_id"}},
		{"element is the wrong JSON type", []any{map[string]any{"person_id": 42}}, RESTIDProducer{Name: "person_id", IDField: "person_id"}},
		{"root itself is nil", nil, RESTIDProducer{Name: "person_id", IDField: "person_id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := ExtractRESTID(tc.body, tc.prod); ok {
				t.Fatalf("ExtractRESTID(%v, %+v) = ok, want a failed extraction", tc.body, tc.prod)
			}
		})
	}
}

func TestResolveRESTIDBindings_SubstitutesQueryParamFromProduced(t *testing.T) {
	request := RESTRequest{
		Query:      url.Values{"type": {"churn_throughput"}, "scope_type": {"repo"}},
		IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
	}
	produced := map[string]string{"repo_id": "r-42"}

	path, query, _, unresolved := ResolveRESTIDBindings("/api/v1/quadrant", request, produced)
	if len(unresolved) != 0 {
		t.Fatalf("unresolved = %v, want none", unresolved)
	}
	if path != "/api/v1/quadrant" {
		t.Fatalf("path = %q, want unchanged (no PathParam binding)", path)
	}
	if got := query.Get("scope_id"); got != "r-42" {
		t.Fatalf("scope_id = %q, want r-42", got)
	}
	// The original request.Query must never be mutated in place -- a
	// second call (a second run, or a retry) must not observe a
	// leftover scope_id from a prior resolution.
	if request.Query.Get("scope_id") != "" {
		t.Fatal("ResolveRESTIDBindings mutated the corpus's own request.Query in place")
	}
}

func TestResolveRESTIDBindings_SubstitutesPathParamPlaceholder(t *testing.T) {
	request := RESTRequest{
		IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
	}
	produced := map[string]string{"person_id": "p-1"}

	path, _, _, unresolved := ResolveRESTIDBindings("/api/v1/people/{person_id}/summary", request, produced)
	if len(unresolved) != 0 {
		t.Fatalf("unresolved = %v, want none", unresolved)
	}
	if path != "/api/v1/people/p-1/summary" {
		t.Fatalf("path = %q, want the placeholder substituted", path)
	}
}

// TestResolveRESTIDBindings_UnresolvedProducerNamesIt is the ruling's own
// "an entry whose id does not resolve is refused with a named reason"
// case at the resolver level: a producer that never ran (or ran but
// yielded nothing) leaves its name in `unresolved`, and NEITHER the path
// nor the query is left holding an invented value.
func TestResolveRESTIDBindings_UnresolvedProducerNamesIt(t *testing.T) {
	request := RESTRequest{
		Query:      url.Values{"scope_type": {"team"}},
		IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
	}
	path, query, _, unresolved := ResolveRESTIDBindings("/api/v1/quadrant", request, map[string]string{})
	if len(unresolved) != 1 || unresolved[0] != "team_id" {
		t.Fatalf("unresolved = %v, want [team_id]", unresolved)
	}
	if path != "/api/v1/quadrant" {
		t.Fatalf("path = %q, want unchanged", path)
	}
	if query.Has("scope_id") {
		t.Fatalf("scope_id = %q, want it never set when the binding is unresolved", query.Get("scope_id"))
	}
}

// TestResolveRESTIDBindings_BodyPathSubstitutesASingleElementList pins a
// POST body's own scope id list ("ids"/"repos", the shape this corpus's
// team/repo-scoped POST bodies both use) getting replaced by a real,
// PRODUCED id -- the mechanism a POST-only route (no query/path binding
// surface at all) needs to prove a scoped branch live against a real id
// rather than a placeholder that matches nothing.
func TestResolveRESTIDBindings_BodyPathSubstitutesASingleElementList(t *testing.T) {
	originalBody := map[string]any{
		"filters": map[string]any{
			"scope": map[string]any{"level": "team", "ids": []string{"placeholder-does-not-exist"}},
		},
	}
	request := RESTRequest{
		Body:       originalBody,
		IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
	}
	produced := map[string]string{"team_id": "team-live-42"}

	_, _, resolvedBody, unresolved := ResolveRESTIDBindings("/api/v1/investment/explain", request, produced)
	if len(unresolved) != 0 {
		t.Fatalf("unresolved = %v, want none", unresolved)
	}
	body, ok := resolvedBody.(map[string]any)
	if !ok {
		t.Fatalf("resolvedBody = %#v, want a map[string]any", resolvedBody)
	}
	filters := body["filters"].(map[string]any)
	scope := filters["scope"].(map[string]any)
	ids, ok := scope["ids"].([]string)
	if !ok || len(ids) != 1 || ids[0] != "team-live-42" {
		t.Fatalf("filters.scope.ids = %#v, want [team-live-42]", scope["ids"])
	}

	// The ORIGINAL corpus literal (a package-level var, reused across
	// every run) must never be mutated in place -- a second run must not
	// observe a leftover live id from a prior resolution.
	origScope := originalBody["filters"].(map[string]any)["scope"].(map[string]any)
	origIDs := origScope["ids"].([]string)
	if len(origIDs) != 1 || origIDs[0] != "placeholder-does-not-exist" {
		t.Fatalf("ResolveRESTIDBindings mutated the corpus's own request.Body in place: filters.scope.ids = %#v", origIDs)
	}
}

// TestResolveRESTIDBindings_BodyPathUnresolvedPathNamesTheProducer pins
// the failure mode: a BodyPath that does not address an existing,
// id-shaped leaf (a corpus authoring error -- a typo'd path, or a body
// shape that changed under it) is reported via `unresolved`, exactly
// like a producer that never ran -- never a silently-unbound request
// sent with its original placeholder still in place.
func TestResolveRESTIDBindings_BodyPathUnresolvedPathNamesTheProducer(t *testing.T) {
	request := RESTRequest{
		Body:       map[string]any{"filters": map[string]any{}},
		IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
	}
	produced := map[string]string{"team_id": "team-live-42"}

	_, _, _, unresolved := ResolveRESTIDBindings("/api/v1/investment/explain", request, produced)
	if len(unresolved) != 1 || unresolved[0] != "team_id" {
		t.Fatalf("unresolved = %v, want [team_id]", unresolved)
	}
}

// TestValidateRESTIDBindingOrder_RefusesAConsumerAheadOfItsProducer is the
// ruling's own "the corpus is ordered so producers run before consumers,
// and this is validated at startup" case: a hand-built run order that
// places a consumer before its declared producer must be refused, not
// silently accepted only to always-refuse at run time.
func TestValidateRESTIDBindingOrder_RefusesAConsumerAheadOfItsProducer(t *testing.T) {
	saved := restRunOrder
	savedSpecs := restEndpointSpecs
	t.Cleanup(func() { restRunOrder = saved; restEndpointSpecs = savedSpecs })
	clearRESTOperatorSuppliedProducers(t)

	restEndpointSpecs = map[string]RESTEndpointSpec{
		"REST:GET:/consumer": {
			Method: "GET", Path: "/consumer",
			Requests: []RESTRequest{{
				Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				IDBindings: []RESTIDBinding{{Producer: "widget_id", QueryParam: "id"}},
			}},
		},
		"REST:GET:/producer": {
			Method: "GET", Path: "/producer",
			Requests: []RESTRequest{{
				Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Produces: []RESTIDProducer{{Name: "widget_id"}},
			}},
		},
	}
	// Consumer listed BEFORE its producer -- the corpus error this
	// function exists to catch.
	restRunOrder = []string{"REST:GET:/consumer", "REST:GET:/producer"}

	if err := ValidateRESTIDBindingOrder(); err == nil {
		t.Fatal("want an error when a consumer is ordered ahead of its producer")
	}
}

// TestValidateRESTIDBindingOrder_AcceptsProducerBeforeConsumer is the
// positive counterpart: the same two operations, correctly ordered, pass.
func TestValidateRESTIDBindingOrder_AcceptsProducerBeforeConsumer(t *testing.T) {
	saved := restRunOrder
	savedSpecs := restEndpointSpecs
	t.Cleanup(func() { restRunOrder = saved; restEndpointSpecs = savedSpecs })
	clearRESTOperatorSuppliedProducers(t)

	restEndpointSpecs = map[string]RESTEndpointSpec{
		"REST:GET:/consumer": {
			Method: "GET", Path: "/consumer",
			Requests: []RESTRequest{{
				Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				IDBindings: []RESTIDBinding{{Producer: "widget_id", QueryParam: "id"}},
			}},
		},
		"REST:GET:/producer": {
			Method: "GET", Path: "/producer",
			Requests: []RESTRequest{{
				Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Produces: []RESTIDProducer{{Name: "widget_id"}},
			}},
		},
	}
	restRunOrder = []string{"REST:GET:/producer", "REST:GET:/consumer"}

	if err := ValidateRESTIDBindingOrder(); err != nil {
		t.Fatalf("ValidateRESTIDBindingOrder: %v", err)
	}
}

// TestValidateRESTIDBindingOrder_RefusesAPathParamWithNoPlaceholder pins
// the defensive PathParam check: a binding that names a PathParam the
// route's own Path never declares as a "{name}" placeholder is a corpus
// authoring error, caught here rather than silently no-op substituting
// nothing at run time.
func TestValidateRESTIDBindingOrder_RefusesAPathParamWithNoPlaceholder(t *testing.T) {
	saved := restRunOrder
	savedSpecs := restEndpointSpecs
	t.Cleanup(func() { restRunOrder = saved; restEndpointSpecs = savedSpecs })
	clearRESTOperatorSuppliedProducers(t)

	restEndpointSpecs = map[string]RESTEndpointSpec{
		"REST:GET:/consumer": {
			Method: "GET", Path: "/consumer/summary", // no {person_id} placeholder
			Requests: []RESTRequest{{
				Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			}},
		},
		"REST:GET:/producer": {
			Method: "GET", Path: "/producer",
			Requests: []RESTRequest{{
				Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Produces: []RESTIDProducer{{Name: "person_id"}},
			}},
		},
	}
	restRunOrder = []string{"REST:GET:/producer", "REST:GET:/consumer"}

	if err := ValidateRESTIDBindingOrder(); err == nil {
		t.Fatal("want an error when a PathParam binding names a placeholder the route's Path does not declare")
	}
}

// TestValidateIDBindingOrder_AcceptsAnOperatorSuppliedProducerConsumedByARequest
// proves the core operator-supplied-id mechanism directly against
// validateIDBindingOrder (no earlier Produces at all, unlike every other
// test in this file): a Producer name declared in operatorSupplied
// resolves without any request in runOrder ever producing it.
func TestValidateIDBindingOrder_AcceptsAnOperatorSuppliedProducerConsumedByARequest(t *testing.T) {
	specs := map[string]RESTEndpointSpec{
		"REST:GET:/consumer": {
			Method: "GET", Path: "/consumer",
			Requests: []RESTRequest{{
				Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				IDBindings: []RESTIDBinding{{Producer: "widget_id", QueryParam: "id"}},
			}},
		},
	}
	runOrder := []string{"REST:GET:/consumer"}
	operatorSupplied := map[string]bool{"widget_id": true}

	if err := validateIDBindingOrder(runOrder, specs, operatorSupplied); err != nil {
		t.Fatalf("validateIDBindingOrder: %v", err)
	}
}

// TestValidateIDBindingOrder_RefusesADeadOperatorSuppliedProducer pins the
// allowlist's own vacuity guard: a name declared in operatorSupplied that
// no request's own IDBindings ever consumes is a corpus authoring error,
// caught here rather than silently carrying a name nothing binds.
func TestValidateIDBindingOrder_RefusesADeadOperatorSuppliedProducer(t *testing.T) {
	specs := map[string]RESTEndpointSpec{
		"REST:GET:/consumer": {
			Method: "GET", Path: "/consumer",
			Requests: []RESTRequest{{
				Name: "req", WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
			}},
		},
	}
	runOrder := []string{"REST:GET:/consumer"}
	operatorSupplied := map[string]bool{"widget_id": true}

	if err := validateIDBindingOrder(runOrder, specs, operatorSupplied); err == nil {
		t.Fatal("want an error when an operator-supplied producer name is declared but consumed by no request")
	}
}

// TestExtractRESTID_JoinFieldComposesRepoIDAndNumber pins flame's own "pr"
// entity_id shape: drilldown/prs' items carry repo_id (string) and number
// (a JSON number, not a string) on the SAME element -- JoinField reads the
// second field off that same element and appends it after IDField's own
// value, joined by ":", formatting the number as a base-10 integer with
// no fractional part or exponent. Decodes through DecodeRESTSnapshot, the
// SAME json.Decoder(UseNumber) path admission.BaselineSnap.Data always
// goes through in production (restadmit.go) -- a raw map[string]any{...,
// "number": float64(42)} literal, as earlier versions of this test built
// by hand, never occurs on that path and would have hidden
// numericRESTIDField's own float64 assumption from this test entirely.
func TestExtractRESTID_JoinFieldComposesRepoIDAndNumber(t *testing.T) {
	snapshot, err := DecodeRESTSnapshot([]byte(`{"items":[{"repo_id":"r1","number":42}]}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := ExtractRESTID(snapshot.Data, RESTIDProducer{Name: "pr_id", ListPath: "items", IDField: "repo_id", JoinField: "number"})
	if !ok {
		t.Fatal("want an id, got none")
	}
	if got != "r1:42" {
		t.Fatalf("got %q, want r1:42", got)
	}
}

// TestExtractRESTID_JoinFieldSkipsElementMissingTheSecondField pins the
// "both fields from the SAME element" rule: an element whose JoinField is
// absent, non-numeric or fractional must be skipped entirely -- never
// joined with an empty or truncated tail -- so ExtractRESTID moves on to
// the next element instead of returning a malformed composite id. Each
// case decodes through DecodeRESTSnapshot, matching the JSON-number
// decode path production always uses (see this test's own sibling above).
func TestExtractRESTID_JoinFieldSkipsElementMissingTheSecondField(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{"number field absent", `{"items":[{"repo_id":"r1"}]}`},
		{"number field is a string", `{"items":[{"repo_id":"r1","number":"42"}]}`},
		{"number field has a fractional part", `{"items":[{"repo_id":"r1","number":42.5}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := DecodeRESTSnapshot([]byte(tc.json))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			_, ok := ExtractRESTID(snapshot.Data, RESTIDProducer{Name: "pr_id", ListPath: "items", IDField: "repo_id", JoinField: "number"})
			if ok {
				t.Fatal("want a failed extraction, got ok")
			}
		})
	}
}

// TestExtractRESTID_JoinFieldSkipsToNextElement pins that a first element
// failing JoinField does not abort the whole search -- ExtractRESTID
// still finds a LATER element whose own repo_id/number pair is complete,
// the same "skip, don't stop" behaviour the existing empty-first-element
// case (TestExtractRESTID_RootArrayOfObjects) already pins for IDField
// alone. Decodes through DecodeRESTSnapshot, matching production.
func TestExtractRESTID_JoinFieldSkipsToNextElement(t *testing.T) {
	snapshot, err := DecodeRESTSnapshot([]byte(`{"items":[{"repo_id":"r1"},{"repo_id":"r2","number":7}]}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := ExtractRESTID(snapshot.Data, RESTIDProducer{Name: "pr_id", ListPath: "items", IDField: "repo_id", JoinField: "number"})
	if !ok || got != "r2:7" {
		t.Fatalf("got (%q, %v), want (r2:7, true)", got, ok)
	}
}
