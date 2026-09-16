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

	path, query, unresolved := ResolveRESTIDBindings("/api/v1/quadrant", request, produced)
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

	path, _, unresolved := ResolveRESTIDBindings("/api/v1/people/{person_id}/summary", request, produced)
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
	path, query, unresolved := ResolveRESTIDBindings("/api/v1/quadrant", request, map[string]string{})
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

// TestValidateRESTIDBindingOrder_RefusesAConsumerAheadOfItsProducer is the
// ruling's own "the corpus is ordered so producers run before consumers,
// and this is validated at startup" case: a hand-built run order that
// places a consumer before its declared producer must be refused, not
// silently accepted only to always-refuse at run time.
func TestValidateRESTIDBindingOrder_RefusesAConsumerAheadOfItsProducer(t *testing.T) {
	saved := restRunOrder
	savedSpecs := restEndpointSpecs
	t.Cleanup(func() { restRunOrder = saved; restEndpointSpecs = savedSpecs })

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
