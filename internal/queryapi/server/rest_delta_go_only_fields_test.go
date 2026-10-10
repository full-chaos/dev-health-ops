package server

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
)

// The REST summary deltas (Home, person) carry Go-only fields after the frozen
// Python fields (CHAOS-9044). The frozen response-model oracle and the venue
// oracles compare bodies with the Python plane, so each is given the frozen
// shape as a typed legacy value (the pattern of explainPythonResponse), and the
// production type is pinned to be exactly that shape plus the declared tail:
// no other field may appear without a declaration.

// homePythonMetricDelta is the frozen Python MetricDelta.
type homePythonMetricDelta struct {
	Metric   string            `json:"metric"`
	Label    string            `json:"label"`
	Value    float64           `json:"value"`
	Unit     string            `json:"unit"`
	DeltaPct float64           `json:"delta_pct"`
	Spark    []home.SparkPoint `json:"spark"`
}

// homePythonResponse is the frozen Python HomeResponse.
type homePythonResponse struct {
	Freshness             home.Freshness               `json:"freshness"`
	Deltas                []homePythonMetricDelta      `json:"deltas"`
	ReworkThemeAllocation []home.ReworkThemeAllocation `json:"rework_theme_allocation"`
	Summary               []home.SummarySentence       `json:"summary"`
	Tiles                 pyjson.OrderedMap[home.Tile] `json:"tiles"`
	Constraint            home.ConstraintCard          `json:"constraint"`
	Events                []home.EventItem             `json:"events"`
	HealthState           home.HealthState             `json:"health_state"`
	Signals               []home.Signal                `json:"signals"`
	LimitingFactor        home.LimitingFactor          `json:"limiting_factor"`
	DataConfidence        home.DataConfidence          `json:"data_confidence"`
}

// peoplePythonDelta is the frozen Python PersonDelta.
type peoplePythonDelta struct {
	Metric   string              `json:"metric"`
	Label    string              `json:"label"`
	Value    float64             `json:"value"`
	Unit     string              `json:"unit"`
	DeltaPct float64             `json:"delta_pct"`
	Spark    []people.SparkPoint `json:"spark"`
}

// peopleSummaryPythonResponse is the frozen Python PersonSummaryResponse.
type peopleSummaryPythonResponse struct {
	Person              people.PersonIdentityWireModel `json:"person"`
	Freshness           people.Freshness               `json:"freshness"`
	IdentityCoveragePct float64                        `json:"identity_coverage_pct"`
	Deltas              []peoplePythonDelta            `json:"deltas"`
	Narrative           []people.SummarySentence       `json:"narrative"`
	Sections            people.PersonSummarySections   `json:"sections"`
}

type fieldShape struct{ name, goType, tag string }

// fieldShapes lists a struct's fields. A DeltaPct that is a nullable number
// (*float64) is listed as the number the frozen Python model has (float64): it
// is null only where the percent is undefined (CHAOS-9063), and the type of
// the null is declared and pinned in TestDeltaPctIsNullableOnTheProductionTypes.
func fieldShapes(typ reflect.Type) []fieldShape {
	out := make([]fieldShape, 0, typ.NumField())
	for index := range typ.NumField() {
		f := typ.Field(index)
		goType := f.Type.String()
		if f.Name == "DeltaPct" && goType == "*float64" {
			goType = "float64"
		}
		out = append(out, fieldShape{f.Name, goType, string(f.Tag)})
	}
	return out
}

func TestDeltaPctIsNullableOnTheProductionTypes(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"homeRESTMetricDelta": reflect.TypeOf(homeRESTMetricDelta{}),
		"people.PersonDelta":  reflect.TypeOf(people.PersonDelta{}),
		"home.MetricDelta":    reflect.TypeOf(home.MetricDelta{}),
	} {
		f, ok := typ.FieldByName("DeltaPct")
		if !ok || f.Type.String() != "*float64" {
			t.Errorf("%s.DeltaPct is %v, want *float64 (null from a measured zero)", name, f.Type)
		}
	}
}

// assertLegacyPlusTail fails unless production's fields are the legacy fields,
// in order, followed by exactly the declared Go-only tail.
func assertLegacyPlusTail(t *testing.T, name string, production, legacy reflect.Type, tail []fieldShape) {
	t.Helper()
	want := append(fieldShapes(legacy), tail...)
	if got := fieldShapes(production); !reflect.DeepEqual(got, want) {
		t.Errorf("%s fields =\n %v\nwant the frozen Python fields, then the declared Go-only tail:\n %v", name, got, want)
	}
}

func TestRESTSummaryDeltasArePythonDeltasPlusTheDeclaredGoOnlyFields(t *testing.T) {
	assertLegacyPlusTail(t, "homeRESTMetricDelta", reflect.TypeOf(homeRESTMetricDelta{}), reflect.TypeOf(homePythonMetricDelta{}), []fieldShape{
		{"HasData", "bool", `json:"has_data"`},
		{"HasPriorData", "bool", `json:"has_prior_data"`},
		{"RateState", "*string", `json:"rate_state"`},
		{"RepoFilterApplied", "*bool", `json:"repo_filter_applied"`},
	})
	assertLegacyPlusTail(t, "people.PersonDelta", reflect.TypeOf(people.PersonDelta{}), reflect.TypeOf(peoplePythonDelta{}), []fieldShape{
		{"HasData", "bool", `json:"has_data"`},
		{"HasPriorData", "bool", `json:"has_prior_data"`},
	})

	// The rest of each response is the frozen shape, field for field, followed by
	// the declared Go-only tail (none for people): only the element type of Deltas differs.
	sameButDeltas := func(production, legacy reflect.Type, tail []fieldShape) {
		t.Helper()
		prod, leg := fieldShapes(production), fieldShapes(legacy)
		if len(prod) != len(leg)+len(tail) {
			t.Errorf("%s has %d fields, the frozen shape %d plus %d declared Go-only", production, len(prod), len(leg), len(tail))
			return
		}
		for index := range leg {
			if prod[index].name == "Deltas" {
				if leg[index].name != "Deltas" || prod[index].tag != leg[index].tag {
					t.Errorf("%s Deltas field = %v, frozen %v", production, prod[index], leg[index])
				}
				continue
			}
			if prod[index] != leg[index] {
				t.Errorf("%s field %d = %v, frozen %v", production, index, prod[index], leg[index])
			}
		}
		for index, want := range tail {
			if got := prod[len(leg)+index]; got != want {
				t.Errorf("%s Go-only field %d = %v, want %v", production, index, got, want)
			}
		}
	}
	sameButDeltas(reflect.TypeOf(homeRESTResponse{}), reflect.TypeOf(homePythonResponse{}), []fieldShape{
		{"FilterEmptyReason", "*string", `json:"filter_empty_reason"`},
	})
	sameButDeltas(reflect.TypeOf(people.SummaryResponse{}), reflect.TypeOf(peopleSummaryPythonResponse{}), nil)
}

// homeDeltaGoOnlyKeys are the keys of a REST Home delta that the frozen Python
// model never had (CHAOS-9044), in the order the production type declares them.
var homeDeltaGoOnlyKeys = []string{"has_data", "has_prior_data", "rate_state", "repo_filter_applied"}

// homeDeltaGoOnlyEnd matches the three keys at the very end of one delta object.
var homeDeltaGoOnlyEnd = regexp.MustCompile(`,"has_data":(?:true|false),"has_prior_data":(?:true|false),"rate_state":(?:null|"[^"\\]*"),"repo_filter_applied":(?:null|true|false)\}$`)

// withoutHomeDeltaGoOnlyFields returns a REST Home body as the production
// writer writes it, without the declared Go-only keys of each delta, so the
// venue oracle compares it with the frozen Python body. The text is edited, not
// re-encoded: the order of every other key is the writer's, which the ledger of
// the Home oracle compares as text. The check is made PER DELTA OBJECT: each
// delta must end with all three keys in the declared order and carry each key
// exactly once, so a delta that lacks them fails whatever another object
// holds, and a 4th Go-only key after them fails too.
func withoutHomeDeltaGoOnlyFields(body string) (string, error) {
	var root struct {
		Deltas []json.RawMessage `json:"deltas"`
	}
	if err := json.Unmarshal([]byte(body), &root); err != nil {
		return "", fmt.Errorf("home body is not a JSON object: %w", err)
	}
	if root.Deltas == nil {
		return "", fmt.Errorf("home body has no deltas list: %s", body)
	}
	var out strings.Builder
	rest := body
	for index, raw := range root.Deltas {
		delta := string(raw)
		if !homeDeltaGoOnlyEnd.MatchString(delta) {
			return "", fmt.Errorf("home delta %d does not end with the declared Go-only keys: %s", index, delta)
		}
		for _, key := range homeDeltaGoOnlyKeys {
			if got := strings.Count(delta, `"`+key+`":`); got != 1 {
				return "", fmt.Errorf("home delta %d carries key %s %d times", index, key, got)
			}
		}
		at := strings.Index(rest, delta)
		if at < 0 {
			return "", fmt.Errorf("home delta %d is not in the body text", index)
		}
		out.WriteString(rest[:at])
		out.WriteString(homeDeltaGoOnlyEnd.ReplaceAllString(delta, "}"))
		rest = rest[at+len(delta):]
	}
	// The one Go-only key of the response itself (CHAOS-9098) is the last key of the body.
	if got := strings.Count(body, `"filter_empty_reason":`); got != 1 || !homeResponseGoOnlyEnd.MatchString(rest) {
		return "", fmt.Errorf("home body must end with the one Go-only key filter_empty_reason (found it %d times)", got)
	}
	out.WriteString(homeResponseGoOnlyEnd.ReplaceAllString(rest, "}"))
	return out.String(), nil
}

// homeResponseGoOnlyEnd matches the Go-only key at the very end of the REST Home body.
var homeResponseGoOnlyEnd = regexp.MustCompile(`,"filter_empty_reason":(?:null|"[^"\\]*")\}\s*$`)

func TestWithoutHomeDeltaGoOnlyFieldsRemovesOnlyTheDeclaredKeys(t *testing.T) {
	body := `{"deltas":[{"metric":"m","value":1,"spark":[],"has_data":true,"has_prior_data":false,"rate_state":null,"repo_filter_applied":null},{"metric":"n","spark":[],"has_data":false,"has_prior_data":false,"rate_state":"measured","repo_filter_applied":false}],"constraint":{"title":"","claim":""},"filter_empty_reason":"repository_not_found"}`
	got, err := withoutHomeDeltaGoOnlyFields(body)
	if err != nil || got != `{"deltas":[{"metric":"m","value":1,"spark":[]},{"metric":"n","spark":[]}],"constraint":{"title":"","claim":""}}` {
		t.Fatalf("got %s, %v", got, err)
	}
	if got, err := withoutHomeDeltaGoOnlyFields(`{"deltas":[],"filter_empty_reason":null}`); err != nil || got != `{"deltas":[]}` {
		t.Fatalf("a null filter_empty_reason: got %s, %v", got, err)
	}
	for name, bad := range map[string]string{
		"a delta without rate_state":          `{"deltas":[{"metric":"m","has_data":true,"has_prior_data":true}]}`,
		"a delta without repo_filter_applied": `{"deltas":[{"metric":"m","has_data":true,"has_prior_data":true,"rate_state":null}]}`,
		"keys out of order":                   `{"deltas":[{"metric":"m","has_prior_data":true,"has_data":true,"rate_state":null,"repo_filter_applied":null}]}`,
		"an extra key after them":             `{"deltas":[{"metric":"m","has_data":true,"has_prior_data":true,"rate_state":null,"repo_filter_applied":null,"other":1}]}`,
		"no deltas list":                      `{"summary":[]}`,
		// The tail of another object must not stand in for a delta that lacks it.
		"a delta without them while another object holds the tail": `{"deltas":[{"metric":"m"}],"other":{"x":1,"has_data":true,"has_prior_data":true,"rate_state":null,"repo_filter_applied":null}}`,
		"a delta with a duplicate key":                             `{"deltas":[{"has_data":true,"has_data":true,"has_prior_data":true,"rate_state":null,"repo_filter_applied":null}]}`,
		// CHAOS-9098: the response-level Go-only key must be served, once, last.
		"no filter_empty_reason":          `{"deltas":[]}`,
		"filter_empty_reason not last":    `{"deltas":[],"filter_empty_reason":null,"other":1}`,
		"filter_empty_reason twice":       `{"deltas":[],"filter_empty_reason":null,"filter_empty_reason":null}`,
		"filter_empty_reason of a number": `{"deltas":[],"filter_empty_reason":7}`,
	} {
		if out, err := withoutHomeDeltaGoOnlyFields(bad); err == nil {
			t.Errorf("%s: got %s, want an error", name, out)
		}
	}
}
