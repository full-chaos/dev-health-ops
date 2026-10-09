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

func fieldShapes(typ reflect.Type) []fieldShape {
	out := make([]fieldShape, 0, typ.NumField())
	for index := range typ.NumField() {
		f := typ.Field(index)
		out = append(out, fieldShape{f.Name, f.Type.String(), string(f.Tag)})
	}
	return out
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
	})
	assertLegacyPlusTail(t, "people.PersonDelta", reflect.TypeOf(people.PersonDelta{}), reflect.TypeOf(peoplePythonDelta{}), []fieldShape{
		{"HasData", "bool", `json:"has_data"`},
		{"HasPriorData", "bool", `json:"has_prior_data"`},
	})

	// The rest of each response is the frozen shape, field for field: only the
	// element type of Deltas differs.
	sameButDeltas := func(production, legacy reflect.Type) {
		t.Helper()
		prod, leg := fieldShapes(production), fieldShapes(legacy)
		if len(prod) != len(leg) {
			t.Errorf("%s has %d fields, the frozen shape %d", production, len(prod), len(leg))
			return
		}
		for index := range prod {
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
	}
	sameButDeltas(reflect.TypeOf(homeRESTResponse{}), reflect.TypeOf(homePythonResponse{}))
	sameButDeltas(reflect.TypeOf(people.SummaryResponse{}), reflect.TypeOf(peopleSummaryPythonResponse{}))
}

// homeDeltaGoOnlyKeys are the keys of a REST Home delta that the frozen Python
// model never had (CHAOS-9044), in the order the production type declares them.
var homeDeltaGoOnlyKeys = []string{"has_data", "has_prior_data", "rate_state"}

// homeDeltaGoOnlyTail matches the three keys at the end of a delta object.
var homeDeltaGoOnlyTail = regexp.MustCompile(`,"has_data":(?:true|false),"has_prior_data":(?:true|false),"rate_state":(?:null|"[^"\\]*")\}`)

// withoutHomeDeltaGoOnlyFields returns a REST Home body as the production
// writer writes it, without the declared Go-only keys of each delta, so the
// venue oracle compares it with the frozen Python body. The text is edited, not
// re-encoded: the order of every other key is the writer's, which the ledger of
// the Home oracle compares as text. Every delta must end with all three keys in
// the declared order (a missing or misplaced key fails, so dropping one is not
// masked), and each key must occur exactly once per delta.
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
	matches := homeDeltaGoOnlyTail.FindAllStringIndex(body, -1)
	if len(matches) != len(root.Deltas) {
		return "", fmt.Errorf("%d of %d home deltas end with the declared Go-only keys", len(matches), len(root.Deltas))
	}
	for _, key := range homeDeltaGoOnlyKeys {
		if got := strings.Count(body, `"`+key+`":`); got != len(root.Deltas) {
			return "", fmt.Errorf("key %s occurs %d times for %d deltas", key, got, len(root.Deltas))
		}
	}
	return homeDeltaGoOnlyTail.ReplaceAllString(body, "}"), nil
}

func TestWithoutHomeDeltaGoOnlyFieldsRemovesOnlyTheDeclaredKeys(t *testing.T) {
	body := `{"deltas":[{"metric":"m","value":1,"spark":[],"has_data":true,"has_prior_data":false,"rate_state":null},{"metric":"n","spark":[],"has_data":false,"has_prior_data":false,"rate_state":"measured"}],"constraint":{"title":"","claim":""}}`
	got, err := withoutHomeDeltaGoOnlyFields(body)
	if err != nil || got != `{"deltas":[{"metric":"m","value":1,"spark":[]},{"metric":"n","spark":[]}],"constraint":{"title":"","claim":""}}` {
		t.Fatalf("got %s, %v", got, err)
	}
	for name, bad := range map[string]string{
		"a delta without rate_state": `{"deltas":[{"metric":"m","has_data":true,"has_prior_data":true}]}`,
		"keys out of order":          `{"deltas":[{"metric":"m","has_prior_data":true,"has_data":true,"rate_state":null}]}`,
		"an extra key after them":    `{"deltas":[{"metric":"m","has_data":true,"has_prior_data":true,"rate_state":null,"other":1}]}`,
		"no deltas list":             `{"summary":[]}`,
	} {
		if out, err := withoutHomeDeltaGoOnlyFields(bad); err == nil {
			t.Errorf("%s: got %s, want an error", name, out)
		}
	}
}
