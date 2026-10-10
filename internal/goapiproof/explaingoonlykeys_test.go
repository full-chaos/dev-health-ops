package goapiproof

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
)

// The recorded reference answers of the explain route: what the Python service
// served. Their keys are the keys the reference had.
var explainRecordedReferences = []string{
	"testdata/explain_cycle_time_ulp_baseline_29615ee0.json",
	"testdata/explain_throughput_avgvssum_baseline_caeb212b.json",
}

// explainAnswerOfTheRealType reads a recorded reference into the production
// answer type and encodes it again: the values of the reference, with every key
// the production type serves.
func explainAnswerOfTheRealType(t *testing.T, path string) (recorded, served []byte) {
	t.Helper()
	recorded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var answer explain.Response
	if err := json.Unmarshal(recorded, &answer); err != nil {
		t.Fatalf("%s does not decode into the production answer type: %v", path, err)
	}
	if len(answer.Drivers) == 0 || len(answer.Contributors) == 0 {
		t.Fatalf("%s holds %d driver(s) and %d contributor(s): the key sets of both lists cannot be read from it", path, len(answer.Drivers), len(answer.Contributors))
	}
	served, err = json.Marshal(answer)
	if err != nil {
		t.Fatalf("encode the production answer: %v", err)
	}
	return recorded, served
}

// explainKeysByPath returns the object keys of an explain body at the three
// places the answer has objects of its own, as declaration paths.
func explainKeysByPath(t *testing.T, body []byte) map[string]bool {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatalf("decode an explain body: %v", err)
	}
	keys := map[string]bool{}
	for key := range root {
		keys["data."+key] = true
	}
	for _, list := range []string{"drivers", "contributors"} {
		var rows []map[string]json.RawMessage
		if err := json.Unmarshal(root[list], &rows); err != nil {
			t.Fatalf("decode %s: %v", list, err)
		}
		for _, row := range rows {
			for key := range row {
				keys["data."+list+"."+key] = true
			}
		}
	}
	return keys
}

// explainOptionsOfEveryRequest returns the options of every explain request the
// proof sends and expects an answer body for, by route and request name: read
// from the registered routes, so a request added with options of its own is held
// too.
func explainOptionsOfEveryRequest(t *testing.T) map[string]Options {
	t.Helper()
	out := map[string]Options{}
	for _, route := range []string{"REST:GET:/api/v1/explain", "REST:POST:/api/v1/explain"} {
		spec, ok := restEndpointSpecs[route]
		if !ok {
			t.Fatalf("%s is not a registered route", route)
		}
		for _, request := range spec.Requests {
			if request.WantCandidateStatus != 200 {
				continue
			}
			out[route+" "+request.Name] = request.Parity
		}
	}
	if len(out) < 6 {
		t.Fatalf("%d explain request(s) with an answer body found, want the plain and the scoped ones: %v", len(out), out)
	}
	return out
}

// Every key the production explain answer serves and the recorded reference
// never had is declared, with its ticket, in the options the proof compares the
// route with; and nothing is declared that the production answer does not serve
// or that the reference had. The served keys come from the production type
// through the encoder, so a field added to it without a declaration fails here
// and not as six findings on the next proof run.
func TestExplainParity_DeclaresExactlyTheKeysTheRecordedReferenceNeverHad(t *testing.T) {
	for _, path := range explainRecordedReferences {
		recorded, served := explainAnswerOfTheRealType(t, path)
		reference, production := explainKeysByPath(t, recorded), explainKeysByPath(t, served)
		for key := range reference {
			if !production[key] {
				t.Errorf("%s: the reference serves %s and the production answer does not", path, key)
			}
		}
		var goOnly []string
		for key := range production {
			if !reference[key] {
				goOnly = append(goOnly, key)
			}
		}
		sort.Strings(goOnly)
		for name, opts := range explainOptionsOfEveryRequest(t) {
			var declared []string
			for key := range opts.GoOnlyKeys {
				declared = append(declared, key)
			}
			sort.Strings(declared)
			if strings.Join(declared, "\n") != strings.Join(goOnly, "\n") {
				t.Errorf("%s declares as Go-only\n  %s\nbut against %s the production answer serves, beyond the reference, exactly\n  %s", name, strings.Join(declared, "\n  "), path, strings.Join(goOnly, "\n  "))
			}
		}
	}
}

// The production answer with the values of a recorded reference is no
// difference at all: the keys only it carries are all declared.
func TestExplainParity_TheProductionAnswerOfARecordedReferenceIsNoDifference(t *testing.T) {
	for _, path := range explainRecordedReferences {
		recorded, served := explainAnswerOfTheRealType(t, path)
		for name, opts := range explainOptionsOfEveryRequest(t) {
			result := Compare(explainRESTSnapshot(t, string(recorded)), explainRESTSnapshot(t, string(served)), opts)
			if len(result.Findings) != 0 {
				t.Errorf("%s against %s: %d finding(s), want none: %+v", name, path, len(result.Findings), result.Findings)
			}
		}
	}
}
