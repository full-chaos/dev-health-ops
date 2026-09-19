package goapiproof

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// A person-scoped drilldown route and its scope-based sibling read the
// same ClickHouse tables through the same reference-plane mechanisms, so
// every mechanism one of them declares is a mechanism the other can hit.
// These tests hold the two Parity values to one set of declarations.

// personRouteSibling pairs a person-scoped route's own Parity with its
// scope-based sibling's.
type personRouteSibling struct {
	operation string
	person    Options
	sibling   Options
}

var personRouteSiblings = []personRouteSibling{
	{"REST:GET:/api/v1/people/{person_id}/drilldown/prs", personDrilldownPRsParity, drilldownPRsParity},
	{"REST:GET:/api/v1/people/{person_id}/drilldown/issues", personDrilldownIssuesParity, drilldownIssuesParity},
}

// personRoutesWithoutSibling names every person-scoped route with no
// scope-based route reading the same rows, and why.
var personRoutesWithoutSibling = map[string]string{
	"REST:GET:/api/v1/people/{person_id}/summary": "a per-identity aggregate over user_metrics_daily/work_item_user_metrics_daily; no scope-based route returns the same summary shape",
	"REST:GET:/api/v1/people/{person_id}/metric":  "a per-identity metric series over user_metrics_daily; no scope-based route returns the same per-person breakdown",
}

// perRequestShapeFields are shape fields that legitimately differ between
// two routes sharing one mechanism: a page's own effective limit and the
// name of a route's own trailing cursor field.
var perRequestShapeFields = map[string]bool{"Limit": true, "RequestLimit": true, "TrailingCursorPath": true, "CursorPath": true}

// mechanismSignatures renders every declared mechanism of opts as one
// comparable string: each BaselineDefect's ticket plus every shape it
// binds (shape type and content, per-request fields cleared) and every
// OrderInsensitiveList. Paths are left out: a route with an extra
// response field cites one more path for the same mechanism.
func mechanismSignatures(t *testing.T, opts Options) []string {
	t.Helper()
	var out []string
	for _, defect := range opts.BaselineDefects {
		value := reflect.ValueOf(defect)
		shapes := []string{}
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			name := value.Type().Field(i).Name
			if field.Kind() != reflect.Pointer || field.IsNil() || !strings.HasSuffix(name, "Shape") {
				continue
			}
			clone := reflect.New(field.Elem().Type()).Elem()
			clone.Set(field.Elem())
			for j := 0; j < clone.NumField(); j++ {
				if perRequestShapeFields[clone.Type().Field(j).Name] {
					clone.Field(j).Set(reflect.Zero(clone.Field(j).Type()))
				}
			}
			encoded, err := json.Marshal(clone.Interface())
			if err != nil {
				t.Fatalf("encode %s: %v", name, err)
			}
			shapes = append(shapes, name+string(encoded))
		}
		out = append(out, "defect "+defect.Ticket+" "+strings.Join(shapes, " "))
	}
	for _, list := range opts.OrderInsensitiveLists {
		out = append(out, "order-insensitive "+list.Ticket+" "+list.Path+" "+strings.Join(list.KeyFields, ","))
	}
	sort.Strings(out)
	return out
}

// missingMechanisms returns every signature in want absent from have,
// counting duplicates.
func missingMechanisms(have, want []string) []string {
	count := map[string]int{}
	for _, s := range have {
		count[s] = count[s] + 1
	}
	var missing []string
	for _, s := range want {
		if count[s] == 0 {
			missing = append(missing, s)
			continue
		}
		count[s] = count[s] - 1
	}
	return missing
}

// TestPersonRoutes_EveryPersonRouteIsClassified keeps the sweep complete:
// a person-scoped route added to the corpus must be paired with its
// sibling or named as having none.
func TestPersonRoutes_EveryPersonRouteIsClassified(t *testing.T) {
	paired := map[string]bool{}
	for _, pair := range personRouteSiblings {
		paired[pair.operation] = true
	}
	found := 0
	for operation := range restEndpointSpecs {
		if !strings.Contains(operation, "/people/{person_id}/") {
			continue
		}
		found++
		if !paired[operation] && personRoutesWithoutSibling[operation] == "" {
			t.Errorf("%s is a person-scoped route with no sibling classification", operation)
		}
	}
	if found != len(personRouteSiblings)+len(personRoutesWithoutSibling) {
		t.Fatalf("found %d person-scoped routes, classified %d", found, len(personRouteSiblings)+len(personRoutesWithoutSibling))
	}
}

// TestPersonRoutes_DeclareEverySiblingMechanism is the executed cell per
// pair: each side declares exactly the mechanisms the other declares.
func TestPersonRoutes_DeclareEverySiblingMechanism(t *testing.T) {
	for _, pair := range personRouteSiblings {
		t.Run(pair.operation, func(t *testing.T) {
			person := mechanismSignatures(t, pair.person)
			sibling := mechanismSignatures(t, pair.sibling)
			if len(sibling) == 0 {
				t.Fatal("sibling declares no mechanism; the pair checks nothing")
			}
			if missing := missingMechanisms(person, sibling); len(missing) > 0 {
				t.Errorf("person route lacks sibling mechanisms:\n  %s", strings.Join(missing, "\n  "))
			}
			if extra := missingMechanisms(sibling, person); len(extra) > 0 {
				t.Errorf("sibling lacks person-route mechanisms:\n  %s", strings.Join(extra, "\n  "))
			}
		})
	}
}

// TestPersonRoutes_SweepNamesExactlyTheDroppedMechanism proves the sweep
// goes red: dropping any one declaration from a person route's copy is
// reported as exactly that one missing signature.
func TestPersonRoutes_SweepNamesExactlyTheDroppedMechanism(t *testing.T) {
	for _, pair := range personRouteSiblings {
		for drop := range pair.person.BaselineDefects {
			stripped := pair.person
			stripped.BaselineDefects = append(append([]BaselineDefect{}, pair.person.BaselineDefects[:drop]...), pair.person.BaselineDefects[drop+1:]...)
			missing := missingMechanisms(mechanismSignatures(t, stripped), mechanismSignatures(t, pair.sibling))
			want := mechanismSignatures(t, Options{BaselineDefects: pair.person.BaselineDefects[drop : drop+1]})
			if !equalStrings(missing, want) {
				t.Errorf("%s: dropping defect %d reported %q, want %q", pair.operation, drop, missing, want)
			}
		}
		for drop := range pair.person.OrderInsensitiveLists {
			stripped := pair.person
			stripped.OrderInsensitiveLists = append(append([]OrderInsensitiveList{}, pair.person.OrderInsensitiveLists[:drop]...), pair.person.OrderInsensitiveLists[drop+1:]...)
			missing := missingMechanisms(mechanismSignatures(t, stripped), mechanismSignatures(t, pair.sibling))
			if len(missing) != 1 || !strings.HasPrefix(missing[0], "order-insensitive ") {
				t.Errorf("%s: dropping order-insensitive list %d reported %q, want one order-insensitive signature", pair.operation, drop, missing)
			}
		}
	}
}

// dedupKeyedShape reports whether a defect binds a shape that identifies
// list elements by the synthetic key InjectRESTDedupKeys writes.
func dedupKeyedShape(defect BaselineDefect) bool {
	switch {
	case defect.WorkGraphEdgeDedupShape != nil && defect.WorkGraphEdgeDedupShape.IDField == RESTDedupKeyField:
		return true
	case defect.DuplicateCollapseLengthShape != nil && defect.DuplicateCollapseLengthShape.IDField == RESTDedupKeyField:
		return true
	case defect.DuplicateCollapsePageCutShape != nil && defect.DuplicateCollapsePageCutShape.IDField == RESTDedupKeyField:
		return true
	}
	return false
}

// TestRESTCorpus_DedupKeyedShapesCarryTheirKeyInjection holds every
// corpus request whose Parity binds a dedup-keyed shape to a dedup
// declaration: without one, the key is never written and the shape can
// never admit anything.
func TestRESTCorpus_DedupKeyedShapesCarryTheirKeyInjection(t *testing.T) {
	checked := 0
	for operation, spec := range restEndpointSpecs {
		for _, req := range spec.Requests {
			keyed := false
			for _, defect := range req.Parity.BaselineDefects {
				keyed = keyed || dedupKeyedShape(defect)
			}
			if !keyed {
				continue
			}
			checked++
			if req.DedupListPath == "" || len(req.DedupKeyFields) == 0 {
				t.Errorf("%s %s binds a dedup-keyed shape but declares no DedupListPath/DedupKeyFields", operation, req.Name)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no corpus request binds a dedup-keyed shape; the check ran on nothing")
	}
}
