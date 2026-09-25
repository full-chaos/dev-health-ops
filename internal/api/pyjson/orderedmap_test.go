package pyjson

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestOrderedMapKeepsInsertionOrder(t *testing.T) {
	m := NewOrderedMap[float64]()
	m.Set("prs", 1)
	m.Set("issues", 2)
	m.Set("prs", 3) // dict.__setitem__: the key keeps its first position.
	if got := m.Keys(); !reflect.DeepEqual(got, []string{"prs", "issues"}) {
		t.Fatalf("Keys() = %v, want [prs issues]", got)
	}
	if value, _ := m.Get("prs"); value != 3 {
		t.Fatalf("Get(prs) = %v, want 3", value)
	}
	var seen []string
	for key := range m.All() {
		seen = append(seen, key)
	}
	if !reflect.DeepEqual(seen, []string{"prs", "issues"}) {
		t.Fatalf("All() order = %v, want [prs issues]", seen)
	}
}

func TestOrderedMapFromGoWritesInsertionOrderAndKeepsFloats(t *testing.T) {
	type body struct {
		Links OrderedMap[float64] `json:"links"`
	}
	value, err := FromGo(body{Links: OrderedMapOf(KeyValue[float64]{"prs", 3}, KeyValue[float64]{"issues", 0.5})})
	if err != nil {
		t.Fatal(err)
	}
	out, err := Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	// pydantic writes a float field's 3.0 as 3.0, and the dict in the
	// order it was built.
	if want := `{"links":{"prs":3.0,"issues":0.5}}`; string(out) != want {
		t.Fatalf("FromGo = %s, want %s", out, want)
	}
}

func TestOrderedMapNilFollowsTheMapRules(t *testing.T) {
	type body struct {
		Plain    OrderedMap[int] `json:"plain"`
		Nullable OrderedMap[int] `json:"nullable" pyjson:"nullable"`
	}
	for _, tc := range []struct {
		name    string
		convert func(any) (Value, error)
		want    string
	}{
		{"FromGo", FromGo, `{"plain":null,"nullable":null}`},
		{"FromGoModel", FromGoModel, `{"plain":{},"nullable":null}`},
	} {
		value, err := tc.convert(body{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != tc.want {
			t.Errorf("%s(zero) = %s, want %s", tc.name, out, tc.want)
		}
	}
	// A non-nil empty map on a nullable field is {}, not null.
	value, err := FromGoModel(body{Nullable: NewOrderedMap[int]()})
	if err != nil {
		t.Fatal(err)
	}
	if out, _ := Marshal(value); string(out) != `{"plain":{},"nullable":{}}` {
		t.Errorf("FromGoModel(empty nullable) = %s", out)
	}
	// A nil pointer to an OrderedMap is null.
	var pointer *OrderedMap[int]
	if converted, err := FromGo(pointer); err != nil || converted != nil {
		t.Errorf("FromGo(nil *OrderedMap) = %v, %v", converted, err)
	}
}

func TestOrderedMapJSONRoundTripKeepsDocumentOrder(t *testing.T) {
	var m OrderedMap[int]
	if err := json.Unmarshal([]byte(`{"b":1,"a":2,"b":3}`), &m); err != nil {
		t.Fatal(err)
	}
	if got := m.Keys(); !reflect.DeepEqual(got, []string{"b", "a"}) {
		t.Fatalf("Keys() = %v, want [b a]", got)
	}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"b":3,"a":2}`; string(out) != want {
		t.Fatalf("json.Marshal = %s, want %s", out, want)
	}
	var null OrderedMap[int]
	if err := json.Unmarshal([]byte(`null`), &null); err != nil || !null.IsNil() {
		t.Fatalf("Unmarshal(null) = %v, IsNil %v", err, null.IsNil())
	}
	if out, _ := json.Marshal(null); string(out) != "null" {
		t.Fatalf("json.Marshal(nil map) = %s, want null", out)
	}
	if err := json.Unmarshal([]byte(`[1]`), &m); err == nil {
		t.Fatal("Unmarshal([1]) succeeded, want an error")
	}
}
