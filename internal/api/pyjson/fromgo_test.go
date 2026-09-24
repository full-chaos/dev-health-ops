package pyjson

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

type fromGoInner struct {
	Shared string  `json:"shared"`
	Deep   float64 `json:"deep"`
	Plain  int
}

type FromGoEmbedded struct {
	Promoted string `json:"promoted"`
	Shared   string `json:"shared"`
}

type fromGoText struct{ n int }

func (t fromGoText) MarshalText() ([]byte, error) { return []byte(fmt.Sprintf("t%d", t.n)), nil }

type fromGoMarshaler struct{ v float64 }

func (m *fromGoMarshaler) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]float64{"v": m.v})
}

type fromGoZeroer struct{ set bool }

func (z fromGoZeroer) IsZero() bool { return !z.set }

type fromGoSample struct {
	FromGoEmbedded
	*fromGoInner
	Shared      string             `json:"shared"`
	Whole       float64            `json:"whole"`
	Small       float32            `json:"small"`
	Count       int64              `json:"count"`
	Unsigned    uint64             `json:"unsigned"`
	Skipped     string             `json:"-"`
	Dash        string             `json:"-,"`
	Empty       string             `json:"empty,omitempty"`
	EmptySlice  []int              `json:"empty_slice,omitempty"`
	NilSlice    []string           `json:"nil_slice"`
	NilMap      map[string]int     `json:"nil_map"`
	Zeroer      fromGoZeroer       `json:"zeroer,omitzero"`
	Quoted      int                `json:"quoted,string"`
	Bytes       []byte             `json:"bytes"`
	When        time.Time          `json:"when"`
	Pointer     *float64           `json:"pointer"`
	NilPointer  *float64           `json:"nil_pointer"`
	Any         any                `json:"any"`
	IntKeys     map[int]float64    `json:"int_keys"`
	TextKeys    map[fromGoText]int `json:"text_keys"`
	Text        fromGoText         `json:"text"`
	Marshaler   *fromGoMarshaler   `json:"marshaler"`
	NilMarshal  *fromGoMarshaler   `json:"nil_marshal"`
	Nested      []map[string]any   `json:"nested"`
	unexported  int
	Untagged    bool
	ArrayOfFour [2]float64 `json:"array"`
}

func fromGoSampleValue() fromGoSample {
	pointer := 2.5
	return fromGoSample{
		FromGoEmbedded: FromGoEmbedded{Promoted: "p", Shared: "loses to the outer field"},
		fromGoInner:    &fromGoInner{Shared: "also loses", Deep: 4, Plain: 7},
		Shared:         "outer", Whole: 3, Small: 0.1, Count: -9, Unsigned: math.MaxUint64,
		Skipped: "x", Dash: "dash", Quoted: 12, Bytes: []byte("hi"),
		When: time.Date(2026, 9, 24, 1, 2, 3, 4000, time.UTC), Pointer: &pointer,
		Any:      map[string]any{"b": 1.0, "a": []any{1, "x"}},
		IntKeys:  map[int]float64{10: 1, 2: 1e-7},
		TextKeys: map[fromGoText]int{{2}: 2, {1}: 1}, Text: fromGoText{5},
		Marshaler:  &fromGoMarshaler{v: 1e21},
		Nested:     []map[string]any{{"k": 1e16}},
		unexported: 1, Untagged: true, ArrayOfFour: [2]float64{math.Copysign(0, -1), 1e-5},
	}
}

// TestFromGoMatchesEncodingJSONExceptFloatSpelling: FromGo must pick the
// same fields, names, order and values encoding/json encodes. The one
// intended difference is a float field written as an integer, so numbers
// compare by value.
func TestFromGoMatchesEncodingJSONExceptFloatSpelling(t *testing.T) {
	for name, value := range map[string]any{
		"sample":         fromGoSampleValue(),
		"sample pointer": new(fromGoSampleValue()),
		"zero sample":    fromGoSample{},
		"slice":          []fromGoSample{fromGoSampleValue(), {}},
		"nil":            nil,
	} {
		t.Run(name, func(t *testing.T) {
			converted, err := FromGo(value)
			if err != nil {
				t.Fatal(err)
			}
			standard, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := Decode(standard)
			if err != nil {
				t.Fatal(err)
			}
			if difference := shapeDifference("$", converted, decoded); difference != "" {
				ours, _ := Marshal(converted)
				t.Fatalf("%s\n FromGo        %s\n encoding/json %s", difference, ours, standard)
			}
		})
	}
}

func TestFromGoKeepsFloatFieldsFloat(t *testing.T) {
	converted, err := FromGo(fromGoSampleValue())
	if err != nil {
		t.Fatal(err)
	}
	object := converted.(*Object)
	for key, want := range map[string]Value{"whole": Float(3), "deep": Float(4), "small": Float(0.1), "count": int64(-9)} {
		if got, _ := object.Get(key); got != want {
			t.Errorf("%s = %#v, want %#v", key, got, want)
		}
	}
	body, err := MarshalModel(converted)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"whole":3.0`, `"small":0.1`, `"int_keys":{"10":1.0,"2":1e-7}`, `"array":[-0.0,0.00001]`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("model body lacks %s:\n%s", want, body)
		}
	}
}

// shapeDifference compares two decoded values: same kinds, same object key
// order, numbers by value (an Int and a Float of the same value match).
func shapeDifference(path string, a, b Value) string {
	if number, ok := numberOf(a); ok {
		other, ok := numberOf(b)
		if !ok || !(number == other || (math.IsNaN(number) && math.IsNaN(other))) {
			return fmt.Sprintf("%s: %#v != %#v", path, a, b)
		}
		return ""
	}
	switch typed := a.(type) {
	case *Object:
		other, ok := b.(*Object)
		if !ok {
			return fmt.Sprintf("%s: object != %T", path, b)
		}
		if fmt.Sprint(typed.Keys()) != fmt.Sprint(other.Keys()) {
			return fmt.Sprintf("%s: keys %v != %v", path, typed.Keys(), other.Keys())
		}
		for _, key := range typed.Keys() {
			left, _ := typed.Get(key)
			right, _ := other.Get(key)
			if difference := shapeDifference(path+"."+key, left, right); difference != "" {
				return difference
			}
		}
		return ""
	case []Value:
		other, ok := b.([]Value)
		if !ok || len(other) != len(typed) {
			return fmt.Sprintf("%s: list %v != %v", path, typed, b)
		}
		for index := range typed {
			if difference := shapeDifference(fmt.Sprintf("%s[%d]", path, index), typed[index], other[index]); difference != "" {
				return difference
			}
		}
		return ""
	}
	if a != b {
		return fmt.Sprintf("%s: %#v != %#v", path, a, b)
	}
	return ""
}

func numberOf(value Value) (float64, bool) {
	switch typed := value.(type) {
	case Int:
		f, _ := typed.Float64()
		return f, true
	case Float:
		return float64(typed), true
	case int64:
		return float64(typed), true
	}
	return 0, false
}

func TestFromGoModelWritesNilListsAndMapsAsEmpty(t *testing.T) {
	type sample struct {
		List    []int          `json:"list"`
		Map     map[string]int `json:"map"`
		Pointer *[]int         `json:"pointer"`
		Omitted []int          `json:"omitted,omitempty"`
	}
	for name, test := range map[string]struct {
		convert func(any) (Value, error)
		want    string
	}{
		"FromGo":      {FromGo, `{"list":null,"map":null,"pointer":null}`},
		"FromGoModel": {FromGoModel, `{"list":[],"map":{},"pointer":null}`},
	} {
		converted, err := test.convert(sample{})
		if err != nil {
			t.Fatal(err)
		}
		body, err := MarshalModel(converted)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != test.want {
			t.Errorf("%s = %s, want %s", name, body, test.want)
		}
	}
}
