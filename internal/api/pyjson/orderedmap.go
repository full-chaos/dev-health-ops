package pyjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
)

// OrderedMap is a dict field of a Go response type that keeps insertion
// order, as a Python dict does. A Go map is written in sorted key order by
// both encoding/json and FromGo; pydantic writes a dict field in the order
// the Python code built it, so a response dict whose keys are not already
// sorted needs this type to match.
//
// Set appends a new key and replaces an existing key's value in place
// (dict.__setitem__). The zero value is a nil map: FromGo writes it as
// null, FromGoModel as {} (or null on a `pyjson:"nullable"` field), and
// MarshalJSON as null, exactly as for a nil Go map. FromGo converts each
// value with its own rules, so a float64 value stays a Float.
type OrderedMap[V any] struct {
	keys   []string
	values map[string]V
}

// NewOrderedMap returns an empty, non-nil map.
func NewOrderedMap[V any]() OrderedMap[V] {
	return OrderedMap[V]{values: map[string]V{}}
}

// OrderedMapOf builds a map from key/value pairs in the order given.
func OrderedMapOf[V any](pairs ...KeyValue[V]) OrderedMap[V] {
	out := NewOrderedMap[V]()
	for _, pair := range pairs {
		out.Set(pair.Key, pair.Value)
	}
	return out
}

// KeyValue is one OrderedMapOf entry.
type KeyValue[V any] struct {
	Key   string
	Value V
}

// Set adds key at the end, or replaces its value in place.
func (m *OrderedMap[V]) Set(key string, value V) {
	if m.values == nil {
		m.values = map[string]V{}
	}
	if _, exists := m.values[key]; !exists {
		m.keys = append(m.keys, key)
	}
	m.values[key] = value
}

// Get returns the value for key.
func (m OrderedMap[V]) Get(key string) (V, bool) {
	value, ok := m.values[key]
	return value, ok
}

// Keys returns the keys in insertion order.
func (m OrderedMap[V]) Keys() []string { return append([]string(nil), m.keys...) }

// Len is len(dict).
func (m OrderedMap[V]) Len() int { return len(m.keys) }

// IsNil reports whether the map is the zero value (a nil map).
func (m OrderedMap[V]) IsNil() bool { return m.values == nil }

// All yields the entries in insertion order.
func (m OrderedMap[V]) All() iter.Seq2[string, V] {
	return func(yield func(string, V) bool) {
		for _, key := range m.keys {
			if !yield(key, m.values[key]) {
				return
			}
		}
	}
}

// MarshalJSON writes the map in insertion order with encoding/json's rules
// for each value, and a nil map as null.
func (m OrderedMap[V]) MarshalJSON() ([]byte, error) {
	if m.values == nil {
		return []byte("null"), nil
	}
	var buffer bytes.Buffer
	buffer.WriteByte('{')
	for index, key := range m.keys {
		if index > 0 {
			buffer.WriteByte(',')
		}
		encodedKey, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		encodedValue, err := json.Marshal(m.values[key])
		if err != nil {
			return nil, err
		}
		buffer.Write(encodedKey)
		buffer.WriteByte(':')
		buffer.Write(encodedValue)
	}
	buffer.WriteByte('}')
	return buffer.Bytes(), nil
}

// UnmarshalJSON reads a JSON object in document order (a repeated key keeps
// its first position and takes the last value, as json.loads does) and
// null as the nil map.
func (m *OrderedMap[V]) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		*m = OrderedMap[V]{}
		return nil
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("pyjson: OrderedMap needs a JSON object, got %v", token)
	}
	out := NewOrderedMap[V]()
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("pyjson: OrderedMap key %v is not a string", keyToken)
		}
		var value V
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		out.Set(key, value)
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	*m = out
	return nil
}

// orderedEntries lets FromGo read the map in order and convert each value
// with its own rules (a json.Marshaler round trip would turn a float64 3.0
// into the int 3).
func (m OrderedMap[V]) orderedEntries() (keys []string, value func(string) reflect.Value, isNil bool) {
	return m.keys, func(key string) reflect.Value { return reflect.ValueOf(m.values[key]) }, m.values == nil
}

type orderedMapper interface {
	orderedEntries() (keys []string, value func(string) reflect.Value, isNil bool)
}

var orderedMapperType = reflect.TypeFor[orderedMapper]()
