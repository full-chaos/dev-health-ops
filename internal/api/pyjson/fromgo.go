package pyjson

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// FromGo converts a typed Go response value into a Value for MarshalModel,
// by the rules encoding/json uses to encode it (field names and tags,
// omitempty, omitzero, "-", promoted embedded fields, json.Marshaler and
// encoding.TextMarshaler, sorted map keys, nil slices and maps as null,
// []byte as base64), with one difference: a float field stays a Float
// whatever its value. encoding/json writes the float64 3.0 as "3", which
// reads back as an int; pydantic writes a float field's 3.0 as "3.0". A
// json.Marshaler's output is read with Decode, so a float it writes as an
// integer comes back as an Int.
func FromGo(value any) (Value, error) {
	return fromGoConverter{}.fromGo(reflect.ValueOf(value))
}

// FromGoModel is FromGo for a response_model body: a nil slice is an empty
// list and a nil map an empty object, except on a struct field tagged
// `pyjson:"nullable"`, where nil stays null. A pydantic model's list or
// dict field is never None unless the model declares it Optional, and the
// Go ports hold "no rows" as a nil slice; FastAPI writes that as []. A
// field the model declares Optional (a list or dict that may be None)
// carries the tag, and the query-api's live oracle checks every such
// field's tag against the model. A pointer field stays null when nil.
func FromGoModel(value any) (Value, error) {
	return fromGoConverter{nilAsEmpty: true}.fromGo(reflect.ValueOf(value))
}

type fromGoConverter struct{ nilAsEmpty bool }

var (
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
)

func (c fromGoConverter) fromGo(value reflect.Value) (Value, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if marshaled, ok, err := fromMarshaler(value); ok {
		return marshaled, err
	}
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		if value.IsNil() {
			return nil, nil
		}
		return c.fromGo(value.Elem())
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return Int{new(big.Int).SetUint64(value.Uint())}, nil
	case reflect.Float32:
		// encoding/json writes a float32 with 32-bit shortest digits.
		parsed, err := strconv.ParseFloat(strconv.FormatFloat(value.Float(), 'g', -1, 32), 64)
		return Float(parsed), err
	case reflect.Float64:
		return Float(value.Float()), nil
	case reflect.String:
		return value.String(), nil
	case reflect.Slice:
		if value.IsNil() {
			if c.nilAsEmpty {
				return []Value{}, nil
			}
			return nil, nil
		}
		if value.Type().Elem().Kind() == reflect.Uint8 && !implementsMarshaler(value.Type().Elem()) {
			return base64.StdEncoding.EncodeToString(value.Bytes()), nil
		}
		return c.fromList(value)
	case reflect.Array:
		return c.fromList(value)
	case reflect.Map:
		if value.IsNil() {
			if c.nilAsEmpty {
				return NewObject(), nil
			}
			return nil, nil
		}
		return c.fromMap(value)
	case reflect.Struct:
		return c.fromStruct(value)
	}
	return nil, fmt.Errorf("pyjson: FromGo cannot convert %s", value.Type())
}

func implementsMarshaler(t reflect.Type) bool {
	return t.Implements(jsonMarshalerType) || t.Implements(textMarshalerType) ||
		reflect.PointerTo(t).Implements(jsonMarshalerType) || reflect.PointerTo(t).Implements(textMarshalerType)
}

// fromMarshaler is encoding/json's marshaler check: the value's own type,
// or its address when it is addressable. A nil pointer is null.
func fromMarshaler(value reflect.Value) (Value, bool, error) {
	candidate := value
	if !candidate.Type().Implements(jsonMarshalerType) && !candidate.Type().Implements(textMarshalerType) {
		if candidate.Kind() == reflect.Pointer || !candidate.CanAddr() {
			return nil, false, nil
		}
		candidate = candidate.Addr()
		if !candidate.Type().Implements(jsonMarshalerType) && !candidate.Type().Implements(textMarshalerType) {
			return nil, false, nil
		}
	}
	if candidate.Kind() == reflect.Pointer && candidate.IsNil() {
		return nil, true, nil
	}
	if marshaler, ok := candidate.Interface().(json.Marshaler); ok {
		raw, err := marshaler.MarshalJSON()
		if err != nil {
			return nil, true, err
		}
		decoded, err := Decode(raw)
		return decoded, true, err
	}
	text, err := candidate.Interface().(encoding.TextMarshaler).MarshalText()
	return string(text), true, err
}

func (c fromGoConverter) fromList(value reflect.Value) (Value, error) {
	out := make([]Value, value.Len())
	for index := range out {
		item, err := c.fromGo(value.Index(index))
		if err != nil {
			return nil, err
		}
		out[index] = item
	}
	return out, nil
}

func (c fromGoConverter) fromMap(value reflect.Value) (Value, error) {
	type entry struct {
		key   string
		value reflect.Value
	}
	entries := make([]entry, 0, value.Len())
	iterator := value.MapRange()
	for iterator.Next() {
		key, err := mapKey(iterator.Key())
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry{key, iterator.Value()})
	}
	slices.SortFunc(entries, func(a, b entry) int { return strings.Compare(a.key, b.key) })
	out := NewObject()
	for _, item := range entries {
		converted, err := c.fromGo(item.value)
		if err != nil {
			return nil, err
		}
		out.Set(item.key, converted)
	}
	return out, nil
}

// mapKey is encoding/json's map key rule: a string kind as is, a
// TextMarshaler's text, an integer in decimal.
func mapKey(key reflect.Value) (string, error) {
	if key.Kind() == reflect.String {
		return key.String(), nil
	}
	if marshaler, ok := key.Interface().(encoding.TextMarshaler); ok {
		if key.Kind() == reflect.Pointer && key.IsNil() {
			return "", nil
		}
		text, err := marshaler.MarshalText()
		return string(text), err
	}
	switch key.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(key.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(key.Uint(), 10), nil
	}
	return "", fmt.Errorf("pyjson: FromGo cannot use %s as a map key", key.Type())
}

func (c fromGoConverter) fromStruct(value reflect.Value) (Value, error) {
	out := NewObject()
	for _, field := range structFields(value.Type()) {
		fieldValue, ok := fieldByIndex(value, field.index)
		if !ok {
			continue
		}
		if field.omitEmpty && isEmptyValue(fieldValue) {
			continue
		}
		if field.omitZero && isZeroValue(fieldValue) {
			continue
		}
		if c.nilAsEmpty && field.nullable && (fieldValue.Kind() == reflect.Slice || fieldValue.Kind() == reflect.Map) && fieldValue.IsNil() {
			out.Set(field.name, nil)
			continue
		}
		converted, err := c.fromGo(fieldValue)
		if err != nil {
			return nil, err
		}
		if field.quoted {
			converted, err = quote(converted)
			if err != nil {
				return nil, err
			}
		}
		out.Set(field.name, converted)
	}
	return out, nil
}

// quote is the ",string" option on a bool, number or string field.
func quote(value Value) (Value, error) {
	switch typed := value.(type) {
	case bool, int64, Int, Float:
		text, err := Marshal(typed)
		return string(text), err
	case string:
		text, err := json.Marshal(typed)
		return string(text), err
	}
	return value, nil
}

// fieldByIndex follows index through embedded structs; a nil embedded
// pointer hides its fields, as in encoding/json.
func fieldByIndex(value reflect.Value, index []int) (reflect.Value, bool) {
	for depth, step := range index {
		if depth > 0 && value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return reflect.Value{}, false
			}
			value = value.Elem()
		}
		value = value.Field(step)
	}
	return value, true
}

func isEmptyValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return value.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return value.IsZero()
	}
	return false
}

func isZeroValue(value reflect.Value) bool {
	if zeroer, ok := value.Interface().(interface{ IsZero() bool }); ok {
		if value.Kind() == reflect.Pointer && value.IsNil() {
			return true
		}
		return zeroer.IsZero()
	}
	return value.IsZero()
}

type goField struct {
	name                        string
	index                       []int
	tagged                      bool
	omitEmpty, omitZero, quoted bool
	// nullable is the `pyjson:"nullable"` tag: under FromGoModel a nil
	// slice or map on this field is null, not empty.
	nullable bool
}

var structFieldCache sync.Map

// structFields is encoding/json's typeFields: exported fields in index
// order, embedded structs' fields promoted, and for each name the
// shallowest field winning, a tagged one over an untagged one at the same
// depth, and a remaining tie dropping the name.
func structFields(t reflect.Type) []goField {
	if cached, ok := structFieldCache.Load(t); ok {
		return cached.([]goField)
	}
	type queued struct {
		t     reflect.Type
		index []int
	}
	var all []goField
	current := []queued{{t: t}}
	visited := map[reflect.Type]bool{}
	for len(current) > 0 {
		var next []queued
		for _, level := range current {
			if visited[level.t] {
				continue
			}
			visited[level.t] = true
			for i := 0; i < level.t.NumField(); i++ {
				field := level.t.Field(i)
				fieldType := field.Type
				if fieldType.Kind() == reflect.Pointer {
					fieldType = fieldType.Elem()
				}
				if field.Anonymous {
					if !field.IsExported() && fieldType.Kind() != reflect.Struct {
						continue
					}
				} else if !field.IsExported() {
					continue
				}
				tag := field.Tag.Get("json")
				if tag == "-" {
					continue
				}
				name, options, _ := strings.Cut(tag, ",")
				index := append(slices.Clone(level.index), i)
				if name == "" && field.Anonymous && fieldType.Kind() == reflect.Struct {
					next = append(next, queued{t: fieldType, index: index})
					continue
				}
				tagged := name != ""
				if name == "" {
					name = field.Name
				}
				quoted := false
				if hasOption(options, "string") {
					switch fieldType.Kind() {
					case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
						reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
						reflect.Float32, reflect.Float64, reflect.String:
						quoted = true
					}
				}
				all = append(all, goField{
					name: name, index: index, tagged: tagged,
					omitEmpty: hasOption(options, "omitempty"), omitZero: hasOption(options, "omitzero"),
					quoted: quoted, nullable: field.Tag.Get("pyjson") == "nullable",
				})
			}
		}
		current = next
	}
	// Dominance per name.
	byName := map[string][]goField{}
	for _, field := range all {
		byName[field.name] = append(byName[field.name], field)
	}
	var out []goField
	for _, field := range all {
		candidates := byName[field.name]
		winner, ok := dominantField(candidates)
		if ok && slices.Equal(winner.index, field.index) {
			out = append(out, field)
		}
	}
	slices.SortStableFunc(out, func(a, b goField) int { return slices.Compare(a.index, b.index) })
	structFieldCache.Store(t, out)
	return out
}

func dominantField(fields []goField) (goField, bool) {
	shallowest := len(fields[0].index)
	for _, field := range fields {
		shallowest = min(shallowest, len(field.index))
	}
	var atDepth []goField
	for _, field := range fields {
		if len(field.index) == shallowest {
			atDepth = append(atDepth, field)
		}
	}
	if len(atDepth) == 1 {
		return atDepth[0], true
	}
	var tagged []goField
	for _, field := range atDepth {
		if field.tagged {
			tagged = append(tagged, field)
		}
	}
	if len(tagged) == 1 {
		return tagged[0], true
	}
	return goField{}, false
}

func hasOption(options, want string) bool {
	for options != "" {
		var option string
		option, options, _ = strings.Cut(options, ",")
		if option == want {
			return true
		}
	}
	return false
}
