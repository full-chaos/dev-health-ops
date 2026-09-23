package recordvalidation

import (
	_ "embed"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// recordModelsGolden is schemas.py's RECORD_KIND_MODELS as pydantic's core
// schemas (each model's fields in declaration order, with alias, default
// presence and the validation schema), generated from the models themselves
// and kept fresh by TestRecordModelsGoldenMatchesLivePython.
//
//go:embed testdata/record_models.json
var recordModelsGolden []byte

// schemaNode is one pydantic core-schema node this package validates:
// str, int, float, bool, datetime, literal, nullable, list, dict, union.
type schemaNode struct {
	Type       string
	MinLength  *int
	MaxLength  *int
	GE, LE     pyjson.Value
	Expected   []string
	Inner      *schemaNode // nullable
	Items      *schemaNode // list
	Values     *schemaNode // dict
	Choices    []*schemaNode
	ChoiceTags []string
}

// modelField is one model field.
type modelField struct {
	Name, Alias string
	HasDefault  bool
	Schema      *schemaNode
}

// recordModel is one record kind's model.
type recordModel struct {
	ExtraForbid    bool
	PopulateByName bool
	Fields         []modelField
}

var (
	recordModels           map[string]*recordModel
	operationalRecordKinds map[string]bool
	recordKinds            []string
)

func init() {
	decoded, err := pyjson.Decode(recordModelsGolden)
	if err != nil {
		panic("externalingest: testdata/record_models.json: " + err.Error())
	}
	root := decoded.(*pyjson.Object)
	kinds, _ := root.Get("kinds")
	recordModels = map[string]*recordModel{}
	for _, kind := range kinds.(*pyjson.Object).Keys() {
		raw, _ := kinds.(*pyjson.Object).Get(kind)
		recordModels[kind] = decodeModel(raw.(*pyjson.Object))
		recordKinds = append(recordKinds, kind)
	}
	sort.Strings(recordKinds)
	operational, _ := root.Get("operational")
	operationalRecordKinds = map[string]bool{}
	for _, kind := range operational.([]pyjson.Value) {
		operationalRecordKinds[kind.(string)] = true
	}
}

func objectString(object *pyjson.Object, key string) string {
	value, _ := object.Get(key)
	text, _ := value.(string)
	return text
}

func decodeModel(object *pyjson.Object) *recordModel {
	model := &recordModel{ExtraForbid: objectString(object, "extra") == "forbid"}
	if value, _ := object.Get("populate_by_name"); value == true {
		model.PopulateByName = true
	}
	fields, _ := object.Get("fields")
	for _, raw := range fields.([]pyjson.Value) {
		field := raw.(*pyjson.Object)
		schema, _ := field.Get("schema")
		hasDefault, _ := field.Get("has_default")
		model.Fields = append(model.Fields, modelField{
			Name: objectString(field, "name"), Alias: objectString(field, "alias"),
			HasDefault: hasDefault == true, Schema: decodeNode(schema.(*pyjson.Object)),
		})
	}
	return model
}

func decodeNode(object *pyjson.Object) *schemaNode {
	node := &schemaNode{Type: objectString(object, "type")}
	for _, key := range object.Keys() {
		value, _ := object.Get(key)
		switch key {
		case "min_length", "max_length":
			n := int(value.(pyjson.Int).Int64())
			if key == "min_length" {
				node.MinLength = &n
			} else {
				node.MaxLength = &n
			}
		case "ge":
			node.GE = value
		case "le":
			node.LE = value
		case "expected":
			for _, item := range value.([]pyjson.Value) {
				node.Expected = append(node.Expected, item.(string))
			}
		case "schema":
			node.Inner = decodeNode(value.(*pyjson.Object))
		case "items_schema":
			node.Items = decodeNode(value.(*pyjson.Object))
		case "values_schema":
			node.Values = decodeNode(value.(*pyjson.Object))
		case "choices":
			for _, choice := range value.([]pyjson.Value) {
				child := decodeNode(choice.(*pyjson.Object))
				node.Choices = append(node.Choices, child)
				node.ChoiceTags = append(node.ChoiceTags, child.Type)
			}
		case "type", "keys_schema", "mode":
		default:
			panic("externalingest: record_models.json: unsupported schema key " + key)
		}
	}
	switch node.Type {
	case "str", "int", "float", "bool", "datetime", "literal", "nullable", "list", "dict", "union":
	default:
		panic("externalingest: record_models.json: unsupported schema type " + node.Type)
	}
	return node
}

// pydanticError is one pydantic error: its type, location and message.
type pydanticError struct {
	Type string
	Loc  []pyjson.Value
	Msg  string
}

func appendLoc(loc []pyjson.Value, part pyjson.Value) []pyjson.Value {
	return append(append(make([]pyjson.Value, 0, len(loc)+1), loc...), part)
}

// validateModel is model.model_validate(payload) in python (lax) mode:
// the fields in declaration order (the alias first, then the field name
// when populate_by_name), each field's errors in place, then one
// extra_forbidden per key no field used, in input order.
func validateModel(model *recordModel, payload *pyjson.Object) []pydanticError {
	var errs []pydanticError
	used := map[string]bool{}
	for _, field := range model.Fields {
		key := field.Name
		if field.Alias != "" {
			key = field.Alias
		}
		value, present := payload.Get(key)
		if !present && field.Alias != "" && model.PopulateByName {
			if byName, ok := payload.Get(field.Name); ok {
				key, value, present = field.Name, byName, true
			}
		}
		if !present {
			if !field.HasDefault {
				errs = append(errs, pydanticError{Type: "missing", Loc: []pyjson.Value{key}, Msg: "Field required"})
			}
			continue
		}
		used[key] = true
		errs = append(errs, validateNode(field.Schema, value, []pyjson.Value{key})...)
	}
	if model.ExtraForbid {
		for _, key := range payload.Keys() {
			if !used[key] {
				errs = append(errs, pydanticError{Type: "extra_forbidden", Loc: []pyjson.Value{key}, Msg: "Extra inputs are not permitted"})
			}
		}
	}
	return errs
}

// validateNode validates value against node in python (lax) mode.
func validateNode(node *schemaNode, value pyjson.Value, loc []pyjson.Value) []pydanticError {
	fail := func(kind, msg string) []pydanticError {
		return []pydanticError{{Type: kind, Loc: loc, Msg: msg}}
	}
	switch node.Type {
	case "nullable":
		if value == nil {
			return nil
		}
		return validateNode(node.Inner, value, loc)
	case "str":
		text, ok := value.(string)
		if !ok {
			return fail("string_type", "Input should be a valid string")
		}
		length := pyjson.Len(text)
		if node.MinLength != nil && length < *node.MinLength {
			return fail("string_too_short", fmt.Sprintf("String should have at least %d character%s", *node.MinLength, plural(*node.MinLength)))
		}
		if node.MaxLength != nil && length > *node.MaxLength {
			return fail("string_too_long", fmt.Sprintf("String should have at most %d character%s", *node.MaxLength, plural(*node.MaxLength)))
		}
		return nil
	case "literal":
		if text, ok := value.(string); ok {
			for _, expected := range node.Expected {
				if text == expected {
					return nil
				}
			}
		}
		return fail("literal_error", "Input should be "+literalChoices(node.Expected))
	case "bool":
		if _, kind, msg := pybody.PydanticBool(value); kind != "" {
			return fail(kind, msg)
		}
		return nil
	case "int":
		number, kind, msg := laxInt(value)
		if kind != "" {
			return fail(kind, msg)
		}
		return intConstraints(node, number, loc)
	case "float":
		number, kind, msg := laxFloat(value)
		if kind != "" {
			return fail(kind, msg)
		}
		return floatConstraints(node, number, loc)
	case "datetime":
		var input any = value
		switch typed := value.(type) {
		case pyjson.Int:
			input = new(big.Int).Set(typed.Int)
		case pyjson.Float:
			input = float64(typed)
		case bool:
			input = nil
		}
		if _, failure := pytime.ParseDatetime(input); failure != nil {
			return fail(failure.Type, failure.Msg)
		}
		return nil
	case "list":
		items, ok := value.([]pyjson.Value)
		if !ok {
			return fail("list_type", "Input should be a valid list")
		}
		if node.MaxLength != nil && len(items) > *node.MaxLength {
			return fail("too_long", fmt.Sprintf("List should have at most %d item%s after validation, not %d",
				*node.MaxLength, plural(*node.MaxLength), len(items)))
		}
		var errs []pydanticError
		for index, item := range items {
			errs = append(errs, validateNode(node.Items, item, appendLoc(loc, int64(index)))...)
		}
		if len(errs) == 0 && node.MinLength != nil && len(items) < *node.MinLength {
			return fail("too_short", fmt.Sprintf("List should have at least %d item%s after validation, not %d",
				*node.MinLength, plural(*node.MinLength), len(items)))
		}
		return errs
	case "dict":
		object, ok := value.(*pyjson.Object)
		if !ok {
			return fail("dict_type", "Input should be a valid dictionary")
		}
		var errs []pydanticError
		for _, key := range object.Keys() {
			item, _ := object.Get(key)
			errs = append(errs, validateNode(node.Values, item, appendLoc(loc, key))...)
		}
		return errs
	case "union":
		// Smart mode: a value any choice accepts strictly (or, failing
		// that, laxly) passes; otherwise every choice's lax error, tagged.
		var errs []pydanticError
		for index, choice := range node.Choices {
			choiceErrs := validateNode(choice, value, appendLoc(loc, node.ChoiceTags[index]))
			if len(choiceErrs) == 0 {
				return nil
			}
			errs = append(errs, choiceErrs...)
		}
		return errs
	}
	return fail("internal", "unsupported schema "+node.Type)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// literalChoices is pydantic's literal message list: repr() of each value,
// ", " between them and " or " before the last.
func literalChoices(expected []string) string {
	quoted := make([]string, len(expected))
	for index, value := range expected {
		quoted[index] = pythonparity.StrRepr(value)
	}
	if len(quoted) == 1 {
		return quoted[0]
	}
	return strings.Join(quoted[:len(quoted)-1], ", ") + " or " + quoted[len(quoted)-1]
}

// laxInt is pydantic's lax int: a bool or int as is; a finite float with no
// fraction; a str pydantic-core parses as an int.
func laxInt(value pyjson.Value) (*big.Int, string, string) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return big.NewInt(1), "", ""
		}
		return big.NewInt(0), "", ""
	case pyjson.Int:
		return typed.Int, "", ""
	case pyjson.Float:
		f := float64(typed)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, "finite_number", "Input should be a finite number"
		}
		if f != math.Trunc(f) {
			return nil, "int_from_float", "Input should be a valid integer, got a number with a fractional part"
		}
		if !(f > -9223372036854775808 && f < 9223372036854775808) {
			return nil, "int_parsing_size", "Unable to parse input string as an integer, exceeded maximum size"
		}
		number, _ := new(big.Float).SetFloat64(f).Int(nil)
		return number, "", ""
	case string:
		number, failure := pybody.ParsePydanticInt(typed)
		if failure != nil {
			return nil, failure.Type, failure.Msg
		}
		return number, "", ""
	}
	return nil, "int_type", "Input should be a valid integer"
}

// laxFloat is pydantic's lax float: a bool, int or float as a float; a str
// pydantic-core parses as a float.
func laxFloat(value pyjson.Value) (float64, string, string) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return 1, "", ""
		}
		return 0, "", ""
	case pyjson.Int:
		f, _ := new(big.Float).SetInt(typed.Int).Float64()
		return f, "", ""
	case pyjson.Float:
		return float64(typed), "", ""
	case string:
		f, ok := parsePydanticFloat(typed)
		if !ok {
			return 0, "float_parsing", "Input should be a valid number, unable to parse string as a number"
		}
		return f, "", ""
	}
	return 0, "float_type", "Input should be a valid number"
}

// parsePydanticFloat is pydantic-core's str-to-float: surrounding
// whitespace stripped, ASCII only, "_" only between two digits (then
// dropped), and the decimal grammar with an exponent, or inf, infinity or
// nan in any case, with a sign. An overflow is an infinity.
func parsePydanticFloat(text string) (float64, bool) {
	trimmed := strings.TrimFunc(text, unicode.IsSpace)
	if trimmed == "" {
		return 0, false
	}
	var cleaned strings.Builder
	for index := 0; index < len(trimmed); index++ {
		c := trimmed[index]
		if c >= 0x80 || c == 'x' || c == 'X' || c == 'p' || c == 'P' {
			return 0, false
		}
		if c == '_' {
			if index == 0 || index == len(trimmed)-1 || !isDigit(trimmed[index-1]) || !isDigit(trimmed[index+1]) {
				return 0, false
			}
			continue
		}
		cleaned.WriteByte(c)
	}
	f, err := strconv.ParseFloat(cleaned.String(), 64)
	if err != nil {
		if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
			return f, true
		}
		return 0, false
	}
	return f, true
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// constraintText is the limit as Python str() prints it.
func constraintText(value pyjson.Value) string {
	switch typed := value.(type) {
	case pyjson.Int:
		return typed.String()
	case pyjson.Float:
		text, _ := pyjson.Marshal(typed)
		return string(text)
	}
	return fmt.Sprint(value)
}

func constraintFloat(value pyjson.Value) float64 {
	switch typed := value.(type) {
	case pyjson.Int:
		f, _ := new(big.Float).SetInt(typed.Int).Float64()
		return f
	case pyjson.Float:
		return float64(typed)
	}
	return math.NaN()
}

// intConstraints is pydantic-core's constrained int: le, then ge; the first
// that fails is the only error.
func intConstraints(node *schemaNode, number *big.Int, loc []pyjson.Value) []pydanticError {
	if node.LE != nil {
		if limit, ok := node.LE.(pyjson.Int); ok && number.Cmp(limit.Int) > 0 {
			return []pydanticError{{Type: "less_than_equal", Loc: loc, Msg: "Input should be less than or equal to " + constraintText(node.LE)}}
		}
	}
	if node.GE != nil {
		if limit, ok := node.GE.(pyjson.Int); ok && number.Cmp(limit.Int) < 0 {
			return []pydanticError{{Type: "greater_than_equal", Loc: loc, Msg: "Input should be greater than or equal to " + constraintText(node.GE)}}
		}
	}
	return nil
}

// floatConstraints is pydantic-core's constrained float: le, then ge; NaN
// fails every comparison.
func floatConstraints(node *schemaNode, number float64, loc []pyjson.Value) []pydanticError {
	if node.LE != nil && !(number <= constraintFloat(node.LE)) {
		return []pydanticError{{Type: "less_than_equal", Loc: loc, Msg: "Input should be less than or equal to " + constraintText(node.LE)}}
	}
	if node.GE != nil && !(number >= constraintFloat(node.GE)) {
		return []pydanticError{{Type: "greater_than_equal", Loc: loc, Msg: "Input should be greater than or equal to " + constraintText(node.GE)}}
	}
	return nil
}
