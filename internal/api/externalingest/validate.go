package externalingest

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// ValidationErrorItem mirrors schemas.py's ValidationErrorItem: one
// rejection reason for one record in a submitted batch.
type ValidationErrorItem struct {
	Index   int    `json:"index"`
	Kind    string `json:"kind"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Path    string `json:"path,omitempty"`
}

// toPyJSON builds the ordered wire shape (schemas.py's ValidationErrorItem
// field order: index, kind, code, message, path) for policy.WriteJSON. Path
// is omitted when empty, matching the struct tag's existing omitempty
// behavior -- this only changes the writer, not the null-vs-omitted
// semantics of any field.
func (item ValidationErrorItem) toPyJSON() *pyjson.Object {
	object := pyjson.NewObject()
	object.Set("index", item.Index)
	object.Set("kind", item.Kind)
	object.Set("code", item.Code)
	object.Set("message", item.Message)
	if item.Path != "" {
		object.Set("path", item.Path)
	}
	return object
}

// validateRecords is validate.py's validate_records: validates each
// record's payload against its kind's field set. An unknown kind produces
// exactly one unknown_kind item; a known kind can produce several (one per
// violated field, matching Pydantic's "collect every error" behavior).
//
// This is a SHAPE port, not a byte-identical error-message port: codes
// (missing_required_field / invalid_literal / invalid_field / unknown_kind)
// and paths (records[i].payload.<field>) match validate.py's contract
// exactly, since CHAOS-6247+ callers and any customer tooling branch on
// those, not on message prose; the message text itself may read differently
// from Pydantic's generated wording. See models.go's doc comment.
func validateRecords(records []Record) []ValidationErrorItem {
	var errors []ValidationErrorItem
	for index, record := range records {
		spec, known := recordKindValidators[record.Kind]
		if !known {
			errors = append(errors, ValidationErrorItem{
				Index:   index,
				Kind:    record.Kind,
				Code:    "unknown_kind",
				Message: "Unknown record kind: " + pythonRepr(record.Kind),
				Path:    fmt.Sprintf("records[%d].kind", index),
			})
			continue
		}
		errors = append(errors, validateRecordPayload(index, record.Kind, spec, record.Payload)...)
	}
	return errors
}

// validateRecordPayload checks one record's payload map against spec:
// every declared field's presence/type/enum/length/range, and (extra=
// "forbid") any key not declared on the model.
func validateRecordPayload(index int, kind string, spec recordSpec, payload map[string]any) []ValidationErrorItem {
	var errors []ValidationErrorItem
	declared := make(map[string]bool, len(spec.fields))
	pathFor := func(name string) string {
		return fmt.Sprintf("records[%d].payload.%s", index, name)
	}
	item := func(name, code, message string) ValidationErrorItem {
		return ValidationErrorItem{Index: index, Kind: kind, Code: code, Message: message, Path: pathFor(name)}
	}

	for _, f := range spec.fields {
		declared[f.name] = true
		value, present := payload[f.name]
		if !present || value == nil {
			if f.required {
				errors = append(errors, item(f.name, "missing_required_field", "Field required"))
			}
			continue
		}
		errors = append(errors, validateFieldValue(index, kind, f, value)...)
	}
	for key := range payload {
		if !declared[key] {
			errors = append(errors, item(key, "invalid_field", fmt.Sprintf("Extra inputs are not permitted: %q", key)))
		}
	}
	return errors
}

func validateFieldValue(index int, kind string, f field, value any) []ValidationErrorItem {
	pathFor := func() string { return fmt.Sprintf("records[%d].payload.%s", index, f.name) }
	item := func(code, message string) ValidationErrorItem {
		return ValidationErrorItem{Index: index, Kind: kind, Code: code, Message: message, Path: pathFor()}
	}

	switch f.typ {
	case fString:
		str, ok := value.(string)
		if !ok {
			return []ValidationErrorItem{item("invalid_field", "Input should be a valid string")}
		}
		if len(f.enum) > 0 && !contains(f.enum, str) {
			return []ValidationErrorItem{item("invalid_literal", fmt.Sprintf("Input should be one of %v", f.enum))}
		}
		var errs []ValidationErrorItem
		if f.minLen > 0 && len(str) < f.minLen {
			errs = append(errs, item("invalid_field", fmt.Sprintf("String should have at least %d characters", f.minLen)))
		}
		if f.hasMax && f.maxLen > 0 && len(str) > f.maxLen {
			errs = append(errs, item("invalid_field", fmt.Sprintf("String should have at most %d characters", f.maxLen)))
		}
		return errs
	case fBool:
		if _, ok := value.(bool); !ok {
			return []ValidationErrorItem{item("invalid_field", "Input should be a valid boolean")}
		}
	case fNumber:
		number, ok := asFloat(value)
		if !ok {
			return []ValidationErrorItem{item("invalid_field", "Input should be a valid number")}
		}
		var errs []ValidationErrorItem
		if f.hasGE && number < f.ge {
			errs = append(errs, item("invalid_field", fmt.Sprintf("Input should be greater than or equal to %v", f.ge)))
		}
		if f.hasLE && number > f.le {
			errs = append(errs, item("invalid_field", fmt.Sprintf("Input should be less than or equal to %v", f.le)))
		}
		return errs
	case fDatetime:
		str, ok := value.(string)
		if !ok || !isRFC3339(str) {
			return []ValidationErrorItem{item("invalid_field", "Input should be a valid datetime")}
		}
	case fStringList:
		list, ok := value.([]any)
		if !ok {
			return []ValidationErrorItem{item("invalid_field", "Input should be a valid list")}
		}
		var errs []ValidationErrorItem
		for _, element := range list {
			if _, ok := element.(string); !ok {
				errs = append(errs, item("invalid_field", "Input should be a valid string"))
				break
			}
		}
		return errs
	case fDict:
		if _, ok := value.(map[string]any); !ok {
			return []ValidationErrorItem{item("invalid_field", "Input should be a valid dictionary")}
		}
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func asFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}
