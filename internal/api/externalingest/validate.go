package externalingest

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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
// is omitted when empty, matching the struct tag's omitempty.
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

// RecordInput is one record for ValidateRecords: its kind and its payload
// object, keys in input order and numbers exact.
type RecordInput struct {
	Kind    string
	Payload *pyjson.Object
}

// ValidateRecords is external_ingest/validate.py validate_records, exact in
// codes, messages and paths: an unknown kind is one unknown_kind item;
// otherwise every pydantic error of model.model_validate(payload) (python
// mode, see validateModel) becomes one item, in pydantic's order. The one
// implementation both /validate routes and the accept path use.
func ValidateRecords(records []RecordInput) []ValidationErrorItem {
	var items []ValidationErrorItem
	for index, record := range records {
		model, known := recordModels[record.Kind]
		if !known {
			items = append(items, ValidationErrorItem{
				Index: index, Kind: record.Kind, Code: "unknown_kind",
				Message: "Unknown record kind: " + pythonparity.StrRepr(record.Kind),
				Path:    fmt.Sprintf("records[%d].kind", index),
			})
			continue
		}
		payload := record.Payload
		if payload == nil {
			payload = pyjson.NewObject()
		}
		for _, err := range validateModel(model, payload) {
			items = append(items, ValidationErrorItem{
				Index: index, Kind: record.Kind, Code: errorCode(err.Type), Message: err.Msg,
				Path: errorPath(index, err.Loc),
			})
		}
	}
	return items
}

// validateRecords is ValidateRecords over parsed envelope records.
func validateRecords(records []Record) []ValidationErrorItem {
	inputs := make([]RecordInput, len(records))
	for index, record := range records {
		payload := record.ordered
		if payload == nil {
			payload = objectFromMap(record.Payload)
		}
		inputs[index] = RecordInput{Kind: record.Kind, Payload: payload}
	}
	return ValidateRecords(inputs)
}

// errorCode is validate.py _error_code_for.
func errorCode(pydanticType string) string {
	switch pydanticType {
	case "missing":
		return "missing_required_field"
	case "literal_error", "enum":
		return "invalid_literal"
	}
	return "invalid_field"
}

// errorPath is validate.py _error_path: records[i].payload, then each loc
// part as str() prints it, joined with ".".
func errorPath(index int, loc []pyjson.Value) string {
	parts := []string{fmt.Sprintf("records[%d].payload", index)}
	for _, part := range loc {
		switch typed := part.(type) {
		case string:
			parts = append(parts, typed)
		case int64:
			parts = append(parts, strconv.FormatInt(typed, 10))
		default:
			parts = append(parts, fmt.Sprint(typed))
		}
	}
	return strings.Join(parts, ".")
}
