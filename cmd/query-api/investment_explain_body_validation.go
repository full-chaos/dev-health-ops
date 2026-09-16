// POST /api/v1/investment/explain's request body decode -- this route's
// own shape (InvestmentExplainRequest, api/models/filters.py:
// theme/subcategory/filters/llm_model, in that field order) built on the
// shared pydanticErrorDetail/validateMetricFilter primitives every
// body-carrying REST route in this binary uses. A JSON syntax error, a
// wrong-typed theme/subcategory/llm_model, or a malformed nested
// "filters" field all answer 422 with FastAPI's own body shape --
// pydantic_validation_error.go/pydantic_metric_filter.go/
// pydantic_json_syntax_error.go carry the shared mechanics; this file is
// only the field-by-field wiring for THIS route's own request shape.
package main

import "encoding/json"

// decodeInvestmentExplainRequestBody decodes bodyBytes into
// investmentExplainRequestBody at loc (["body"]). A non-empty errs
// return means the body failed validation -- the caller must answer 422
// via writePydanticValidationError and must not use the returned
// (zero-value) body. Every failing field is collected and returned
// together, in InvestmentExplainRequest's own field order (theme,
// subcategory, filters, llm_model), matching FastAPI's own aggregation.
func decodeInvestmentExplainRequestBody(loc []any, bodyBytes []byte) (investmentExplainRequestBody, []pydanticErrorDetail) {
	var reqBody investmentExplainRequestBody
	if len(bodyBytes) == 0 {
		return reqBody, nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return investmentExplainRequestBody{}, []pydanticErrorDetail{jsonSyntaxErrorDetail(loc, bodyBytes)}
	}

	var errs []pydanticErrorDetail

	if v, has := raw["theme"]; has {
		s, decodeErr := decodeOptionalStringField(v)
		if decodeErr != nil {
			errs = append(errs, stringBodyFieldError(appendLoc(loc, "theme"), rawJSONToAny(v)))
		} else {
			reqBody.Theme = s
		}
	}
	if v, has := raw["subcategory"]; has {
		s, decodeErr := decodeOptionalStringField(v)
		if decodeErr != nil {
			errs = append(errs, stringBodyFieldError(appendLoc(loc, "subcategory"), rawJSONToAny(v)))
		} else {
			reqBody.Subcategory = s
		}
	}

	var filtersValue any
	if v, has := raw["filters"]; has {
		filtersValue = rawJSONToAny(v)
		errs = append(errs, validateMetricFilter(appendLoc(loc, "filters"), filtersValue)...)
	}

	if v, has := raw["llm_model"]; has {
		s, decodeErr := decodeOptionalStringField(v)
		if decodeErr != nil {
			errs = append(errs, stringBodyFieldError(appendLoc(loc, "llm_model"), rawJSONToAny(v)))
		} else {
			reqBody.LLMModel = s
		}
	}

	if len(errs) > 0 {
		return investmentExplainRequestBody{}, errs
	}

	if filtersMap, ok := filtersValue.(map[string]any); ok {
		reqBody.Filters = filtersMap
	}

	return reqBody, nil
}

// decodeOptionalStringField decodes one JSON value into a *string --
// theme/subcategory/llm_model are all `*string`, matching
// InvestmentExplainRequest's `str | None` fields; a JSON null decodes to
// a nil pointer (a valid, absent value), same as Pydantic accepts null
// for an Optional field.
func decodeOptionalStringField(raw json.RawMessage) (*string, error) {
	var s *string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return s, nil
}

// rawJSONToAny decodes one JSON value generically, for building a
// validation error's "input" field -- best-effort: a value this route
// already knows is well-formed JSON (it came from a successful top-level
// json.Unmarshal into map[string]json.RawMessage) always decodes here
// too, so the error case is unreachable in practice.
func rawJSONToAny(raw json.RawMessage) any {
	var v any
	_ = json.Unmarshal(raw, &v)
	return v
}
