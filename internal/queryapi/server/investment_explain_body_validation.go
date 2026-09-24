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
package server

import "github.com/full-chaos/dev-health-ops/internal/api/pyjson"

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

	decoded, empty, syntaxDetail := decodeRequestBody(loc, bodyBytes)
	if syntaxDetail != nil {
		return investmentExplainRequestBody{}, []pydanticErrorDetail{*syntaxDetail}
	}
	if empty {
		// A JSON null body is accepted like an empty one, as before: both
		// are FastAPI's missing body, a named gap with its own ticket.
		return reqBody, nil
	}
	raw, isObject := decoded.(*pyjson.Object)
	if !isObject {
		return investmentExplainRequestBody{}, []pydanticErrorDetail{modelAttributesTypeError(loc, decoded)}
	}

	var errs []pydanticErrorDetail
	optionalString := func(name string) *string {
		v, has := raw.Get(name)
		if !has || v == nil {
			return nil
		}
		s, isString := v.(string)
		if !isString {
			errs = append(errs, stringBodyFieldError(appendLoc(loc, name), v))
			return nil
		}
		return &s
	}

	reqBody.Theme = optionalString("theme")
	reqBody.Subcategory = optionalString("subcategory")

	filtersValue, hasFilters := raw.Get("filters")
	if hasFilters {
		errs = append(errs, validateMetricFilter(appendLoc(loc, "filters"), filtersValue)...)
	}

	reqBody.LLMModel = optionalString("llm_model")

	if len(errs) > 0 {
		return investmentExplainRequestBody{}, errs
	}

	if filtersMap, ok := legacyJSON(filtersValue).(map[string]any); ok {
		reqBody.Filters = filtersMap
	}

	return reqBody, nil
}
