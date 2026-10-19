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

import (
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// explainMeasureValues are the measures the explain request names: ISSUE_SIZE (the page's measure and the default,
// CHAOS-9809), COUNT and THROUGHPUT (the same number as COUNT). Lines of code are not an Investment measure
// (CHAOS-9695, D6014).
var explainMeasureValues = []string{"ISSUE_SIZE", "COUNT", "THROUGHPUT"}

// decodeExplainDateRange reads the page's dateRange: both dates are required, and the range
// obeys the page's own rule (analytics.ValidateDateRange).
func decodeExplainDateRange(loc []any, value pyjson.Value) (*investmentExplainDateRange, []pydanticErrorDetail) {
	object, isObject := value.(*pyjson.Object)
	if !isObject {
		return nil, []pydanticErrorDetail{modelAttributesTypeError(loc, value)}
	}
	var errs []pydanticErrorDetail
	read := func(name string) (parsed time.Time, ok bool) {
		fieldLoc := appendLoc(loc, name)
		field, has := object.Get(name)
		if !has || field == nil {
			errs = append(errs, pydanticErrorDetail{Type: "missing", Loc: fieldLoc, Msg: "Field required", Input: value})
			return time.Time{}, false
		}
		if detail := validateBodyDateField(fieldLoc, field); detail != nil {
			errs = append(errs, *detail)
			return time.Time{}, false
		}
		parsed, ok = dateFromAny(legacyJSON(field))
		return parsed, ok
	}
	start, startOK := read("start_date")
	end, endOK := read("end_date")
	if startOK && endOK {
		if err := analytics.ValidateDateRange(graphqldate.New(start), graphqldate.New(end)); err != nil {
			errs = append(errs, pydanticErrorDetail{Type: "value_error", Loc: loc, Msg: "Value error, " + err.Error(), Input: value})
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return &investmentExplainDateRange{StartDate: start.UTC(), EndDate: end.UTC()}, nil
}

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

	// The mix fields of the Investment page (CHAOS-9555), Go-only.
	reqBody.Measure = optionalString("measure")
	if reqBody.Measure != nil {
		if _, known := investmentexplain.NormalizeMeasure(*reqBody.Measure); !known || strings.TrimSpace(*reqBody.Measure) == "" {
			errs = append(errs, literalErrorDetail(appendLoc(loc, "measure"), *reqBody.Measure, explainMeasureValues))
		}
	}
	if useValue, has := raw.Get("use_investment"); has {
		useInvestment, present, detail := coerceBoolBodyField(appendLoc(loc, "use_investment"), useValue)
		if detail != nil {
			errs = append(errs, *detail)
		} else if present {
			reqBody.UseInvestment = &useInvestment
		}
	}
	if rangeValue, has := raw.Get("date_range"); has && rangeValue != nil {
		dateRange, rangeErrs := decodeExplainDateRange(appendLoc(loc, "date_range"), rangeValue)
		errs = append(errs, rangeErrs...)
		reqBody.DateRange = dateRange
	}

	if len(errs) > 0 {
		return investmentExplainRequestBody{}, errs
	}

	if filtersMap, ok := legacyJSON(filtersValue).(map[string]any); ok {
		reqBody.Filters = filtersMap
	}

	return reqBody, nil
}
