package server

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// REST /investment reads the issue size of the work units since CHAOS-9809 (D5999; 1 per unsized unit, so the seeded
// fixture without sizes reads its COUNT); the frozen Python answer weighs every unit by effort_value. The two are
// different measures by ruling, so the VALUES of the two distributions cannot agree, and the dict-order oracle
// compares what it exists to compare: the key order of the answer, with the values of the two distributions zeroed
// on both sides. Go's own values are returned for the caller to assert against the seeded COUNT, and the Go-only
// keys (measure, denominator, and the sizing coverage of D6027: sizing_coverage, unsized_unit_count, sizing_note)
// are checked and removed.
var (
	investmentGoOnlyTail    = regexp.MustCompile(`,"measure":"ISSUE_SIZE","denominator":([0-9eE.+-]+)(?:,"sizing_coverage":[0-9eE.+-]+)?,"unsized_unit_count":[0-9]+(?:,"sizing_note":"[^"\\]*")?\}$`)
	investmentDistributions = regexp.MustCompile(`"(theme_distribution|subcategory_distribution)"\s*:\s*\{([^{}]*)\}`)
	investmentNumberAfter   = regexp.MustCompile(`"([^"]+)"\s*:\s*(-?[0-9][0-9eE.+-]*)`)
)

type investmentGoValues struct {
	Denominator  float64
	Distribution map[string]map[string]float64 // distribution name -> key -> Go's COUNT
}

func investmentForOrderComparison(goBody, pythonBody string) (goOut, pythonOut string, values investmentGoValues, err error) {
	match := investmentGoOnlyTail.FindStringSubmatch(goBody)
	if match == nil {
		return "", "", values, fmt.Errorf("investment body has no measure/denominator tail: %s", goBody)
	}
	if values.Denominator, err = strconv.ParseFloat(match[1], 64); err != nil {
		return "", "", values, fmt.Errorf("investment denominator %q: %w", match[1], err)
	}
	goOut = strings.TrimSuffix(goBody, match[0]) + "}"
	values.Distribution = map[string]map[string]float64{}
	for _, found := range investmentDistributions.FindAllStringSubmatch(goOut, -1) {
		keyed := map[string]float64{}
		for _, pair := range investmentNumberAfter.FindAllStringSubmatch(found[2], -1) {
			number, parseErr := strconv.ParseFloat(pair[2], 64)
			if parseErr != nil {
				return "", "", values, parseErr
			}
			keyed[pair[1]] = number
		}
		values.Distribution[found[1]] = keyed
	}
	zero := func(body string) string {
		return investmentDistributions.ReplaceAllStringFunc(body, func(object string) string {
			return investmentNumberAfter.ReplaceAllString(object, `"$1":0`)
		})
	}
	return zero(goOut), zero(pythonBody), values, nil
}
