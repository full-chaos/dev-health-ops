package analytics

import (
	"fmt"
	"strings"
)

// exactSumScale is the Decimal128 scale an analytics sum is taken at. A
// Float64 term converts exactly to 18 decimal places, less what lies past the
// 18th (under 1e-18 per term, below one Float64 ULP of any value above 1e-2),
// and 38-18 = 20 integer digits hold any work-unit count, hour or second sum.
const exactSumScale = 18

// exactSum is SUM(expr) taken in Decimal128: decimal addition is exact, so
// the total does not depend on the order ClickHouse merges partial
// aggregates in. A Float64 SUM does (CHAOS-5451): two identical requests
// differed in the last bits of every investment value and, on near-ties,
// in the row order the ORDER BY ... LIMIT cut reads.
//
// The term goes through toFloat64 first: ClickHouse converts a Float32 to
// Decimal128 at scale 18 lossily (0.1f reads 0.0999999984) and wraps past
// about 9.22, and subcategory weights are Float32. Float32 to Float64 is exact.
func exactSum(expr string) string {
	return fmt.Sprintf("toFloat64(sum(%s))", exactTerm(expr))
}

// exactSumIf is sumIf(expr, cond) taken the same way as exactSum.
func exactSumIf(expr, cond string) string {
	return fmt.Sprintf("toFloat64(sumIf(%s, %s))", exactTerm(expr), cond)
}

// exactAvg is AVG(expr) as the exact sum over the non-NULL count: the same
// value, NaN over no rows and NULL over only NULLs, as avg gives.
func exactAvg(expr string) string {
	return fmt.Sprintf("(%s / count(%s))", exactSum(expr), expr)
}

func exactTerm(expr string) string {
	return fmt.Sprintf("toDecimal128(toFloat64(%s), %d)", expr, exactSumScale)
}

// exactSums rewrites every SUM(...) and AVG(...) of a measure expression into
// exactSum and exactAvg. Lower-case sum() in a measure (the pull-request rework
// rate) adds integer counts, which ClickHouse already sums exactly.
func exactSums(expr string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(expr); {
		rewrite, open := exactAggregateAt(expr, i)
		if rewrite == nil {
			out.WriteByte(expr[i])
			i++
			continue
		}
		start := i + len(open)
		end, err := matchingParen(expr, start)
		if err != nil {
			return "", fmt.Errorf("analytics: exactSums: %w in %q", err, expr)
		}
		inner, err := exactSums(expr[start:end])
		if err != nil {
			return "", err
		}
		out.WriteString(rewrite(inner))
		i = end + 1
	}
	return out.String(), nil
}

func exactAggregateAt(expr string, i int) (func(string) string, string) {
	if i > 0 && isIdentByte(expr[i-1]) {
		return nil, ""
	}
	for _, aggregate := range []struct {
		open    string
		rewrite func(string) string
	}{{"SUM(", exactSum}, {"AVG(", exactAvg}} {
		if strings.HasPrefix(expr[i:], aggregate.open) {
			return aggregate.rewrite, aggregate.open
		}
	}
	return nil, ""
}

// matchingParen returns the index of the ')' closing the group whose body
// starts at start, skipping single-quoted literals.
func matchingParen(expr string, start int) (int, error) {
	depth, quoted := 1, false
	for i := start; i < len(expr); i++ {
		switch c := expr[i]; {
		case quoted:
			if c == '\\' {
				i++
			} else if c == '\'' {
				quoted = false
			}
		case c == '\'':
			quoted = true
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return 0, fmt.Errorf("unbalanced aggregate")
}

func isIdentByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
