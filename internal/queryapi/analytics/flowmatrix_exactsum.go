package analytics

import (
	"fmt"
	"strings"
)

// exactSumScale is the Decimal128 scale a flow-matrix sum is taken at. A
// Float64 term converts exactly to 18 decimal places, less what lies past the
// 18th (under 1e-18 per term, below one Float64 ULP of any value above 1e-2),
// and 38-18 = 20 integer digits hold any work-unit count or hour sum.
const exactSumScale = 18

// exactSum is SUM(expr) taken in Decimal128: decimal addition is exact, so
// the total does not depend on the order ClickHouse merges partial
// aggregates in. A Float64 SUM does (CHAOS-5451): two identical requests
// differed in the last bits of every investment value and, on near-ties,
// in the row order the ORDER BY ... LIMIT cut reads.
func exactSum(expr string) string {
	return fmt.Sprintf("toFloat64(sum(toDecimal128(%s, %d)))", expr, exactSumScale)
}

// exactSums rewrites every SUM(...) in a measure expression into exactSum.
// The investment measures are SUM, SUM/SUM, or AVG over integer hours, whose
// sum ClickHouse already takes exactly.
func exactSums(expr string) (string, error) {
	const open = "SUM("
	var out strings.Builder
	for i := 0; i < len(expr); {
		if !strings.HasPrefix(expr[i:], open) || (i > 0 && isIdentByte(expr[i-1])) {
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
		out.WriteString(exactSum(inner))
		i = end + 1
	}
	return out.String(), nil
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
	return 0, fmt.Errorf("unbalanced SUM(")
}

func isIdentByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
