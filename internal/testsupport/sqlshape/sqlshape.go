// Package sqlshape provides structural, parenthesis-aware inspection of SQL
// text for guards that must reject a forbidden statement SHAPE rather than a
// fixed list of statements.
//
// It lives under internal/testsupport because it is test-support code shared
// by two packages' guards (internal/providersync and internal/syncreconciler),
// not production behaviour: nothing outside a _test.go file imports it. The
// alternative -- duplicating the scanner in both packages' test files -- is
// what allowed the two copies of the previous regexp-based detector to carry
// the SAME false negative.
package sqlshape

import "regexp"

// Concat is one SQL `||` operator together with the operand text immediately
// to its left and to its right.
type Concat struct {
	Left  string
	Right string
}

// operandBoundary reports whether a character at the operator's own nesting
// depth ends an operand. A `,` separates arguments, an `=` separates a SET
// target from its expression, and a `|` is the next concatenation.
func operandBoundary(character byte) bool {
	return character == ',' || character == '=' || character == '|'
}

// scan returns, per byte of sql, the parenthesis nesting depth that byte sits
// at and whether it is inside a single-quoted literal.
//
// An opening `(` and its matching `)` are both recorded at the OUTER depth, so
// a whole parenthesised group reads as one unit at the depth it appears in --
// which is what lets an operand span `jsonb_build_object(a, to_jsonb(b))`
// without stopping at either inner `)`.
func scan(sql string) (depths []int, literals []bool) {
	depths = make([]int, len(sql))
	literals = make([]bool, len(sql))
	depth := 0
	inLiteral := false
	for index := 0; index < len(sql); index++ {
		character := sql[index]
		depths[index] = depth
		if inLiteral {
			literals[index] = true
			if character == '\'' {
				inLiteral = false
			}
			continue
		}
		switch character {
		case '\'':
			inLiteral = true
			literals[index] = true
		case '(':
			depth++
		case ')':
			depth--
			depths[index] = depth
		}
	}
	return depths, literals
}

// ConcatOperands returns every `||` operator in sql with its two operands.
//
// This is a scan rather than a regexp because the regexp it replaced spanned
// jsonb_build_object's argument list with `[^)]*`, which stops at the first
// NESTED `)`. Every lease-repair stamp nests `to_jsonb(...)` calls, so a merge
// written in the preserving direction read as clean -- a false negative on the
// exact statement shape this repository writes.
func ConcatOperands(sql string) []Concat {
	depths, literals := scan(sql)
	var concats []Concat
	for index := 0; index+1 < len(sql); index++ {
		if literals[index] || sql[index] != '|' || sql[index+1] != '|' {
			continue
		}
		depth := depths[index]
		left := 0
		for cursor := index - 1; cursor >= 0; cursor-- {
			if depths[cursor] < depth ||
				(depths[cursor] == depth && !literals[cursor] && operandBoundary(sql[cursor])) {
				left = cursor + 1
				break
			}
		}
		right := len(sql)
		for cursor := index + 2; cursor < len(sql); cursor++ {
			if depths[cursor] < depth ||
				(depths[cursor] == depth && !literals[cursor] && operandBoundary(sql[cursor])) {
				right = cursor
				break
			}
		}
		concats = append(concats, Concat{Left: sql[left:index], Right: sql[index+2 : right]})
		index++ // do not re-read the operator's second '|'
	}
	return concats
}

var (
	readsPriorCategory  = regexp.MustCompile(`result\s*->>?\s*'error_category'`)
	referencesPriorRow  = regexp.MustCompile(`(?:^|[^\w.])(?:\w+\.)?result\b`)
	singleQuotedLiteral = regexp.MustCompile(`'[^']*'`)
)

// referencesPriorResultDocument reports whether an operand reads the row's
// existing `result` column. String literals are stripped first so a key NAMED
// 'result' inside a jsonb_build_object cannot be mistaken for a column read.
func referencesPriorResultDocument(operand string) bool {
	return referencesPriorRow.MatchString(singleQuotedLiteral.ReplaceAllString(operand, "''"))
}

// PreservesPriorResultCategory reports whether a jsonb result stamp could
// leave a PRIOR result document's 'error_category' in place. Two shapes do
// that:
//
//  1. reading the old value back out (`unit.result->>'error_category'`), or
//  2. concatenating with the existing document on the RIGHT of `||`
//     (`jsonb_build_object(...) || COALESCE(unit.result...)`, or the bare
//     `... || unit.result`), since jsonb `||` resolves duplicate keys in
//     favour of its right operand.
//
// Shape 2 is measured by DIRECTION over the parsed operands, with arbitrary
// nesting on either side: the prior document appearing anywhere on the right
// of a concatenation preserves every key it carries, error_category included.
// A merge with the prior document on the LEFT is the safe direction and is
// what the production release-for-retry stamp uses.
//
// The check is shape-based rather than a fixed blocklist so a new stamp
// written in either shape is caught without the guards being edited.
func PreservesPriorResultCategory(sql string) bool {
	if readsPriorCategory.MatchString(sql) {
		return true
	}
	for _, concat := range ConcatOperands(sql) {
		if referencesPriorResultDocument(concat.Right) {
			return true
		}
	}
	return false
}
