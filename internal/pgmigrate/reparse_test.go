package pgmigrate_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// Postgres does not reproduce every expression byte for byte when it
// restores a dump: it re-parses the deparsed text. Two such re-parse forms
// occur in this schema, and nothing else may differ between the dump of the
// database the Python upgrade built and the dump of the database dho built
// from it:
//
//  1. a varchar IN list, deparsed as `= ANY ((ARRAY['a'::character varying,
//     ...])::text[])`, comes back as `= ANY (ARRAY[('a'::character
//     varying)::text, ...])` (and the same for `<> ALL`);
//  2. a conjunction nested as the first operand of another, `((A) AND (B))
//     AND (C)`, comes back flat, `(A) AND (B) AND (C)`.
//
// reparse applies both to a line of the ORIGINAL dump. reparseOnlyDiff
// requires every differing line of the restored dump to equal its original
// after reparse, so a difference of any other kind fails.

var varcharArray = regexp.MustCompile(`\(\(ARRAY\[((?:'(?:[^']|'')*'::character varying(?:, )?)+)\]\)::text\[\]\)`)

var varcharElement = regexp.MustCompile(`'(?:[^']|'')*'::character varying`)

func reparse(line string) string {
	line = varcharArray.ReplaceAllStringFunc(line, func(match string) string {
		inner := varcharArray.FindStringSubmatch(match)[1]
		elements := varcharElement.FindAllString(inner, -1)
		for index, element := range elements {
			elements[index] = "(" + element + ")::text"
		}
		return "(ARRAY[" + strings.Join(elements, ", ") + "])"
	})
	return flattenLeftConjunctions(line)
}

// flattenLeftConjunctions removes the parentheses around a conjunction that
// is the first operand of an enclosing conjunction: "((A) AND (B)) AND" ->
// "(A) AND (B) AND", repeated until nothing changes.
func flattenLeftConjunctions(text string) string {
	for {
		changed := false
		for open := 0; open+1 < len(text); open++ {
			if text[open] != '(' || text[open+1] != '(' {
				continue
			}
			inner := open + 1
			close := matching(text, inner)
			if close < 0 || !strings.HasPrefix(text[close+1:], " AND ") || !topLevelAnd(text[inner+1:close]) {
				continue
			}
			text = text[:inner] + text[inner+1:close] + text[close+1:]
			changed = true
			break
		}
		if !changed {
			return text
		}
	}
}

// matching returns the index of the parenthesis closing the one at open,
// ignoring parentheses inside single-quoted strings, or -1.
func matching(text string, open int) int {
	depth, quoted := 0, false
	for index := open; index < len(text); index++ {
		switch c := text[index]; {
		case c == '\'':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

// topLevelAnd reports whether " AND " occurs in expression outside every
// parenthesis and string.
func topLevelAnd(expression string) bool {
	depth, quoted := 0, false
	for index := 0; index < len(expression); index++ {
		switch c := expression[index]; {
		case c == '\'':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(expression[index:], " AND "):
			return true
		}
	}
	return false
}

// reparseOnlyDiff returns "" when the restored dump differs from the original
// only by the two re-parse forms, else the first line that differs otherwise.
// It also returns how many lines differed, so a caller can see the classes
// were exercised.
func reparseOnlyDiff(original, restored string) (string, int) {
	a, b := strings.Split(original, "\n"), strings.Split(restored, "\n")
	if len(a) != len(b) {
		return fmt.Sprintf("the dumps have %d and %d lines", len(a), len(b)), 0
	}
	differing := 0
	for index := range a {
		if a[index] == b[index] {
			continue
		}
		differing++
		if reparse(a[index]) != b[index] {
			return fmt.Sprintf("line %d differs beyond the known re-parse forms:\n  original %q\n  reparsed %q\n  restored %q",
				index+1, a[index], reparse(a[index]), b[index]), differing
		}
	}
	return "", differing
}

// The two forms, on lines taken from this schema's dumps, and a difference
// of another kind that must not be absorbed.
func TestReparse(t *testing.T) {
	for name, testCase := range map[string]struct{ original, restored string }{
		"a varchar IN list": {
			`    CONSTRAINT ck_x CHECK (((status)::text = ANY ((ARRAY['succeeded'::character varying, 'failed'::character varying])::text[])))`,
			`    CONSTRAINT ck_x CHECK (((status)::text = ANY (ARRAY[('succeeded'::character varying)::text, ('failed'::character varying)::text])))`,
		},
		"a varchar NOT IN list": {
			`CREATE INDEX ix ON public.sync_runs USING btree (created_at, id) WHERE ((status)::text <> ALL ((ARRAY['success'::character varying, 'partial_failed'::character varying, 'failed'::character varying])::text[]));`,
			`CREATE INDEX ix ON public.sync_runs USING btree (created_at, id) WHERE ((status)::text <> ALL (ARRAY[('success'::character varying)::text, ('partial_failed'::character varying)::text, ('failed'::character varying)::text]));`,
		},
		"a nested conjunction": {
			`    CONSTRAINT k_check CHECK ((((length(k) >= 1) AND (length(k) <= 256)) AND (k ~ '^[a-z]{0,95}$'::text)))`,
			`    CONSTRAINT k_check CHECK (((length(k) >= 1) AND (length(k) <= 256) AND (k ~ '^[a-z]{0,95}$'::text)))`,
		},
		"a nested conjunction inside a disjunction": {
			`    CONSTRAINT p_check CHECK (((p IS NULL) OR (((length(p) >= 1) AND (length(p) <= 256)) AND (p ~ '(x)'::text))))`,
			`    CONSTRAINT p_check CHECK (((p IS NULL) OR ((length(p) >= 1) AND (length(p) <= 256) AND (p ~ '(x)'::text))))`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if diff, count := reparseOnlyDiff(testCase.original, testCase.restored); diff != "" || count != 1 {
				t.Fatalf("%s (differing %d)", diff, count)
			}
		})
	}
	if diff, _ := reparseOnlyDiff(`CHECK ((a >= 1))`, `CHECK ((a >= 2))`); diff == "" {
		t.Fatal("a changed constant passed as a re-parse form")
	}
	if diff, _ := reparseOnlyDiff(`CHECK ((((a) AND (b)) OR (c)))`, `CHECK (((a) AND (b) OR (c)))`); diff == "" {
		t.Fatal("dropping the parentheses of a conjunction under OR passed as a re-parse form")
	}
}
