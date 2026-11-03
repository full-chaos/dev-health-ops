package pgmigrate

import (
	"regexp"
	"strings"
	"testing"
)

// A SECURITY DEFINER function runs with its owner's privileges for any caller
// that holds EXECUTE. Its search_path is pinned to pg_catalog, pg_temp, so a
// caller cannot shadow a relation, but an unqualified operator, function or
// type still resolves through that path at call time. Every chain file that
// creates one must therefore name every operator as OPERATOR(pg_catalog.x),
// every function and type by schema, and every relation in public.

var (
	definerFunction  = regexp.MustCompile(`(?is)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(public\.[a-z0-9_]+)\s*\(.*?SECURITY\s+DEFINER.*?\bAS\s+\$\$(.*?)\$\$`)
	definerPath      = regexp.MustCompile(`(?is)SET\s+search_path\s*=\s*pg_catalog\s*,\s*pg_temp\b`)
	definerLiteral   = regexp.MustCompile(`'(?:[^']|'')*'`)
	lineComment      = regexp.MustCompile(`--[^\n]*`)
	qualifiedOp      = regexp.MustCompile(`(?i)OPERATOR\(pg_catalog\.[^)\s]+\)`)
	castSuffix       = regexp.MustCompile(`::\s*([a-z_][a-z0-9_]*)?(\.)?`)
	callSite         = regexp.MustCompile(`(?i)(^|[^a-z0-9_.])([a-z_][a-z0-9_]*)\s*\(`)
	relationSite     = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+([a-z0-9_.]+)`)
	operatorChar     = regexp.MustCompile(`[=<>!~+\-*/%^|&#@?]`)
	sqlFormsNotCalls = map[string]bool{
		"exists": true, "any": true, "all": true, "coalesce": true, "nullif": true,
		"greatest": true, "least": true, "in": true, "and": true, "or": true, "not": true,
		"select": true, "where": true, "values": true, "array": true,
	}
)

// definerBodyProblems names every unqualified operator, function, cast type or
// relation in the body of a SECURITY DEFINER function.
func definerBodyProblems(name, body string) []string {
	var problems []string
	text := lineComment.ReplaceAllString(body, " ")
	text = definerLiteral.ReplaceAllString(text, " '' ")
	for _, match := range castSuffix.FindAllStringSubmatch(text, -1) {
		if !strings.EqualFold(match[1], "pg_catalog") || match[2] != "." {
			problems = append(problems, name+": a cast names a type outside pg_catalog: "+strings.TrimSpace(match[0]))
		}
	}
	text = castSuffix.ReplaceAllString(text, " ")
	text = qualifiedOp.ReplaceAllString(text, " ")
	if found := operatorChar.FindAllString(text, -1); len(found) > 0 {
		problems = append(problems, name+": unqualified operator(s) "+strings.Join(found, " "))
	}
	for _, match := range callSite.FindAllStringSubmatch(text, -1) {
		if !sqlFormsNotCalls[strings.ToLower(match[2])] {
			problems = append(problems, name+": unqualified function "+match[2])
		}
	}
	for _, match := range relationSite.FindAllStringSubmatch(text, -1) {
		if !strings.HasPrefix(strings.ToLower(match[1]), "public.") {
			problems = append(problems, name+": relation outside public: "+match[1])
		}
	}
	return problems
}

func TestEveryDefinerFunctionInTheChainQualifiesEveryName(t *testing.T) {
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	chain = append(archivedChainFiles(t), chain...)
	found := 0
	for _, file := range chain {
		for _, match := range definerFunction.FindAllStringSubmatch(file.SQL, -1) {
			found++
			if !definerPath.MatchString(match[0]) {
				t.Errorf("%s: %s does not pin search_path = pg_catalog, pg_temp", file.Name, match[1])
			}
			for _, problem := range definerBodyProblems(file.Name+" "+match[1], match[2]) {
				t.Error(problem)
			}
		}
	}
	if found < 2 {
		t.Fatalf("found %d SECURITY DEFINER functions in the chain; 0156 and the categorize settings revision hold one each", found)
	}
}

func TestTheDefinerBodyCheckCatchesEachUnqualifiedForm(t *testing.T) {
	cases := map[string]string{
		"operator":      `SELECT 1 FROM public.settings AS s WHERE s.key = 'x'`,
		"any operator":  `SELECT 1 FROM public.settings AS s WHERE s.key OPERATOR(pg_catalog.=) 'a' AND s.org_id <> 'b'`,
		"function":      `SELECT lower(s.key) FROM public.settings AS s`,
		"cast":          `SELECT 1 FROM public.settings AS s WHERE s.key OPERATOR(pg_catalog.=) ANY (ARRAY['a']::text[])`,
		"relation":      `SELECT 1 FROM settings AS s`,
		"concatenation": `SELECT s.key || 'x' FROM public.settings AS s`,
	}
	for name, body := range cases {
		if problems := definerBodyProblems(name, body); len(problems) == 0 {
			t.Errorf("%s: %q passed the check", name, body)
		}
	}
	clean := `SELECT COALESCE((SELECT s.value OPERATOR(pg_catalog.=) 'a=b' FROM public.settings AS s
		WHERE s.key OPERATOR(pg_catalog.=) ANY (ARRAY['x', 'y']::pg_catalog.text[]) AND pg_catalog.length(s.key) OPERATOR(pg_catalog.>) 0), false) -- a = b`
	if problems := definerBodyProblems("clean", clean); len(problems) != 0 {
		t.Errorf("a fully qualified body was refused: %v", problems)
	}
}
