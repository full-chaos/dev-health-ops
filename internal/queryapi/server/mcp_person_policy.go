package server

import (
	"sort"
	"strings"
	"unicode"

	"github.com/vektah/gqlparser/v2/ast"
)

// The MCP caller class is person-free (CHAOS-7087). What selects a person is
// decided from the SCHEMA, at the typed position where a value lands, never
// from a hand-kept list of names: an argument, an input-object field or an
// enum value selects a person when its name carries a person word. A schema
// addition that carries one is refused automatically; one that selects a
// person under a name with no person word is caught by the reachable-input
// golden (testdata/mcp_reachable_inputs.golden), which fails until the new
// member is classified by review.

// mcpPersonWords are the stems of the words that name a person, matched per
// token of a camelCase / snake_case / SCREAMING_CASE name, after stripping a
// plural "s".
var mcpPersonWords = map[string]bool{
	"developer": true, "author": true, "assignee": true, "reviewer": true,
	"committer": true, "contributor": true, "engineer": true, "person": true,
	"people": true, "user": true, "who": true, "member": true, "email": true,
	"login": true, "identity": true, "individual": true,
}

// mcpNameTokens splits GraphQL name into lower-case word tokens.
func mcpNameTokens(name string) []string {
	var tokens []string
	var current []rune
	flush := func() {
		if len(current) > 0 {
			tokens = append(tokens, strings.ToLower(string(current)))
			current = current[:0]
		}
	}
	runes := []rune(name)
	for i, r := range runes {
		switch {
		case r == '_' || r == '-' || r == '.' || unicode.IsSpace(r):
			flush()
		case unicode.IsUpper(r):
			// Boundary: lower->Upper, or the last capital of an acronym run
			// followed by a lower-case letter.
			if i > 0 && (unicode.IsLower(runes[i-1]) || (unicode.IsUpper(runes[i-1]) && i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
				flush()
			}
			current = append(current, r)
		default:
			current = append(current, r)
		}
	}
	flush()
	return tokens
}

// mcpIsPersonName reports whether a schema name carries a person word.
func mcpIsPersonName(name string) bool {
	for _, token := range mcpNameTokens(name) {
		if mcpPersonWords[token] || mcpPersonWords[strings.TrimSuffix(token, "s")] {
			return true
		}
	}
	return false
}

// mcpValueIsEmpty is a value that selects nothing: absent, an empty string or
// an empty list. An input object is never empty here -- a person filter object
// is refused even when its members are.
func mcpValueIsEmpty(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return v == ""
	case []any:
		return len(v) == 0
	}
	return false
}

// mcpValueSelectsPerson walks a resolved value (variables substituted) along
// its declared type and reports whether it selects a person: a person-named
// input-object field carrying a value, or an enum value with a person word.
func mcpValueSelectsPerson(schema *ast.Schema, t *ast.Type, value any) bool {
	if value == nil || t == nil {
		return false
	}
	if t.Elem != nil {
		if list, ok := value.([]any); ok {
			for _, member := range list {
				if mcpValueSelectsPerson(schema, t.Elem, member) {
					return true
				}
			}
			return false
		}
		return mcpValueSelectsPerson(schema, t.Elem, value)
	}
	definition := schema.Types[t.NamedType]
	if definition == nil {
		return false
	}
	switch definition.Kind {
	case ast.Enum:
		s, _ := value.(string)
		return mcpIsPersonName(s)
	case ast.InputObject:
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, field := range definition.Fields {
			member, present := object[field.Name]
			if !present || member == nil {
				continue
			}
			if mcpIsPersonName(field.Name) && !mcpValueIsEmpty(member) {
				return true
			}
			if mcpValueSelectsPerson(schema, field.Type, member) {
				return true
			}
		}
	}
	return false
}

// mcpArgumentSelectsPerson is mcpValueSelectsPerson for a field argument.
func mcpArgumentSelectsPerson(schema *ast.Schema, definition *ast.ArgumentDefinition, value any) bool {
	if definition == nil || value == nil {
		return false
	}
	if mcpIsPersonName(definition.Name) && !mcpValueIsEmpty(value) {
		return true
	}
	return mcpValueSelectsPerson(schema, definition.Type, value)
}

// mcpReachableInputMembers lists, for the given root fields, every argument,
// every input-object field and every enum value reachable through their
// arguments, each classified person / other. It is the golden's content.
func mcpReachableInputMembers(schema *ast.Schema, roots []string) []string {
	seen := map[string]bool{}
	var lines []string
	add := func(line string) {
		if !seen[line] {
			seen[line] = true
			lines = append(lines, line)
		}
	}
	class := func(name string) string {
		if mcpIsPersonName(name) {
			return "person"
		}
		return "other"
	}
	visited := map[string]bool{}
	var visitType func(t *ast.Type)
	visitType = func(t *ast.Type) {
		for t != nil && t.Elem != nil {
			t = t.Elem
		}
		if t == nil || visited[t.NamedType] {
			return
		}
		visited[t.NamedType] = true
		definition := schema.Types[t.NamedType]
		if definition == nil {
			return
		}
		switch definition.Kind {
		case ast.Enum:
			for _, value := range definition.EnumValues {
				add("enum " + definition.Name + "." + value.Name + " " + class(value.Name))
			}
		case ast.InputObject:
			for _, field := range definition.Fields {
				add("input " + definition.Name + "." + field.Name + " " + class(field.Name))
				visitType(field.Type)
			}
		}
	}
	for _, root := range roots {
		field := schema.Query.Fields.ForName(root)
		if field == nil {
			continue
		}
		for _, argument := range field.Arguments {
			add("arg Query." + root + "." + argument.Name + " " + class(argument.Name))
			visitType(argument.Type)
		}
	}
	sort.Strings(lines)
	return lines
}
