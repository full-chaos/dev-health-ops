package server

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/vektah/gqlparser/v2/ast"
)

// The MCP caller class is person-free (CHAOS-7087). What selects a person is
// an explicit CLASS, person or other, for every position a request value can
// land on: an argument of a reachable field, a field of an input object, an
// enum value. The classes live in mcp_input_classes.go (Go source, reviewed by
// PR, no human gate at runtime). A position the table does not list is
// UNCLASSIFIED and the listener refuses it (fail closed) until it is classified;
// the test below fails the build for the same reason. The person words are only
// a lint over the table: a position whose name carries one must be classed
// person.

// mcpInputClasses maps "<kind> <position>" (arg Query.catalog.dimension,
// input FilterInput.who, enum ScopeLevelInput.DEVELOPER) to "person"/"other".
var mcpInputClasses = mustParseMCPInputClasses(mcpInputClassesFile)

func mustParseMCPInputClasses(raw string) map[string]string {
	classes := map[string]string{}
	for _, line := range strings.Split(strings.TrimRight(raw, "\n"), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || (fields[2] != "person" && fields[2] != "other") {
			panic(fmt.Sprintf("mcp_input_classes.go: malformed line %q (want: kind position person|other)", line))
		}
		classes[fields[0]+" "+fields[1]] = fields[2]
	}
	return classes
}

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

// mcpInputVerdict is what the classes say about one value.
type mcpInputVerdict int

const (
	mcpInputOK mcpInputVerdict = iota
	mcpInputPerson
	mcpInputUnclassified
)

func worse(a, b mcpInputVerdict) mcpInputVerdict {
	if b > a {
		return b
	}
	return a
}

// mcpClassifyValue walks a resolved value along its declared type and returns
// the worst verdict: a position classed person that carries a value, or a
// position no class lists. Position keys are the ones mcpReachableInputMembers
// produces.
func mcpClassifyValue(schema *ast.Schema, t *ast.Type, value any) mcpInputVerdict {
	if value == nil || t == nil {
		return mcpInputOK
	}
	if t.Elem != nil {
		verdict := mcpInputOK
		if list, ok := value.([]any); ok {
			for _, member := range list {
				verdict = worse(verdict, mcpClassifyValue(schema, t.Elem, member))
			}
			return verdict
		}
		return mcpClassifyValue(schema, t.Elem, value)
	}
	definition := schema.Types[t.NamedType]
	if definition == nil {
		return mcpInputOK
	}
	switch definition.Kind {
	case ast.Enum:
		s, _ := value.(string)
		switch mcpInputClasses["enum "+definition.Name+"."+s] {
		case "other":
			return mcpInputOK
		case "person":
			return mcpInputPerson
		}
		return mcpInputUnclassified
	case ast.InputObject:
		object, ok := value.(map[string]any)
		if !ok {
			return mcpInputOK
		}
		verdict := mcpInputOK
		for _, field := range definition.Fields {
			member, present := object[field.Name]
			if !present || member == nil {
				continue
			}
			switch mcpInputClasses["input "+definition.Name+"."+field.Name] {
			case "person":
				if !mcpValueIsEmpty(member) {
					verdict = worse(verdict, mcpInputPerson)
				}
			case "other":
			default:
				verdict = worse(verdict, mcpInputUnclassified)
			}
			verdict = worse(verdict, mcpClassifyValue(schema, field.Type, member))
		}
		return verdict
	}
	return mcpInputOK
}

// mcpClassifyArgument is mcpClassifyValue for a field argument; the object is
// the type that declares the field.
func mcpClassifyArgument(schema *ast.Schema, object, field string, definition *ast.ArgumentDefinition, value any) mcpInputVerdict {
	if definition == nil || value == nil {
		return mcpInputOK
	}
	verdict := mcpInputOK
	switch mcpInputClasses["arg "+object+"."+field+"."+definition.Name] {
	case "person":
		if !mcpValueIsEmpty(value) {
			verdict = mcpInputPerson
		}
	case "other":
	default:
		verdict = mcpInputUnclassified
	}
	return worse(verdict, mcpClassifyValue(schema, definition.Type, value))
}

// mcpReachableInputMembers lists, for the given root fields, every position
// a request value can land on -- an argument of the root or of any field
// reachable through its result types, every input-object field and every enum
// value reachable through those arguments -- as "<kind> <position>".
func mcpReachableInputMembers(schema *ast.Schema, roots []string) []string {
	seen := map[string]bool{}
	var lines []string
	add := func(line string) {
		if !seen[line] {
			seen[line] = true
			lines = append(lines, line)
		}
	}
	visitedInput := map[string]bool{}
	var visitInput func(t *ast.Type)
	visitInput = func(t *ast.Type) {
		for t != nil && t.Elem != nil {
			t = t.Elem
		}
		if t == nil || visitedInput[t.NamedType] {
			return
		}
		visitedInput[t.NamedType] = true
		definition := schema.Types[t.NamedType]
		if definition == nil {
			return
		}
		switch definition.Kind {
		case ast.Enum:
			for _, value := range definition.EnumValues {
				add("enum " + definition.Name + "." + value.Name)
			}
		case ast.InputObject:
			for _, field := range definition.Fields {
				add("input " + definition.Name + "." + field.Name)
				visitInput(field.Type)
			}
		}
	}
	visitedOutput := map[string]bool{}
	var visitFields func(object string, fields ast.FieldList)
	visitFields = func(object string, fields ast.FieldList) {
		for _, field := range fields {
			for _, argument := range field.Arguments {
				add("arg " + object + "." + field.Name + "." + argument.Name)
				visitInput(argument.Type)
			}
			t := field.Type
			for t != nil && t.Elem != nil {
				t = t.Elem
			}
			if t == nil || visitedOutput[t.NamedType] {
				continue
			}
			visitedOutput[t.NamedType] = true
			if definition := schema.Types[t.NamedType]; definition != nil && (definition.Kind == ast.Object || definition.Kind == ast.Interface) {
				visitFields(definition.Name, definition.Fields)
			}
		}
	}
	for _, root := range roots {
		field := schema.Query.Fields.ForName(root)
		if field == nil {
			continue
		}
		visitFields("Query", ast.FieldList{field})
	}
	sort.Strings(lines)
	return lines
}
