// Command registrydump enumerates query-api's registered GraphQL
// documents by static reflection over cmd/query-api/query_route.go's
// actual source -- never a hand-maintained list.
//
// Why source-level reflection rather than Go's runtime reflect package:
// digestByOperation (the route's real source of truth -- see that file's
// doc comment on newQueryHandler) is a local variable built inside a
// closure, not a package-level, importable symbol; Go's runtime
// reflection has nothing to walk for an unexported local. This tool
// instead parses the file with go/parser/go/ast -- the same source the
// running binary compiles from -- and extracts two things structurally,
// never by name-matching a copy pasted elsewhere:
//
//  1. every top-level `const registered*Document = ...` declaration's
//     literal text (the "registered*Document consts" the lane brief
//     names as one half of the source of truth), and
//  2. the digestByOperation composite literal's key/value pairs (the
//     other half): each key is the operation name a real request must
//     resolve to, each value is a digestHex(<constIdent>) call whose
//     argument identifies which const above carries that operation's
//     document text.
//
// A document added to production tomorrow needs no change here: it
// shows up as a new entry in digestByOperation and a new const, and this
// tool discovers both on its next run because it re-parses the real
// file every invocation. If the two halves ever disagree in COUNT (a
// const with no map entry, or a map entry whose digestHex argument does
// not resolve to a known const), that is a finding, not a tool bug --
// this prints it to stderr and exits non-zero rather than silently
// emitting a partial list.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strconv"
)

// registeredDocument is one row of the enumeration this tool emits: the
// operation name a real request resolves to (digestByOperation's key)
// paired with the exact registered document text (the const that
// operation's digestHex(...) call names).
type registeredDocument struct {
	Operation string `json:"operation"`
	Document  string `json:"document"`
	ConstName string `json:"const_name"`
}

var documentConstPattern = regexp.MustCompile(`^registered.*Document$`)

func main() {
	filePath := flag.String("file", "", "path to cmd/query-api/query_route.go (required)")
	flag.Parse()
	if *filePath == "" {
		fmt.Fprintln(os.Stderr, "registrydump: -file is required")
		os.Exit(2)
	}

	docs, err := enumerate(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "registrydump: %v\n", err)
		os.Exit(1)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(docs); err != nil {
		fmt.Fprintf(os.Stderr, "registrydump: encode: %v\n", err)
		os.Exit(1)
	}
}

func enumerate(filePath string) ([]registeredDocument, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filePath, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", filePath, err)
	}

	// Pass 1: every `const registered*Document = "..."` at package scope
	// -- the const-identifier half of the source of truth.
	documentByConstName := map[string]string{}
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			// A single `const NAME = "..."` line is the overwhelmingly
			// common case in this file today (len(Names) == len(Values)
			// == 1), but Go also allows a GROUPED declaration on one
			// line, `const A, B = "x", "y"` -- codex review (2026-08-30)
			// caught that an earlier version of this loop required
			// exactly one name/value pair and silently `continue`d past
			// anything else, so a registered*Document const written in
			// grouped form vanished from documentByConstName with NO
			// error, only becoming visible if digestByOperation also
			// referenced it (the cross-check below would then report it
			// as "not found among registered*Document consts" -- correct
			// but confusing) or, if digestByOperation did NOT reference
			// it either, not becoming visible AT ALL: exactly the class
			// of silent under-enumeration this whole tool exists to
			// prevent, one syntax form lower than the document list
			// itself. Iterating Names/Values pairwise (Go requires equal
			// lengths whenever both are present) closes that gap outright
			// rather than converting it into a different loud failure.
			if len(valueSpec.Names) != len(valueSpec.Values) {
				return nil, fmt.Errorf("const declaration has %d name(s) but %d value(s) -- this tool only understands `const NAME = \"...\"` or `const A, B = \"...\", \"...\"` with matching counts", len(valueSpec.Names), len(valueSpec.Values))
			}
			for i, nameIdent := range valueSpec.Names {
				name := nameIdent.Name
				if !documentConstPattern.MatchString(name) {
					continue
				}
				lit, ok := valueSpec.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return nil, fmt.Errorf("const %s is not a string literal (got %T) -- registered document consts must stay literal text so this tool can read the exact bytes a real request digests", name, valueSpec.Values[i])
				}
				text, err := strconv.Unquote(lit.Value)
				if err != nil {
					return nil, fmt.Errorf("const %s: unquote: %w", name, err)
				}
				documentByConstName[name] = text
			}
		}
	}

	// Pass 2: the digestByOperation composite literal -- the
	// operation-name half of the source of truth, wherever in the file
	// it is assigned (currently inside newQueryHandler; this walk does
	// not assume that function's name so a future refactor that moves
	// the map does not silently stop being found).
	operationToConst := map[string]string{}
	var mapErr error
	ast.Inspect(file, func(n ast.Node) bool {
		if mapErr != nil {
			return false
		}
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		lhsIdent, ok := assign.Lhs[0].(*ast.Ident)
		if !ok || lhsIdent.Name != "digestByOperation" {
			return true
		}
		composite, ok := assign.Rhs[0].(*ast.CompositeLit)
		if !ok {
			mapErr = fmt.Errorf("digestByOperation is assigned from a %T, not a composite literal -- this tool only understands a literal map[string]string{...}", assign.Rhs[0])
			return false
		}
		for _, elt := range composite.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				mapErr = fmt.Errorf("digestByOperation has a non key:value element %T", elt)
				return false
			}
			keyLit, ok := kv.Key.(*ast.BasicLit)
			if !ok || keyLit.Kind != token.STRING {
				mapErr = fmt.Errorf("digestByOperation key %v is not a string literal", kv.Key)
				return false
			}
			operation, err := strconv.Unquote(keyLit.Value)
			if err != nil {
				mapErr = fmt.Errorf("digestByOperation key: unquote: %w", err)
				return false
			}
			call, ok := kv.Value.(*ast.CallExpr)
			if !ok {
				mapErr = fmt.Errorf("digestByOperation[%q] value is not a call expression (got %T) -- expected digestHex(<constIdent>)", operation, kv.Value)
				return false
			}
			funIdent, ok := call.Fun.(*ast.Ident)
			if !ok || funIdent.Name != "digestHex" || len(call.Args) != 1 {
				mapErr = fmt.Errorf("digestByOperation[%q] value is not a digestHex(<constIdent>) call", operation)
				return false
			}
			argIdent, ok := call.Args[0].(*ast.Ident)
			if !ok {
				mapErr = fmt.Errorf("digestByOperation[%q]: digestHex argument is not a bare identifier (got %T) -- this tool cannot resolve it to a registered*Document const", operation, call.Args[0])
				return false
			}
			operationToConst[operation] = argIdent.Name
		}
		return true
	})
	if mapErr != nil {
		return nil, mapErr
	}
	if len(operationToConst) == 0 {
		return nil, fmt.Errorf("found no digestByOperation assignment in %s -- the map may have been renamed or restructured; this tool needs updating, not the caller silently getting zero documents", filePath)
	}

	// Cross-check: every registered*Document const must be referenced by
	// digestByOperation, and vice versa. A mismatch here is exactly the
	// class of gap CHAOS-4466/CHAOS-4495 produced with a hand-maintained
	// list -- surfaced loudly instead of silently under-enumerating.
	var docs []registeredDocument
	for operation, constName := range operationToConst {
		text, ok := documentByConstName[constName]
		if !ok {
			return nil, fmt.Errorf("digestByOperation[%q] names const %s, which was not found among registered*Document consts", operation, constName)
		}
		docs = append(docs, registeredDocument{Operation: operation, Document: text, ConstName: constName})
		delete(documentByConstName, constName)
	}
	if len(documentByConstName) != 0 {
		var orphaned []string
		for name := range documentByConstName {
			orphaned = append(orphaned, name)
		}
		sort.Strings(orphaned)
		return nil, fmt.Errorf("registered*Document consts with no digestByOperation entry (unreachable by any request, or the map extraction above missed them): %v", orphaned)
	}

	sort.Slice(docs, func(i, j int) bool { return docs[i].Operation < docs[j].Operation })
	return docs, nil
}
