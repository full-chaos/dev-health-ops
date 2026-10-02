package synclog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"go/types"
	"golang.org/x/tools/go/packages"
)

// CHAOS-7933 (D4262 a): Failure is the ONLY function that takes an error. Its output stays in a closed alphabet: a class from a
// closed list, a Go type name and a validated SQLSTATE; a planted error whose text holds a secret-shaped string never shows.
var failureClasses = map[string]bool{"none": true, "canceled": true, "deadline": true, "postgres": true, "decode": true, "other": true,
	"dns": true, "refused": true, "reset": true, "timeout": true, "tls": true, "eof": true, "protocol": true}

var (
	goTypeName   = regexp.MustCompile(`^[*]?[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	sqlStateText = regexp.MustCompile(`^[0-9A-Z]{5}$`)
)

func TestFailureStaysInItsClosedAlphabet(t *testing.T) {
	const marker = "the planted detail of ticket 7933 behaviour"
	for name, err := range map[string]error{
		"plain":    errors.New("failed: " + marker),
		"wrapped":  fmt.Errorf("https://u:%s@host.example.test/x?access_token=%s: %w", marker, marker, errors.New(marker)),
		"postgres": &pgconn.PgError{Code: "23505", Message: marker, Detail: "Key (" + marker + ")", Hint: marker, ConstraintName: marker, TableName: marker, ColumnName: marker},
		"badstate": &pgconn.PgError{Code: "23505 " + marker, Message: marker},
		"canceled": fmt.Errorf("x %s: %w", marker, context.Canceled),
	} {
		var out bytes.Buffer
		New(slog.New(slog.NewJSONHandler(&out, nil))).Warn(context.Background(), Msg{"x"}, Failure(err))
		line := out.String()
		if strings.Contains(line, "planted") || strings.Contains(line, "example") {
			t.Fatalf("%s: the log line carries the planted text: %s", name, line)
		}
		var group struct {
			Error map[string]string `json:"error"`
		}
		if decodeErr := json.Unmarshal(out.Bytes(), &group); decodeErr != nil {
			t.Fatalf("%s: %v", name, decodeErr)
		}
		for key, value := range group.Error {
			switch key {
			case "class":
				if !failureClasses[value] {
					t.Fatalf("%s: class %q is not in the closed list", name, value)
				}
			case "type":
				if !goTypeName.MatchString(value) {
					t.Fatalf("%s: type %q is not a Go type name", name, value)
				}
			case "code":
				if !sqlStateText.MatchString(value) {
					t.Fatalf("%s: code %q is not a SQLSTATE", name, value)
				}
			default:
				t.Fatalf("%s: unexpected key %q in the failure group: %s", name, key, line)
			}
		}
		if group.Error["class"] == "" || group.Error["type"] == "" {
			t.Fatalf("%s: class and type are required: %s", name, line)
		}
	}
}

// The parsers fail closed: whatever is not a UUID / a short lower-case token is the marker "invalid"; an error's text, a URL, a
// sentence and a secret-shaped token with a hyphen or upper case never get through.
func TestParsersFailClosed(t *testing.T) {
	const marker = "the planted detail of ticket 7933 parsers"
	for _, text := range []string{marker, "https://u:p@host.example.test/x?token=abc", "Sk-Planted-Marker-7933", "sk-planted-marker-7933", "ghp_PlantedMarker7933", "a b", "", strings.Repeat("a", 49), "UPPER", "with\nnewline", "x/y"} {
		if got := ParseLabel(text).text; got != "invalid" {
			t.Fatalf("ParseLabel(%q) = %q, want invalid", text, got)
		}
	}
	for _, text := range []string{marker, "run-1", "x123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174000x", "123e4567-e89b-12d3-a456-42661417400", "123e4567-e89b-12d3-a456-4266141740000", "sk-planted-marker-7933", ""} {
		if got := ParseID(text).text; got != "invalid" {
			t.Fatalf("ParseID(%q) = %q, want invalid", text, got)
		}
	}
	if got := ParseID("123e4567-e89b-12d3-a456-426614174000").text; got != "123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("a UUID was refused: %q", got)
	}
	for _, token := range []string{"cooldown_active", "github", "light", "a.b:c", "rate_limit_deferral_budget_exhausted"} {
		if got := ParseLabel(token).text; got != token {
			t.Fatalf("ParseLabel(%q) = %q", token, got)
		}
	}
}

func TestParseIDsParsesEveryElement(t *testing.T) {
	got := ParseIDs([]string{"123e4567-e89b-12d3-a456-426614174000", "the planted detail of ticket 7933 list", "sk-planted-marker-7933"})
	if got[0].text != "123e4567-e89b-12d3-a456-426614174000" || got[1].text != "invalid" || got[2].text != "invalid" {
		t.Fatalf("ParseIDs = %v", got)
	}
}

// The exported API of this package is PINNED (D4262/F5): no exported function or method takes a string, an any, an error or a
// list of strings, except the ones named here (the two parsers, the one error entry and the constructor); no exported struct has an
// exported field; every exported variable is one of the opaque vocabulary types. A new function that widens the API fails here by
// name, whatever its body does.
func TestTheExportedAPIIsPinned(t *testing.T) {
	allowed := map[string]string{
		"New":        "*log/slog.Logger",
		"ParseID":    "string",
		"ParseIDs":   "[]string",
		"ParseLabel": "string",
		"Failure":    "error",
	}
	config := &packages.Config{Dir: ".", Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedSyntax | packages.NeedImports | packages.NeedDeps}
	loaded, err := packages.Load(config, ".")
	if err != nil || len(loaded) != 1 || len(loaded[0].Errors) > 0 {
		t.Fatalf("load: %v %v", err, loaded)
	}
	scope := loaded[0].Types.Scope()
	var forbidden func(kind types.Type) bool
	forbidden = func(kind types.Type) bool {
		switch typed := types.Unalias(kind).(type) {
		case *types.Basic:
			return typed.Info()&types.IsString != 0
		case *types.Interface:
			return true // any, error and every other interface
		case *types.Slice:
			return forbidden(typed.Elem())
		case *types.Named:
			if typed.String() == "context.Context" {
				return false
			}
			if _, isInterface := typed.Underlying().(*types.Interface); isInterface {
				return true
			}
		}
		return false
	}
	checkSignature := func(name string, signature *types.Signature) {
		params := signature.Params()
		for index := 0; index < params.Len(); index++ {
			kind := params.At(index).Type()
			if variadic := signature.Variadic() && index == params.Len()-1; variadic {
				kind = kind.(*types.Slice).Elem()
			}
			if forbidden(kind) {
				if want, ok := allowed[name]; !ok || want != kind.String() {
					t.Errorf("%s takes a %s: the exported API may not widen", name, kind)
				}
			}
		}
	}
	// the exported identifiers are an ALLOW-LIST: a new function, type or method fails until this list is edited in the same diff
	// (the vocabulary vars of vocab.go are matched by their type, not listed one by one)
	allowedFunctions := map[string]bool{"New": true, "Default": true, "ParseID": true, "ParseIDs": true, "ParseLabel": true, "Run": true, "Unit": true,
		"Org": true, "Integration": true, "Source": true, "Dispatch": true, "Provider": true, "Text": true, "Count": true, "Flag": true,
		"Elapsed": true, "Instant": true, "IDs": true, "Group": true, "Failure": true}
	allowedTypes := map[string]bool{"Msg": true, "Key": true, "Label": true, "ID": true, "Attr": true, "Logger": true}
	allowedMethods := map[string]bool{"Logger.Debug": true, "Logger.Info": true, "Logger.Warn": true, "Logger.Error": true}
	exported, functions := 0, 0
	for _, name := range scope.Names() {
		object := scope.Lookup(name)
		if !object.Exported() {
			continue
		}
		exported++
		switch object.(type) {
		case *types.Func:
			if !allowedFunctions[name] {
				t.Errorf("exported function %s is not on the allow-list", name)
			}
		case *types.TypeName:
			if !allowedTypes[name] {
				t.Errorf("exported type %s is not on the allow-list", name)
			}
		}
		switch typed := object.(type) {
		case *types.Func:
			functions++
			checkSignature(name, typed.Type().(*types.Signature))
		case *types.Var:
			kind := types.Unalias(typed.Type())
			if named, ok := kind.(*types.Named); !ok || named.Obj().Pkg() != loaded[0].Types || named.Obj().Name() == "Logger" {
				t.Errorf("exported var %s has type %s: only the opaque vocabulary types are allowed", name, typed.Type())
			}
		case *types.TypeName:
			named, ok := typed.Type().(*types.Named)
			if !ok {
				continue
			}
			if structure, ok := named.Underlying().(*types.Struct); ok {
				for index := 0; index < structure.NumFields(); index++ {
					if structure.Field(index).Exported() {
						t.Errorf("%s has the exported field %s", name, structure.Field(index).Name())
					}
				}
			}
			for index := 0; index < named.NumMethods(); index++ {
				method := named.Method(index)
				if method.Exported() {
					if !allowedMethods[name+"."+method.Name()] {
						t.Errorf("exported method %s.%s is not on the allow-list", name, method.Name())
					}
					checkSignature(name+"."+method.Name(), method.Type().(*types.Signature))
				}
			}
		}
	}
	if exported < 50 || functions < 15 {
		t.Fatalf("saw %d exported objects and %d functions: the walk reads too little", exported, functions)
	}
	for name := range allowed {
		if scope.Lookup(name) == nil {
			t.Errorf("allowed entry %s no longer exists: delete it from the allow-list", name)
		}
	}
}
