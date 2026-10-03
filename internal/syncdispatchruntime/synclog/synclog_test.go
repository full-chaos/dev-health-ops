package synclog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
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

// The exported API of this package is PINNED by exact signature (D4315): the surface computed from the type information must equal
// exportedAPI line for line: no export may be added, removed or change a parameter or result type unnoticed.
func TestTheExportedAPIIsPinned(t *testing.T) {
	config := &packages.Config{Dir: ".", Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedSyntax | packages.NeedImports | packages.NeedDeps}
	loaded, err := packages.Load(config, ".")
	if err != nil || len(loaded) != 1 || len(loaded[0].Errors) > 0 {
		t.Fatalf("load: %v %v", err, loaded)
	}
	qualifier := func(p *types.Package) string { return p.Name() }
	var got []string
	scope := loaded[0].Types.Scope()
	for _, name := range scope.Names() {
		object := scope.Lookup(name)
		if !object.Exported() {
			continue
		}
		got = append(got, types.ObjectString(object, qualifier))
		if typeName, ok := object.(*types.TypeName); ok {
			if named, ok := typeName.Type().(*types.Named); ok {
				for index := 0; index < named.NumMethods(); index++ {
					if method := named.Method(index); method.Exported() {
						got = append(got, types.ObjectString(method, qualifier))
					}
				}
				if structure, ok := named.Underlying().(*types.Struct); ok {
					for index := 0; index < structure.NumFields(); index++ {
						field := structure.Field(index)
						got = append(got, fmt.Sprintf("field %s.%s %s exported=%v", name, field.Name(), field.Type(), field.Exported()))
					}
				}
			}
		}
	}
	sort.Strings(got)
	want := append([]string(nil), exportedAPI...)
	sort.Strings(want)
	if len(got) < 100 {
		t.Fatalf("saw %d exported objects: the walk reads too little", len(got))
	}
	missing, extra := difference(want, got), difference(got, want)
	if len(missing) != 0 || len(extra) != 0 {
		t.Fatalf("the exported API changed.\nremoved or changed: %v\nadded or changed: %v\n(edit exportedAPI in the same diff)", missing, extra)
	}
}

func difference(left, right []string) []string {
	present := map[string]bool{}
	for _, line := range right {
		present[line] = true
	}
	var out []string
	for _, line := range left {
		if !present[line] {
			out = append(out, line)
		}
	}
	return out
}
