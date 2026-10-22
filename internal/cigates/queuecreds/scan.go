// Package queuecreds is the credential-scan gate for queue payloads and job
// diagnostics. It reads the checked-in job contract, the Go queue argument
// types and the operator JSON structs, and reports any place that can carry a
// reusable credential. The gate tests in this package are its only caller.
package queuecreds

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Finding is one place where the scan found a credential carrier.
type Finding struct {
	Where string
	Why   string
}

func (finding Finding) String() string { return finding.Where + ": " + finding.Why }

var (
	deniedNameTokens = set("token", "tokens", "secret", "secrets", "password", "passwd", "passphrase", "pwd",
		"bearer", "authorization", "cookie", "cookies", "jwt", "credential", "credentials", "dsn", "apikey")
	keyQualifiers = set("api", "private", "signing", "encryption", "secret", "access", "auth", "master")
	// A denied word that ends in one of these words names a non-reusable
	// attribute of a credential (its fingerprint, its hash, its id), not the
	// credential itself.
	safeNameSuffixes = set("fingerprint", "hash", "digest", "id", "kind", "class", "count", "name", "ref", "key")
)

func set(words ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(words))
	for _, word := range words {
		out[word] = struct{}{}
	}
	return out
}

func nameWords(name string) []string {
	runes := []rune(name)
	var words []string
	var current []rune
	flush := func() {
		if len(current) > 0 {
			words = append(words, strings.ToLower(string(current)))
			current = current[:0]
		}
	}
	for index, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if unicode.IsUpper(r) && len(current) > 0 {
			previous := runes[index-1]
			nextLower := index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && nextLower) {
				flush()
			}
		}
		current = append(current, r)
	}
	flush()
	return words
}

// CredentialName reports whether a property, field or tag name reads as a
// reusable credential. Names are split into words, so "idempotency_key" and
// "trace_parent" pass while "refresh_token" and "APIKey" do not.
func CredentialName(name string) bool {
	words := nameWords(name)
	for index, word := range words {
		if word == "key" && index > 0 {
			if _, ok := keyQualifiers[words[index-1]]; ok {
				return true
			}
		}
	}
	denied := false
	for _, word := range words {
		if _, ok := deniedNameTokens[word]; ok {
			denied = true
		}
	}
	if !denied {
		return false
	}
	if _, safe := safeNameSuffixes[words[len(words)-1]]; safe {
		return false
	}
	return true
}

// ValueScanner matches credential-shaped string values.
type ValueScanner struct {
	rules []valueRule
}

type valueRule struct {
	name    string
	pattern *regexp.Regexp
}

var staticValueRules = []valueRule{
	{"jwt", regexp.MustCompile(`eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*`)},
	{"private-key-block", regexp.MustCompile(`-----BEGIN [A-Z ]*(PRIVATE KEY|CERTIFICATE)-----`)},
	{"url-userinfo", regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:[^/\s@]+@`)},
	{"provider-token", regexp.MustCompile(`\b(gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|glpat-[A-Za-z0-9_-]{16,}|xox[abprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|sk_live_[A-Za-z0-9]{16,})`)},
}

// NewValueScanner derives the value rules from the credential-classes contract:
// every credential_prefix marker becomes a prefix rule, every
// authorization_scheme marker a scheme rule. The static shapes (JWT, PEM,
// URL userinfo, provider tokens) are added to them. A contract with no prefix
// marker is an error: a scan with no auth-derived rule did not measure.
func NewValueScanner(credentialClasses []byte) (*ValueScanner, error) {
	var contract struct {
		Classes []struct {
			Markers []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"markers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(credentialClasses, &contract); err != nil {
		return nil, fmt.Errorf("decode credential classes: %w", err)
	}
	prefixes, schemes := map[string]struct{}{}, map[string]struct{}{}
	for _, class := range contract.Classes {
		for _, marker := range class.Markers {
			switch marker.Kind {
			case "credential_prefix":
				prefixes[marker.Name] = struct{}{}
			case "authorization_scheme":
				schemes[marker.Name] = struct{}{}
			}
		}
	}
	if len(prefixes) == 0 || len(schemes) == 0 {
		return nil, errors.New("credential classes carry no credential_prefix or authorization_scheme marker")
	}
	scanner := &ValueScanner{rules: append([]valueRule(nil), staticValueRules...)}
	for _, prefix := range sortedKeys(prefixes) {
		scanner.rules = append(scanner.rules, valueRule{
			"prefix:" + prefix, regexp.MustCompile(`(?i)(^|[^A-Za-z0-9])` + regexp.QuoteMeta(prefix) + `[A-Za-z0-9_-]{8,}`)})
	}
	for _, scheme := range sortedKeys(schemes) {
		scanner.rules = append(scanner.rules, valueRule{
			"scheme:" + scheme, regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(scheme) + `\s+[A-Za-z0-9._~+/=-]{8,}`)})
	}
	return scanner, nil
}

// Match returns the name of the first rule that matches, or "".
func (scanner *ValueScanner) Match(value string) string {
	for _, rule := range scanner.rules {
		if rule.pattern.MatchString(value) {
			return rule.name
		}
	}
	return ""
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// ContractScan is the result of scanning the job contract directory.
type ContractScan struct {
	Findings []Finding
	Schemas  int
	Examples int
}

const contractDir = "contracts/jobs/v1"

// ScanContracts scans the envelope schema, every payload schema and every
// example under root/contracts/jobs/v1.
func ScanContracts(root string, values *ValueScanner) (ContractScan, error) {
	var scan ContractScan
	dir := filepath.Join(root, filepath.FromSlash(contractDir))
	schemaFiles, err := filepath.Glob(filepath.Join(dir, "schemas", "*.json"))
	if err != nil {
		return scan, err
	}
	schemaFiles = append([]string{filepath.Join(dir, "envelope.schema.json")}, schemaFiles...)
	for _, file := range schemaFiles {
		node, err := readJSON(file)
		if err != nil {
			return scan, err
		}
		scan.Schemas++
		label := relativeLabel(dir, file)
		walkSchema(&scan, values, label, "$", node, false)
	}
	exampleFiles, err := filepath.Glob(filepath.Join(dir, "examples", "*.json"))
	if err != nil {
		return scan, err
	}
	for _, file := range exampleFiles {
		node, err := readJSON(file)
		if err != nil {
			return scan, err
		}
		scan.Examples++
		walkValue(&scan, values, relativeLabel(dir, file), "$", node)
	}
	return scan, nil
}

func relativeLabel(dir, file string) string {
	rel, err := filepath.Rel(dir, file)
	if err != nil {
		return file
	}
	return filepath.ToSlash(rel)
}

func readJSON(file string) (any, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var node any
	if err := decoder.Decode(&node); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	return node, nil
}

func (scan *ContractScan) add(where, why string) {
	scan.Findings = append(scan.Findings, Finding{Where: where, Why: why})
}

var schemaChildKeywords = []string{"items", "not", "if", "then", "else", "contains", "propertyNames"}

func walkSchema(scan *ContractScan, values *ValueScanner, file, path string, node any, envelopePayload bool) {
	schema, ok := node.(map[string]any)
	if !ok {
		return
	}
	where := file + " " + path
	properties, hasProperties := schema["properties"].(map[string]any)
	typeName, _ := schema["type"].(string)
	if _, patterned := schema["patternProperties"]; patterned {
		scan.add(where, "patternProperties lets a payload carry names the contract does not list")
	}
	if hasProperties || typeName == "object" {
		// The envelope's payload slot is open by design; each kind's own
		// schema closes it, and those schemas are scanned here too.
		if additional, present := schema["additionalProperties"]; !envelopePayload && (!present || additional != false) {
			scan.add(where, "object schema is not closed with additionalProperties false")
		}
		if !hasProperties && !envelopePayload {
			scan.add(where, "object schema lists no properties")
		}
	}
	if typeName == "string" {
		_, format := schema["format"]
		_, pattern := schema["pattern"]
		_, enum := schema["enum"]
		_, constant := schema["const"]
		if !format && !pattern && !enum && !constant {
			scan.add(where, "string property has no format, pattern, enum or const")
		}
	}
	if typeName == "" {
		_, ref := schema["$ref"]
		_, enum := schema["enum"]
		_, constant := schema["const"]
		_, oneOf := schema["oneOf"]
		_, anyOf := schema["anyOf"]
		_, allOf := schema["allOf"]
		if !hasProperties && !ref && !enum && !constant && !oneOf && !anyOf && !allOf {
			scan.add(where, "schema node has no type")
		}
	}
	for _, keyword := range []string{"enum", "const", "default", "examples"} {
		if value, present := schema[keyword]; present {
			walkValue(scan, values, file, path+"."+keyword, value)
		}
	}
	for _, name := range sortedMapKeys(properties) {
		if CredentialName(name) {
			scan.add(where+".properties."+name, "property name reads as a credential")
		}
		walkSchema(scan, values, file, path+".properties."+name, properties[name],
			file == "envelope.schema.json" && path == "$" && name == "payload")
	}
	for _, keyword := range schemaChildKeywords {
		if child, present := schema[keyword]; present {
			walkSchema(scan, values, file, path+"."+keyword, child, false)
		}
	}
	for _, keyword := range []string{"oneOf", "anyOf", "allOf", "prefixItems"} {
		if children, present := schema[keyword].([]any); present {
			for index, child := range children {
				walkSchema(scan, values, file, path+"."+keyword+"["+strconv.Itoa(index)+"]", child, false)
			}
		}
	}
	for _, keyword := range []string{"$defs", "definitions"} {
		if defs, present := schema[keyword].(map[string]any); present {
			for _, name := range sortedMapKeys(defs) {
				walkSchema(scan, values, file, path+"."+keyword+"."+name, defs[name], false)
			}
		}
	}
}

func walkValue(scan *ContractScan, values *ValueScanner, file, path string, node any) {
	switch typed := node.(type) {
	case map[string]any:
		for _, name := range sortedMapKeys(typed) {
			if CredentialName(name) {
				scan.add(file+" "+path+"."+name, "key name reads as a credential")
			}
			walkValue(scan, values, file, path+"."+name, typed[name])
		}
	case []any:
		for index, child := range typed {
			walkValue(scan, values, file, path+"["+strconv.Itoa(index)+"]", child)
		}
	case string:
		if rule := values.Match(typed); rule != "" {
			scan.add(file+" "+path, "value matches the credential rule "+rule)
		}
	}
}

func sortedMapKeys[V any](values map[string]V) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// ScanType walks the serialized shape of one Go type. A field is reported when
// its JSON name reads as a credential or when it can carry an open value (an
// interface, a map or raw bytes). Fields tagged json:"-" are not serialized and
// are skipped.
func ScanType(label string, typ reflect.Type) []Finding {
	var findings []Finding
	walkType(&findings, label, typ, map[reflect.Type]bool{})
	return findings
}

func walkType(findings *[]Finding, path string, typ reflect.Type, active map[reflect.Type]bool) {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Interface:
		*findings = append(*findings, Finding{path, "interface value can carry an open payload"})
	case reflect.Map:
		*findings = append(*findings, Finding{path, "map value can carry an open payload"})
	case reflect.Slice, reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 {
			*findings = append(*findings, Finding{path, "byte slice can carry an opaque payload"})
			return
		}
		walkType(findings, path+"[]", typ.Elem(), active)
	case reflect.Struct:
		if active[typ] {
			return
		}
		active[typ] = true
		defer delete(active, typ)
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index)
			name, skip := jsonFieldName(field)
			if skip {
				continue
			}
			if field.Anonymous && name == "" {
				walkType(findings, path, field.Type, active)
				continue
			}
			if !field.IsExported() {
				continue
			}
			child := path + "." + name
			if CredentialName(name) {
				*findings = append(*findings, Finding{child, "field name reads as a credential"})
			}
			walkType(findings, child, field.Type, active)
		}
	}
}

// jsonFieldName returns the serialized name of a field. For an embedded field
// without a tag name it returns "" so the caller flattens it.
func jsonFieldName(field reflect.StructField) (name string, skip bool) {
	tag, tagged := field.Tag.Lookup("json")
	if tag == "-" {
		return "", true
	}
	if tagged {
		if before, _, _ := strings.Cut(tag, ","); before != "" {
			return before, false
		}
	}
	if field.Anonymous {
		return "", false
	}
	return field.Name, false
}

// PackageTypes parses the non-test Go files of one package directory.
type PackageTypes struct {
	Dir   string
	files []*ast.File
}

// ParsePackage parses the non-test Go files directly under dir.
func ParsePackage(dir string) (PackageTypes, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return PackageTypes{}, err
	}
	parsed := PackageTypes{Dir: dir}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			return PackageTypes{}, err
		}
		parsed.files = append(parsed.files, file)
	}
	if len(parsed.files) == 0 {
		return PackageTypes{}, fmt.Errorf("%s holds no Go files", dir)
	}
	return parsed, nil
}

// StructTypesWithSuffix lists the struct types whose name ends in suffix.
func (parsed PackageTypes) StructTypesWithSuffix(suffix string) []string {
	var names []string
	for _, file := range parsed.files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				typeSpec := spec.(*ast.TypeSpec)
				if _, isStruct := typeSpec.Type.(*ast.StructType); isStruct && strings.HasSuffix(typeSpec.Name.Name, suffix) {
					names = append(names, typeSpec.Name.Name)
				}
			}
		}
	}
	sort.Strings(names)
	return names
}

// TypesWithMethod lists the receiver type names that declare method.
func (parsed PackageTypes) TypesWithMethod(method string) []string {
	seen := map[string]struct{}{}
	for _, file := range parsed.files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != method || len(fn.Recv.List) != 1 {
				continue
			}
			expr := fn.Recv.List[0].Type
			if star, isStar := expr.(*ast.StarExpr); isStar {
				expr = star.X
			}
			if ident, isIdent := expr.(*ast.Ident); isIdent {
				seen[ident.Name] = struct{}{}
			}
		}
	}
	return sortedKeys(seen)
}

// TaggedChild is one field of a tagged struct-typed field.
type TaggedChild struct {
	Name string
	Type string
}

// TaggedField is one field of the package that carries a json tag.
type TaggedField struct {
	// Func is the function whose body declares the field's struct, or "" for a
	// package-level type.
	Func string
	// Name is the json tag name.
	Name string
	// Struct is true when the field's type is an anonymous struct.
	Struct   bool
	Children []TaggedChild
}

// TaggedFields lists every field with a json tag in the package.
func (parsed PackageTypes) TaggedFields() []TaggedField {
	var fields []TaggedField
	collect := func(function string, root ast.Node) {
		ast.Inspect(root, func(node ast.Node) bool {
			field, ok := node.(*ast.Field)
			if !ok || field.Tag == nil {
				return true
			}
			name, ok := jsonName(field)
			if !ok {
				return true
			}
			tagged := TaggedField{Func: function, Name: name}
			if structType, isStruct := field.Type.(*ast.StructType); isStruct {
				tagged.Struct = true
				for _, child := range structType.Fields.List {
					childName, named := jsonName(child)
					if !named && len(child.Names) == 1 {
						childName = child.Names[0].Name
					}
					typeName := "?"
					if ident, isIdent := child.Type.(*ast.Ident); isIdent {
						typeName = ident.Name
					}
					tagged.Children = append(tagged.Children, TaggedChild{Name: childName, Type: typeName})
				}
			}
			fields = append(fields, tagged)
			return true
		})
	}
	for _, file := range parsed.files {
		for _, decl := range file.Decls {
			if fn, isFunc := decl.(*ast.FuncDecl); isFunc {
				collect(fn.Name.Name, fn)
			} else {
				collect("", decl)
			}
		}
	}
	return fields
}

func jsonName(field *ast.Field) (string, bool) {
	if field.Tag == nil {
		return "", false
	}
	raw, err := strconv.Unquote(field.Tag.Value)
	if err != nil {
		return "", false
	}
	tag, ok := reflect.StructTag(raw).Lookup("json")
	if !ok {
		return "", false
	}
	name, _, _ := strings.Cut(tag, ",")
	return name, name != "" && name != "-"
}

// Exemption pins one operator JSON field whose name reads as a credential but
// whose value is not one. It matches only while the field keeps its tag, its
// enclosing function and its exact struct shape (acp, 2026-10-22).
type Exemption struct {
	Func     string
	Tag      string
	Children []string
	Reason   string
}

// OperatorExemptions is the complete exemption list for the operator JSON scan.
var OperatorExemptions = []Exemption{{
	Func:     "logResolvedDatabase",
	Tag:      "dsn",
	Children: []string{"database", "form", "name"},
	Reason: "the record names which database form the CLI resolved; its children are a label, the form and a database " +
		"identity, and carry no connection string",
}}

func (exemption Exemption) matches(field TaggedField) bool {
	if field.Func != exemption.Func || field.Name != exemption.Tag || !field.Struct || len(field.Children) != len(exemption.Children) {
		return false
	}
	names := make([]string, 0, len(field.Children))
	for _, child := range field.Children {
		if child.Type != "string" || CredentialName(child.Name) {
			return false
		}
		names = append(names, child.Name)
	}
	sort.Strings(names)
	for index, want := range exemption.Children {
		if names[index] != want {
			return false
		}
	}
	return true
}

// OperatorFindings reports every tagged field whose name reads as a credential
// and is not pinned by an exemption, plus every exemption that no field matches
// any more (a stale row is a failure, not a silent pass).
func (parsed PackageTypes) OperatorFindings(label string, exemptions []Exemption) []Finding {
	var findings []Finding
	used := make([]bool, len(exemptions))
	for _, field := range parsed.TaggedFields() {
		if !CredentialName(field.Name) {
			continue
		}
		exempt := false
		for index, exemption := range exemptions {
			if exemption.matches(field) {
				used[index], exempt = true, true
			}
		}
		if !exempt {
			findings = append(findings, Finding{label + " json tag " + field.Name, "tag name reads as a credential"})
		}
	}
	for index, exemption := range exemptions {
		if !used[index] {
			findings = append(findings, Finding{label + " exemption " + exemption.Func + "/" + exemption.Tag, "exemption matches no field of its pinned shape"})
		}
	}
	return findings
}
