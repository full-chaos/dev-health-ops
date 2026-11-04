package contracts

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

var readAPIRequestSurfaces = []string{
	"principal-get-request", "principal-list-request",
	"organization-get-request", "organization-list-request",
	"membership-get-request", "membership-list-request",
	"grant-get-request", "grant-list-request",
}

var readAPIResourceSurfaces = []string{"principal-read", "organization-read", "membership-read", "grant-read"}

var readAPIListSurfaces = []string{"principal-list", "organization-list", "membership-list", "grant-list"}

func readAPISurfaces() []string {
	var all []string
	all = append(all, readAPIRequestSurfaces...)
	all = append(all, readAPIResourceSurfaces...)
	return append(all, readAPIListSurfaces...)
}

var readAPIKeywordTokens = map[string]string{
	"additionalProperties": "unexpected additional properties",
	"const":                "const:",
	"enum":                 "enum:",
	"maxItems":             "maxItems:",
	"maxLength":            "maxLength:",
	"maximum":              "maximum:",
	"minLength":            "minLength:",
	"minimum":              "minimum:",
	"pattern":              "pattern:",
	"required":             "required:",
	"type":                 "type:",
}

func readAPIDecode(t *testing.T, raw []byte) any {
	t.Helper()
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	return doc
}

// Every example of a read surface is claimed by its manifest, and the Go
// validator reaches the manifest's verdict on each.
func TestReadAPIExampleCorporaAreFullyClaimedAndAgreeWithTheSchemas(t *testing.T) {
	root := testRoot(t)
	for _, name := range readAPISurfaces() {
		name := name
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(root, "contracts/auth/v1/examples", name)
			raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			var m provisioningResourceManifest
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			if len(m.Accept) == 0 || len(m.Reject) == 0 {
				t.Fatalf("empty group: accept=%d reject=%d", len(m.Accept), len(m.Reject))
			}
			claimed := map[string]bool{"manifest.json": true}
			for _, e := range m.Accept {
				claimed[e.File] = true
				body, err := os.ReadFile(filepath.Join(dir, e.File))
				if err != nil {
					t.Fatal(err)
				}
				if err := Validate(root, name+".v1", readAPIDecode(t, body)); err != nil {
					t.Errorf("accept %s refused: %v", e.File, err)
				}
			}
			for _, e := range m.Reject {
				claimed[e.File] = true
				token, ok := readAPIKeywordTokens[e.ExpectKeyword]
				if !ok {
					t.Fatalf("no Go message token for keyword %q", e.ExpectKeyword)
				}
				body, err := os.ReadFile(filepath.Join(dir, e.File))
				if err != nil {
					t.Fatal(err)
				}
				err = Validate(root, name+".v1", readAPIDecode(t, body))
				if err == nil || !strings.Contains(err.Error(), token) {
					t.Errorf("reject %s: err %v, want the %s keyword", e.File, err, e.ExpectKeyword)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !claimed[entry.Name()] {
					t.Errorf("unclaimed fixture %s", entry.Name())
				}
			}
		})
	}
}

const readAPIGoldenDir = "contracts/auth/v1/golden/read-api"

// readAPICorpusDigest pins every file of golden/read-api. Moving it is a
// deliberate edit beside the corpus change.
const readAPICorpusDigest = "sha256:75d9df449af79340eb0e7e0bb7f99d696bfc9b0555b5a5012a740aecff68cb63"

func readAPIGoldenFiles(t *testing.T) []decisionFile {
	t.Helper()
	root := filepath.Join(testRoot(t), readAPIGoldenDir)
	var files []decisionFile
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(root, path)
		files = append(files, decisionFile{filepath.ToSlash(rel), raw})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return files
}

func TestReadAPICorpusDigestIsPinned(t *testing.T) {
	files := readAPIGoldenFiles(t)
	if len(files) == 0 {
		t.Fatal("the read-api golden corpus is empty")
	}
	if got := decisionDigest(files); got != readAPICorpusDigest {
		t.Fatalf("golden/read-api digest = %s, pinned %s: move readAPICorpusDigest with the corpus change", got, readAPICorpusDigest)
	}
}

type readVector struct {
	ID             string          `json:"id"`
	Kind           string          `json:"kind"`
	Operation      string          `json:"operation"`
	Request        json.RawMessage `json:"request"`
	RequestSchema  string          `json:"request_schema"`
	Response       json.RawMessage `json:"response"`
	ResponseRaw    string          `json:"response_raw"`
	ResponseSchema string          `json:"response_schema"`
	HTTPStatus     int             `json:"http_status"`
	Body           json.RawMessage `json:"body"`
	Parse          string          `json:"parse"`
	Rule           string          `json:"rule"`
}

type readManifest struct {
	Surface string `json:"surface"`
	Vectors []struct {
		ID   string `json:"id"`
		File string `json:"file"`
		Kind string `json:"kind"`
	} `json:"vectors"`
}

func readVectors(t *testing.T) []readVector {
	t.Helper()
	dir := filepath.Join(testRoot(t), readAPIGoldenDir)
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m readManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.Surface != "read-api-golden.v1" || len(m.Vectors) == 0 {
		t.Fatalf("manifest: surface %q, %d vectors", m.Surface, len(m.Vectors))
	}
	claimed := map[string]bool{}
	var out []readVector
	for _, row := range m.Vectors {
		if claimed[row.File] {
			t.Fatalf("vector file %s listed twice", row.File)
		}
		claimed[row.File] = true
		body, err := os.ReadFile(filepath.Join(dir, "vectors", row.File))
		if err != nil {
			t.Fatal(err)
		}
		var v readVector
		if err := json.Unmarshal(body, &v); err != nil {
			t.Fatalf("%s: %v", row.File, err)
		}
		if v.ID != row.ID || v.Kind != row.Kind || row.File != row.ID+".json" {
			t.Fatalf("%s: manifest row and vector disagree", row.File)
		}
		out = append(out, v)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "vectors"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !claimed[entry.Name()] {
			t.Errorf("unclaimed vector %s", entry.Name())
		}
	}
	return out
}

// refusedBy is true when err is the refusal a vector names: a schema or decode
// stage, or a client rule.
func refusedBy(err error, rule string) bool {
	var refusal *CredentialRefusal
	if !errors.As(err, &refusal) {
		return err != nil && (rule == "schema" || rule == "envelope")
	}
	switch rule {
	case "schema":
		return refusal.Stage == StageSchema
	case "decode":
		return refusal.Stage == StageDecode
	case "envelope":
		return true
	}
	return refusal.Stage == StageClientRule && refusal.Rule == rule
}

var readVectorNow = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

func TestReadAPIGoldensSchemaAndParserAgreeOnEveryVector(t *testing.T) {
	root := testRoot(t)
	counts := map[string]int{}
	for _, v := range readVectors(t) {
		v := v
		counts[v.Kind]++
		t.Run(v.ID, func(t *testing.T) {
			switch v.Kind {
			case "request":
				op, ok := ReadOperationByName(v.Operation)
				if !ok {
					t.Fatalf("no operation %q", v.Operation)
				}
				schemaErr := Validate(root, op.RequestSurface, readAPIDecode(t, v.Request))
				if (schemaErr == nil) != (v.RequestSchema == "accept") {
					t.Errorf("schema verdict %v, vector says %s", schemaErr, v.RequestSchema)
				}
				_, err := ParseReadRequest(root, v.Operation, v.Request)
				if (err == nil) != (v.Parse == "accept") {
					t.Fatalf("parse verdict %v, vector says %s", err, v.Parse)
				}
				if err != nil && !refusedBy(err, v.Rule) {
					t.Errorf("refusal %v is not the rule %q", err, v.Rule)
				}
			case "exchange":
				op, ok := ReadOperationByName(v.Operation)
				if !ok {
					t.Fatalf("no operation %q", v.Operation)
				}
				req, err := ParseReadRequest(root, v.Operation, v.Request)
				if err != nil {
					t.Fatalf("the vector's request is refused: %v", err)
				}
				response := []byte(v.Response)
				if v.ResponseRaw != "" {
					response = []byte(v.ResponseRaw)
				}
				schemaErr := Validate(root, op.ResponseSurface, readAPIDecode(t, response))
				if (schemaErr == nil) != (v.ResponseSchema == "accept") {
					t.Errorf("schema verdict %v, vector says %s", schemaErr, v.ResponseSchema)
				}
				_, err = ParseReadResponse(root, req, response)
				if (err == nil) != (v.Parse == "accept") {
					t.Fatalf("parse verdict %v, vector says %s", err, v.Parse)
				}
				if err != nil && !refusedBy(err, v.Rule) {
					t.Errorf("refusal %v is not the rule %q", err, v.Rule)
				}
			case "error":
				err := ParseReadError(root, v.Body, v.HTTPStatus, readVectorNow)
				if (err == nil) != (v.Parse == "accept") {
					t.Fatalf("parse verdict %v, vector says %s", err, v.Parse)
				}
				if err != nil && !refusedBy(err, v.Rule) {
					t.Errorf("refusal %v is not the rule %q", err, v.Rule)
				}
			default:
				t.Fatalf("unknown vector kind %q", v.Kind)
			}
		})
	}
	for _, kind := range []string{"request", "exchange", "error"} {
		if counts[kind] == 0 {
			t.Errorf("the corpus has no %s vector", kind)
		}
	}
}

// Each operation has a vector the parser accepts and one it refuses with a
// client rule, so no operation rides on the schema alone.
func TestReadAPIEveryOperationHasAnAcceptedAndAParserRefusedVector(t *testing.T) {
	accepted, refused := map[string]bool{}, map[string]bool{}
	for _, v := range readVectors(t) {
		if v.Kind != "exchange" {
			continue
		}
		if v.Parse == "accept" {
			accepted[v.Operation] = true
		}
		if v.Parse == "reject" && v.ResponseSchema == "accept" {
			refused[v.Operation] = true
		}
	}
	if len(ReadOperations()) != 8 {
		t.Fatalf("operations = %d, want 8", len(ReadOperations()))
	}
	for _, op := range ReadOperations() {
		if !accepted[op.Name] || !refused[op.Name] {
			t.Errorf("%s: accepted vector %v, parser-only refusal vector %v", op.Name, accepted[op.Name], refused[op.Name])
		}
	}
}

func TestReadAPIOperationTable(t *testing.T) {
	root := testRoot(t)
	ops := ReadOperations()
	if len(ops) != 8 {
		t.Fatalf("operations = %d, want 8 (get and list of four aggregates)", len(ops))
	}
	names, routes := map[string]bool{}, map[string]bool{}
	for _, op := range ops {
		if names[op.Name] || routes[op.Route] {
			t.Errorf("%s: repeated name or route", op.Name)
		}
		names[op.Name], routes[op.Route] = true, true
		if !strings.HasPrefix(op.Route, "/internal/v1/") {
			t.Errorf("%s: route %s", op.Name, op.Route)
		}
		aggregate := strings.SplitN(strings.TrimPrefix(op.Route, "/internal/v1/"), "/", 2)[0]
		if op.Scope != aggregate+":read" {
			t.Errorf("%s: scope %s for route %s", op.Name, op.Scope, op.Route)
		}
		for _, surface := range []string{op.RequestSurface, op.ResponseSurface} {
			if _, err := os.Stat(SchemaPath(root, surface)); err != nil {
				t.Errorf("%s: %v", op.Name, err)
			}
		}
		got, ok := ReadOperationByName(op.Name)
		if !ok || got != op {
			t.Errorf("%s: ReadOperationByName disagrees", op.Name)
		}
		if op.List {
			node := schemaNodeOf(t, strings.TrimSuffix(op.ResponseSurface, ".v1"), "properties", "next_cursor")
			if node == nil {
				t.Errorf("%s: a list response has no next_cursor", op.Name)
			}
		}
	}
}

// schemaNodeOf is schemaNode for a surface named with its version.
func schemaNodeOf(t *testing.T, surface string, path ...string) any {
	return schemaNode(t, surface, path...)
}

func TestReadAPIPageSizeIsDefault50AndMax100(t *testing.T) {
	root := testRoot(t)
	if ReadDefaultLimit != 50 || ReadMaxLimit != 100 {
		t.Fatalf("limits = %d, %d", ReadDefaultLimit, ReadMaxLimit)
	}
	body := func(limit string) []byte {
		member := ""
		if limit != "" {
			member = `,"limit":` + limit
		}
		return []byte(`{"schema_version":"membership-list-request.v1","organization_id":"org_EXAMPLE0000000000000001"` + member + `}`)
	}
	for limit, want := range map[string]int{"": 50, "1": 1, "50": 50, "100": 100} {
		req, err := ParseReadRequest(root, "memberships_list", body(limit))
		if err != nil || req.Limit != want {
			t.Errorf("limit %q: %v, %+v (want %d)", limit, err, req, want)
		}
	}
	for _, limit := range []string{"0", "-1", "101", "1000", "1.5", `"50"`, "null"} {
		if _, err := ParseReadRequest(root, "memberships_list", body(limit)); err == nil {
			t.Errorf("limit %s was accepted", limit)
		}
	}
	for _, name := range readAPIRequestSurfaces {
		if !strings.HasSuffix(name, "list-request") {
			continue
		}
		limit := schemaNode(t, name, "properties", "limit").(map[string]any)
		if limit["minimum"] != float64(1) || limit["maximum"] != float64(ReadMaxLimit) {
			t.Errorf("%s: limit bounds %v %v", name, limit["minimum"], limit["maximum"])
		}
	}
	for _, name := range readAPIListSurfaces {
		items := schemaNode(t, name, "properties", "items").(map[string]any)
		if items["maxItems"] != float64(ReadMaxLimit) {
			t.Errorf("%s: maxItems %v", name, items["maxItems"])
		}
	}
}

func TestReadAPIKeysetCursor(t *testing.T) {
	id := "mbr_EXAMPLE0000000000000007"
	cursor := EncodeKeysetCursor(id)
	if !strings.HasPrefix(cursor, "k1_") || strings.ContainsAny(cursor, "=+/ ") {
		t.Fatalf("cursor %q", cursor)
	}
	if got, err := DecodeKeysetCursor(cursor, "mbr_"); err != nil || got != id {
		t.Fatalf("round trip = %q, %v", got, err)
	}
	padded := cursor + "=="
	cases := map[string]string{
		"no version prefix": cursor[3:],
		"another prefix":    "k2_" + cursor[3:],
		"empty payload":     "k1_",
		"not base64url":     "k1_!!!!",
		"padded":            padded,
		"wrong id kind":     EncodeKeysetCursor("org_EXAMPLE0000000000000001"),
		"id with no body":   EncodeKeysetCursor("mbr_"),
		"id with a slash":   EncodeKeysetCursor("mbr_a/b"),
		"id too long":       EncodeKeysetCursor("mbr_" + strings.Repeat("a", 65)),
		"trailing bits":     "k1_" + base64.RawURLEncoding.EncodeToString([]byte("mbr_EXAMPLE00000000000000007x"))[:38] + "B",
	}
	for name, c := range cases {
		if _, err := DecodeKeysetCursor(c, "mbr_"); err == nil {
			t.Errorf("%s: %q was decoded", name, c)
		}
	}
}

// A principal read holds four members and cannot hold a personal one.
func TestReadAPIPrincipalHasNoPersonalFields(t *testing.T) {
	root := testRoot(t)
	props := schemaNode(t, "principal-read", "properties").(map[string]any)
	var got []string
	for name := range props {
		got = append(got, name)
	}
	sort.Strings(got)
	want := []string{"kind", "principal_id", "revision", "schema_version", "status"}
	if !equalLists(got, want) {
		t.Fatalf("principal-read members = %v, want %v", got, want)
	}
	if schemaNode(t, "principal-read", "additionalProperties") != false {
		t.Fatal("principal-read is not closed")
	}
	base := `{"schema_version":"principal-read.v1","principal_id":"prn_EXAMPLE0000000000000001","kind":"user","status":"active","revision":3`
	for _, member := range []string{"email", "display_name", "name", "password_hash", "password", "identities", "avatar_url", "phone", "is_superuser", "permissions", "role", "last_login_at", "disabled_at", "external_id"} {
		doc := readAPIDecode(t, []byte(base+`,"`+member+`":"x"}`))
		if err := Validate(root, "principal-read.v1", doc); err == nil {
			t.Errorf("principal-read accepted %q", member)
		}
		list := readAPIDecode(t, []byte(`{"schema_version":"principal-list.v1","items":[`+base+`,"`+member+`":"x"}]}`))
		if err := Validate(root, "principal-list.v1", list); err == nil {
			t.Errorf("principal-list accepted %q in an item", member)
		}
	}
}

// A refusal of a grant read never repeats the repository key, and no error
// body can hold one.
func TestReadAPIResourceKeyNeverAppearsInARefusalOrAnErrorBody(t *testing.T) {
	root := testRoot(t)
	const key = "secretowner/secretrepo"
	req, err := ParseReadRequest(root, "grants_get", []byte(`{"schema_version":"grant-get-request.v1","organization_id":"org_EXAMPLE0000000000000001","grant_id":"grt_EXAMPLE0000000000000001"}`))
	if err != nil {
		t.Fatal(err)
	}
	grantBody := func(mutate string) []byte {
		return []byte(`{"schema_version":"grant-read.v1","grant_id":"grt_EXAMPLE0000000000000001","organization_id":"org_EXAMPLE0000000000000001","principal_id":"prn_EXAMPLE0000000000000001","role":"viewer","resource_key":"` + key + `","expires_at":null,"status":"active","revision":1,"created_at":"2026-11-02T10:00:00Z"` + mutate + `}`)
	}
	for name, body := range map[string][]byte{
		"extra member":   grantBody(`,"granted_by":"x"`),
		"bad role":       bytes.Replace(grantBody(""), []byte(`"viewer"`), []byte(`"owner"`), 1),
		"decimal":        bytes.Replace(grantBody(""), []byte(`"revision":1`), []byte(`"revision":1.0`), 1),
		"duplicate key":  grantBody(`,"resource_key":"` + key + `"`),
		"other grant id": bytes.Replace(grantBody(""), []byte(`grt_EXAMPLE0000000000000001`), []byte(`grt_EXAMPLE0000000000000009`), 1),
		"bad key":        bytes.Replace(grantBody(""), []byte(key), []byte("Secret/Repo/"+key), 1),
	} {
		_, err := ParseReadResponse(root, req, body)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if strings.Contains(err.Error(), "secretrepo") || strings.Contains(err.Error(), "secretowner") {
			t.Errorf("%s: the refusal repeats the key: %v", name, err)
		}
	}
	for name, member := range map[string]string{"resource_key": key, "resource": key, "repository": key} {
		body := []byte(`{"schema_version":"error.v1","status":404,"reason_code":"not_found","request_id":"req_AAAAAAAAAAAAAAAAAAAAAAAAAA","occurred_at":"2026-11-02T11:30:00Z","` + name + `":"` + member + `"}`)
		err := ParseReadError(root, body, 404, readVectorNow)
		if err == nil {
			t.Errorf("an error body with %s was accepted", name)
		} else if strings.Contains(err.Error(), "secretrepo") {
			t.Errorf("%s: the refusal repeats the key: %v", name, err)
		}
	}
	for _, name := range []string{"resource_key", "resource", "repositories", "candidates"} {
		if props := schemaNode(t, "error", "properties").(map[string]any); props[name] != nil {
			t.Errorf("error.v1 has a %s member", name)
		}
	}
}

func TestReadScopesAreReadOnlyAndTheSessionIssuerClassNamesThem(t *testing.T) {
	want := []string{ScopeGrantsRead, ScopeMembershipsRead, ScopeOrganizationsRead, ScopePrincipalsRead}
	sort.Strings(want)
	if got := ReadScopes(); !equalLists(got, want) {
		t.Fatalf("ReadScopes = %v, want %v", got, want)
	}
	for _, scope := range ReadScopes() {
		if !strings.HasSuffix(scope, ":read") {
			t.Errorf("scope %q is not a read scope", scope)
		}
	}
	writes := []string{"memberships:write", "sessions:issue", "workers:operate", "entitlements:read", "llm_config:read", "principal_access:read", "workers:read"}
	for _, op := range ReadOperations() {
		if !strings.HasSuffix(op.Scope, ":read") {
			t.Errorf("%s needs %q, which is not a read scope", op.Name, op.Scope)
		}
		if ReadScopeAllows(writes, op.Name) {
			t.Errorf("%s is satisfied by a scope that is not %s", op.Name, op.Scope)
		}
		if ReadScopeAllows(nil, op.Name) || ReadScopeAllows([]string{}, op.Name) {
			t.Errorf("%s is satisfied by no scope", op.Name)
		}
		if !ReadScopeAllows([]string{"sessions:issue", op.Scope}, op.Name) {
			t.Errorf("%s is not satisfied by its own scope", op.Name)
		}
		for _, other := range ReadScopes() {
			if other != op.Scope && ReadScopeAllows([]string{other}, op.Name) {
				t.Errorf("%s is satisfied by %s", op.Name, other)
			}
		}
	}
	if ReadScopeAllows(ReadScopes(), "no_such_operation") {
		t.Error("an unknown operation was allowed")
	}
	var doc struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Auth    struct {
				Scopes []string `json:"scopes_or_actions"`
			} `json:"audience_and_scope"`
		} `json:"classes"`
	}
	raw, err := os.ReadFile(filepath.Join(testRoot(t), "contracts/auth/v1/credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var held []string
	for _, class := range doc.Classes {
		if class.ClassID == "internal_svc_authsess_token" {
			held = class.Auth.Scopes
		}
	}
	sort.Strings(held)
	for _, scope := range ReadScopes() {
		i := sort.SearchStrings(held, scope)
		if i == len(held) || held[i] != scope {
			t.Errorf("credential-classes.json: internal_svc_authsess_token does not name %s (has %v)", scope, held)
		}
	}
}

// The read resources keep the members of the v1 resources they mirror, with the
// id spelling the events use.
func TestReadResourcesMirrorTheV1ResourcesAndTheirStores(t *testing.T) {
	for _, pair := range [][2]string{{"organization-read", "organization"}, {"membership-read", "membership"}} {
		for _, key := range []string{"required", "properties"} {
			read, v1 := schemaNode(t, pair[0], key), schemaNode(t, pair[1], key)
			names := func(node any) []string {
				var out []string
				switch n := node.(type) {
				case map[string]any:
					for k := range n {
						out = append(out, k)
					}
				case []any:
					for _, item := range n {
						out = append(out, fmt.Sprint(item))
					}
				}
				sort.Strings(out)
				return out
			}
			if !equalLists(names(read), names(v1)) {
				t.Errorf("%s %s = %v, %s = %v", pair[0], key, names(read), pair[1], names(v1))
			}
		}
		for _, member := range []string{"status", "role", "slug"} {
			read, v1 := schemaNode(t, pair[0], "properties", member), schemaNode(t, pair[1], "properties", member)
			if v1 == nil {
				continue
			}
			a, _ := json.Marshal(read.(map[string]any)["enum"])
			b, _ := json.Marshal(v1.(map[string]any)["enum"])
			if !bytes.Equal(a, b) {
				t.Errorf("%s %s enum %s, %s %s", pair[0], member, a, pair[1], b)
			}
		}
	}
	var kinds []string
	files, _ := filepath.Glob(filepath.Join(testRoot(t), "internal/storage/postgres/authschema/migrations/0001_principals*.sql"))
	sort.Strings(files)
	re := regexp.MustCompile(`(?s)kind\s+text\s+NOT NULL\s+CHECK \(kind IN \(([^)]*)\)\)`)
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if m := re.FindStringSubmatch(string(raw)); m != nil {
			kinds = nil
			for _, item := range strings.Split(m[1], ",") {
				kinds = append(kinds, strings.Trim(strings.TrimSpace(item), "'"))
			}
		}
	}
	sort.Strings(kinds)
	if !equalLists(stringList(t, schemaNode(t, "principal-read", "properties", "kind", "enum")), kinds) || len(kinds) == 0 {
		t.Errorf("principal-read kind != principals kind CHECK %v", kinds)
	}
}
