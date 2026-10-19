package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type credentialCorpus struct {
	ClockNow string `json:"clock_now"`
	Accept   []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
		Why                    string `json:"why"`
	} `json:"reject"`
	RejectByClient []struct {
		File  string `json:"file"`
		After string `json:"after"`
		Why   string `json:"why"`
	} `json:"reject_by_client"`
}

type credentialSurface struct {
	surface string
	dir     string
	parse   func(root string, raw []byte, now time.Time) error
}

var credentialSurfaces = []credentialSurface{
	{CredentialEntrySurface, "contracts/auth/v1/examples/credential-entry", func(root string, raw []byte, now time.Time) error {
		_, err := ParseCredentialEntry(root, raw, now)
		return err
	}},
	{CredentialListSurface, "contracts/auth/v1/examples/credential-list", func(root string, raw []byte, now time.Time) error {
		_, err := ParseCredentialList(root, raw, now)
		return err
	}},
	{CredentialReceiptSurface, "contracts/auth/v1/examples/credential-receipt", func(root string, raw []byte, now time.Time) error {
		_, err := ParseCredentialReceipt(root, raw, now)
		return err
	}},
}

var goMessageForCredentialKeyword = map[string]string{
	"not": "not:", "maxLength": "maxLength:", "maxItems": "maxItems:", "uniqueItems": "uniqueItems:",
	"const": "const:", "enum": "enum:", "required": "required:", "pattern": "pattern:",
	"additionalProperties": "unexpected additional properties",
}

func (s credentialSurface) corpus(t *testing.T) credentialCorpus {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), s.dir, manifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	var c credentialCorpus
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func (s credentialSurface) read(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), s.dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func clockOf(t *testing.T, c credentialCorpus) time.Time {
	t.Helper()
	if c.ClockNow == "" {
		return time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	}
	now, err := time.Parse(time.RFC3339, c.ClockNow)
	if err != nil {
		t.Fatal(err)
	}
	return now
}

func forEachCredentialSurface(t *testing.T, body func(t *testing.T, s credentialSurface, c credentialCorpus, now time.Time)) {
	for _, s := range credentialSurfaces {
		t.Run(s.surface, func(t *testing.T) {
			c := s.corpus(t)
			body(t, s, c, clockOf(t, c))
		})
	}
}

func TestCredentialCorporaAreNotEmptyAndEveryFixtureIsClaimed(t *testing.T) {
	forEachCredentialSurface(t, func(t *testing.T, s credentialSurface, c credentialCorpus, _ time.Time) {
		if len(c.Accept) == 0 || len(c.Reject) == 0 || len(c.RejectByClient) == 0 {
			t.Fatal("a manifest category is empty")
		}
		claimed := map[string]bool{}
		for _, a := range c.Accept {
			claimed[a.File] = true
		}
		for _, r := range c.Reject {
			claimed[r.File] = true
		}
		for _, r := range c.RejectByClient {
			claimed[r.File] = true
			if r.After != "" && !claimed[r.After] {
				// `after` must name an accept fixture (declared above in file order).
				found := false
				for _, a := range c.Accept {
					found = found || a.File == r.After
				}
				if !found {
					t.Errorf("%s: after %q is not an accept fixture", r.File, r.After)
				}
			}
		}
		entries, err := os.ReadDir(filepath.Join(testRoot(t), s.dir))
		if err != nil {
			t.Fatal(err)
		}
		var unclaimed []string
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".json") || name == manifestFileName {
				continue
			}
			if !claimed[name] {
				unclaimed = append(unclaimed, name)
			}
			delete(claimed, name)
		}
		var missing []string
		for name := range claimed {
			missing = append(missing, name)
		}
		sort.Strings(unclaimed)
		sort.Strings(missing)
		if len(unclaimed) > 0 || len(missing) > 0 {
			t.Fatalf("unclaimed on disk: %v; claimed but absent: %v", unclaimed, missing)
		}
	})
}

func TestAcceptedCredentialFixturesValidateAndParse(t *testing.T) {
	forEachCredentialSurface(t, func(t *testing.T, s credentialSurface, c credentialCorpus, now time.Time) {
		root := testRoot(t)
		for _, entry := range c.Accept {
			t.Run(entry.File, func(t *testing.T) {
				raw := s.read(t, entry.File)
				var document any
				if err := json.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				if err := Validate(root, s.surface, document); err != nil {
					t.Fatalf("schema refused an accept fixture: %v", err)
				}
				if err := s.parse(root, raw, now); err != nil {
					t.Fatalf("client refused an accept fixture: %v", err)
				}
			})
		}
	})
}

func TestRejectedCredentialFixturesFailByTheDeclaredKeyword(t *testing.T) {
	forEachCredentialSurface(t, func(t *testing.T, s credentialSurface, c credentialCorpus, now time.Time) {
		root := testRoot(t)
		for _, entry := range c.Reject {
			t.Run(entry.File, func(t *testing.T) {
				raw := s.read(t, entry.File)
				var document any
				if err := json.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				err := Validate(root, s.surface, document)
				if err == nil {
					t.Fatalf("validated cleanly; expected %q at %q (%s)", entry.ExpectKeyword, entry.ExpectInstanceLocation, entry.Why)
				}
				token, ok := goMessageForCredentialKeyword[entry.ExpectKeyword]
				if !ok {
					t.Fatalf("no Go message token for keyword %q", entry.ExpectKeyword)
				}
				if !strings.Contains(err.Error(), token) {
					t.Fatalf("rejected, but not by %q (token %q absent): %v", entry.ExpectKeyword, token, err)
				}
				if s.parse(root, raw, now) == nil {
					t.Fatal("the client accepted a document the schema refuses")
				}
			})
		}
	})
}

func TestClientEnforcedCredentialFixturesValidateButTheClientRefusesThem(t *testing.T) {
	forEachCredentialSurface(t, func(t *testing.T, s credentialSurface, c credentialCorpus, now time.Time) {
		root := testRoot(t)
		for _, entry := range c.RejectByClient {
			t.Run(entry.File, func(t *testing.T) {
				raw := s.read(t, entry.File)
				var document any
				if err := json.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				if err := Validate(root, s.surface, document); err != nil {
					t.Fatalf("meant to be schema-valid; schema said: %v", err)
				}
				if entry.After == "" {
					if err := s.parse(root, raw, now); err == nil {
						t.Fatalf("the client accepted a document only it can refuse (%s)", entry.Why)
					}
					return
				}
				// A transition: next must parse on its own (the document is fine),
				// and following `after` must be refused.
				prevRaw := s.read(t, entry.After)
				prev, err := ParseCredentialEntry(root, prevRaw, now)
				if err != nil {
					t.Fatalf("the `after` fixture does not parse: %v", err)
				}
				var next CredentialEntry
				if err := json.Unmarshal(raw, &next); err != nil {
					t.Fatal(err)
				}
				if err := CheckCredentialTransition(prev.CredentialBody, next.CredentialBody); err == nil {
					t.Fatalf("the transition %s -> %s was accepted (%s)", entry.After, entry.File, entry.Why)
				}
			})
		}
	})
}

func TestEveryAcceptedEntryTransitionsToItselfAndNarrowingIsAllowed(t *testing.T) {
	// The positive controls for CheckCredentialTransition: a rule that refuses
	// everything would pass every refusal test above.
	s := credentialSurfaces[0]
	c := s.corpus(t)
	now := clockOf(t, c)
	root := testRoot(t)
	for _, a := range c.Accept {
		entry, err := ParseCredentialEntry(root, s.read(t, a.File), now)
		if err != nil {
			t.Fatal(err)
		}
		if err := CheckCredentialTransition(entry.CredentialBody, entry.CredentialBody); err != nil {
			t.Errorf("%s -> itself refused: %v", a.File, err)
		}
	}
	active, _ := ParseCredentialEntry(root, s.read(t, "valid-active.json"), now)
	narrowed := active.CredentialBody
	narrowed.Scopes = []string{"api:read"}
	if err := CheckCredentialTransition(active.CredentialBody, narrowed); err != nil {
		t.Errorf("narrowing refused: %v", err)
	}
	revoked, _ := ParseCredentialEntry(root, s.read(t, "valid-revoked.json"), now)
	rev := active.CredentialBody
	rev.Status, rev.RevokedAt, rev.RevokedReason = "revoked", revoked.RevokedAt, revoked.RevokedReason
	if err := CheckCredentialTransition(active.CredentialBody, rev); err != nil {
		t.Errorf("active -> revoked refused: %v", err)
	}
}

// descendCredentialSchema walks properties.<step>; the step "[]" means the array's items schema.
func descendCredentialSchema(node map[string]any, steps ...string) map[string]any {
	for _, step := range steps {
		if step == "[]" {
			node = node["items"].(map[string]any)
		} else {
			node = node["properties"].(map[string]any)[step].(map[string]any)
		}
	}
	return node
}

func memberNames(t *testing.T, surface string, steps ...string) []string {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), surface))
	if err != nil {
		t.Fatal(err)
	}
	var node map[string]any
	if err := json.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	var names []string
	for name := range descendCredentialSchema(node, steps...)["properties"].(map[string]any) {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func fieldNamesOf(typ reflect.Type) []string {
	var tags []string
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			tags = append(tags, fieldNamesOf(f.Type)...)
			continue
		}
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			tags = append(tags, name)
		}
	}
	sort.Strings(tags)
	return tags
}

func TestCredentialTypesCarryEverySchemaMember(t *testing.T) {
	entryMembers := schemaPropertyNames(t, CredentialEntrySurface)
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want []string
	}{
		{"CredentialEntry", reflect.TypeOf(CredentialEntry{}), entryMembers},
		{"CredentialBody (list item)", reflect.TypeOf(CredentialBody{}), memberNames(t, CredentialListSurface, "items", "[]")},
		{"CredentialList", reflect.TypeOf(CredentialList{}), schemaPropertyNames(t, CredentialListSurface)},
		{"CredentialReceipt", reflect.TypeOf(CredentialReceipt{}), schemaPropertyNames(t, CredentialReceiptSurface)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := fieldNamesOf(tc.typ)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("members = %v, schema = %v", got, tc.want)
			}
		})
	}
}

func TestListItemsEqualTheEntryAndReceiptCredentialIsAStricterSubset(t *testing.T) {
	// There is no cross-file $ref (Go, Python and ajv differ on external refs),
	// so the copies are kept honest by test.
	load := func(surface string, path ...string) map[string]json.RawMessage {
		raw, err := os.ReadFile(SchemaPath(testRoot(t), surface))
		if err != nil {
			t.Fatal(err)
		}
		var node map[string]any
		if err := json.Unmarshal(raw, &node); err != nil {
			t.Fatal(err)
		}
		node = descendCredentialSchema(node, path...)
		out := map[string]json.RawMessage{}
		for name, v := range node["properties"].(map[string]any) {
			b, _ := json.Marshal(v)
			out[name] = b
		}
		return out
	}
	entry := load(CredentialEntrySurface)
	delete(entry, "schema_version")
	items := load(CredentialListSurface, "items", "[]")
	if len(items) != len(entry) {
		t.Fatalf("list items have %d members, entry %d", len(items), len(entry))
	}
	for name, v := range entry {
		if string(items[name]) != string(v) {
			t.Errorf("list item member %q differs from the entry's", name)
		}
	}
	for name, v := range load(CredentialReceiptSurface, "credential") {
		e, ok := entry[name]
		if !ok {
			t.Errorf("receipt credential member %q is not an entry member", name)
			continue
		}
		if name != "status" && string(e) != string(v) {
			t.Errorf("receipt credential member %q differs from the entry's", name)
		}
	}
}

func TestOnlyTheReceiptSchemaCanCarryASecret(t *testing.T) {
	// Walk every property name declared anywhere in a schema: the secret-bearing
	// names must appear in the receipt and nowhere else.
	secretish := map[string]bool{"secret": true, "secret_hash": true, "hash_algorithm": true, "token": true, "password": true, "api_key": true}
	var names func(node any, into map[string]bool)
	names = func(node any, into map[string]bool) {
		switch v := node.(type) {
		case map[string]any:
			if props, ok := v["properties"].(map[string]any); ok {
				for name := range props {
					into[name] = true
				}
			}
			for _, child := range v {
				names(child, into)
			}
		case []any:
			for _, child := range v {
				names(child, into)
			}
		}
	}
	for _, surface := range []string{CredentialEntrySurface, CredentialListSurface, CredentialReceiptSurface} {
		raw, err := os.ReadFile(SchemaPath(testRoot(t), surface))
		if err != nil {
			t.Fatal(err)
		}
		var node any
		_ = json.Unmarshal(raw, &node)
		declared := map[string]bool{}
		names(node, declared)
		for name := range declared {
			if secretish[name] && surface != CredentialReceiptSurface {
				t.Errorf("%s declares a secret-bearing member %q", surface, name)
			}
		}
		if surface == CredentialReceiptSurface && !declared["secret"] {
			t.Error("the receipt schema no longer declares `secret`: the one place it may exist")
		}
	}
}

func TestCredentialClassEnumEqualsPrincipalV1sEnum(t *testing.T) {
	enumOf := func(surface string, path ...string) []string {
		raw, err := os.ReadFile(SchemaPath(testRoot(t), surface))
		if err != nil {
			t.Fatal(err)
		}
		var node map[string]any
		_ = json.Unmarshal(raw, &node)
		for _, step := range path {
			node = node["properties"].(map[string]any)[step].(map[string]any)
		}
		var out []string
		for _, v := range node["enum"].([]any) {
			out = append(out, v.(string))
		}
		sort.Strings(out)
		return out
	}
	principal := enumOf(PrincipalSurface, "credential", "class")
	if len(principal) == 0 {
		t.Fatal("principal.v1 declares no credential classes; the comparison would be vacuous")
	}
	for _, surface := range []struct {
		name string
		got  []string
	}{
		{CredentialEntrySurface, enumOf(CredentialEntrySurface, "credential_class")},
		{CredentialReceiptSurface, enumOf(CredentialReceiptSurface, "credential", "credential_class")},
	} {
		if strings.Join(surface.got, ",") != strings.Join(principal, ",") {
			t.Errorf("%s credential_class enum differs from principal.v1's", surface.name)
		}
	}
}

func TestEverySecretInTheReceiptCorpusIsMarkedSynthetic(t *testing.T) {
	// The fixture secret scanner (prefix, pattern, entropy) covers every file;
	// this adds the positive marker for the one member that exists to hold a
	// secret: each receipt fixture's secret carries the EXAMPLE filler, so a
	// value pasted from a real issuance cannot pass for a fixture (G-73).
	s := credentialSurfaces[2]
	checked := 0
	for _, entry := range s.corpus(t).Accept {
		var document struct {
			Secret string `json:"secret"`
		}
		if err := json.Unmarshal(s.read(t, entry.File), &document); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(document.Secret, "EXAMPLE") {
			t.Errorf("%s: secret %q lacks the synthetic EXAMPLE marker", entry.File, document.Secret)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no receipt fixtures checked")
	}
}

// prefixShapes are display_prefix values the ENTRY schema admits: every accepted
// fixture's, the empty prefix, the 16-character maximum, and the alphabet's
// corners. The receipt must accept a secret that begins with each of them.
func prefixShapes(t *testing.T) []string {
	t.Helper()
	shapes := []string{"", "a", strings.Repeat("a", 16), "Ab_9Ab_9Ab_9Ab_9", "_", strings.Repeat("_", 16), "ABCDEFGH_ijklm_9", "exmpl_abcd"}
	s := credentialSurfaces[0]
	for _, a := range s.corpus(t).Accept {
		var entry struct {
			DisplayPrefix string `json:"display_prefix"`
		}
		if err := json.Unmarshal(s.read(t, a.File), &entry); err != nil {
			t.Fatal(err)
		}
		shapes = append(shapes, entry.DisplayPrefix)
	}
	return shapes
}

func TestEveryDisplayPrefixTheEntryAllowsCanBeginAValidReceiptSecret(t *testing.T) {
	// The contract's allowed states must be closed under issue and rotate:
	// an entry that validates describes a credential whose receipt can
	// validate. Round 1 found a 10-character prefix the receipt's secret
	// pattern could not begin with.
	r := credentialSurfaces[2]
	raw := r.read(t, "valid-issue.json")
	root := testRoot(t)
	now := clockOf(t, r.corpus(t))
	for _, prefix := range prefixShapes(t) {
		var document map[string]any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		document["credential"].(map[string]any)["display_prefix"] = prefix
		document["secret"] = prefix + strings.Repeat("EXAMPLE", 6)
		if err := Validate(root, CredentialReceiptSurface, document); err != nil {
			t.Errorf("a secret beginning with the allowed prefix %q is refused by the schema: %v", prefix, err)
			continue
		}
		encoded, _ := json.Marshal(document)
		if _, err := ParseCredentialReceipt(root, encoded, now); err != nil {
			t.Errorf("a secret beginning with the allowed prefix %q is refused by the client: %v", prefix, err)
		}
	}
}

func TestAListLargerThanAnyBoundIsRepresentable(t *testing.T) {
	// With no pagination, a count bound would make a big registry unrepresentable
	// with no truncation signal (round 2 executed a 201-item list against the old
	// maxItems 200).
	s := credentialSurfaces[1]
	var base struct {
		SchemaVersion string           `json:"schema_version"`
		Items         []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(s.read(t, "valid-two-items.json"), &base); err != nil {
		t.Fatal(err)
	}
	var items []map[string]any
	for i := 0; i < 1000; i++ {
		item := map[string]any{}
		for k, v := range base.Items[0] {
			item[k] = v
		}
		item["id"] = fmt.Sprintf("crd_EXAMPLE%020d", i)
		items = append(items, item)
	}
	document := map[string]any{"schema_version": CredentialListSurface, "items": items}
	if err := Validate(testRoot(t), CredentialListSurface, document); err != nil {
		t.Fatalf("a 1000-item list was refused: %v", err)
	}
	encoded, _ := json.Marshal(document)
	if _, err := ParseCredentialList(testRoot(t), encoded, clockOf(t, s.corpus(t))); err != nil {
		t.Fatalf("the client refused a 1000-item list: %v", err)
	}
}

func rotationPair(t *testing.T) (CredentialBody, CredentialBody) {
	t.Helper()
	root := testRoot(t)
	var receiptSurface, entrySurface credentialSurface
	for _, s := range credentialSurfaces {
		switch s.surface {
		case CredentialReceiptSurface:
			receiptSurface = s
		case CredentialEntrySurface:
			entrySurface = s
		}
	}
	rc, ec := receiptSurface.corpus(t), entrySurface.corpus(t)
	receipt, err := ParseCredentialReceipt(root, receiptSurface.read(t, "valid-rotate.json"), clockOf(t, rc))
	if err != nil {
		t.Fatal(err)
	}
	active, err := ParseCredentialEntry(root, entrySurface.read(t, "valid-active.json"), clockOf(t, ec))
	if err != nil {
		t.Fatal(err)
	}
	parent := active.CredentialBody
	parent.ID = receipt.Credential.RotationParentID
	return parent, receipt.Credential
}

func TestARotationThatPreservesOrNarrowsItsParentIsAccepted(t *testing.T) {
	parent, child := rotationPair(t)
	if err := CheckCredentialRotation(parent, child); err != nil {
		t.Fatalf("a preserving rotation was refused: %v", err)
	}
	child.Scopes = []string{"api:read"}
	if err := CheckCredentialRotation(parent, child); err != nil {
		t.Fatalf("a narrowing rotation was refused: %v", err)
	}
}

func TestARotationThatWidensOrChangesIdentityIsRefused(t *testing.T) {
	revoked := func(p *CredentialBody, _ *CredentialBody) {
		now := Timestamp{Time: time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)}
		p.Status, p.RevokedAt, p.RevokedReason = "revoked", &now, "compromised"
	}
	cases := map[string]func(p, c *CredentialBody){
		"widened scopes":    func(_, c *CredentialBody) { c.Scopes = []string{"api:read", "api:write", "api:admin"} },
		"widened audiences": func(_, c *CredentialBody) { c.Audiences = []string{"dev-health-api", "another-audience"} },
		"other principal":   func(_, c *CredentialBody) { c.PrincipalID = "prn_EXAMPLE0000000000000002" },
		"other class":       func(_, c *CredentialBody) { c.CredentialClass = "api_key" },
		"other owner":       func(_, c *CredentialBody) { c.DomainOwner = "other-owner" },
		"other parent id":   func(_, c *CredentialBody) { c.RotationParentID = "crd_EXAMPLE0000000000000009" },
		"no parent id":      func(_, c *CredentialBody) { c.RotationParentID = "" },
		"revoked parent":    revoked,
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			parent, child := rotationPair(t)
			mutate(&parent, &child)
			if err := CheckCredentialRotation(parent, child); err == nil {
				t.Fatal("the rotation was accepted")
			}
		})
	}
}
