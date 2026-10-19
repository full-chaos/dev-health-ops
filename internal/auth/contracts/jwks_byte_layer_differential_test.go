package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
)

// THE BYTE LAYER OF THE JWKS DIFFERENTIAL.
//
// The structural generator in jwks_document_differential_test.go builds a Go
// map and serialises it with json.Marshal, so the serialisation itself is a
// frame it never varies: duplicate members, a BOM, whitespace around the
// document, member order, escapes and trailing content cannot be produced. This
// file generates RAW TEXT. Each case is a byte string written to a file and
// read by the real consumer (authverify Keys()); the schema path is
// json.Unmarshal followed by Validate, the rule the sweep's byte rows use.
//
// Every case belongs to one class. A class row is a pair of literals written by
// a human: what the consumer does with every member of the class and what the
// schema path does. Nothing is computed from the code under test. A class that
// produced no case, a case whose class is not in the table, and a count that
// differs from its literal all fail.

const (
	accept = "accept"
	refuse = "refuse"
)

type byteCase struct {
	class string
	raw   []byte
}

type byteClass struct {
	consumer string
	schema   string
	count    int
	why      string
}

const (
	bk  = `{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"k","x":"ExamplePublicKeyForContractFixturesOnlyAAAA"}`
	bx  = "ExamplePublicKeyForContractFixturesOnlyAAAA"
	bdc = `{"keys":[` + bk + `]}`
)

var keyMembers = []struct{ name, value, bad string }{
	{"kty", `"OKP"`, `"RSA"`},
	{"crv", `"Ed25519"`, `"X25519"`},
	{"alg", `"EdDSA"`, `"HS256"`},
	{"kid", `"k"`, `""`},
	{"x", `"` + bx + `"`, `"A"`},
}

// useMember is the optional key member. It is not in the base document, so the
// duplicate generator adds it.
var useMember = struct{ name, value, bad string }{"use", `"sig"`, `"enc"`}

func keyWith(members []string) string { return "{" + strings.Join(members, ",") + "}" }

func member(name, value string) string { return `"` + name + `":` + value }

func docOf(key string) string { return `{"keys":[` + key + `]}` }

func baseMembers() []string {
	out := make([]string, 0, len(keyMembers))
	for _, m := range keyMembers {
		out = append(out, member(m.name, m.value))
	}
	return out
}

func permutations(items []string) [][]string {
	if len(items) <= 1 {
		return [][]string{append([]string(nil), items...)}
	}
	var out [][]string
	for i := range items {
		rest := append(append([]string(nil), items[:i]...), items[i+1:]...)
		for _, p := range permutations(rest) {
			out = append(out, append([]string{items[i]}, p...))
		}
	}
	return out
}

// byteClasses is the human-owned table. count is the exact number of cases the
// generator must emit for the class.
var byteClasses = map[string]byteClass{
	"ws-json-around":         {accept, accept, 24, "JSON whitespace (SP TAB LF CR), runs of 1 and 3, at the start, the end, both"},
	"ws-nonjson-around":      {refuse, refuse, 21, "VT FF NBSP NEL LS U+3000 NUL around the document are not JSON whitespace"},
	"bom-or-wide-prefix":     {refuse, refuse, 5, "UTF-8, UTF-16 and UTF-32 byte-order marks before the document"},
	"trailing-content":       {refuse, refuse, 36, "any second value or stray token after the document, with 0, 1 or a newline between"},
	"leading-content":        {refuse, refuse, 4, "a value before the document"},
	"member-order":           {accept, accept, 120, "all 120 orders of the five required key members"},
	"indentation":            {accept, accept, 4, "compact, two-space, tab and one-member-per-line layouts"},
	"token-ws-json":          {accept, accept, 81, "one JSON whitespace character before each of the 27 tokens (11 strings, 6 braces and brackets, 6 colons, 4 commas), 3 characters"},
	"token-ws-nonjson":       {refuse, refuse, 27, "one vertical tab before each of the 27 tokens"},
	"dup-key-same-value":     {refuse, accept, 6, "a key member repeated with an equal value: parsers keep the last, the consumer refuses the bytes"},
	"dup-key-first-bad":      {refuse, accept, 6, "a key member repeated, first value invalid and last valid: the schema path sees only the last"},
	"dup-key-last-bad":       {refuse, refuse, 6, "a key member repeated, last value invalid: both refuse"},
	"dup-key-fold":           {refuse, refuse, 12, "kid and KID style pairs: the consumer folds names and refuses; the schema sees an undeclared member"},
	"dup-key-escaped":        {refuse, accept, 6, "a member and its \\u-escaped spelling: equal after decoding, a duplicate"},
	"dup-top-same":           {refuse, accept, 1, "keys repeated with equal arrays"},
	"dup-top-first-empty":    {refuse, accept, 1, "keys: [] then a valid array"},
	"dup-top-last-empty":     {refuse, refuse, 1, "keys: valid then []"},
	"dup-in-second-key":      {refuse, accept, 1, "a repeat inside the second of two keys"},
	"escaped-name-one":       {accept, accept, 17, "one character of a member name written as a \\u escape"},
	"escaped-name-all":       {accept, accept, 6, "a whole member name written as \\u escapes"},
	"escaped-value":          {accept, accept, 5, "a literal value written with \\u escapes"},
	"escaped-solidus":        {accept, accept, 1, "kid written with an escaped solidus"},
	"escaped-astral":         {accept, refuse, 1, "kid holding a surrogate-pair escape"},
	"escaped-lone-surrogate": {accept, refuse, 2, "kid holding an unpaired surrogate escape: Go substitutes U+FFFD"},
	"escaped-nul":            {accept, refuse, 1, "kid holding \\u0000"},
	"raw-invalid-utf8-kid":   {accept, refuse, 3, "invalid UTF-8 bytes inside kid: Go substitutes U+FFFD"},
	"raw-invalid-utf8-x":     {refuse, refuse, 1, "invalid UTF-8 inside x"},
	"raw-control-in-string":  {refuse, refuse, 3, "a raw control character inside a JSON string"},
	"bad-escape":             {refuse, refuse, 3, "an invalid or truncated escape sequence"},
	"comment":                {refuse, refuse, 4, "block and line comments"},
	"trailing-comma":         {refuse, refuse, 2, "a trailing comma in the object and in the array"},
	"non-json-literals":      {refuse, refuse, 7, "single quotes, unquoted names, NaN, Infinity, leading zero, bare words"},
	"wrong-type-values":      {refuse, refuse, 6, "numbers, booleans, arrays and objects where a string is required"},
	"empty-name":             {refuse, refuse, 1, "an empty member name"},
}

var wsRuns = []string{" ", "\t", "\n", "\r"}

func around(prefix, suffix string) []byte { return []byte(prefix + bdc + suffix) }

func generateByteCases() []byteCase {
	var cs []byteCase
	add := func(class string, raw string) { cs = append(cs, byteCase{class, []byte(raw)}) }
	addB := func(class string, raw []byte) { cs = append(cs, byteCase{class, raw}) }

	// D1: bytes around the document.
	for _, ws := range wsRuns {
		for _, n := range []int{1, 3} {
			run := strings.Repeat(ws, n)
			addB("ws-json-around", around(run, ""))
			addB("ws-json-around", around("", run))
			addB("ws-json-around", around(run, run))
		}
	}
	for _, ws := range []string{"\v", "\f", " ", "\u0085", " ", "　", "\x00"} {
		addB("ws-nonjson-around", around(ws, ""))
		addB("ws-nonjson-around", around("", ws))
		addB("ws-nonjson-around", around(ws, ws))
	}
	for _, bom := range []string{"\xef\xbb\xbf", "\xff\xfe", "\xfe\xff", "\xff\xfe\x00\x00", "\x00\x00\xfe\xff"} {
		addB("bom-or-wide-prefix", around(bom, ""))
	}

	// D2: trailing and leading content.
	for _, sep := range []string{"", " ", "\n"} {
		for _, tail := range []string{`null`, `0`, `"s"`, `{}`, `[]`, `true`, bdc, `{"keys"`, `}`, `,`, `]`, `x`} {
			add("trailing-content", bdc+sep+tail)
		}
	}
	for _, head := range []string{`null `, `0 `, `"s" `, `{} `} {
		add("leading-content", head+bdc)
	}

	// D3: member order and layout.
	for _, order := range permutations(baseMembers()) {
		add("member-order", docOf(keyWith(order)))
	}
	members := baseMembers()
	add("indentation", docOf(keyWith(members)))
	add("indentation", "{\n  \"keys\": [\n    {\n      "+strings.Join(members, ",\n      ")+"\n    }\n  ]\n}\n")
	add("indentation", "{\n\t\"keys\": [\n\t\t{\n\t\t\t"+strings.Join(members, ",\n\t\t\t")+"\n\t\t}\n\t]\n}\n")
	add("indentation", "{ \"keys\" : [ { "+strings.Join(members, " , ")+" } ] }")
	for _, tok := range tokenStarts(bdc) {
		for _, ws := range []string{" ", "\t", "\n"} {
			add("token-ws-json", bdc[:tok]+ws+bdc[tok:])
		}
		add("token-ws-nonjson", bdc[:tok]+"\v"+bdc[tok:])
	}

	// D4: duplicate members.
	for _, m := range append(append([]struct{ name, value, bad string }(nil), keyMembers...), useMember) {
		dup := func(first, last string) string {
			out := make([]string, 0, len(keyMembers)+1)
			for _, mm := range keyMembers {
				if mm.name == m.name {
					out = append(out, member(m.name, first))
					out = append(out, member(m.name, last))
				} else {
					out = append(out, member(mm.name, mm.value))
				}
			}
			if m.name == useMember.name {
				out = append(out, member(m.name, first), member(m.name, last))
			}
			return docOf(keyWith(out))
		}
		add("dup-key-same-value", dup(m.value, m.value))
		add("dup-key-first-bad", dup(m.bad, m.value))
		add("dup-key-last-bad", dup(m.value, m.bad))
		upper := strings.ToUpper(m.name)
		with := func(firstName, secondName string) string {
			out := make([]string, 0, len(keyMembers)+1)
			for _, mm := range keyMembers {
				if mm.name == m.name {
					out = append(out, member(firstName, m.value), member(secondName, m.value))
				} else {
					out = append(out, member(mm.name, mm.value))
				}
			}
			if m.name == useMember.name {
				out = append(out, member(firstName, m.value), member(secondName, m.value))
			}
			return docOf(keyWith(out))
		}
		add("dup-key-fold", with(m.name, upper))
		add("dup-key-fold", with(upper, m.name))
		add("dup-key-escaped", with(m.name, `\u`+fmt.Sprintf("%04x", m.name[0])+m.name[1:]))
	}
	add("dup-top-same", `{"keys":[`+bk+`],"keys":[`+bk+`]}`)
	add("dup-top-first-empty", `{"keys":[],"keys":[`+bk+`]}`)
	add("dup-top-last-empty", `{"keys":[`+bk+`],"keys":[]}`)
	second := strings.Replace(bk, `"kid":"k"`, `"kid":"k2"`, 1)
	add("dup-in-second-key", `{"keys":[`+bk+`,`+strings.Replace(second, `"kid":"k2"`, `"kid":"k2","kid":"k2"`, 1)+`]}`)

	// D5: spelling.
	for _, name := range []string{"keys", "kty", "crv", "alg", "kid", "x"} {
		for i := 0; i < len(name); i++ {
			escaped := name[:i] + fmt.Sprintf(`\u%04x`, name[i]) + name[i+1:]
			add("escaped-name-one", strings.Replace(bdc, `"`+name+`"`, `"`+escaped+`"`, 1))
		}
		var all strings.Builder
		for i := 0; i < len(name); i++ {
			all.WriteString(fmt.Sprintf(`\u%04x`, name[i]))
		}
		add("escaped-name-all", strings.Replace(bdc, `"`+name+`"`, `"`+all.String()+`"`, 1))
	}
	for _, v := range []struct{ plain, esc string }{
		{`"OKP"`, `"\u004fKP"`}, {`"Ed25519"`, `"\u0045d25519"`}, {`"EdDSA"`, `"Ed\u0044SA"`},
		{`"k"`, `"\u006b"`}, {`"` + bx + `"`, `"\u0045` + bx[1:] + `"`},
	} {
		add("escaped-value", strings.Replace(bdc, v.plain, v.esc, 1))
	}
	kidWith := func(v string) string { return strings.Replace(bdc, `"kid":"k"`, `"kid":`+v, 1) }
	add("escaped-solidus", kidWith(`"a\/b"`))
	add("escaped-astral", kidWith(`"\ud83d\ude00"`))
	add("escaped-lone-surrogate", kidWith(`"a\ud800b"`))
	add("escaped-lone-surrogate", kidWith(`"a\udc00b"`))
	add("escaped-nul", kidWith(`"a\u0000b"`))
	addB("raw-invalid-utf8-kid", []byte(strings.Replace(bdc, `"kid":"k"`, "\"kid\":\"a\xffb\"", 1)))
	addB("raw-invalid-utf8-kid", []byte(strings.Replace(bdc, `"kid":"k"`, "\"kid\":\"a\xc0\x80b\"", 1)))
	addB("raw-invalid-utf8-kid", []byte(strings.Replace(bdc, `"kid":"k"`, "\"kid\":\"a\xe2\x82b\"", 1)))
	addB("raw-invalid-utf8-x", []byte(strings.Replace(bdc, bx[:3], "\xff\xfe\xfd", 1)))
	for _, ctl := range []string{"\x01", "\n", "\t"} {
		add("raw-control-in-string", kidWith(`"a`+ctl+`b"`))
	}
	for _, esc := range []string{`"a\qb"`, `"a\u12"`, `"a\u12zzb"`} {
		add("bad-escape", kidWith(esc))
	}

	// D6: syntax slack.
	add("comment", strings.Replace(bdc, `"keys"`, `/* c */"keys"`, 1))
	add("comment", strings.Replace(bdc, `[`, `[/* c */`, 1))
	add("comment", bdc+" // c")
	add("comment", "// c\n"+bdc)
	add("trailing-comma", strings.Replace(bdc, `}]}`, `},]}`, 1))
	add("trailing-comma", strings.Replace(bdc, `"x":"`+bx+`"}`, `"x":"`+bx+`",}`, 1))
	add("non-json-literals", strings.ReplaceAll(bdc, `"`, `'`))
	add("non-json-literals", `{keys:[`+bk+`]}`)
	add("non-json-literals", kidWith(`NaN`))
	add("non-json-literals", kidWith(`Infinity`))
	add("non-json-literals", kidWith(`01`))
	add("non-json-literals", kidWith(`k`))
	add("non-json-literals", `{"keys":[`+bk+`]`)
	for _, v := range []string{`1`, `1e999`, `true`, `[]`, `{}`, `["k"]`} {
		add("wrong-type-values", kidWith(v))
	}
	add("empty-name", strings.Replace(bdc, `"kid":"k"`, `"":1,"kid":"k"`, 1))
	return cs
}

// tokenStarts returns the byte offset of each of the document's tokens.
func tokenStarts(doc string) []int {
	var out []int
	inString := false
	for i := 0; i < len(doc); i++ {
		c := doc[i]
		if inString {
			if c == '\\' {
				i++
			} else if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			out = append(out, i)
			inString = true
		case '{', '}', '[', ']', ':', ',':
			out = append(out, i)
		}
	}
	return out
}

func TestTheByteLayerClassesAgreeWithTheRealConsumerAndTheSchemaPath(t *testing.T) {
	cases := generateByteCases()
	root := testRoot(t)
	path := filepath.Join(t.TempDir(), "jwks.json")

	const wantTotal = 425
	seen := map[string]int{}
	var ended, unclassed int
	for _, c := range cases {
		class, ok := byteClasses[c.class]
		if !ok {
			unclassed++
			t.Errorf("case in class %q, which is not in the table: %q", c.class, c.raw)
			continue
		}
		seen[c.class]++
		if err := os.WriteFile(path, c.raw, 0o600); err != nil {
			t.Fatal(err)
		}
		consumer := accept
		if _, err := authverify.NewEd25519JWKSVerifier(path).Keys(); err != nil {
			consumer = refuse
		}
		var decoded any
		schema := refuse
		if json.Unmarshal(c.raw, &decoded) == nil && Validate(root, JWKSSurface, decoded) == nil {
			schema = accept
		}
		ended++
		if consumer != class.consumer {
			t.Errorf("%s: the real consumer %ss %q, the class says %s", c.class, consumer, c.raw, class.consumer)
		}
		if schema != class.schema {
			t.Errorf("%s: the schema path %ss %q, the class says %s", c.class, schema, c.raw, class.schema)
		}
	}
	t.Logf("byte-layer cases: %d in %d classes", len(cases), len(seen))
	if len(cases) != wantTotal || ended != wantTotal || unclassed != 0 {
		t.Errorf("cases=%d ended=%d unclassed=%d, want %d cases all ended", len(cases), ended, unclassed, wantTotal)
	}
	for name, class := range byteClasses {
		if class.count <= 0 {
			t.Errorf("class %s has a zero count in the table: a class that may emit nothing guards nothing", name)
		}
	}
	var names []string
	for name := range byteClasses {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if seen[name] != byteClasses[name].count {
			t.Errorf("class %s emitted %d cases, the table says %d (%s)", name, seen[name], byteClasses[name].count, byteClasses[name].why)
		}
	}
}

// The manifest's two known-gap notes are prose about what the consumer does.
// Their claims are checked against the executed behaviour above: a note that
// says a reader does not refuse duplicates or trailing content while the real
// consumer refuses both is stale.
func TestTheManifestGapNotesAgreeWithTheExecutedConsumer(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, jwksFixtureManifestPath))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	refusesBytes := func(doc string) bool {
		if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := authverify.NewEd25519JWKSVerifier(path).Keys()
		return err != nil
	}
	dup := strings.Replace(bdc, `"kid":"k"`, `"kid":"k","kid":"k"`, 1)
	for key, behaviour := range map[string]struct {
		refused bool
		stale   []string
		mustSay string
	}{
		"known_gap_duplicate_json_members": {refusesBytes(dup), []string{"is NOT refused by any current reader"}, "[C2]"},
		"known_gap_trailing_content":       {refusesBytes(bdc + " null"), []string{"consumer silently ignores it", "are BOTH accepted"}, "agreement"},
	} {
		note, _ := manifest[key].(string)
		if note == "" {
			t.Errorf("manifest has no %s", key)
			continue
		}
		if !behaviour.refused {
			t.Errorf("%s: the consumer no longer refuses these bytes; the byte-layer classes must change", key)
			continue
		}
		if !strings.Contains(note, behaviour.mustSay) {
			t.Errorf("%s does not say %q", key, behaviour.mustSay)
		}
		for _, phrase := range behaviour.stale {
			if strings.Contains(note, phrase) {
				t.Errorf("%s still says %q, but the executed consumer refuses these bytes", key, phrase)
			}
		}
	}
}

// An escape class must actually carry escapes. Each case has to differ from the
// plain document, contain a backslash, and (where the escape only respells the
// same characters) decode to the very same document as the plain one.
func TestEveryEscapeClassCarriesARealEscape(t *testing.T) {
	var plain any
	if err := json.Unmarshal([]byte(bdc), &plain); err != nil {
		t.Fatal(err)
	}
	sameDecoding := map[string]bool{"escaped-name-one": true, "escaped-name-all": true, "escaped-value": true}
	perClass := map[string]int{}
	for _, c := range generateByteCases() {
		if !strings.HasPrefix(c.class, "escaped-") {
			continue
		}
		perClass[c.class]++
		raw := string(c.raw)
		if raw == bdc {
			t.Errorf("%s: a case equal to the plain document", c.class)
		}
		if !strings.Contains(raw, "\\") {
			t.Errorf("%s: no backslash in %q", c.class, raw)
		}
		if sameDecoding[c.class] {
			var got any
			if err := json.Unmarshal(c.raw, &got); err != nil || fmt.Sprint(got) != fmt.Sprint(plain) {
				t.Errorf("%s: %q does not decode to the plain document (err=%v)", c.class, raw, err)
			}
		}
	}
	if len(perClass) == 0 {
		t.Fatal("no escape class was generated")
	}
}

// The schema's own description must not call the byte layer unreachable now
// that the corpus reaches it.
func TestTheSchemaDescriptionDoesNotCallTheByteLayerUnreachable(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(testRoot(t), "contracts/auth/v1/jsonschema/jwks.v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(schema.Description, "not reachable from this corpus") {
		t.Error("the schema description still says the byte layer is not reachable from this corpus")
	}
}
