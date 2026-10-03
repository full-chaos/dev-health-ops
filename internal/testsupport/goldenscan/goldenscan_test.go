package goldenscan

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func pack(t *testing.T, text string) string {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return packedPrefix + base64.StdEncoding.EncodeToString(buffer.Bytes())
}

func golden(t *testing.T, leaves map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"header": map[string]any{"python_build": "x"}, "requests": []any{leaves}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// derivedUUID is a UUID-shaped value made from a name at run time, so no credential-shaped literal is in the source.
func derivedUUID(name string) string {
	sum := sha256.Sum256([]byte(name))
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

const (
	someUUID = "3f2b8c1e-5a47-4d9e-9b6a-0c1d2e3f4a5b"
	someHex  = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
)

// generatedValue is a high-entropy value made at run time from a fixed-seed generator (36 characters of letters and digits): no
// literal fragment of it is in the source, and it is the same on every run.
func generatedValue(seed int64) string {
	r := rand.New(rand.NewSource(seed))
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]byte, 36)
	for i := range out {
		out[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(out)
}

func randomValue() string { return generatedValue(7890) }

func TestLeavesWalkPackedBodiesAndJSONTextsWithTheirKey(t *testing.T) {
	inner, _ := json.Marshal(map[string]any{"api_key": "innerValue1234"})
	leaves, err := Leaves(golden(t, map[string]any{
		"plain":  "plain text",
		"packed": pack(t, "packed plain text"),
		"asjson": string(inner),
		"nested": pack(t, string(inner)),
		"list":   []any{"first", "second"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, leaf := range leaves {
		got[leaf.Key+"="+leaf.Value] = true
	}
	for _, want := range []string{"plain=plain text", "packed=packed plain text", "api_key=innerValue1234", "list=first", "list=second", "python_build=x"} {
		if !got[want] {
			t.Errorf("leaf %q was not found: %v", want, got)
		}
	}
	if _, err := Leaves(golden(t, map[string]any{"bad": packedPrefix + "not base64!"})); err == nil {
		t.Error("a packed body that does not unpack was scanned as if it were empty")
	}
}

func TestTheRuleFindsAKeyedHighEntropyValueInEveryPlace(t *testing.T) {
	value := randomValue()
	for name, leaves := range map[string]map[string]any{
		"under its key":        {"api_key": value},
		"inside a packed body": {"body": pack(t, `{"client_secret": "`+value+`"}`)},
		"inside a JSON text":   {"body": `{"auth_token": "` + value + `"}`},
		"name=value in text":   {"rows": "id | name\n1 | something\ndb_password=" + value + " end"},
		"typed leaf text":      {"v": "API:" + value},
	} {
		leaves2, err := Leaves(golden(t, leaves))
		if err != nil {
			t.Fatal(err)
		}
		if hits := Hits(leaves2); len(hits) != 1 {
			t.Errorf("%s: %d hit(s), want 1", name, len(hits))
		}
	}
}

func TestTheRuleLeavesWhatTheScannerLeaves(t *testing.T) {
	value := randomValue()
	for name, leaves := range map[string]map[string]any{
		"a high-entropy value under a plain key": {"name": value},
		"a short value":                          {"api_key": "abc123"},
		"a stopword inside":                      {"api_key": "ab12" + "password" + value[:10]},
		"low entropy":                            {"api_key": strings.Repeat("a1", 20)},
		"an allowlisted key":                     {"key_alias": value},
	} {
		leaves2, err := Leaves(golden(t, leaves))
		if err != nil {
			t.Fatal(err)
		}
		if hits := Hits(leaves2); len(hits) != 0 {
			t.Errorf("%s: %d hit(s), want 0", name, len(hits))
		}
	}
}

func TestShapes(t *testing.T) {
	if Shape(someUUID) != "uuid" || Shape(strings.ToUpper(someUUID)) != "uuid" || Shape(someHex) != "hex64" || Shape(someHex[:32]) != "hex32" || Shape(someHex[:63]) != "other" || Shape(someUUID+"x") != "other" {
		t.Error("a shape is wrong")
	}
}

func rowsFor(path, key, shape string, count int) []Row {
	return []Row{{Path: path, Key: key, Shape: shape, Count: count, Triage: "row 1"}}
}

func TestACheckPassesOnlyThroughAnExactRow(t *testing.T) {
	g := golden(t, map[string]any{"client_secret": someUUID, "other": map[string]any{"client_secret": derivedUUID("another")}})
	problems, err := Check("a/g.json", g, rowsFor("a/g.json", "client_secret", "uuid", 2))
	if err != nil || len(problems) != 0 {
		t.Fatalf("an exact row did not pass: %v %v", problems, err)
	}
	for name, rows := range map[string][]Row{
		"no row":            nil,
		"another file":      rowsFor("b/g.json", "client_secret", "uuid", 2),
		"another key":       rowsFor("a/g.json", "client_secret_ref", "uuid", 2),
		"count too high":    rowsFor("a/g.json", "client_secret", "uuid", 3),
		"count too low":     rowsFor("a/g.json", "client_secret", "uuid", 1),
		"another shape row": rowsFor("a/g.json", "client_secret", "hex64", 2),
	} {
		problems, err := Check("a/g.json", g, rows)
		if err != nil || len(problems) == 0 {
			t.Errorf("%s: the check passed (%v %v)", name, problems, err)
		}
		for _, problem := range problems {
			if strings.Contains(problem, someUUID) {
				t.Errorf("%s: a problem shows a value: %s", name, problem)
			}
		}
	}
}

func TestAValueOfAnotherShapeUnderARowKeyIsRefused(t *testing.T) {
	g := golden(t, map[string]any{"client_secret": someUUID, "nested": []any{map[string]any{"client_secret": randomValue()}}})
	problems, _ := Check("a/g.json", g, rowsFor("a/g.json", "client_secret", "uuid", 2))
	if len(problems) == 0 {
		t.Fatal("a random value under an allowlisted key passed beside a UUID")
	}
}

func TestACheckReadsInsideAPackedBody(t *testing.T) {
	g := golden(t, map[string]any{"body": pack(t, `{"client_secret": "`+someUUID+`", "x": {"api_key": "`+randomValue()+`"}}`)})
	problems, _ := Check("a/g.json", g, rowsFor("a/g.json", "client_secret", "uuid", 1))
	if len(problems) != 1 || !strings.Contains(problems[0], `"api_key"`) {
		t.Fatalf("a credential inside a packed body was not found or not named: %v", problems)
	}
}

func TestTheAllowlistRefusesWhatFormOneForbids(t *testing.T) {
	ok := "a/g.json\tcredential_id\tuuid\t2\trow 1 of the triage"
	if rows, err := ParseAllowlist("# c\n" + ok + "\n"); err != nil || len(rows) != 1 {
		t.Fatalf("a good row was refused: %v", err)
	}
	for name, text := range map[string]string{
		"wildcard path":    "a/*.json\tcredential_id\tuuid\t2\trow 1",
		"wildcard key":     "a/g.json\t*\tuuid\t2\trow 1",
		"other shape":      "a/g.json\tcredential_id\tother\t2\trow 1",
		"zero count":       "a/g.json\tcredential_id\tuuid\t0\trow 1",
		"no triage":        "a/g.json\tcredential_id\tuuid\t2\t ",
		"four fields":      "a/g.json\tcredential_id\tuuid\t2",
		"duplicate":        ok + "\n" + ok,
		"unsorted":         "b/g.json\tcredential_id\tuuid\t2\trow 1\na/g.json\tcredential_id\tuuid\t2\trow 1",
		"not a number":     "a/g.json\tcredential_id\tuuid\ttwo\trow 1",
		"rule-wide (path)": "\tcredential_id\tuuid\t2\trow 1",
	} {
		if _, err := ParseAllowlist(text); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestTheShippedAllowlistParsesAndCitesItsTriage(t *testing.T) {
	rows, err := Allowlist()
	if err != nil || len(rows) == 0 {
		t.Fatalf("the shipped allowlist: %d rows, %v", len(rows), err)
	}
	for _, row := range rows {
		if !strings.HasPrefix(row.Path, "internal/") || !strings.Contains(row.Triage, "triage") {
			t.Errorf("row %s %s: path or triage cite looks wrong", row.Path, row.Key)
		}
	}
}

func TestACheckTreeFindsAStaleRowAndAMissingFile(t *testing.T) {
	g := golden(t, map[string]any{"client_secret": someUUID})
	clean := golden(t, map[string]any{"name": "x"})
	read := func(path string) ([]byte, error) {
		if path == "a/clean.json" {
			return clean, nil
		}
		return g, nil
	}
	rows := []Row{
		{Path: "a/g.json", Key: "client_secret", Shape: "uuid", Count: 1, Triage: "row 1"},
		{Path: "a/clean.json", Key: "client_secret", Shape: "uuid", Count: 1, Triage: "row 2"},
		{Path: "a/gone.json", Key: "client_secret", Shape: "uuid", Count: 1, Triage: "row 3"},
	}
	problems, err := CheckTree([]string{"a/clean.json", "a/g.json"}, read, rows)
	if err != nil {
		t.Fatal(err)
	}
	joined := fmt.Sprint(problems)
	if len(problems) != 2 || !strings.Contains(joined, "a/clean.json") || !strings.Contains(joined, "a/gone.json") {
		t.Fatalf("a stale row and a row for a missing file were not both found: %v", problems)
	}
}

// The windowed scan is the expression over the whole text (CHAOS-7890: the expression over a long leaf took 74 s over the tree).
func TestTheWindowedScanEqualsTheExpressionOverTheWholeText(t *testing.T) {
	r := rand.New(rand.NewSource(7890))
	pieces := []string{"api_key", "API:", "Api-token=", "password: ", "secret", `"`, " ", "\n", "=", ":", generatedValue(1)[:19], strings.Repeat("a1", 40), strings.Repeat("Zx9", 60), "plain words here", "key", "KEY", "ſecret", "token\\n", "access ", "creds=", "|", "auth_", "x-y.z", strings.Repeat("\u023a", 70), "\u212a"}
	for iter := 0; iter < 3000; iter++ {
		var b strings.Builder
		for n := r.Intn(14); n >= 0; n-- {
			b.WriteString(pieces[r.Intn(len(pieces))])
			if r.Intn(3) == 0 {
				b.WriteString(strings.Repeat("x", r.Intn(120)))
			}
		}
		text := b.String()
		var want []string
		for _, loc := range generic.FindAllStringIndex(text, -1) {
			if full, secret := secretOf(text[loc[0]:loc[1]]); accepted(full, secret, lineOf(text, loc[0], loc[0]+len(full))) {
				want = append(want, secret)
			}
		}
		if got := SecretsIn(text); !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Fatalf("the windowed scan differs from the expression over the whole text, %q: %q vs %q", text, got, want)
		}
	}
}

// A text whose lower-cased form has other byte offsets (a capital A with a stroke is two bytes, its lower case three) is run whole, so the
// windows never point at the wrong bytes.
func TestATextWithRelocatedLowerCaseIsStillScanned(t *testing.T) {
	text := strings.Repeat("\u023a", 70) + `"api_key": "` + randomValue() + `"`
	if got := SecretsIn(text); len(got) != 1 {
		t.Fatalf("%d secret(s) found after a run of relocating characters, want 1", len(got))
	}
}

// Text after the JSON value is not a JSON document: the walk refuses it (a measurement that did not happen is a failure), and a JSON
// text inside a leaf that has text after it is scanned whole instead of being half-read.
func TestTextAfterTheJSONValueIsRefused(t *testing.T) {
	if _, err := Leaves([]byte(`{"a": "b"} trailing`)); err == nil {
		t.Fatal("a document with text after its value was read")
	}
	inner := `{"api_key": "` + randomValue() + `"} and more text`
	leaves, err := Leaves(golden(t, map[string]any{"body": inner}))
	if err != nil {
		t.Fatal(err)
	}
	if hits := Hits(leaves); len(hits) != 1 {
		t.Fatalf("a JSON text with text after it was not scanned whole: %d hit(s)", len(hits))
	}
}

// A second-form secret has no upper length: the secret found is the whole run, not the part a window cut (it decides the shape and the
// allowlist row).
func TestALongSecretIsFoundWhole(t *testing.T) {
	value := valueOf(t, "r:alnum:400:86") // the vector second-form-400-no-keyword-inside, which the real scanner flags
	got := SecretsIn(`"api_key": "` + value + `"`)
	if len(got) != 1 || got[0] != value {
		t.Fatalf("%d secret(s) found, want one of the whole 400 characters", len(got))
	}
}
