package goldenscan

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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

// a synthetic random-looking value made at run time, so no secret-shaped literal is in the source
func randomValue() string {
	return strings.Repeat("aB3x", 2) + "Zq9Lm2Pw7Rt5Yk8Nc4Vd6Hs" + "J1"
}

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

func TestTheRuleLeavesWhatIsNotACredential(t *testing.T) {
	value := randomValue()
	for name, leaves := range map[string]map[string]any{
		"a high-entropy value under a plain key":  {"name": value},
		"a snake_case identifier under a key key": {"dataset_key": "orders_by_team_week"},
		"a counter name":                {"idempotencyKey": "create-config.run_two_a1"},
		"a short value":                 {"api_key": "abc123"},
		"a placeholder word":            {"api_key": "synthetic" + value},
		"low entropy":                   {"api_key": strings.Repeat("a", 40)},
		"entropy below the bound":       {"api_key": strings.Repeat("abcdefghij", 3)},
		"a 19-letter segment, no digit": {"api_key": "aBxZqLmPwRtYkNcVdHs"},
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
	if Shape(someUUID) != "uuid" || Shape(strings.ToUpper(someUUID)) != "uuid" || Shape(someHex) != "hex64" || Shape(someHex[:63]) != "other" || Shape(someUUID+"x") != "other" {
		t.Error("a shape is wrong")
	}
}

func rowsFor(path, key, shape string, count int) []Row {
	return []Row{{Path: path, Key: key, Shape: shape, Count: count, Triage: "row 1"}}
}

func TestACheckPassesOnlyThroughAnExactRow(t *testing.T) {
	g := golden(t, map[string]any{"credential_id": someUUID, "other": map[string]any{"credential_id": derivedUUID("another")}})
	problems, err := Check("a/g.json", g, rowsFor("a/g.json", "credential_id", "uuid", 2))
	if err != nil || len(problems) != 0 {
		t.Fatalf("an exact row did not pass: %v %v", problems, err)
	}
	for name, rows := range map[string][]Row{
		"no row":            nil,
		"another file":      rowsFor("b/g.json", "credential_id", "uuid", 2),
		"another key":       rowsFor("a/g.json", "credential_ref", "uuid", 2),
		"count too high":    rowsFor("a/g.json", "credential_id", "uuid", 3),
		"count too low":     rowsFor("a/g.json", "credential_id", "uuid", 1),
		"another shape row": rowsFor("a/g.json", "credential_id", "hex64", 2),
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
	g := golden(t, map[string]any{"credential_id": someUUID, "nested": []any{map[string]any{"credential_id": randomValue()}}})
	problems, _ := Check("a/g.json", g, rowsFor("a/g.json", "credential_id", "uuid", 2))
	if len(problems) == 0 {
		t.Fatal("a random value under an allowlisted key passed beside a UUID")
	}
}

func TestACheckReadsInsideAPackedBody(t *testing.T) {
	g := golden(t, map[string]any{"body": pack(t, `{"credential_id": "`+someUUID+`", "x": {"api_key": "`+randomValue()+`"}}`)})
	problems, _ := Check("a/g.json", g, rowsFor("a/g.json", "credential_id", "uuid", 1))
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
	g := golden(t, map[string]any{"credential_id": someUUID})
	clean := golden(t, map[string]any{"name": "x"})
	read := func(path string) ([]byte, error) {
		if path == "a/clean.json" {
			return clean, nil
		}
		return g, nil
	}
	rows := []Row{
		{Path: "a/g.json", Key: "credential_id", Shape: "uuid", Count: 1, Triage: "row 1"},
		{Path: "a/clean.json", Key: "credential_id", Shape: "uuid", Count: 1, Triage: "row 2"},
		{Path: "a/gone.json", Key: "credential_id", Shape: "uuid", Count: 1, Triage: "row 3"},
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

// The windowed rule is the expression over the whole text (CHAOS-7890: the expression over a long leaf took 74 s over the tree).
func TestTheWindowedRuleEqualsTheExpressionOverTheWholeText(t *testing.T) {
	r := rand.New(rand.NewSource(7890))
	pieces := []string{"api_key", "API:", "Api-token=", "password: ", "secret", `"`, " ", "\n", "=", ":", "aB3xZq9Lm2Pw7Rt5Yk8", strings.Repeat("a1", 40), strings.Repeat("Zx9", 60), "plain words here", "key", "KEY", "ſecret", "token\\n", "access ", "creds=", "|"}
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
		for _, match := range generic.FindAllStringSubmatch(text, -1) {
			if accepted(match[1]) {
				want = append(want, match[1])
			}
		}
		var got []string
		for _, captured := range genericCaptures(text) {
			if accepted(captured) {
				got = append(got, captured)
			}
		}
		if !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
			t.Fatalf("windowed captures differ from the expression over %q: %q vs %q", text, got, want)
		}
	}
}

// A letters-only segment is random-looking from 20 characters, a mixed one from 12: the two bounds are pinned from both sides.
func TestTheSegmentBoundsArePinnedFromBothSides(t *testing.T) {
	for name, tc := range map[string]struct {
		value string
		hit   bool
	}{
		"20 letters":                     {"aBxZqLmPwRtYkNcVdHsE", true},
		"19 letters":                     {"aBxZqLmPwRtYkNcVdHs", false},
		"12 mixed":                       {"aB3xZq9Lm2Pw", true},
		"11 mixed, short words after it": {"aB3xZq9Lm2P_qw_er_ty_ui", false},
		"12 mixed, short words after":    {"aB3xZq9Lm2Pw_qw_er_ty", true},
	} {
		leaves, err := Leaves(golden(t, map[string]any{"api_key": tc.value}))
		if err != nil {
			t.Fatal(err)
		}
		if got := len(Hits(leaves)) == 1; got != tc.hit {
			t.Errorf("%s: hit = %v, want %v (entropy %.2f)", name, got, tc.hit, entropy(tc.value))
		}
	}
}

// A row that pins one exact value by its sha256 passes that value and no other.
func TestAnExactRowPassesOnlyItsOwnValue(t *testing.T) {
	value := randomValue()
	sum := sha256.Sum256([]byte(value))
	row := Row{Path: "a/g.json", Key: "api_key", Shape: "sha256:" + hex.EncodeToString(sum[:]), Count: 1, Triage: "triage line"}
	if problems, _ := Check("a/g.json", golden(t, map[string]any{"api_key": value}), []Row{row}); len(problems) != 0 {
		t.Fatalf("the exact value was refused: %v", problems)
	}
	other := strings.Replace(value, "aB3x", "bC4y", 1)
	if problems, _ := Check("a/g.json", golden(t, map[string]any{"api_key": other}), []Row{row}); len(problems) == 0 {
		t.Fatal("another value passed through an exact row")
	}
	if _, err := ParseAllowlist("a/g.json\tapi_key\t" + row.Shape + "\t1\ttriage line\n"); err != nil {
		t.Fatalf("an exact row was refused by the parser: %v", err)
	}
	if _, err := ParseAllowlist("a/g.json\tapi_key\tsha256:abc\t1\ttriage line\n"); err == nil {
		t.Fatal("a short digest was accepted")
	}
}
