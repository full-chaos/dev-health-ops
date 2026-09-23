package pyjson

import (
	"encoding/json"
	"strings"
	"testing"
)

func uEscape(hex string) string { return string(rune(0x5c)) + "u" + hex }

// TestMarshalCanonicalSortsKeysAndEscapesNonASCII pins the two flags that
// distinguish this from Marshal: sort_keys=True (insertion order is NOT
// preserved) and the DEFAULT ensure_ascii=True (opposite of Marshal's
// ensure_ascii=False).
func TestMarshalCanonicalSortsKeysAndEscapesNonASCII(t *testing.T) {
	value, err := DecodeString(`{"b":1,"a":"café","c":[3,2,1]}`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MarshalCanonical(value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":"caf` + uEscape("00e9") + `","b":1,"c":[3,2,1]}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestMarshalCanonicalAstralSurrogatePair(t *testing.T) {
	value, err := DecodeString(`{"emoji":"😀"}`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MarshalCanonical(value)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"emoji":"` + uEscape("d83d") + uEscape("de00") + `"}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// TestMarshalCanonicalAcceptsTheStdlibDecodedShape proves the second
// supported representation: encoding/json's own map[string]any/json.Number/
// []any/float64, the shape a caller using json.Decoder.UseNumber() holds --
// no conversion into this package's Value type is required.
func TestMarshalCanonicalAcceptsTheStdlibDecodedShape(t *testing.T) {
	// UseNumber() matches externalingest's own decodeGolden -- without it,
	// encoding/json decodes every number to float64 (losing the source's
	// own int/float distinction), which is a stdlib property, not something
	// this test should paper over by asserting the wrong expectation.
	decoder := json.NewDecoder(strings.NewReader(`{"b":1,"a":[1,2,3],"c":"café"}`))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		t.Fatal(err)
	}
	got, err := MarshalCanonical(document)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":[1,2,3],"b":1,"c":"caf` + uEscape("00e9") + `"}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// TestMarshalCanonicalPreservesJSONNumberDigitTextExactly proves the
// motivating property for json.Number support: a caller decoding with
// UseNumber() (to avoid float64 precision loss on a large integer) gets
// that exact digit text back, not a reparsed-and-reformatted number.
func TestMarshalCanonicalPreservesJSONNumberDigitTextExactly(t *testing.T) {
	decoder := json.NewDecoder(strings.NewReader(`{"id":9007199254740993}`))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		t.Fatal(err)
	}
	got, err := MarshalCanonical(document)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":9007199254740993}`
	if string(got) != want {
		t.Fatalf("got %s, want %s (precision must survive a round trip json.Unmarshal into float64 would lose)", got, want)
	}
}

func TestMarshalCanonicalScalarsAndEmptyContainers(t *testing.T) {
	cases := []struct {
		name, json, want string
	}{
		{"null", `null`, `null`},
		{"true", `true`, `true`},
		{"false", `false`, `false`},
		{"empty object", `{}`, `{}`},
		{"empty array", `[]`, `[]`},
		{"nested empty", `{"x":{}}`, `{"x":{}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			value, err := DecodeString(c.json)
			if err != nil {
				t.Fatal(err)
			}
			got, err := MarshalCanonical(value)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}
