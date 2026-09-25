package reports

import "testing"

// Each row is what json.dumps(json.loads(input)) writes on the Python plane
// (the live differential against the real resolvers is the venue oracle).
// bs avoids writing a backslash-u sequence literally in this source.
const bs = "\x5c"

func TestPythonDumps(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, in, want string }{
		{"separators and insertion order", `{"b":1,"a":[1,2.0,"x"]}`, `{"b": 1, "a": [1, 2.0, "x"]}`},
		{"repeated key keeps first position, last value", `{"a":1,"b":2,"a":3}`, `{"a": 3, "b": 2}`},
		{"repeated key compared by decoded text", `{"a":1,"` + bs + `u0061":2}`, `{"a": 2}`},
		{"non-ascii is escaped", "\"\u00e9" + bs + "u00e9\"", `"` + bs + `u00e9` + bs + `u00e9"`},
		{"astral literal becomes a surrogate pair", "\"\U0001F600\"", `"` + bs + `ud83d` + bs + `ude00"`},
		{"astral pair escape is the same character", `"` + bs + `ud83d` + bs + `ude00"`, `"` + bs + `ud83d` + bs + `ude00"`},
		{"lone surrogate is kept", `"` + bs + `ud800"`, `"` + bs + `ud800"`},
		{"DEL is escaped", "\"\x7f\"", `"` + bs + `u007f"`},
		{"short escapes and control", `"\b\f\n\r\t` + bs + `u0001\/"`, `"\b\f\n\r\t` + bs + `u0001/"`},
		{"float repr", `[1e5,1.5,0.1,1e-7,123456789012345680.0]`, `[100000.0, 1.5, 0.1, 1e-07, 1.2345678901234568e+17]`},
		{"negative zero int is 0", `-0`, `0`},
		{"negative zero float", `-0.0`, `-0.0`},
		{"big int is exact", `123456789012345678901234567890`, `123456789012345678901234567890`},
		{"overflowing float is Infinity", `[1e999,-1e999]`, `[Infinity, -Infinity]`},
		{"empty containers", `[{},[]]`, `[{}, []]`},
		{"literals", `[true,false,null]`, `[true, false, null]`},
		{"key with escapes", "{\"a\u00e9\\\"\":1}", `{"a` + bs + `u00e9\"": 1}`},
	} {
		got, err := PythonDumps([]byte(tc.in))
		if err != nil || got != tc.want {
			t.Errorf("%s: PythonDumps(%s) = %q, %v; want %q", tc.name, tc.in, got, err, tc.want)
		}
	}
}

func TestPythonDumpsObjectStoresOnlyObjects(t *testing.T) {
	t.Parallel()
	for name, in := range map[string]string{
		"absent": ``, "null": `null`, "array": `[1]`, "string": `"x"`, "number": `1`, "bool": `true`,
	} {
		if got, err := PythonDumpsObject([]byte(in)); err != nil || got != "{}" {
			t.Errorf("%s: got %q, %v; want {}", name, got, err)
		}
	}
	if got, err := PythonDumpsObject([]byte(`{"z":1,"a":{"y":2,"b":3}}`)); err != nil || got != `{"z": 1, "a": {"y": 2, "b": 3}}` {
		t.Errorf("object: got %q, %v", got, err)
	}
}

func TestCloneParameters(t *testing.T) {
	t.Parallel()
	str := func(s string) *string { return &s }
	for _, tc := range []struct {
		name      string
		stored    *string
		overrides string
		want      string
		wantErr   bool
	}{
		{"absent stored, no overrides", nil, ``, `{}`, false},
		{"falsy stored is {}", str(`[]`), ``, `{}`, false},
		{"falsy stored null", str(`null`), `{"a":1}`, `{"a": 1}`, false},
		{"merge keeps position, appends new keys", str(`{"a": 1, "b": 2}`), `{"c":3,"a":9}`, `{"a": 9, "b": 2, "c": 3}`, false},
		{"empty override object changes nothing", str(`{"a": 1}`), `{}`, `{"a": 1}`, false},
		{"non-object override changes nothing", str(`{"a": 1}`), `[1]`, `{"a": 1}`, false},
		{"stored list with a non-empty override fails", str(`[1]`), `{"a":1}`, ``, true},
		{"stored list, no override, is kept", str(`[1]`), ``, `[1]`, false},
	} {
		got, err := CloneParameters(tc.stored, []byte(tc.overrides))
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("%s: got %q, %v; want %q (error %v)", tc.name, got, err, tc.want, tc.wantErr)
		}
	}
}
