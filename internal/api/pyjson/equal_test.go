package pyjson

import "testing"

func mustDecode(t *testing.T, text string) Value {
	t.Helper()
	value, err := DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestEqual(t *testing.T) {
	cases := []struct {
		a, b  string
		equal bool
	}{
		{`{"a":1,"b":2}`, `{"b":2,"a":1}`, true},
		{`{"a":1}`, `{"a":true}`, true},
		{`{"a":1}`, `{"a":1.0}`, true},
		{`{"a":0}`, `{"a":false}`, true},
		{`{"a":1}`, `{"a":"1"}`, false},
		{`{"a":[1,2]}`, `{"a":[2,1]}`, false},
		{`{"a":1}`, `{"a":1,"b":2}`, false},
		{`{"a":null}`, `{"a":null}`, true},
		{`{"a":null}`, `{"a":0}`, false},
		{`{"a":12345678901234567890}`, `{"a":12345678901234567891}`, false},
		{`{"a":12345678901234567890}`, `{"a":12345678901234567890}`, true},
		{`{"a":{"b":[{"c":1}]}}`, `{"a":{"b":[{"c":true}]}}`, true},
		{`{"a":"x"}`, `{"a":"y"}`, false},
	}
	for _, c := range cases {
		if got := Equal(mustDecode(t, c.a), mustDecode(t, c.b)); got != c.equal {
			t.Errorf("Equal(%s, %s) = %v, want %v", c.a, c.b, got, c.equal)
		}
	}
}
