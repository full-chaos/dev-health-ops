package pyjson

import "testing"

func TestSyntaxErrorText(t *testing.T) {
	cases := []struct {
		document string
		want     string
	}{
		{"", "Expecting value: line 1 column 1 (char 0)"},
		{"{", "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"},
		{"  \n  oops", "Expecting value: line 2 column 3 (char 5)"},
		{"[\"日\",\n日]", "Expecting value: line 2 column 1 (char 6)"},
		{"{\"a\":1,\n}", "Illegal trailing comma before end of object: line 1 column 7 (char 6)"},
	}
	for _, c := range cases {
		_, err := DecodeString(c.document)
		syntax, ok := err.(*SyntaxError)
		if !ok {
			t.Fatalf("DecodeString(%q) error = %v, want a SyntaxError", c.document, err)
		}
		if got := syntax.Text(c.document); got != c.want {
			t.Errorf("Text(%q) = %q, want %q", c.document, got, c.want)
		}
	}
}
