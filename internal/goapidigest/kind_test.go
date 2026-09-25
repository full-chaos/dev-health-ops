package goapidigest

import "testing"

func TestDocumentKind(t *testing.T) {
	for _, tc := range []struct {
		name, text, want string
		wantErr          bool
	}{
		{"query", "query A($x: Int) { a(x: $x) { b } }", KindQuery, false},
		{"shorthand query", "{ a }", KindQuery, false},
		{"mutation", "mutation M($x: Int!) { m(x: $x) }", KindMutation, false},
		{"leading comment and blank lines", "# note\n\n  mutation M { m }", KindMutation, false},
		{"query whose field is named mutation", "query Q { mutation { id } }", KindQuery, false},
		{"query with a mutation-named alias", "query Q { mutationResult: a }", KindQuery, false},
		{"subscription", "subscription S { s }", "", true},
		{"two operations", "query A { a } mutation M { m }", "", true},
		{"no operation", "fragment F on T { a }", "", true},
		{"unparseable", "mutation {", "", true},
		{"empty", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DocumentKind(tc.text)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("DocumentKind(%q) = %q, %v; want %q (error %v)", tc.text, got, err, tc.want, tc.wantErr)
			}
		})
	}
}
