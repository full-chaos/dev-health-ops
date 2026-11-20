package affected_test

import (
	"strings"
	"testing"
)

// A pure function of the module may call the encode and decode methods of a base64 encoding value: they compute from their
// arguments and read, run and open nothing. Any other method of a standard-library value, a value of another package and
// a name the file declares itself keep the name a read.
func TestPlanReadsBareNameIntoAPureLeafWithAStdlibValueMethod(t *testing.T) {
	classes := "docs\tdocs/**\nREADME.md\tREADME.md\n"
	head := "package p\n\nimport (\n\t\"os\"\n\t\"testing\"\n\n\t\"example.com/m/y\"\n)\n\nvar _ = os.Getenv\nvar _ = testing.Short\n\n"
	yhead := "package y\n\nimport (\n\t\"encoding/base64\"\n\t\"os\"\n)\n\nvar _ = base64.StdEncoding\nvar _ = os.Stdout\n\n"
	cases := []struct {
		name, lib string
		keep      bool
	}{
		{"std_encoding_encode", "func Req(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }\n", false},
		{"raw_url_encoding_decode", "func Req(s string) bool { _, err := base64.RawURLEncoding.DecodeString(s); return err == nil }\n", false},
		{"decode_into_a_caller_buffer", "func Req(s string) int { b := make([]byte, 64); n, _ := base64.StdEncoding.Decode(b, []byte(s)); return n }\n", true},
		{"encode_into_a_caller_buffer", "func Req(s string) int { b := make([]byte, 64); base64.StdEncoding.Encode(b, []byte(s)); return len(b) }\n", true},
		{"method_not_on_the_list", "func Req(s string) string { return string(base64.StdEncoding.WithPadding('*').EncodeToString([]byte(s))) }\n", true},
		{"writer_value_method", "func Req(s string) int { n, _ := os.Stdout.WriteString(s); return n }\n", true},
		{"package_name_declared_locally", "type enc interface{ EncodeToString([]byte) string }\n\nfunc Req(s string) (base64 struct{ StdEncoding enc }, out string) {\n\tout = base64.StdEncoding.EncodeToString([]byte(s))\n\treturn\n}\n", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": head + "var _ = y.Req(\"README.md\")\n", "y/y.go": yhead + c.lib,
				"go.mod": "module example.com/m\n", "README.md": "r\n", "internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if got := strings.Contains(out, "p\ttest\tREADME.md"); got != c.keep {
				t.Fatalf("a README.md reader row is %v, want %v:\n%s", got, c.keep, out)
			}
		})
	}
}
