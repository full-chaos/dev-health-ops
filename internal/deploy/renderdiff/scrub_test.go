package renderdiff

import (
	"os"
	"path/filepath"
	"testing"
)

// secretLikeRows is the count of rows the kit's secret-like tests match in the 12 goldens-1 render inputs: an env-like
// entry (a mapping with a str name and a value key) whose name matches the SECRETY pattern, and a ConfigMap
// data/binaryData key that matches the CMKEY pattern. The synthetic renders were drawn so that each row keeps the
// verdict it had in the prod render it replaces (the lane's synthesizer asserts it per row); 143 is that count, the
// same before and after the scrub.
const secretLikeRows = 143

var syntheticRenders = []string{"v3n-741-a.yaml", "v3n-741-b.yaml", "v3w-s76-a.yaml", "v3w-s76-b.yaml", "v3z18-13954-main.yaml",
	"v3z18-13954-1110.yaml", "v3z19-13954-snip.yaml", "v3z19-13954-sniphash.yaml", "v3x-np-b.yaml", "hooks-auth-migrate.yaml",
	"ingress-live.manifest", "ingress-dry.manifest"}

func countSecretLike(o any) int {
	n := 0
	switch x := o.(type) {
	case *pyDict:
		if name, ok := x.getS("name"); ok {
			if s, isStr := name.(string); isStr && x.has("value") && secretyRe.MatchString(s) {
				n++
			}
		}
		if kind, ok := x.getS("kind"); ok && pyEq(kind, "ConfigMap") {
			for _, sec := range []string{"data", "binaryData"} {
				if m, ok := x.getS(sec); ok {
					if md, isDict := m.(*pyDict); isDict {
						for _, k := range md.keys {
							if s, isStr := k.(string); isStr && cmKeyRe.MatchString(s) {
								n++
							}
						}
					}
				}
			}
		}
		for _, v := range x.vals {
			n += countSecretLike(v)
		}
	case []any:
		for _, v := range x {
			n += countSecretLike(v)
		}
	}
	return n
}

func TestTheScrubKeptEverySecretLikeVerdict(t *testing.T) {
	n := 0
	for _, f := range syntheticRenders {
		raw, err := os.ReadFile(filepath.Join("testdata/goldens-1/in", f))
		if err != nil {
			t.Fatal(err)
		}
		docs, err := loadAll(string(raw))
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range docs {
			n += countSecretLike(d)
		}
	}
	if n != secretLikeRows {
		t.Fatalf("%d secret-like rows in the synthetic renders, the prod renders had %d", n, secretLikeRows)
	}
}
