package main

import (
	"strings"
	"testing"
)

// CHAOS-8000: a legacy text is a registered*Document const named in legacyDigestsByOperation and nowhere in
// digestByOperation; registrydump lists it after its operation's current text, marked legacy.
const legacyHappySrc = `package main

const registeredFooDocument = "query Foo { foo new }"
const registeredFooV1Document = "query Foo { foo }"
const registeredBarDocument = "query Bar { bar }"

var legacyDigestsByOperation = map[string][]string{
	"Foo": {digestHex(registeredFooV1Document)},
}

func newQueryHandler() {
	digestByOperation := map[string]string{
		"Foo": digestHex(registeredFooDocument),
		"Bar": digestHex(registeredBarDocument),
	}
	_ = digestByOperation
}
`

func TestEnumerate_LegacyTextIsListedAfterTheCurrentOneAndMarked(t *testing.T) {
	docs, err := enumerate(writeFixture(t, legacyHappySrc))
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("got %d documents, want 3: %+v", len(docs), docs)
	}
	// Bar, then Foo's current text, then Foo's legacy text.
	if docs[0].Operation != "Bar" || docs[0].Legacy {
		t.Errorf("docs[0] = %+v, want the current Bar", docs[0])
	}
	if docs[1].Operation != "Foo" || docs[1].Legacy || docs[1].ConstName != "registeredFooDocument" {
		t.Errorf("docs[1] = %+v, want the current Foo", docs[1])
	}
	if docs[2].Operation != "Foo" || !docs[2].Legacy || docs[2].ConstName != "registeredFooV1Document" {
		t.Errorf("docs[2] = %+v, want the legacy Foo", docs[2])
	}
	if docs[1].Digest == docs[2].Digest {
		t.Errorf("current and legacy Foo share digest %s", docs[1].Digest)
	}
}

func TestEnumerate_EmptyLegacyMapOrNoLegacyVarIsFine(t *testing.T) {
	for name, src := range map[string]string{
		"empty": strings.Replace(legacyHappySrc, `"Foo": {digestHex(registeredFooV1Document)},`, "", 1),
		"none":  strings.Replace(legacyHappySrc, "var legacyDigestsByOperation", "var somethingElse", 1),
	} {
		src = strings.Replace(src, `const registeredFooV1Document = "query Foo { foo }"`, "", 1)
		docs, err := enumerate(writeFixture(t, src))
		if err != nil {
			t.Fatalf("%s: enumerate: %v", name, err)
		}
		for _, d := range docs {
			if d.Legacy {
				t.Errorf("%s: %+v is marked legacy with no legacy map", name, d)
			}
		}
	}
}

func TestEnumerate_LegacyRefusals(t *testing.T) {
	cases := map[string]struct {
		mutate func(string) string
		want   string
	}{
		"legacy for an operation digestByOperation does not register": {
			func(s string) string { return strings.Replace(s, `"Foo": {digestHex(registeredFooV1Document)}`, `"Ghost": {digestHex(registeredFooV1Document)}`, 1) },
			"not in digestByOperation",
		},
		"a const named both current and legacy": {
			func(s string) string { return strings.Replace(s, `"Foo": {digestHex(registeredFooV1Document)}`, `"Foo": {digestHex(registeredFooDocument)}`, 1) },
			"not found among the unreferenced",
		},
		"a legacy text with the same digest as another document": {
			func(s string) string {
				return strings.Replace(s, `const registeredFooV1Document = "query Foo { foo }"`, `const registeredFooV1Document = "query Bar { bar }"`, 1)
			},
			"already registered for",
		},
		"a legacy const that does not exist": {
			func(s string) string { return strings.Replace(s, "digestHex(registeredFooV1Document)", "digestHex(registeredNopeDocument)", 1) },
			"not found among the unreferenced",
		},
		"a legacy entry with no text": {
			func(s string) string { return strings.Replace(s, `{digestHex(registeredFooV1Document)}`, `{}`, 1) },
			"has no text",
		},
		"an element that is not digestHex(const)": {
			func(s string) string { return strings.Replace(s, "digestHex(registeredFooV1Document)", `"abc"`, 1) },
			"not a digestHex",
		},
		"the same operation twice": {
			func(s string) string {
				return strings.Replace(s, `"Foo": {digestHex(registeredFooV1Document)},`, `"Foo": {digestHex(registeredFooV1Document)},
	"Foo": {digestHex(registeredFooV1Document)},`, 1)
			},
			"twice",
		},
		"a const left unreferenced by both maps": {
			func(s string) string { return strings.Replace(s, `"Foo": {digestHex(registeredFooV1Document)},`, "", 1) },
			"no digestByOperation entry",
		},
	}
	for name, c := range cases {
		_, err := enumerate(writeFixture(t, c.mutate(legacyHappySrc)))
		if err == nil {
			t.Errorf("%s: enumerate succeeded, want an error containing %q", name, c.want)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q does not contain %q", name, err, c.want)
		}
	}
}
