package saml_test

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// FuzzParseSAMLResponse: no input panics Verify, every refusal is a
// RefusedError, and an accepted input carries the signed claims.
func FuzzParseSAMLResponse(f *testing.F) {
	idp := newTestIdP(f)
	valid := idp.response(f, resp{})
	f.Add(valid)
	f.Add(idp.response(f, resp{signResponse: true, unsignedAssertion: true}))
	f.Add(idp.response(f, resp{unsignedAssertion: true}))
	f.Add([]byte(`<!DOCTYPE r [<!ENTITY x "y">]><r/>`))
	f.Add([]byte(strings.Replace(string(valid), "user-1", "user-2", 1)))
	f.Add([]byte(`<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"><saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"/></samlp:Response>`))
	sp := idp.sp()
	f.Fuzz(func(t *testing.T, raw []byte) {
		v, err := saml.Verify(raw, sp, time.Now())
		if err != nil {
			if _, ok := saml.IsRefused(err); !ok {
				t.Fatalf("refusal is not a RefusedError: %v", err)
			}
			return
		}
		if v.Issuer != idpEntityID || v.AssertionID == "" {
			t.Fatalf("accepted response without signed claims: %+v", v)
		}
		if _, err := saml.Subject(v.NameID); err != nil {
			t.Fatalf("accepted response whose NameID cannot key an identity: %+v", v.NameID)
		}
	})
}

// FuzzSAMLSubject: Subject is injective and ParseSubject inverts it.
func FuzzSAMLSubject(f *testing.F) {
	f.Add("", "", "user-1", "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent", "", "user-1")
	f.Add("a|b", "", "c", "a", "b|", "c")
	f.Add("a%7C", "", "x", "a|", "", "x")
	f.Add(saml.NameIDFormatUnspecified, "", "x", "", "", "x")
	f.Fuzz(func(t *testing.T, f1, q1, v1, f2, q2, v2 string) {
		a := saml.NameID{Format: f1, SPNameQualifier: q1, Value: v1}
		b := saml.NameID{Format: f2, SPNameQualifier: q2, Value: v2}
		sa, errA := saml.Subject(a)
		sb, errB := saml.Subject(b)
		if errA == nil {
			back, err := saml.ParseSubject(sa)
			if err != nil {
				t.Fatalf("ParseSubject(%q): %v", sa, err)
			}
			if again, _ := saml.Subject(back); again != sa {
				t.Fatalf("round trip changed %q to %q", sa, again)
			}
			if len(sa) > 1024 || strings.ContainsAny(sa, "\x00\n\r\t ") {
				t.Fatalf("subject %q is not a valid identity subject", sa)
			}
		}
		if errA == nil && errB == nil && sa == sb && normalize(a) != normalize(b) {
			t.Fatalf("two NameIDs share subject %q: %+v %+v", sa, a, b)
		}
	})
}

func normalize(n saml.NameID) saml.NameID {
	if n.Format == "" {
		n.Format = saml.NameIDFormatUnspecified
	}
	return n
}

func TestSAMLSubjectIsStableAndRefusesTransient(t *testing.T) {
	s, err := saml.Subject(saml.NameID{Format: "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent", SPNameQualifier: "sp", Value: "a|b c"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "saml1|urn:oasis:names:tc:SAML:2.0:nameid-format:persistent|sp|a%7Cb%20c"; s != want {
		t.Fatalf("Subject = %q, want %q", s, want)
	}
	for _, n := range []saml.NameID{
		{Format: saml.NameIDFormatTransient, Value: "x"},
		{Value: ""},
		{Value: "a\x00b"},
		{Value: strings.Repeat("x", 1100)},
	} {
		if _, err := saml.Subject(n); err == nil {
			t.Fatalf("Subject accepted %+v", n)
		}
	}
}
