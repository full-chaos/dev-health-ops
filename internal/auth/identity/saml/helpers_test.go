package saml_test

import (
	"bytes"
	"compress/flate"
	"encoding/base64"
	"io"
	"net/url"
	"regexp"
	"testing"
)

func b64(raw []byte) string { return base64.StdEncoding.EncodeToString(raw) }

func encodeCert(idp *testIdP) string { return base64.StdEncoding.EncodeToString(idp.cert.Raw) }

var requestIDPattern = regexp.MustCompile(`\bID="([^"]+)"`)

// requestIDFromRedirect inflates the redirect binding's SAMLRequest and
// returns its ID.
func requestIDFromRedirect(t testing.TB, redirect string) string {
	t.Helper()
	u, err := url.Parse(redirect)
	if err != nil {
		t.Fatal(err)
	}
	deflated, err := base64.StdEncoding.DecodeString(u.Query().Get("SAMLRequest"))
	if err != nil {
		t.Fatal(err)
	}
	xmlBytes, err := io.ReadAll(flate.NewReader(bytes.NewReader(deflated)))
	if err != nil {
		t.Fatal(err)
	}
	m := requestIDPattern.FindSubmatch(xmlBytes)
	if m == nil {
		t.Fatalf("no ID in AuthnRequest %s", xmlBytes)
	}
	return string(m[1])
}
