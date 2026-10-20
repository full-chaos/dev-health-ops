package saml

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"net/url"
	"strings"
	"time"
)

func xmlAttr(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// SPMetadataXML is the SP metadata document the go-api route serves.
func SPMetadataXML(entityID, acsURL, nameIDFormat string) string {
	return `<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
                     entityID="` + xmlAttr(entityID) + `">
    <md:SPSSODescriptor AuthnRequestsSigned="false"
                        WantAssertionsSigned="true"
                        protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
        <md:NameIDFormat>` + xmlAttr(nameIDFormat) + `</md:NameIDFormat>
        <md:AssertionConsumerService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
                                     Location="` + xmlAttr(acsURL) + `"
                                     index="0"
                                     isDefault="true"/>
    </md:SPSSODescriptor>
</md:EntityDescriptor>`
}

// authnRequestXML is the go-api AuthnRequest document.
func authnRequestXML(requestID string, issueInstant time.Time, destination, acsURL, spEntityID, nameIDFormat string) string {
	return `<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
    ID="` + xmlAttr(requestID) + `"
    Version="2.0"
    IssueInstant="` + issueInstant.UTC().Format("2006-01-02T15:04:05Z") + `"
    Destination="` + xmlAttr(destination) + `"
    ProtocolBinding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
    AssertionConsumerServiceURL="` + xmlAttr(acsURL) + `">
    <saml:Issuer>` + xmlAttr(spEntityID) + `</saml:Issuer>
    <samlp:NameIDPolicy Format="` + xmlAttr(nameIDFormat) + `"
                        AllowCreate="true"/>
</samlp:AuthnRequest>`
}

// redirectURL is the HTTP-Redirect binding URL: raw DEFLATE, base64.
func redirectURL(destination, request, relayState string) (string, error) {
	var buf bytes.Buffer
	fw, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return "", err
	}
	if _, err := fw.Write([]byte(request)); err != nil {
		return "", err
	}
	if err := fw.Close(); err != nil {
		return "", err
	}
	params := url.Values{"SAMLRequest": {base64.StdEncoding.EncodeToString(buf.Bytes())}}
	if relayState != "" {
		params.Set("RelayState", relayState)
	}
	return destination + "?" + params.Encode(), nil
}

// newRequestID is "_id" and 16 random bytes in hex, the go-api shape.
func newRequestID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "_id" + hex.EncodeToString(b), nil
}

func newNonce() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// BindingCookieName is the per-request browser binding cookie. It is derived
// from the request id, which the ACS reads from the verified assertion.
func BindingCookieName(requestID string) string {
	sum := sha256.Sum256([]byte(requestID))
	return "dho_saml_login_nonce_" + base64.RawURLEncoding.EncodeToString(sum[:16])
}

// ACSPath is the path of the advertised ACS URL, the one the binding cookie is
// scoped to. It is derived from ACSURL, the single source of the ACS location.
func ACSPath(p Provider, base string) string {
	u, err := url.Parse(p.ACSURL(base))
	if err != nil || u.Path == "" {
		return "/"
	}
	return u.Path
}

func bindingHash(nonce string) [32]byte { return sha256.Sum256([]byte(nonce)) }
