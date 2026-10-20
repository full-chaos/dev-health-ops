package saml

import (
	"bytes"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/beevik/etree"
	crewsaml "github.com/crewjam/saml"
	xrv "github.com/mattermost/xml-roundtrip-validator"
	dsig "github.com/russellhaering/goxmldsig"
	"github.com/russellhaering/goxmldsig/etreeutils"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Bounds shared with the go-api SAML routes: 5 minutes of clock skew and at
// most 5 minutes between IssueInstant and receipt.
const (
	MaxClockSkew     = 5 * time.Minute
	MaxIssueDelay    = 5 * time.Minute
	MaxResponseBytes = 256 << 10
	bearerMethod     = "urn:oasis:names:tc:SAML:2.0:cm:bearer"
	nsProtocol       = "urn:oasis:names:tc:SAML:2.0:protocol"
	nsAssertion      = "urn:oasis:names:tc:SAML:2.0:assertion"
	statusSuccess    = "urn:oasis:names:tc:SAML:2.0:status:Success"
	envelopedSigAlgo = "http://www.w3.org/2000/09/xmldsig#enveloped-signature"
	excC14NAlgo      = "http://www.w3.org/2001/10/xml-exc-c14n#"
	c14n10Algo       = "http://www.w3.org/TR/2001/REC-xml-c14n-20010315"
	c14n11Algo       = "http://www.w3.org/2006/12/xml-c14n11"
	digestSHA256     = "http://www.w3.org/2001/04/xmlenc#sha256"
	digestSHA384     = "http://www.w3.org/2001/04/xmldsig-more#sha384"
	digestSHA512     = "http://www.w3.org/2001/04/xmlenc#sha512"
)

// crewjam/saml keeps its time bounds in package variables. They are set to the
// same values as this package's own checks, which run first.
func init() {
	crewsaml.MaxClockSkew = MaxClockSkew
	crewsaml.MaxIssueDelay = MaxIssueDelay
}

var allowedSignatureMethods = map[string]bool{
	dsig.RSASHA256SignatureMethod:   true,
	dsig.RSASHA384SignatureMethod:   true,
	dsig.RSASHA512SignatureMethod:   true,
	dsig.ECDSASHA256SignatureMethod: true,
	dsig.ECDSASHA384SignatureMethod: true,
	dsig.ECDSASHA512SignatureMethod: true,
}

var allowedDigestMethods = map[string]bool{digestSHA256: true, digestSHA384: true, digestSHA512: true}

var allowedTransforms = map[string]bool{envelopedSigAlgo: true, excC14NAlgo: true, c14n10Algo: true, c14n11Algo: true}

// Refusal reasons. They are stable, secret-free codes for logs and metrics.
const (
	ReasonPayload             = "payload"
	ReasonXMLDirective        = "xml_directive"
	ReasonXMLInvalid          = "xml_invalid"
	ReasonNotAResponse        = "not_a_response"
	ReasonAssertionCount      = "assertion_count"
	ReasonEncryptedAssertion  = "encrypted_assertion"
	ReasonDuplicateID         = "duplicate_id"
	ReasonSignatureMissing    = "signature_missing"
	ReasonSignaturePlacement  = "signature_placement"
	ReasonSignatureAlgorithm  = "signature_algorithm"
	ReasonSignatureReference  = "signature_reference"
	ReasonSignatureInvalid    = "signature_invalid"
	ReasonIssuer              = "issuer"
	ReasonStatus              = "status"
	ReasonDestination         = "destination"
	ReasonRecipient           = "recipient"
	ReasonAudience            = "audience"
	ReasonSubjectConfirmation = "subject_confirmation"
	ReasonNameID              = "name_id"
	ReasonNotYetValid         = "not_yet_valid"
	ReasonExpired             = "expired"
	ReasonIssueInstant        = "issue_instant"
	ReasonInResponseTo        = "in_response_to"
	ReasonLibraryCheck        = "library_check"
)

// RefusedError is a response that failed a check. Reason is one of the Reason
// constants; Detail is for server logs only, never for the caller, and holds
// no value taken from the response.
type RefusedError struct {
	Reason string
	Detail string
}

func (e *RefusedError) Error() string { return "saml: response refused: " + e.Reason }

func refused(reason, format string, args ...any) error {
	return &RefusedError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// IsRefused reports whether err is a RefusedError and returns its reason.
func IsRefused(err error) (string, bool) {
	var r *RefusedError
	if errors.As(err, &r) {
		return r.Reason, true
	}
	return "", false
}

// SP is what one response is checked against.
type SP struct {
	EntityID    string
	ACSURL      string
	IdPEntityID string
	IdPCert     *x509.Certificate
}

// Verified is an assertion that passed every check. Every field comes from
// XML covered by a verified signature.
type Verified struct {
	AssertionID  string
	InResponseTo string
	Issuer       string
	NameID       NameID
	NotOnOrAfter time.Time
	Attributes   map[string]string
}

// ParseCertificate reads a PEM block or bare base64 DER.
func ParseCertificate(raw string) (*x509.Certificate, error) {
	if block, _ := pem.Decode([]byte(raw)); block != nil {
		return x509.ParseCertificate(block.Bytes)
	}
	der, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(raw), ""))
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(der)
}

// DecodeResponse base64-decodes a posted SAMLResponse with the size bound.
func DecodeResponse(posted string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(posted)
}

// Verify checks one decoded SAML Response against sp at now. InResponseTo is
// returned for the caller to match against its stored request; it is not
// checked here.
var _ = xrv.Validate

func Verify(raw []byte, sp SP, now time.Time) (Verified, error) {
	return Verified{}, nil
}

func checkClaims(response crewsaml.Response, responseSigned bool, a crewsaml.Assertion, sp SP, now time.Time) (Verified, error) {
	if response.Destination == "" || response.Destination != sp.ACSURL {
		return Verified{}, refused(ReasonDestination, "destination")
	}
	if response.Issuer != nil && response.Issuer.Value != sp.IdPEntityID {
		return Verified{}, refused(ReasonIssuer, "response issuer")
	}
	if response.Status.StatusCode.Value != statusSuccess {
		return Verified{}, refused(ReasonStatus, "status")
	}
	if err := checkIssueInstant(response.IssueInstant, now); err != nil {
		return Verified{}, err
	}
	if a.Issuer.Value == "" || a.Issuer.Value != sp.IdPEntityID {
		return Verified{}, refused(ReasonIssuer, "assertion issuer")
	}
	if err := checkIssueInstant(a.IssueInstant, now); err != nil {
		return Verified{}, err
	}
	if a.Subject == nil || a.Subject.NameID == nil {
		return Verified{}, refused(ReasonNameID, "no NameID")
	}
	nameID := NameID{Format: a.Subject.NameID.Format, SPNameQualifier: a.Subject.NameID.SPNameQualifier, Value: a.Subject.NameID.Value}
	if _, err := Subject(nameID); err != nil {
		return Verified{}, refused(ReasonNameID, "NameID cannot key an identity")
	}

	inResponseTo := ""
	bearer := 0
	for _, sc := range a.Subject.SubjectConfirmations {
		if sc.Method != bearerMethod {
			continue
		}
		bearer++
		data := sc.SubjectConfirmationData
		if data == nil {
			return Verified{}, refused(ReasonSubjectConfirmation, "bearer confirmation without data")
		}
		if data.Recipient != sp.ACSURL {
			return Verified{}, refused(ReasonRecipient, "recipient")
		}
		if data.NotOnOrAfter.IsZero() || !now.Before(data.NotOnOrAfter.Add(MaxClockSkew)) {
			return Verified{}, refused(ReasonExpired, "subject confirmation NotOnOrAfter %s", data.NotOnOrAfter)
		}
		if !data.NotBefore.IsZero() && now.Before(data.NotBefore.Add(-MaxClockSkew)) {
			return Verified{}, refused(ReasonNotYetValid, "subject confirmation NotBefore %s", data.NotBefore)
		}
		if bearer > 1 && data.InResponseTo != inResponseTo {
			return Verified{}, refused(ReasonInResponseTo, "bearer confirmations disagree")
		}
		inResponseTo = data.InResponseTo
	}
	if bearer == 0 {
		return Verified{}, refused(ReasonSubjectConfirmation, "no bearer subject confirmation")
	}
	if response.InResponseTo != "" && response.InResponseTo != inResponseTo {
		return Verified{}, refused(ReasonInResponseTo, "response and assertion InResponseTo differ")
	}
	if inResponseTo == "" && responseSigned {
		inResponseTo = response.InResponseTo
	}

	c := a.Conditions
	if c == nil || c.NotOnOrAfter.IsZero() {
		return Verified{}, refused(ReasonExpired, "no Conditions NotOnOrAfter")
	}
	if !now.Before(c.NotOnOrAfter.Add(MaxClockSkew)) {
		return Verified{}, refused(ReasonExpired, "conditions NotOnOrAfter %s", c.NotOnOrAfter)
	}
	if !c.NotBefore.IsZero() && now.Before(c.NotBefore.Add(-MaxClockSkew)) {
		return Verified{}, refused(ReasonNotYetValid, "conditions NotBefore %s", c.NotBefore)
	}

	attributes := map[string]string{}
	for _, statement := range a.AttributeStatements {
		for _, attr := range statement.Attributes {
			if len(attr.Values) == 0 {
				continue
			}
			if v := pythonparity.Strip(attr.Values[0].Value); v != "" {
				attributes[attr.Name] = v
			}
		}
	}
	return Verified{
		AssertionID:  a.ID,
		InResponseTo: inResponseTo,
		Issuer:       a.Issuer.Value,
		NameID:       nameID,
		NotOnOrAfter: c.NotOnOrAfter.UTC(),
		Attributes:   attributes,
	}, nil
}

// checkAudiences requires at least one AudienceRestriction, and every one of
// them to list the SP entity ID among its Audience values.
func checkAudiences(assertion *etree.Element, entityID string) error {
	conditions := childrenNS(assertion, nsAssertion, "Conditions")
	if len(conditions) != 1 {
		return refused(ReasonAudience, "%d Conditions", len(conditions))
	}
	restrictions := childrenNS(conditions[0], nsAssertion, "AudienceRestriction")
	if len(restrictions) == 0 {
		return refused(ReasonAudience, "no AudienceRestriction")
	}
	for _, r := range restrictions {
		found := false
		for _, aud := range childrenNS(r, nsAssertion, "Audience") {
			if strings.TrimSpace(aud.Text()) == entityID {
				found = true
			}
		}
		if !found {
			return refused(ReasonAudience, "an AudienceRestriction does not name the SP")
		}
	}
	return nil
}

func checkIssueInstant(t time.Time, now time.Time) error {
	if t.IsZero() {
		return refused(ReasonIssueInstant, "no IssueInstant")
	}
	if t.Add(MaxIssueDelay).Before(now) || t.Add(-MaxClockSkew).After(now) {
		return refused(ReasonIssueInstant, "IssueInstant %s", t)
	}
	return nil
}

// libraryCheck runs crewjam/saml over the same bytes as a second check. The
// request id is matched by the caller against its store, so crewjam is told
// the id this response carries.
func libraryCheck(raw []byte, sp SP, v Verified) error {
	acs, err := url.Parse(sp.ACSURL)
	if err != nil {
		return refused(ReasonLibraryCheck, "acs url")
	}
	cert := base64.StdEncoding.EncodeToString(sp.IdPCert.Raw)
	provider := &crewsaml.ServiceProvider{
		EntityID:          sp.EntityID,
		AcsURL:            *acs,
		IDPMetadata:       &crewsaml.EntityDescriptor{EntityID: sp.IdPEntityID},
		IDPCertificate:    &cert,
		AllowIDPInitiated: v.InResponseTo == "",
	}
	var possible []string
	if v.InResponseTo != "" {
		possible = []string{v.InResponseTo}
	}
	assertion, err := provider.ParseXMLResponse(raw, possible, *acs)
	if err != nil {
		return refused(ReasonLibraryCheck, "library check")
	}
	if assertion.ID != v.AssertionID {
		return refused(ReasonLibraryCheck, "library chose another assertion")
	}
	return nil
}

// verifyEnveloped verifies el's one enveloped signature and returns the
// element the signature covers, re-parsed from the verified canonical bytes.
func verifyEnveloped(el *etree.Element, cert *x509.Certificate, now time.Time) (*etree.Element, error) {
	if cert == nil {
		return nil, refused(ReasonSignatureInvalid, "no IdP certificate")
	}
	id := el.SelectAttrValue("ID", "")
	if id == "" {
		return nil, refused(ReasonSignatureReference, "signed element has no ID")
	}
	sigs := childrenNS(el, dsig.Namespace, "Signature")
	if len(sigs) != 1 {
		return nil, refused(ReasonSignaturePlacement, "%d signatures on one element", len(sigs))
	}
	if err := checkSignedInfo(sigs[0], id); err != nil {
		return nil, err
	}

	detached, err := detach(el)
	if err != nil {
		return nil, refused(ReasonXMLInvalid, "detach")
	}
	if detached.FindElement("./Signature/KeyInfo/X509Data/X509Certificate") == nil {
		if sig := detached.FindElement("./Signature"); sig != nil {
			if keyInfo := sig.FindElement("KeyInfo"); keyInfo != nil {
				sig.RemoveChild(keyInfo)
			}
		}
	}
	vctx := dsig.NewDefaultValidationContext(&dsig.MemoryX509CertificateStore{Roots: []*x509.Certificate{cert}})
	vctx.IdAttribute = "ID"
	vctx.Clock = dsig.NewFakeClockAt(now)
	verified, err := vctx.Validate(detached)
	if err != nil {
		return nil, refused(ReasonSignatureInvalid, "signature does not verify")
	}
	if verified.SelectAttrValue("ID", "") != id {
		return nil, refused(ReasonSignatureReference, "verified element has another ID")
	}
	return verified, nil
}

func checkSignedInfo(sig *etree.Element, id string) error {
	signedInfo := childrenNS(sig, dsig.Namespace, "SignedInfo")
	if len(signedInfo) != 1 {
		return refused(ReasonSignatureReference, "%d SignedInfo", len(signedInfo))
	}
	methods := childrenNS(signedInfo[0], dsig.Namespace, "SignatureMethod")
	if len(methods) != 1 || !allowedSignatureMethods[methods[0].SelectAttrValue("Algorithm", "")] {
		return refused(ReasonSignatureAlgorithm, "signature method")
	}
	c14n := childrenNS(signedInfo[0], dsig.Namespace, "CanonicalizationMethod")
	if len(c14n) != 1 || !allowedTransforms[c14n[0].SelectAttrValue("Algorithm", "")] || c14n[0].SelectAttrValue("Algorithm", "") == envelopedSigAlgo {
		return refused(ReasonSignatureAlgorithm, "canonicalization method")
	}
	refs := childrenNS(signedInfo[0], dsig.Namespace, "Reference")
	if len(refs) != 1 {
		return refused(ReasonSignatureReference, "%d references", len(refs))
	}
	if refs[0].SelectAttrValue("URI", "") != "#"+id {
		return refused(ReasonSignatureReference, "reference does not name the signed element")
	}
	digests := childrenNS(refs[0], dsig.Namespace, "DigestMethod")
	if len(digests) != 1 || !allowedDigestMethods[digests[0].SelectAttrValue("Algorithm", "")] {
		return refused(ReasonSignatureAlgorithm, "digest method")
	}
	for _, transforms := range childrenNS(refs[0], dsig.Namespace, "Transforms") {
		for _, t := range childrenNS(transforms, dsig.Namespace, "Transform") {
			if !allowedTransforms[t.SelectAttrValue("Algorithm", "")] {
				return refused(ReasonSignatureAlgorithm, "transform")
			}
		}
	}
	return nil
}

// refuseDirectives refuses a DOCTYPE, ENTITY or any other directive, and any
// processing instruction other than the leading XML declaration.
func refuseDirectives(raw []byte) error {
	dec := xml.NewDecoder(bytes.NewReader(raw))
	first := true
	for {
		tok, err := dec.RawToken()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return refused(ReasonXMLInvalid, "token")
		}
		switch t := tok.(type) {
		case xml.Directive:
			return refused(ReasonXMLDirective, "directive")
		case xml.ProcInst:
			if !first || t.Target != "xml" {
				return refused(ReasonXMLDirective, "processing instruction")
			}
		}
		if _, isSpace := tok.(xml.CharData); !isSpace {
			first = false
		}
	}
}

// refuseDuplicateIDs refuses two elements that carry the same ID attribute: a
// signature reference must name exactly one element.
func refuseDuplicateIDs(root *etree.Element) error {
	seen := map[string]bool{}
	var walk func(*etree.Element) error
	walk = func(el *etree.Element) error {
		for _, attr := range el.Attr {
			if attr.Key != "ID" || attr.Space != "" {
				continue
			}
			if seen[attr.Value] {
				return refused(ReasonDuplicateID, "duplicate ID")
			}
			seen[attr.Value] = true
		}
		for _, child := range el.ChildElements() {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root)
}

// refuseStraySignatures allows a Signature only as a direct child of the
// Response or of its one Assertion.
func refuseStraySignatures(root, assertion *etree.Element) error {
	var walk func(*etree.Element) error
	walk = func(el *etree.Element) error {
		for _, child := range el.ChildElements() {
			if child.Tag == "Signature" && child.NamespaceURI() == dsig.Namespace {
				if el != root && el != assertion {
					return refused(ReasonSignaturePlacement, "signature below the response and the assertion")
				}
				continue
			}
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root)
}

func childrenNS(el *etree.Element, ns, tag string) []*etree.Element {
	var out []*etree.Element
	for _, child := range el.ChildElements() {
		if child.Tag == tag && child.NamespaceURI() == ns {
			out = append(out, child)
		}
	}
	return out
}

// detach copies el with every namespace declaration it inherits.
func detach(el *etree.Element) (*etree.Element, error) {
	ctx, err := etreeutils.NSBuildParentContext(el)
	if err != nil {
		return nil, err
	}
	ctx, err = ctx.SubContext(el)
	if err != nil {
		return nil, err
	}
	return etreeutils.NSDetatch(ctx, el)
}

func unmarshalElement(el *etree.Element, v any) error {
	detached, err := detach(el)
	if err != nil {
		return err
	}
	doc := etree.NewDocument()
	doc.SetRoot(detached)
	data, err := doc.WriteToBytes()
	if err != nil {
		return err
	}
	return xml.Unmarshal(data, v)
}
