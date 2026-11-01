package idpstub

import "errors"

import (
	"net/http"

	"time"

	"github.com/beevik/etree"
)

const (
	nsProtocol  = "urn:oasis:names:tc:SAML:2.0:protocol"
	nsAssertion = "urn:oasis:names:tc:SAML:2.0:assertion"
	nsMetadata  = "urn:oasis:names:tc:SAML:2.0:metadata"

	// DefaultIdPEntityID, DefaultSPEntityID and DefaultACSURL are what a zero SAMLOptions and a zero ResponseSpec use.
	DefaultIdPEntityID = "https://idp.example.test/metadata"
	DefaultSPEntityID  = "https://sp.example.test/saml/metadata"
	DefaultACSURL      = "https://sp.example.test/saml/acs"

	NameIDPersistent = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"
	statusSuccess    = "urn:oasis:names:tc:SAML:2.0:status:Success"
)

// NameID is a SAML NameID.
type NameID struct {
	Format, SPNameQualifier, Value string
}

// SAMLUser is one account the stub signs in.
type SAMLUser struct {
	NameID   NameID
	Email    string
	FullName string
}

// SAMLOptions configures NewSAML.
type SAMLOptions struct {
	Keys     *Keys
	Clock    func() time.Time
	EntityID string
	// SSOURL is the single sign-on URL the metadata names. Empty means EntityID with /sso in place of /metadata.
	SSOURL string
	// SPEntityID and ACSURL are the defaults of a ResponseSpec.
	SPEntityID, ACSURL string
	// AllowedACS, when set, lists the only ACS URLs the sign-on handler posts to.
	AllowedACS []string
	Users      []SAMLUser
}

// ResponseSpec describes one response. The zero value is a valid, assertion-signed response for the default SP,
// issued now, for the first user.
type ResponseSpec struct {
	ResponseID, AssertionID string
	SPEntityID, ACSURL      string
	Destination, Recipient  *string
	Audiences               [][]string
	NoAudience              bool
	InResponseTo            string
	NameID                  NameID
	Email, FullName         string
	Issuer                  string
	// WrongIssuerAt is "response" or "assertion": only that element carries a foreign issuer.
	WrongIssuerAt                                            string
	Status                                                   string
	IssueInstant, NotBefore, NotOnOrAfter                    time.Time
	ConditionsOnlyNotOnOrAfter, ConfirmationOnlyNotOnOrAfter time.Time
	NoConfirmation, SenderVouches                            bool
	SignResponse, UnsignedAssertion                          bool
	SigMethod                                                string
	NameIDComment                                            bool
	// SkipSign strips every signature block after signing; TamperAfterSign edits the email in the signed text.
	SkipSign, TamperAfterSign bool
}

// SAML is a SAML identity provider that signs responses.
type SAML struct {
	opts SAMLOptions
	keys *Keys
	now  func() time.Time
}

// NewSAML builds the provider; keys are generated unless opts.Keys is set.
func NewSAML(opts SAMLOptions) (*SAML, error) {
	return *new(*SAML), errors.New("idpstub: not implemented")
}

// Keys returns the signing key material.
func (s *SAML) Keys() *Keys { return *new(*Keys) }

// EntityID is the issuer of every response.
func (s *SAML) EntityID() string { return *new(string) }

// User returns the configured user with this NameID value or email.
func (s *SAML) User(nameIDOrEmail string) (SAMLUser, bool) { return *new(SAMLUser), *new(bool) }

func randomHex() string { return *new(string) }

func strp(s string) *string { return *new(*string) }

// Build assembles and signs one response and returns the document, the Response element and the Assertion element.
func (s *SAML) Build(spec ResponseSpec) (*etree.Document, *etree.Element, *etree.Element, error) {
	return *new(*etree.Document), *new(*etree.Element), *new(*etree.Element), errors.New("idpstub: not implemented")
}

func (s *SAML) sign(el *etree.Element, method string) (*etree.Element, error) {
	return *new(*etree.Element), errors.New("idpstub: not implemented")
}

// Response is the serialized response XML.
func (s *SAML) Response(spec ResponseSpec) ([]byte, error) {
	return *new([]byte), errors.New("idpstub: not implemented")
}

// ResponseB64 is the response base64-encoded, the form a browser posts as SAMLResponse.
func (s *SAML) ResponseB64(spec ResponseSpec) (string, error) {
	return *new(string), errors.New("idpstub: not implemented")
}

// Metadata is the IdP metadata document: entity id, signing certificate and the single sign-on endpoints.
func (s *SAML) Metadata() []byte { return *new([]byte) }

// Handler serves GET /saml/metadata, GET /saml/cert (PEM) and GET or POST /saml/sso.
func (s *SAML) Handler() http.Handler {
	return *new(http.Handler)
}

func (s *SAML) serveSSO(w http.ResponseWriter, r *http.Request) {}

func decodeAuthnRequest(posted string) (*etree.Document, error) {
	return *new(*etree.Document), errors.New("idpstub: not implemented")
}
