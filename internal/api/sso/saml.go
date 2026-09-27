// samlMetadata, initiateSAMLAuth and samlACSCallback are
// api/auth/sso/router.py:get_saml_metadata / initiate_saml_auth /
// saml_acs_callback (CHAOS-6659), backed by services/sso.py's
// generate_saml_sp_metadata / generate_saml_auth_request_url /
// process_saml_response.
//
// Unlike CHAOS-6658's OIDC routes, nothing about the SAML flow is
// structurally dead in Python: process_saml_response never checks
// InResponseTo against the AuthnRequest initiateSAMLAuth generated, so the
// callback needs nothing persisted or embedded from the initiate step --
// its correctness rests entirely on the provider's own stored config
// (entity_id, sso_url, sp_acs_url, certificate) and the signed assertion's
// own claims. The one real reason this family is unusable in a real
// Python deployment today is a missing OPTIONAL dependency: signxml is
// declared under pyproject.toml's "enterprise-sso" extra, which the
// deployed image does not install, so _validate_saml_signature's
// importlib.import_module("signxml") always raises ImportError in
// production. That is a deployment gap, not a code gap, and it does not
// change what this port does: the real signature-verification logic is
// ported faithfully (goxmldsig, the XML-DSig engine
// github.com/crewjam/saml itself delegates to -- ADR CHAOS-4883 names
// crewjam/saml as the mandated SAML dependency), not skipped, exactly as
// D2725/the lead's STEP-0 answer records for this gap.
//
// The three routes share this file's entitlement gate (requireEntitlement,
// gate.go, D2725), the same real gate CHAOS-6658 built for OIDC: unlike
// OIDC's two routes, ALL THREE SAML routes -- including metadata, which
// Python also gates -- get the real gate here, since none of them needs
// the state-order workaround OIDC's public callback needed (see gate.go's
// own doc comment): metadata and initiate both look up the provider first
// too, for the same reason (no authenticated caller, org id comes from the
// row).
//
// Accepted simplifications, not requiring a ruling:
//   - The exact compressed bytes of the redirect AuthnRequest (raw DEFLATE
//   - base64) are not byte-for-byte matched against Python's zlib
//     output: an IdP only cares about the decompressed XML content, and
//     Go's compress/flate and Python's zlib do not produce identical
//     compressed bytes for the same input at "default" compression even
//     though both correctly implement RFC 1951 -- there is no real
//     "wrong" answer to converge on here, only two valid encodings of the
//     same request.
//   - The query string's key order (RelayState/SAMLRequest) is not
//     matched either: Go's url.Values.Encode sorts keys, Python's
//     urlencode(dict) preserves insertion order. A redirect URL's query
//     order has no functional meaning to any real HTTP client or IdP.
//   - Go's XML parsing (encoding/xml, which beevik/etree wraps) does not
//     resolve external entities or DTDs at all, structurally, unlike
//     Python's stdlib xml.etree (which needs defusedxml, imported here as
//     DefusedElementTree, specifically to add that protection). No
//     equivalent defusedxml import is needed on this side; the platform
//     itself does not have the vulnerability class defusedxml exists to
//     close.
package sso

import (
	"bytes"
	"compress/flate"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/crewjam/saml"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// defaultNameIDFormat is get_saml_config()'s own default for
// name_id_format, used by samlMetadata/initiateSAMLAuth (both still
// hand-built XML, unaffected by the crewjam/saml pivot below -- neither
// touches signature verification).
const defaultNameIDFormat = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"

// samlConfigValues is SSOProvider.get_saml_config().
type samlConfigValues struct {
	EntityID, SSOURL, SLOURL, Certificate, SPEntityID, SPACSURL, NameIDFormat string
	AttributeMapping                                                          map[string]string
}

func decodeSAMLConfig(raw *string) (samlConfigValues, error) {
	object, err := decodeConfigObject(raw)
	if err != nil {
		return samlConfigValues{}, err
	}
	mapping, err := objStringMap(object, "attribute_mapping")
	if err != nil {
		return samlConfigValues{}, err
	}
	nameIDFormat := objString(object, "name_id_format")
	if nameIDFormat == "" {
		nameIDFormat = defaultNameIDFormat
	}
	return samlConfigValues{
		EntityID: objString(object, "entity_id"), SSOURL: objString(object, "sso_url"),
		SLOURL: objString(object, "slo_url"), Certificate: objString(object, "certificate"),
		SPEntityID: objString(object, "sp_entity_id"), SPACSURL: objString(object, "sp_acs_url"),
		NameIDFormat: nameIDFormat, AttributeMapping: mapping,
	}, nil
}

// pyNone is a raw dict-subscript read of a key get_saml_config() always
// populates (with None when unconfigured): Python's f-string then
// interpolates that None as the literal text "None", not empty. Used only
// where router.py/services/sso.py reads config["sso_url"] this way (the
// AuthnRequest's Destination attribute and the redirect base URL); every
// other empty SAML config value in this file is a real "falls back to a
// computed default" case, not this one.
func pyNone(value string) string {
	if value == "" {
		return "None"
	}
	return value
}

func spEntityIDOf(cfg samlConfigValues, providerID, baseURL string) string {
	if cfg.SPEntityID != "" {
		return cfg.SPEntityID
	}
	return baseURL + "/saml/" + providerID + "/metadata"
}

func spACSURLOf(cfg samlConfigValues, providerID, baseURL string) string {
	if cfg.SPACSURL != "" {
		return cfg.SPACSURL
	}
	return baseURL + "/saml/" + providerID + "/acs"
}

// samlSPMetadataXML is generate_saml_sp_metadata's f-string, verbatim
// (indentation included -- this is a real response field, metadata_xml,
// not merely internal plumbing).
func samlSPMetadataXML(entityID, acsURL, nameIDFormat string) string {
	return `<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
                     entityID="` + entityID + `">
    <md:SPSSODescriptor AuthnRequestsSigned="false"
                        WantAssertionsSigned="true"
                        protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
        <md:NameIDFormat>` + nameIDFormat + `</md:NameIDFormat>
        <md:AssertionConsumerService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
                                     Location="` + acsURL + `"
                                     index="0"
                                     isDefault="true"/>
    </md:SPSSODescriptor>
</md:EntityDescriptor>`
}

// samlMetadata is GET /saml/{provider_id}/metadata.
func (h handlers) samlMetadata(w http.ResponseWriter, r *http.Request) {
	providerID, err := pythonparity.ParseUUID(r.PathValue("provider_id"))
	if err != nil {
		h.fail(w, r, "parse provider id", err)
		return
	}
	ctx := r.Context()
	row, err := scanProvider(h.Pool.QueryRow(ctx, `SELECT `+providerColumns+` FROM sso_providers WHERE id = $1::uuid`, providerID))
	if errorsIsNoRows(err) {
		policy.WriteDetail(w, http.StatusNotFound, "SSO provider not found", nil)
		return
	}
	if err != nil {
		h.fail(w, r, "load provider", err)
		return
	}
	if !h.requireEntitlement(w, r, row.OrgID) {
		return
	}
	if row.Protocol != "saml" {
		policy.WriteDetail(w, http.StatusBadRequest, "Provider is not SAML", nil)
		return
	}
	config, err := decodeSAMLConfig(row.Config)
	if err != nil {
		h.fail(w, r, "decode saml config", err)
		return
	}
	base := appBaseURL()
	entityID := spEntityIDOf(config, row.ID, base)
	acsURL := spACSURLOf(config, row.ID, base)
	out := pyjson.NewObject()
	out.Set("metadata_xml", samlSPMetadataXML(entityID, acsURL, config.NameIDFormat))
	out.Set("entity_id", entityID)
	out.Set("acs_url", acsURL)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// deflateRaw is zlib.compress(data)[2:-4]: the raw DEFLATE stream with no
// zlib header or Adler-32 trailer -- what compress/flate already produces
// natively, with no header to strip.
func deflateRaw(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	fw, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(data); err != nil {
		return nil, err
	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// initiateSAMLAuth is POST /saml/{provider_id}/initiate.
func (h handlers) initiateSAMLAuth(w http.ResponseWriter, r *http.Request) {
	providerID, err := pythonparity.ParseUUID(r.PathValue("provider_id"))
	if err != nil {
		h.fail(w, r, "parse provider id", err)
		return
	}
	rawBody, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	object, ok := errs.Object(rawBody)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	relayState, relaySet := errs.OptionalString(object, "relay_state", 0, 0)
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	ctx := r.Context()
	row, err := scanProvider(h.Pool.QueryRow(ctx, `SELECT `+providerColumns+` FROM sso_providers WHERE id = $1::uuid`, providerID))
	if errorsIsNoRows(err) {
		policy.WriteDetail(w, http.StatusNotFound, "SSO provider not found", nil)
		return
	}
	if err != nil {
		h.fail(w, r, "load provider", err)
		return
	}
	if !h.requireEntitlement(w, r, row.OrgID) {
		return
	}
	if row.Protocol != "saml" {
		policy.WriteDetail(w, http.StatusBadRequest, "Provider is not SAML", nil)
		return
	}
	if row.Status != "active" {
		policy.WriteDetail(w, http.StatusBadRequest, "SSO provider is not active", nil)
		return
	}
	config, err := decodeSAMLConfig(row.Config)
	if err != nil {
		h.fail(w, r, "decode saml config", err)
		return
	}
	base := appBaseURL()
	entityID := spEntityIDOf(config, row.ID, base)
	acsURL := spACSURLOf(config, row.ID, base)
	requestIDHex, err := newJTI() // secrets.token_hex(16): 16 random bytes, lowercase hex.
	if err != nil {
		h.fail(w, r, "generate request id", err)
		return
	}
	requestID := "_id" + requestIDHex
	issueInstant := h.Now().UTC().Format("2006-01-02T15:04:05Z")
	destination := pyNone(config.SSOURL)

	authnRequest := `<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
    ID="` + requestID + `"
    Version="2.0"
    IssueInstant="` + issueInstant + `"
    Destination="` + destination + `"
    ProtocolBinding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
    AssertionConsumerServiceURL="` + acsURL + `">
    <saml:Issuer>` + entityID + `</saml:Issuer>
    <samlp:NameIDPolicy Format="` + config.NameIDFormat + `"
                        AllowCreate="true"/>
</samlp:AuthnRequest>`

	deflated, err := deflateRaw([]byte(authnRequest))
	if err != nil {
		h.fail(w, r, "deflate authn request", err)
		return
	}
	params := url.Values{"SAMLRequest": {base64.StdEncoding.EncodeToString(deflated)}}
	if relaySet && relayState != "" {
		params.Set("RelayState", relayState)
	}
	out := pyjson.NewObject()
	out.Set("redirect_url", destination+"?"+params.Encode())
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// parseSAMLCertificate reads a stored sso_providers.config "certificate"
// value as either a PEM block or bare base64 DER -- signxml's x509_cert
// accepts either shape in practice, and this repo's own config schema
// (SAMLConfigInput) declares the field as an unconstrained str, so this
// port accepts whichever an admin actually pasted in. Its output is
// re-encoded to bare base64 for saml.ServiceProvider.IDPCertificate,
// which (like signxml's x509_cert) wants exactly that shape.
func parseSAMLCertificate(raw string) (*x509.Certificate, error) {
	if block, _ := pem.Decode([]byte(raw)); block != nil {
		return x509.ParseCertificate(block.Bytes)
	}
	der, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(raw), ""))
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(der)
}

// samlClaims is process_saml_response's returned dict, the fields the
// router reads from it (relay_state and attributes are read by
// router.py's caller in this repo's Python source, but neither is used by
// anything the ACS route's response depends on, so this port carries only
// what actually reaches an observable outcome).
type samlClaims struct {
	email, fullName, externalID string
}

// processSAMLResponse is process_saml_response, delegating XML parsing,
// signature verification and the OASIS structural checks (Status, Issuer,
// Audience, Recipient, Conditions/SubjectConfirmationData timestamps) to
// github.com/crewjam/saml's own ServiceProvider.ParseXMLResponse -- the
// ADR CHAOS-4883-mandated SAML dependency's actual exported API, not a
// hand-assembled sequence of goxmldsig calls.
//
// D2737 (team-lead): a hand-written first cut of this function (calling
// goxmldsig's ValidationContext.Validate directly on a found signature
// element) failed to verify EVERY correctly-signed test fixture in this
// environment, tampered or not -- confirmed via ~8 isolated repro
// programs across two goxmldsig versions, with matched dependency pins
// (github.com/beevik/etree v1.7.0, exactly what goxmldsig v1.6.1's own
// go.mod names; go mod graph confirms a single goxmldsig version in the
// graph). Reproduced with crewjam/saml's OWN full IdentityProvider-signs
// / ServiceProvider-verifies round trip (a throwaway program built from
// their exported types, not the hand-assembled path): that round trip
// verifies correctly in this same environment. So the fault was in this
// package's own low-level use of goxmldsig, not the environment or the
// dependency itself; using crewjam/saml's own verification entry point
// (which also calls goxmldsig internally, just via a path this package's
// hand-written one didn't replicate correctly) is the fix, per D2737.
//
// Some structural checks Python performs are RELAXED or absent here,
// each recorded rather than silently matched or silently improved:
//   - InResponseTo: Python's process_saml_response never checks it (no
//     state persists between initiate and callback, matching the same
//     stateless design OIDC's port needed a real fix for -- SAML simply
//     never depended on any generated-at-initiate value to begin with,
//     so there is nothing broken to fix here). ServiceProvider.
//     AllowIDPInitiated=true, with possibleRequestIDs left nil, makes
//     crewjam/saml skip this check the same way -- a real, supported
//     mode for exactly this shape of deployment, not a workaround.
//   - The trusted certificate's own NotBefore/NotAfter window IS checked
//     by goxmldsig (Python's bare signxml call does not); recorded as an
//     accepted, minor, security-positive delta, not a ruling.
func processSAMLResponse(row *providerRow, config samlConfigValues, base, samlResponse string, now time.Time) (samlClaims, error) {
	spEntityID := spEntityIDOf(config, row.ID, base)
	spACSURL := spACSURLOf(config, row.ID, base)

	// D2738: the payload itself (missing, or not valid base64) is entirely
	// caller-controlled -- an attacker chooses to send an empty body or
	// garbage with zero credentials -- so it goes in the unauthenticated
	// bucket, same as OIDC's state-authentication failures: no provider
	// row mutation, audit-only. This is distinct from the certificate/ACS
	// URL checks just below, which are derived from the PROVIDER's own
	// stored config and fail identically for every caller including a
	// genuine IdP -- those legitimately flip the row.
	if samlResponse == "" {
		return samlClaims{}, ssoAuthErr("Missing SAMLResponse payload")
	}
	decoded, err := base64.StdEncoding.DecodeString(samlResponse)
	if err != nil {
		return samlClaims{}, ssoAuthErr("Invalid base64 SAMLResponse")
	}
	if config.Certificate == "" {
		return samlClaims{}, ssoErr("SAML certificate is required for validation")
	}
	cert, err := parseSAMLCertificate(config.Certificate)
	if err != nil {
		return samlClaims{}, ssoErr("SAML signature validation failed")
	}
	certBase64 := base64.StdEncoding.EncodeToString(cert.Raw)
	acsURL, err := url.Parse(spACSURL)
	if err != nil {
		return samlClaims{}, ssoErr("Invalid SAML XML")
	}
	sp := &saml.ServiceProvider{
		EntityID:          spEntityID,
		AcsURL:            *acsURL,
		IDPMetadata:       &saml.EntityDescriptor{EntityID: config.EntityID},
		IDPCertificate:    &certBase64,
		AllowIDPInitiated: true,
	}
	// D2738: ParseXMLResponse is this protocol's authentication step --
	// the SAML analog of OIDC's verifyOIDCState -- so its failure (a
	// forged, unsigned, tampered, expired, or wrong-issuer assertion) is
	// unauthenticated, exactly like an OIDC state that fails to verify:
	// no provider row mutation, audit-only. See ssoUnauthenticated's doc
	// comment (login.go) for why this differs from the cert/ACS-URL
	// config checks above.
	//
	// An earlier version of this function carved the response-level issuer
	// check ("response Issuer does not match the IDP metadata") out into
	// the flipping bucket, reasoning it was provider-config-derived and
	// not attacker-forgeable. Team-lead's D2738 follow-up correctly
	// challenged that: crewjam/saml's own parseResponse (service_provider.go)
	// computes the Response signature's validity into a local variable but
	// explicitly DEFERS acting on it ("we're deferring taking action on
	// the signature validation until after we've processed the request
	// attributes") until AFTER this exact issuer check, and returns early
	// on a mismatch before ever consulting that deferred error. So an
	// entirely UNSIGNED, attacker-forged Response with a garbage Issuer
	// value reaches this identical error text -- there is no way for this
	// package to tell "genuinely signed, wrong config" apart from
	// "unsigned garbage" from the returned error alone. The carve-out is
	// therefore unsafe and removed: every ParseXMLResponse failure,
	// including a genuine entity_id misconfiguration, is unauthenticated.
	// (The assertion-level issuer check, a separate code path gated behind
	// signature verification, IS safe by this same reasoning, but nothing
	// here can select for it: ParseXMLResponse just returns whichever
	// error fires first.) Recorded, not re-litigated: a real config
	// mismatch simply surfaces via the audit trail rather than
	// sso_providers.status/last_error -- the same tradeoff OIDC's cross-
	// provider-replay case already makes.
	assertion, err := sp.ParseXMLResponse(decoded, nil, *acsURL)
	if err != nil {
		reason := err.Error()
		if invalid, ok := err.(*saml.InvalidResponseError); ok && invalid.PrivateErr != nil {
			// The caller-visible detail stays the router's fixed message
			// (samlACSCallback's own "SAML authentication failed",
			// matching Python's collapse of every SSOProcessingError into
			// one string); ONLY the recorded reason (last_error/audit,
			// never shown to the caller) gets the real one.
			reason = invalid.PrivateErr.Error()
		}
		return samlClaims{}, ssoAuthErr(reason)
	}
	// r1 review (CHAOS-6659, P2): crewjam/saml's own validateAssertion
	// (service_provider.go) iterates assertion.Subject.SubjectConfirmations
	// but does not reject an EMPTY list -- an assertion with zero
	// SubjectConfirmation elements skips the per-confirmation Recipient and
	// NotOnOrAfter checks entirely (the range loop body simply never
	// runs) and sails through ParseXMLResponse with no error. Python's own
	// process_saml_response explicitly requires at least one
	// (`if subject_confirmation is None: raise SAMLProcessingError(...)`,
	// sso.py:576-578) -- a genuine parity break, Go strictly more
	// permissive than Python, not a delta to preserve. Closed here by
	// requiring the same precondition Python does, in the same relative
	// position (after signature/status/issuer/audience, before this
	// port's own attribute extraction).
	if assertion.Subject == nil || len(assertion.Subject.SubjectConfirmations) == 0 {
		return samlClaims{}, ssoErr("SAML subject confirmation missing")
	}

	var nameID string
	if assertion.Subject != nil && assertion.Subject.NameID != nil {
		nameID = assertion.Subject.NameID.Value
	}
	attributes := map[string]string{}
	for _, statement := range assertion.AttributeStatements {
		for _, attr := range statement.Attributes {
			if len(attr.Values) == 0 {
				continue
			}
			if value := pythonparity.Strip(attr.Values[0].Value); value != "" {
				attributes[attr.Name] = value
			}
		}
	}
	mapped := mapAttributes(toAnyMap(attributes), config.AttributeMapping)

	email := mapped["email"]
	if email == "" {
		email = attributes["email"]
	}
	if email == "" && nameID != "" && strings.Contains(nameID, "@") {
		email = nameID
	}
	if email == "" {
		return samlClaims{}, ssoErr("SAML assertion missing email attribute")
	}
	fullName := mapped["full_name"]
	if fullName == "" {
		fullName = mapped["name"]
	}
	return samlClaims{email: email, fullName: fullName, externalID: nameID}, nil
}

func toAnyMap(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// samlACSCallback is POST /saml/{provider_id}/acs.
func (h handlers) samlACSCallback(w http.ResponseWriter, r *http.Request) {
	providerID, err := pythonparity.ParseUUID(r.PathValue("provider_id"))
	if err != nil {
		h.fail(w, r, "parse provider id", err)
		return
	}
	rawBody, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	object, ok := errs.Object(rawBody)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	m := pybody.Model{Errors: &errs, Object: object, Loc: []pyjson.Value{"body"}}
	samlResponseField := pybody.GetAlias(m, "SAMLResponse", "saml_response", pybody.Required, pybody.Str)
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	samlResponse := samlResponseField.Value

	ctx := r.Context()
	row, err := scanProvider(h.Pool.QueryRow(ctx, `SELECT `+providerColumns+` FROM sso_providers WHERE id = $1::uuid`, providerID))
	if errorsIsNoRows(err) {
		policy.WriteDetail(w, http.StatusNotFound, "SSO provider not found", nil)
		return
	}
	if err != nil {
		h.fail(w, r, "load provider", err)
		return
	}
	if !h.requireEntitlement(w, r, row.OrgID) {
		return
	}
	if row.Protocol != "saml" {
		policy.WriteDetail(w, http.StatusBadRequest, "Provider is not SAML", nil)
		return
	}
	if row.Status != "active" {
		policy.WriteDetail(w, http.StatusBadRequest, "SSO provider is not active", nil)
		return
	}
	config, err := decodeSAMLConfig(row.Config)
	if err != nil {
		h.fail(w, r, "decode saml config", err)
		return
	}

	claims, err := processSAMLResponse(row, config, appBaseURL(), samlResponse, h.Now())
	if err != nil {
		var unauth ssoUnauthenticated
		if asSSOUnauthenticated(err, &unauth) {
			h.recordSSOUnauthenticated(ctx, w, r, row.OrgID, providerID, unauth.msg, http.StatusBadRequest,
				"SAML authentication failed", "saml", "signature_auth")
			return
		}
		var processing ssoProcessing
		if !asSSOProcessing(err, &processing) {
			h.fail(w, r, "saml callback", err)
			return
		}
		h.recordSSOFailure(ctx, w, r, row.OrgID, providerID, processing.msg, http.StatusBadRequest,
			"SAML authentication failed", "saml", "")
		return
	}

	email := claims.email
	if email == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "Email address is missing from SAML response", nil)
		return
	}
	allowedDomains, err := decodeAllowedDomains(row.AllowedDomains)
	if err != nil {
		h.fail(w, r, "decode allowed domains", err)
		return
	}
	if len(allowedDomains) > 0 {
		at := strings.LastIndex(email, "@")
		if at < 0 {
			policy.WriteDetail(w, http.StatusBadRequest, "Email address from SAML response is malformed", nil)
			return
		}
		domain := email[at+1:]
		if !containsFold(allowedDomains, domain) {
			policy.WriteDetail(w, http.StatusForbidden, fmt.Sprintf("Email domain '%s' is not allowed for this provider", domain), nil)
			return
		}
	}

	user, membership, err := h.provisionOrGetUser(ctx, row, email, claims.fullName, providerID, claims.externalID)
	if err != nil {
		var processing ssoProcessing
		if !asSSOProcessing(err, &processing) {
			h.fail(w, r, "saml provisioning", err)
			return
		}
		h.recordSSOFailure(ctx, w, r, row.OrgID, providerID, processing.msg, http.StatusBadRequest,
			"SAML user provisioning failed", "saml", "provisioning")
		return
	}

	h.finishSSOLogin(ctx, w, r, providerID, row.OrgID, user, membership, "saml")
}
