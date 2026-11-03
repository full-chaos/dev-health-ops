package tokencorpus

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"sort"
	"strings"
)

// The fixed values the service account vectors add to the shared ones. Only
// the keys are fresh.
const (
	TypServiceAccount     = "sa+jwt"
	ServiceAccountCeiling = 300
	ServiceAccountLife    = 300
	ServiceAccountSub     = "prn_AAAAAAAAAAAAAAAAAAAAAA"
	ServiceAccountOrg     = "00000000-0000-4000-8000-000000000001"
	ServiceAccountCid     = "crd_AAAAAAAAAAAAAAAAAAAAAA"
	ServiceAccountNow     = FixedIat + 100
)

// serviceAccountClaims is the payload of a service account vector: the
// account, its organization and the key row, no session, no scope. extra holds
// members a refusal vector adds, as raw text; drop names members it leaves out.
type serviceAccountClaims struct {
	iss, aud, sub, ptype, org, cid, jti string
	iat, nbf, exp                       int64
	rawSub, rawAud                      string
	drop                                []string
	extra                               []member
}

func baseServiceAccount() serviceAccountClaims {
	return serviceAccountClaims{
		iss: Issuer, aud: AudienceAPI, sub: ServiceAccountSub, ptype: "service_account",
		org: ServiceAccountOrg, cid: ServiceAccountCid,
		jti: "000000000000000000000001", iat: FixedIat, nbf: FixedIat, exp: FixedIat + ServiceAccountLife,
	}
}

func (c serviceAccountClaims) text() string {
	q := func(s string) string { return fmt.Sprintf("%q", s) }
	sub, aud := q(c.sub), q(c.aud)
	if c.rawSub != "" {
		sub = c.rawSub
	}
	if c.rawAud != "" {
		aud = c.rawAud
	}
	all := []member{
		{"iss", q(c.iss)}, {"aud", aud}, {"sub", sub}, {"principal_type", q(c.ptype)},
		{"org_id", q(c.org)}, {"cid", q(c.cid)},
		{"jti", q(c.jti)}, {"iat", fmt.Sprint(c.iat)}, {"nbf", fmt.Sprint(c.nbf)}, {"exp", fmt.Sprint(c.exp)},
	}
	var members []member
	for _, m := range all {
		dropped := false
		for _, d := range c.drop {
			dropped = dropped || d == m.key
		}
		if !dropped {
			members = append(members, m)
		}
	}
	return object(append(members, c.extra...)...)
}

func serviceAccountContractRefusal(stage, why string) Entry {
	return Entry{Now: FixedNow, VerifyAs: AudienceAPI, Expect: Reject, Reason: ReasonContract, Stage: stage, Why: why}
}

// GenerateServiceAccount builds the service-account-token.v1 corpus with fresh
// keys drawn from random (crypto/rand when nil). Everything but the keys, and
// so the signatures, is fixed.
func GenerateServiceAccount(random io.Reader) (*Corpus, error) {
	newKey := func() (ed25519.PublicKey, ed25519.PrivateKey, error) { return ed25519.GenerateKey(random) }
	activePub, activeKey, err := newKey()
	if err != nil {
		return nil, err
	}
	overlapPub, overlapKey, err := newKey()
	if err != nil {
		return nil, err
	}
	_, strangerKey, err := newKey()
	if err != nil {
		return nil, err
	}
	removedPub, removedKey, err := newKey()
	if err != nil {
		return nil, err
	}
	hs256 := func(secret []byte) signer {
		return func(input []byte) []byte {
			mac := hmac.New(sha256.New, secret)
			mac.Write(input)
			return mac.Sum(nil)
		}
	}
	none := func([]byte) []byte { return nil }

	b := &builder{vectors: map[string]Vector{}}
	good := headerText("EdDSA", TypServiceAccount, KidActive)
	w := baseServiceAccount()
	now := int64(ServiceAccountNow)
	active := edSigner(activeKey)
	ok := func(name string, header string, c serviceAccountClaims, sign signer, at int64, why string) {
		b.add(name, header, c.text(), sign, accept(at, AudienceAPI, why))
	}
	refuse := func(name string, header string, payload string, sign signer, e Entry) {
		b.add(name, header, payload, sign, e)
	}
	schemaRefusal := func(why string) Entry { return serviceAccountContractRefusal("schema", why) }

	ok("valid-dev-health-api", good, w, active, now, "The ordinary service account token for dev-health-api, typ sa+jwt, signed by the active key.")
	ok("valid-signed-by-overlap-key", headerText("EdDSA", TypServiceAccount, KidOverlap), w, edSigner(overlapKey), now, "Signed by the key published for the rotation overlap: both keys of the set verify.")
	ok("valid-at-nbf", good, w, active, w.nbf, "The clock equals nbf: valid from nbf inclusive.")
	ok("valid-last-second-before-exp", good, w, active, w.exp-1, "One second before exp: the last valid instant.")
	atCeiling := w
	atCeiling.exp = atCeiling.iat + ServiceAccountCeiling
	ok("valid-lifetime-at-the-ceiling", good, atCeiling, active, now, "exp - iat equals the ceiling (300 s): the longest lifetime a verifier accepts.")
	short := w
	short.exp = short.iat + 42
	ok("valid-lifetime-short-key-expiry", good, short, active, short.iat+10, "exp - iat of 42 s: a key close to its expiry gets a shorter token, never a longer one.")
	delayed := w
	delayed.nbf = delayed.iat + 60
	ok("valid-delayed-nbf-at-nbf", good, delayed, active, delayed.nbf, "nbf later than iat: the clock equals nbf, so the token is valid.")

	refuse("invalid-alg-none", headerText("none", TypServiceAccount, KidActive), w.text(), none, schemaRefusal("alg none with an empty signature."))
	refuse("invalid-alg-hs256-public-key-as-secret", headerText("HS256", TypServiceAccount, KidActive), w.text(), hs256(activePub), schemaRefusal("alg HS256 with a valid HMAC keyed by the published public key: the symmetric-confusion shape."))
	refuse("invalid-alg-hs256-unrelated-secret", headerText("HS256", TypServiceAccount, KidActive), w.text(), hs256([]byte("corpus-hs256-secret-not-a-real-one")), schemaRefusal("alg HS256 with a valid HMAC under some other secret."))
	refuse("invalid-alg-lowercase", headerText("eddsa", TypServiceAccount, KidActive), w.text(), active, schemaRefusal("alg spelled eddsa with a valid EdDSA signature: the name is case-sensitive."))

	for _, c := range []struct{ name, typ, why string }{
		{"invalid-typ-access-token", "at+jwt", "typ at+jwt with a valid signature: the user access token's type is never a service account token's."},
		{"invalid-typ-workload-token", "workload+jwt", "typ workload+jwt with a valid signature: the workload token's type is never a service account token's."},
		{"invalid-typ-jwt-uppercase", "JWT", "typ JWT: the HS256 token's type."},
		{"invalid-typ-jwt-lowercase", "jwt", "typ jwt: the effective principal envelope's type."},
		{"invalid-typ-uppercase", "SA+JWT", "typ is case-sensitive: exact bytes."},
		{"invalid-typ-mixed-case", "Sa+Jwt", "typ is case-sensitive: no mixed-case form."},
		{"invalid-typ-application-prefix", "application/sa+jwt", "an application/-prefixed media type is not the typ."},
		{"invalid-typ-trailing-space", "sa+jwt ", "a trailing space is not the typ."},
		{"invalid-typ-empty", "", "an empty typ."},
	} {
		refuse(c.name, headerText("EdDSA", c.typ, KidActive), w.text(), active, schemaRefusal(c.why))
	}
	refuse("invalid-typ-absent", `{"alg":"EdDSA","kid":"`+KidActive+`"}`, w.text(), active, schemaRefusal("no typ member: a missing typ is refused."))
	refuse("invalid-kid-absent", `{"alg":"EdDSA","typ":"`+TypServiceAccount+`"}`, w.text(), active, schemaRefusal("no kid member."))
	refuse("invalid-kid-empty", headerText("EdDSA", TypServiceAccount, ""), w.text(), active, schemaRefusal("an empty kid."))

	refuse("invalid-duplicate-header-typ", `{"alg":"EdDSA","typ":"at+jwt","typ":"`+TypServiceAccount+`","kid":"`+KidActive+`"}`, w.text(), active, serviceAccountContractRefusal("duplicate_member", "typ appears twice (at+jwt, then sa+jwt); the signature is valid over those bytes."))
	refuse("invalid-duplicate-header-alg", `{"alg":"EdDSA","alg":"none","typ":"`+TypServiceAccount+`","kid":"`+KidActive+`"}`, w.text(), active, serviceAccountContractRefusal("duplicate_member", "alg appears twice in the header."))
	refuse("invalid-duplicate-header-kid", `{"alg":"EdDSA","typ":"`+TypServiceAccount+`","kid":"`+KidActive+`","kid":"`+KidOverlap+`"}`, w.text(), active, serviceAccountContractRefusal("duplicate_member", "kid appears twice with two published kids."))
	refuse("invalid-duplicate-claim-aud", good, strings.Replace(w.text(), `"aud":"dev-health-api"`, `"aud":"acr-api","aud":"dev-health-api"`, 1), active, serviceAccountContractRefusal("duplicate_member", "aud appears twice (acr-api, then dev-health-api); the signature is valid over those bytes."))
	refuse("invalid-duplicate-claim-cid", good, strings.Replace(w.text(), `"cid":"`+ServiceAccountCid+`"`, `"cid":"crd_BBBBBBBBBBBBBBBBBBBBBB","cid":"`+ServiceAccountCid+`"`, 1), active, serviceAccountContractRefusal("duplicate_member", "cid appears twice with two key row ids."))
	refuse("invalid-duplicate-claim-exp", good, strings.Replace(w.text(), fmt.Sprintf(`"exp":%d`, w.exp), fmt.Sprintf(`"exp":%d,"exp":%d`, w.exp, w.exp+3600), 1), active, serviceAccountContractRefusal("duplicate_member", "exp appears twice."))

	for _, c := range []struct{ name, aud, why string }{
		{"invalid-aud-auth-service", "auth-service", "auth-service: a service account token has one audience, dev-health-api."},
		{"invalid-aud-acr-api", AudienceACR, "acr-api: carried by the user access token only."},
		{"invalid-aud-acr-mcp", AudienceMCP, "acr-mcp: carried by the user access token only."},
		{"invalid-aud-query-api", "query-api", "query-api: carried only by the effective principal envelope."},
		{"invalid-aud-unknown", "billing-edge", "an audience no token carries."},
	} {
		x := w
		x.aud = c.aud
		refuse(c.name, good, x.text(), active, schemaRefusal(c.why))
	}
	arrayAud := w
	arrayAud.rawAud = `["dev-health-api"]`
	refuse("invalid-aud-array", good, arrayAud.text(), active, schemaRefusal("an array of one audience: the claim is one string."))

	for _, c := range []struct{ name, ptype, why string }{
		{"invalid-principal-type-human", "human", "principal_type human: a human holds a user access token."},
		{"invalid-principal-type-workload", "workload", "principal_type workload: a workload holds a workload token."},
	} {
		x := w
		x.ptype = c.ptype
		refuse(c.name, good, x.text(), active, schemaRefusal(c.why))
	}
	for _, c := range []struct{ name, member, why string }{
		{"invalid-org-id-absent", "org_id", "no org_id: a service account token names its organization."},
		{"invalid-cid-absent", "cid", "no cid: the key row the token came from is named."},
		{"invalid-principal-type-absent", "principal_type", "no principal_type member."},
		{"invalid-sub-absent", "sub", "no sub member."},
		{"invalid-jti-absent", "jti", "no jti member."},
		{"invalid-nbf-absent", "nbf", "no nbf member."},
	} {
		x := w
		x.drop = []string{c.member}
		refuse(c.name, good, x.text(), active, schemaRefusal(c.why))
	}
	for _, c := range []struct{ name, org, cid, why string }{
		{"invalid-org-id-uppercase", "00000000-0000-4000-8000-00000000000A", w.cid, "an upper-case UUID: the lowercase form only."},
		{"invalid-org-id-not-a-uuid", "org_AAAAAAAAAAAAAAAAAAAAAA", w.cid, "the organization wire id, not the UUID."},
		{"invalid-cid-no-prefix", w.org, "AAAAAAAAAAAAAAAAAAAAAA", "a bare id without the crd_ prefix."},
		{"invalid-cid-principal-prefix", w.org, "prn_AAAAAAAAAAAAAAAAAAAAAA", "a principal id in place of the key row id."},
		{"invalid-cid-over-length", w.org, "crd_" + strings.Repeat("A", 65), "a key row id over 64 characters after the prefix."},
	} {
		x := w
		x.org, x.cid = c.org, c.cid
		refuse(c.name, good, x.text(), active, schemaRefusal(c.why))
	}
	for _, c := range []struct{ name, member, raw, why string }{
		{"invalid-claim-sid", "sid", `"ses_AAAAAAAAAAAAAAAAAAAAAQ"`, "sid: a service account has no session."},
		{"invalid-claim-scope", "scope", `"api:access"`, "scope: nothing in a service account token grants a permission."},
		{"invalid-claim-audiences", "audiences", `["dev-health-api"]`, "audiences: a key's audience list is never carried."},
		{"invalid-claim-act", "act", `{"sub":"` + ServiceAccountSub + `"}`, "act: no delegation."},
	} {
		x := w
		x.extra = []member{{c.member, c.raw}}
		refuse(c.name, good, x.text(), active, schemaRefusal("a "+c.why))
	}
	userSub := w
	userSub.sub = "usr_AAAAAAAAAAAAAAAAAAAAAA"
	refuse("invalid-sub-user-prefix", good, userSub.text(), active, schemaRefusal("sub with the user-id prefix, not a principal id."))
	intSub := w
	intSub.rawSub = "12345"
	refuse("invalid-sub-integer", good, intSub.text(), active, schemaRefusal("sub as a JSON number."))
	httpIss := w
	httpIss.iss = "http://auth.example.test"
	refuse("invalid-iss-not-an-https-url", good, httpIss.text(), active, schemaRefusal("an http issuer."))

	over := w
	over.exp = over.iat + ServiceAccountCeiling + 1
	refuse("invalid-lifetime-over-the-ceiling", good, over.text(), active, serviceAccountContractRefusal("client_rule", "exp - iat one second over the 300 s ceiling."))
	hour := w
	hour.exp = hour.iat + 3600
	refuse("invalid-lifetime-one-hour", good, hour.text(), active, serviceAccountContractRefusal("client_rule", "exp - iat of 3600 s, the user access token's ceiling: a service account token never lives that long."))
	notAfter := w
	notAfter.exp = notAfter.nbf
	refuse("invalid-exp-not-after-nbf", good, notAfter.text(), active, serviceAccountContractRefusal("client_rule", "exp equals nbf: an empty validity window."))
	earlyNbf := w
	earlyNbf.nbf = earlyNbf.iat - 1
	refuse("invalid-nbf-before-iat", good, earlyNbf.text(), active, serviceAccountContractRefusal("client_rule", "nbf one second before iat: the schema accepts it, only the client rule refuses it."))
	refuse("invalid-exp-decimal-form", good, strings.Replace(w.text(), fmt.Sprintf(`"exp":%d`, w.exp), fmt.Sprintf(`"exp":%d.0`, w.exp), 1), active, serviceAccountContractRefusal("decode", "exp in decimal form (a NumericDate is an integer token): the schema accepts 1760000300.0, a client that reads it as a float or as an integer decides differently."))

	userToken := `{"iss":"` + Issuer + `","aud":"` + AudienceAPI + `","sub":"` + ServiceAccountSub + `","principal_type":"human","sid":"ses_AAAAAAAAAAAAAAAAAAAAAQ","jti":"000000000000000000000001","iat":1760000000,"nbf":1760000000,"exp":1760000600,"org_id":"` + ServiceAccountOrg + `","scope":"api:access"}`
	refuse("invalid-access-token-signed-by-the-active-key", headerText("EdDSA", "at+jwt", KidActive), userToken, active, schemaRefusal("a genuine user access token signed by the active key and presented as a service account token."))
	workloadToken := `{"iss":"` + Issuer + `","aud":"` + AudienceAPI + `","sub":"` + ServiceAccountSub + `","principal_type":"workload","jti":"000000000000000000000001","iat":1760000000,"nbf":1760000000,"exp":1760000300}`
	refuse("invalid-workload-token-signed-by-the-active-key", headerText("EdDSA", TypWorkload, KidActive), workloadToken, active, schemaRefusal("a genuine workload token for dev-health-api signed by the active key and presented as a service account token."))

	otherIss := w
	otherIss.iss = "https://other.example.test"
	refuse("invalid-iss-other-issuer", good, otherIss.text(), active, reject(now, AudienceAPI, ReasonIssuer, "A well-formed issuer that is not the one the verifier pins, signed by a published key."))
	refuse("invalid-aud-wrong-service", good, w.text(), active, reject(now, AudienceAuth, ReasonAudience, "A valid dev-health-api service account token presented to auth-service: the audience is exactly one service."))
	refuse("invalid-exp-equals-now", good, w.text(), active, reject(w.exp, AudienceAPI, ReasonExpired, "The clock equals exp: expired from exp inclusive."))
	refuse("invalid-exp-one-second-ago", good, w.text(), active, reject(w.exp+1, AudienceAPI, ReasonExpired, "One second past exp."))
	refuse("invalid-nbf-one-second-ahead", good, w.text(), active, reject(w.nbf-1, AudienceAPI, ReasonNotYetValid, "One second before nbf."))
	future := w
	future.iat, future.nbf, future.exp = now+1, now+1, now+1+ServiceAccountLife
	refuse("invalid-iat-one-second-ahead", good, future.text(), active, reject(now, AudienceAPI, ReasonNotYetValid, "iat and nbf one second after the clock."))
	refuse("invalid-delayed-nbf-before-nbf", good, delayed.text(), active, reject(delayed.nbf-1, AudienceAPI, ReasonNotYetValid, "nbf later than iat: the clock is after iat and one second before nbf."))

	refuse("invalid-kid-unknown", headerText("EdDSA", TypServiceAccount, KidUnknown), w.text(), edSigner(strangerKey), reject(now, AudienceAPI, ReasonUnknownKid, "A kid that was never published."))
	refuse("invalid-kid-removed", headerText("EdDSA", TypServiceAccount, KidRemoved), w.text(), edSigner(removedKey),
		withRemovedKey(reject(now, AudienceAPI, ReasonUnknownKid, "Valid claims signed by a key that was published and has been removed. The row runs with that key in the verifier's key set (a stale cache): it must still refuse the kid.")))
	refuse("invalid-signature-wrong-key", good, w.text(), edSigner(strangerKey), reject(now, AudienceAPI, ReasonSignature, "The active kid, signed by a key outside the set."))
	refuse("invalid-signature-wrong-published-key", good, w.text(), edSigner(overlapKey), reject(now, AudienceAPI, ReasonSignature, "The active kid, signed by the other published key."))
	refuse("invalid-signature-bit-flip", good, w.text(), func(in []byte) []byte {
		sig := ed25519.Sign(activeKey, in)
		sig[len(sig)-1] ^= 0x01
		return sig
	}, reject(now, AudienceAPI, ReasonSignature, "A valid signature with its last bit flipped."))
	refuse("invalid-signature-truncated", good, w.text(), func(in []byte) []byte {
		return ed25519.Sign(activeKey, in)[:ed25519.SignatureSize-1]
	}, reject(now, AudienceAPI, ReasonSignature, "A 63-byte signature."))
	refuse("invalid-signature-empty", good, w.text(), none, reject(now, AudienceAPI, ReasonSignature, "An empty signature under alg EdDSA."))
	tampered := strings.Replace(w.text(), `"principal_type":"service_account"`, `"principal_type":"service_account" `, 1)
	refuse("invalid-signature-payload-tampered", good, tampered, func([]byte) []byte {
		return ed25519.Sign(activeKey, Vector{HeaderJSON: good, PayloadJSON: w.text()}.SigningInput())
	}, reject(now, AudienceAPI, ReasonSignature, "A payload changed by one byte after signing."))
	crossOrg := w
	crossOrg.org = "00000000-0000-4000-8000-000000000002"
	refuse("invalid-signature-payload-org-swapped", good, crossOrg.text(), func([]byte) []byte {
		return ed25519.Sign(activeKey, Vector{HeaderJSON: good, PayloadJSON: w.text()}.SigningInput())
	}, reject(now, AudienceAPI, ReasonSignature, "org_id swapped for another organization after signing: the signature no longer covers the bytes."))

	refuse("invalid-order-contract-over-unknown-kid", headerText("EdDSA", "at+jwt", KidUnknown), w.text(), edSigner(strangerKey), schemaRefusal("Two failures: typ at+jwt and a kid never published. The contract refusal comes first."))
	refuse("invalid-order-signature-over-issuer", good, otherIss.text(), edSigner(strangerKey), reject(now, AudienceAPI, ReasonSignature, "Two failures: another issuer and a signature by a key outside the set. The signature refusal comes first."))
	refuse("invalid-order-issuer-over-audience", good, otherIss.text(), active, reject(now, AudienceAuth, ReasonIssuer, "Two failures: another issuer and the wrong service. The issuer refusal comes first."))
	refuse("invalid-order-audience-over-not-yet-valid", good, w.text(), active, reject(w.nbf-1, AudienceAuth, ReasonAudience, "Two failures: the wrong service and a clock before nbf. The audience refusal comes first."))
	refuse("invalid-order-audience-over-expiry", good, w.text(), active, reject(w.exp, AudienceAuth, ReasonAudience, "Two failures: the wrong service and an expired token. The audience refusal comes first."))

	sort.Slice(b.entries, func(i, j int) bool { return b.entries[i].File < b.entries[j].File })
	x := func(p ed25519.PublicKey) string { return base64.RawURLEncoding.EncodeToString(p) }
	return &Corpus{
		Manifest: Manifest{
			Description:   "The signed golden corpus for service-account-token.v1. Each vector is a header text, a payload text and a hex signature; the signing input is base64url(header) + \".\" + base64url(payload). A verifier decides every vector at the fixed clock and audience (the service it is) of its row, in decision_order, and must reach the row's decision. Only the public JWKS is committed.",
			Surface:       SurfaceServiceAccount,
			Issuer:        Issuer,
			LeewaySeconds: 0,
			RemovedKids:   []string{KidRemoved},
			RemovedKeys:   []JwkKey{{Kty: "OKP", Crv: "Ed25519", Alg: "EdDSA", Use: "sig", Kid: KidRemoved, X: x(removedPub)}},
			ReasonOrder:   append([]string{}, DecisionOrder...),
			Vectors:       b.entries,
		},
		Jwks: Jwks{Keys: []JwkKey{
			{Kty: "OKP", Crv: "Ed25519", Alg: "EdDSA", Use: "sig", Kid: KidActive, X: x(activePub)},
			{Kty: "OKP", Crv: "Ed25519", Alg: "EdDSA", Use: "sig", Kid: KidOverlap, X: x(overlapPub)},
		}},
		Vectors: b.vectors,
	}, nil
}
