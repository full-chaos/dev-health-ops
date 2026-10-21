package tokencorpus

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
)

// The fixed values every vector is built from. Only the keys are fresh.
const (
	Issuer        = "https://auth.example.test"
	AudienceAPI   = "dev-health-api"
	AudienceAuth  = "auth-service"
	KidActive     = "auth-example-2026-10"
	KidOverlap    = "auth-example-2026-11"
	KidUnknown    = "auth-example-2027-01"
	KidRemoved    = "auth-example-2026-04"
	FixedIat      = 1760000000
	FixedNow      = 1760000300
	LifetimeShort = 600
	Ceiling       = 3600
)

type member struct{ key, raw string }

func object(members ...member) string {
	parts := make([]string, len(members))
	for i, m := range members {
		parts[i] = fmt.Sprintf("%q:%s", m.key, m.raw)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

type claimSet struct {
	iss, aud, sub, ptype, sid, jti, scope string
	iat, nbf, exp                         int64
	org                                   string
	rawSub                                string
}

func baseClaims() claimSet {
	return claimSet{
		iss: Issuer, aud: AudienceAPI, sub: "prn_AAAAAAAAAAAAAAAAAAAAAA", ptype: "human",
		sid: "ses_AAAAAAAAAAAAAAAAAAAAAQ", jti: "000000000000000000000001", scope: "api:access",
		iat: FixedIat, nbf: FixedIat, exp: FixedIat + LifetimeShort,
		org: "00000000-0000-4000-8000-000000000001",
	}
}

func (c claimSet) text() string {
	q := func(s string) string { return fmt.Sprintf("%q", s) }
	sub := q(c.sub)
	if c.rawSub != "" {
		sub = c.rawSub
	}
	members := []member{
		{"iss", q(c.iss)}, {"aud", q(c.aud)}, {"sub", sub}, {"principal_type", q(c.ptype)},
		{"sid", q(c.sid)}, {"jti", q(c.jti)}, {"iat", fmt.Sprint(c.iat)}, {"nbf", fmt.Sprint(c.nbf)},
		{"exp", fmt.Sprint(c.exp)},
	}
	if c.org != "" {
		members = append(members, member{"org_id", q(c.org)})
	}
	members = append(members, member{"scope", q(c.scope)})
	return object(members...)
}

func headerText(alg, typ, kid string) string {
	return fmt.Sprintf(`{"alg":%q,"typ":%q,"kid":%q}`, alg, typ, kid)
}

type signer func(input []byte) []byte

func edSigner(k ed25519.PrivateKey) signer {
	return func(input []byte) []byte { return ed25519.Sign(k, input) }
}

type builder struct {
	vectors map[string]Vector
	entries []Entry
}

func (b *builder) add(name string, header, payload string, sign signer, e Entry) {
	v := Vector{HeaderJSON: header, PayloadJSON: payload}
	sig := sign(v.SigningInput())
	v.SignatureHex = hex.EncodeToString(sig)
	e.File = name + ".json"
	b.vectors[e.File] = v
	b.entries = append(b.entries, e)
}

func accept(now int64, aud, why string) Entry {
	return Entry{Now: now, VerifyAs: aud, Expect: Accept, Why: why}
}

func reject(now int64, aud, reason, why string) Entry {
	return Entry{Now: now, VerifyAs: aud, Expect: Reject, Reason: reason, Why: why}
}

func contractRefusal(stage, why string) Entry {
	return Entry{Now: FixedNow, VerifyAs: AudienceAPI, Expect: Reject, Reason: ReasonContract, Stage: stage, Why: why}
}

// Generate builds a corpus with fresh keys drawn from random (crypto/rand when
// nil). Everything but the keys, and so the signatures, is fixed: the same
// bytes come out for the same key material, and the only bytes that change with
// other keys are the public keys and the signatures.
func Generate(random io.Reader) (*Corpus, error) {
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
	good := headerText("EdDSA", "at+jwt", KidActive)
	api := baseClaims()
	now := int64(FixedNow)

	b.add("valid-api", good, api.text(), edSigner(activeKey),
		accept(now, AudienceAPI, "The ordinary token for go-api and query-api, signed by the active key."))
	noOrg := api
	noOrg.org = ""
	b.add("valid-api-no-organization", good, noOrg.text(), edSigner(activeKey),
		accept(now, AudienceAPI, "No org_id: the session acts in no organization."))
	auth := api
	auth.aud, auth.scope = AudienceAuth, "auth:session"
	b.add("valid-auth-service", good, auth.text(), edSigner(activeKey),
		accept(now, AudienceAuth, "The token for auth-service's own session routes, verified as that audience."))
	b.add("valid-signed-by-overlap-key", headerText("EdDSA", "at+jwt", KidOverlap), api.text(), edSigner(overlapKey),
		accept(now, AudienceAPI, "Signed by the key published for the rotation overlap: both keys of the set verify."))
	b.add("valid-at-nbf", good, api.text(), edSigner(activeKey),
		accept(api.nbf, AudienceAPI, "The clock equals nbf: valid from nbf inclusive."))
	b.add("valid-last-second-before-exp", good, api.text(), edSigner(activeKey),
		accept(api.exp-1, AudienceAPI, "One second before exp: the last valid instant."))
	ceiling := api
	ceiling.exp = ceiling.iat + Ceiling
	b.add("valid-lifetime-at-the-ceiling", good, ceiling.text(), edSigner(activeKey),
		accept(now, AudienceAPI, "exp - iat equals the ceiling (3600 s): the longest lifetime a verifier accepts."))

	b.add("invalid-alg-none", headerText("none", "at+jwt", KidActive), api.text(), none,
		contractRefusal("schema", "alg none with an empty signature."))
	b.add("invalid-alg-hs256-public-key-as-secret", headerText("HS256", "at+jwt", KidActive), api.text(), hs256(activePub),
		contractRefusal("schema", "alg HS256 with a valid HMAC keyed by the published public key: the symmetric-confusion shape."))
	b.add("invalid-alg-hs256-unrelated-secret", headerText("HS256", "at+jwt", KidActive), api.text(), hs256([]byte("corpus-hs256-secret-not-a-real-one")),
		contractRefusal("schema", "alg HS256 with a valid HMAC under some other secret."))
	b.add("invalid-alg-lowercase", headerText("eddsa", "at+jwt", KidActive), api.text(), edSigner(activeKey),
		contractRefusal("schema", "alg spelled eddsa with a valid EdDSA signature: the name is case-sensitive."))

	dupHeader := `{"alg":"EdDSA","alg":"none","typ":"at+jwt","kid":"` + KidActive + `"}`
	b.add("invalid-duplicate-header-alg", dupHeader, api.text(), edSigner(activeKey),
		contractRefusal("duplicate_member", "alg appears twice in the header; the signature is valid over those bytes."))
	dupKid := `{"alg":"EdDSA","typ":"at+jwt","kid":"` + KidActive + `","kid":"` + KidOverlap + `"}`
	b.add("invalid-duplicate-header-kid", dupKid, api.text(), edSigner(activeKey),
		contractRefusal("duplicate_member", "kid appears twice in the header with two published kids."))
	dupAud := strings.Replace(api.text(), `"aud":"dev-health-api"`, `"aud":"dev-health-api","aud":"auth-service"`, 1)
	b.add("invalid-duplicate-claim-aud", good, dupAud, edSigner(activeKey),
		contractRefusal("duplicate_member", "aud appears twice with two valid audiences; the signature is valid over those bytes."))
	dupExp := strings.Replace(api.text(), `"exp":1760000600`, `"exp":1760000600,"exp":1760003600`, 1)
	b.add("invalid-duplicate-claim-exp", good, dupExp, edSigner(activeKey),
		contractRefusal("duplicate_member", "exp appears twice."))

	b.add("invalid-typ-jwt", headerText("EdDSA", "JWT", KidActive), api.text(), edSigner(activeKey),
		contractRefusal("schema", "typ JWT: the token's type is at+jwt."))
	b.add("invalid-typ-uppercase", headerText("EdDSA", "AT+JWT", KidActive), api.text(), edSigner(activeKey),
		contractRefusal("schema", "typ is case-sensitive."))
	b.add("invalid-kid-empty", headerText("EdDSA", "at+jwt", ""), api.text(), edSigner(activeKey),
		contractRefusal("schema", "an empty kid."))
	b.add("invalid-kid-absent", `{"alg":"EdDSA","typ":"at+jwt"}`, api.text(), edSigner(activeKey),
		contractRefusal("schema", "no kid member."))

	otherIss := api
	otherIss.iss = "https://other.example.test"
	b.add("invalid-iss-other-issuer", good, otherIss.text(), edSigner(activeKey),
		reject(now, AudienceAPI, ReasonIssuer, "A well-formed issuer that is not the one the verifier pins, signed by a published key."))
	b.add("invalid-iss-not-an-https-url", good, strings.Replace(api.text(), Issuer, "http://auth.example.test", 1), edSigner(activeKey),
		contractRefusal("schema", "an http issuer."))
	b.add("invalid-aud-unknown", good, strings.Replace(api.text(), `"dev-health-api"`, `"dev-health-workload"`, 1), edSigner(activeKey),
		contractRefusal("schema", "an audience outside the contract's two."))
	b.add("invalid-aud-api-token-at-auth-service", good, api.text(), edSigner(activeKey),
		reject(now, AudienceAuth, ReasonAudience, "A valid dev-health-api token presented to auth-service."))
	b.add("invalid-aud-auth-token-at-api", good, auth.text(), edSigner(activeKey),
		reject(now, AudienceAPI, ReasonAudience, "A valid auth-service token presented to dev-health-api."))
	wrongSub := api
	wrongSub.sub = "usr_AAAAAAAAAAAAAAAAAAAAAA"
	b.add("invalid-sub-user-prefix", good, wrongSub.text(), edSigner(activeKey),
		contractRefusal("schema", "sub with the user-id prefix, not a principal id."))
	intSub := api
	intSub.rawSub = "12345"
	b.add("invalid-sub-integer", good, intSub.text(), edSigner(activeKey),
		contractRefusal("schema", "sub as a JSON number."))
	workload := api
	workload.ptype = "workload"
	b.add("invalid-principal-type-workload", good, workload.text(), edSigner(activeKey),
		contractRefusal("schema", "principal_type workload: a user access token is human only."))

	b.add("invalid-exp-equals-now", good, api.text(), edSigner(activeKey),
		reject(api.exp, AudienceAPI, ReasonExpired, "The clock equals exp: expired from exp inclusive."))
	b.add("invalid-exp-one-second-ago", good, api.text(), edSigner(activeKey),
		reject(api.exp+1, AudienceAPI, ReasonExpired, "One second past exp."))
	b.add("invalid-nbf-one-second-ahead", good, api.text(), edSigner(activeKey),
		reject(api.nbf-1, AudienceAPI, ReasonNotYetValid, "One second before nbf."))
	future := api
	future.iat, future.nbf, future.exp = now+1, now+1, now+1+LifetimeShort
	b.add("invalid-iat-one-second-ahead", good, future.text(), edSigner(activeKey),
		reject(now, AudienceAPI, ReasonNotYetValid, "iat and nbf one second after the clock."))
	notAfter := api
	notAfter.exp = notAfter.nbf
	b.add("invalid-exp-not-after-nbf", good, notAfter.text(), edSigner(activeKey),
		contractRefusal("client_rule", "exp equals nbf: an empty validity window."))
	over := api
	over.exp = over.iat + Ceiling + 1
	b.add("invalid-lifetime-over-the-ceiling", good, over.text(), edSigner(activeKey),
		contractRefusal("client_rule", "exp - iat one second over the ceiling."))

	b.add("invalid-kid-unknown", headerText("EdDSA", "at+jwt", KidUnknown), api.text(), edSigner(strangerKey),
		reject(now, AudienceAPI, ReasonUnknownKid, "A kid that was never published."))
	b.add("invalid-kid-removed", headerText("EdDSA", "at+jwt", KidRemoved), api.text(), edSigner(removedKey),
		reject(now, AudienceAPI, ReasonUnknownKid, "Valid claims signed by a key that was published and has been removed from the set; a verifier that still holds that key (a stale cache) must refuse it."))
	b.add("invalid-signature-wrong-key", good, api.text(), edSigner(strangerKey),
		reject(now, AudienceAPI, ReasonSignature, "The active kid, signed by a key outside the set."))
	b.add("invalid-signature-wrong-published-key", good, api.text(), edSigner(overlapKey),
		reject(now, AudienceAPI, ReasonSignature, "The active kid, signed by the other published key."))
	b.add("invalid-signature-bit-flip", good, api.text(), func(in []byte) []byte {
		sig := ed25519.Sign(activeKey, in)
		sig[len(sig)-1] ^= 0x01
		return sig
	}, reject(now, AudienceAPI, ReasonSignature, "A valid signature with its last bit flipped."))
	b.add("invalid-signature-truncated", good, api.text(), func(in []byte) []byte {
		return ed25519.Sign(activeKey, in)[:ed25519.SignatureSize-1]
	}, reject(now, AudienceAPI, ReasonSignature, "A 63-byte signature."))
	b.add("invalid-signature-empty", good, api.text(), none,
		reject(now, AudienceAPI, ReasonSignature, "An empty signature under alg EdDSA."))
	b.add("invalid-signature-payload-tampered", good, strings.Replace(api.text(), `"scope":"api:access"`, `"scope":"api:access" `, 1), func([]byte) []byte {
		return ed25519.Sign(activeKey, Vector{HeaderJSON: good, PayloadJSON: api.text()}.SigningInput())
	}, reject(now, AudienceAPI, ReasonSignature, "A payload changed by one byte after signing."))

	sort.Slice(b.entries, func(i, j int) bool { return b.entries[i].File < b.entries[j].File })
	x := func(p ed25519.PublicKey) string { return base64.RawURLEncoding.EncodeToString(p) }
	return &Corpus{
		Manifest: Manifest{
			Description:   "The signed golden corpus for access-token.v1. Each vector is a header text, a payload text and a hex signature; the signing input is base64url(header) + \".\" + base64url(payload). A verifier decides every vector at the fixed clock and audience of its row, in decision_order, and must reach the row's decision. Only the public JWKS is committed.",
			Surface:       Surface,
			Issuer:        Issuer,
			LeewaySeconds: 0,
			RemovedKids:   []string{KidRemoved},
			RemovedKeys:   []JwkKey{{Kty: "OKP", Crv: "Ed25519", Alg: "EdDSA", Use: "sig", Kid: KidRemoved, X: x(removedPub)}},
			ReasonOrder:   []string{ReasonContract, ReasonUnknownKid, ReasonSignature, ReasonIssuer, ReasonAudience, ReasonNotYetValid, ReasonExpired},
			Vectors:       b.entries,
		},
		Jwks: Jwks{Keys: []JwkKey{
			{Kty: "OKP", Crv: "Ed25519", Alg: "EdDSA", Use: "sig", Kid: KidActive, X: x(activePub)},
			{Kty: "OKP", Crv: "Ed25519", Alg: "EdDSA", Use: "sig", Kid: KidOverlap, X: x(overlapPub)},
		}},
		Vectors: b.vectors,
	}, nil
}
