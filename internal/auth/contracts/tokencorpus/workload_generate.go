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

// The fixed values the workload vectors add to the shared ones. Only the keys
// are fresh.
const (
	TypWorkload     = "workload+jwt"
	WorkloadCeiling = 300
	WorkloadLife    = 300
	WorkloadSub     = "prn_AAAAAAAAAAAAAAAAAAAAAA"
	AudienceOther   = "dev-health-api"
	WorkloadNow     = FixedIat + 100
)

// workloadClaims is the payload of a workload vector: no organization, no
// session, no scope. extra holds members a refusal vector adds, as raw text.
type workloadClaims struct {
	iss, aud, sub, ptype, jti string
	iat, nbf, exp             int64
	rawSub, rawAud            string
	extra                     []member
}

func baseWorkload() workloadClaims {
	return workloadClaims{
		iss: Issuer, aud: AudienceAuth, sub: WorkloadSub, ptype: "workload",
		jti: "000000000000000000000001", iat: FixedIat, nbf: FixedIat, exp: FixedIat + WorkloadLife,
	}
}

func (c workloadClaims) text() string {
	q := func(s string) string { return fmt.Sprintf("%q", s) }
	sub, aud := q(c.sub), q(c.aud)
	if c.rawSub != "" {
		sub = c.rawSub
	}
	if c.rawAud != "" {
		aud = c.rawAud
	}
	members := []member{
		{"iss", q(c.iss)}, {"aud", aud}, {"sub", sub}, {"principal_type", q(c.ptype)},
		{"jti", q(c.jti)}, {"iat", fmt.Sprint(c.iat)}, {"nbf", fmt.Sprint(c.nbf)}, {"exp", fmt.Sprint(c.exp)},
	}
	return object(append(members, c.extra...)...)
}

func workloadContractRefusal(stage, why string) Entry {
	return Entry{Now: FixedNow, VerifyAs: AudienceAuth, Expect: Reject, Reason: ReasonContract, Stage: stage, Why: why}
}

// GenerateWorkload builds the workload-token.v1 corpus with fresh keys drawn
// from random (crypto/rand when nil). Everything but the keys, and so the
// signatures, is fixed.
func GenerateWorkload(random io.Reader) (*Corpus, error) {
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
	good := headerText("EdDSA", TypWorkload, KidActive)
	w := baseWorkload()
	now := int64(WorkloadNow)
	active := edSigner(activeKey)
	ok := func(name string, header string, c workloadClaims, sign signer, at int64, aud, why string) {
		b.add(name, header, c.text(), sign, accept(at, aud, why))
	}
	refuse := func(name string, header string, payload string, sign signer, e Entry) {
		b.add(name, header, payload, sign, e)
	}

	ok("valid-auth-service", good, w, active, now, AudienceAuth, "The ordinary workload token for auth-service, typ workload+jwt, signed by the active key.")
	ok("valid-signed-by-overlap-key", headerText("EdDSA", TypWorkload, KidOverlap), w, edSigner(overlapKey), now, AudienceAuth, "Signed by the key published for the rotation overlap: both keys of the set verify.")
	ok("valid-at-nbf", good, w, active, w.nbf, AudienceAuth, "The clock equals nbf: valid from nbf inclusive.")
	ok("valid-last-second-before-exp", good, w, active, w.exp-1, AudienceAuth, "One second before exp: the last valid instant.")
	atCeiling := w
	atCeiling.exp = atCeiling.iat + WorkloadCeiling
	ok("valid-lifetime-at-the-ceiling", good, atCeiling, active, now, AudienceAuth, "exp - iat equals the ceiling (300 s): the longest lifetime a verifier accepts.")
	delayed := w
	delayed.nbf = delayed.iat + 60
	ok("valid-delayed-nbf-at-nbf", good, delayed, active, delayed.nbf, AudienceAuth, "nbf later than iat: the clock equals nbf, so the token is valid.")

	refuse("invalid-alg-none", headerText("none", TypWorkload, KidActive), w.text(), none, workloadContractRefusal("schema", "alg none with an empty signature."))
	refuse("invalid-alg-hs256-public-key-as-secret", headerText("HS256", TypWorkload, KidActive), w.text(), hs256(activePub), workloadContractRefusal("schema", "alg HS256 with a valid HMAC keyed by the published public key: the symmetric-confusion shape."))
	refuse("invalid-alg-hs256-unrelated-secret", headerText("HS256", TypWorkload, KidActive), w.text(), hs256([]byte("corpus-hs256-secret-not-a-real-one")), workloadContractRefusal("schema", "alg HS256 with a valid HMAC under some other secret."))
	refuse("invalid-alg-lowercase", headerText("eddsa", TypWorkload, KidActive), w.text(), active, workloadContractRefusal("schema", "alg spelled eddsa with a valid EdDSA signature: the name is case-sensitive."))

	for _, c := range []struct{ name, typ, why string }{
		{"invalid-typ-access-token", "at+jwt", "typ at+jwt with a valid signature: the user access token's type is never a workload token's."},
		{"invalid-typ-jwt-uppercase", "JWT", "typ JWT: the HS256 token's type."},
		{"invalid-typ-jwt-lowercase", "jwt", "typ jwt: the effective principal envelope's type."},
		{"invalid-typ-uppercase", "WORKLOAD+JWT", "typ is case-sensitive: exact bytes."},
		{"invalid-typ-mixed-case", "Workload+JWT", "typ is case-sensitive: no mixed-case form."},
		{"invalid-typ-application-prefix", "application/workload+jwt", "an application/-prefixed media type is not the typ."},
		{"invalid-typ-trailing-space", "workload+jwt ", "a trailing space is not the typ."},
		{"invalid-typ-empty", "", "an empty typ."},
	} {
		refuse(c.name, headerText("EdDSA", c.typ, KidActive), w.text(), active, workloadContractRefusal("schema", c.why))
	}
	refuse("invalid-typ-absent", `{"alg":"EdDSA","kid":"`+KidActive+`"}`, w.text(), active, workloadContractRefusal("schema", "no typ member: a missing typ is refused."))
	refuse("invalid-kid-absent", `{"alg":"EdDSA","typ":"`+TypWorkload+`"}`, w.text(), active, workloadContractRefusal("schema", "no kid member."))
	refuse("invalid-kid-empty", headerText("EdDSA", TypWorkload, ""), w.text(), active, workloadContractRefusal("schema", "an empty kid."))

	refuse("invalid-duplicate-header-typ", `{"alg":"EdDSA","typ":"at+jwt","typ":"`+TypWorkload+`","kid":"`+KidActive+`"}`, w.text(), active, workloadContractRefusal("duplicate_member", "typ appears twice (at+jwt, then workload+jwt); the signature is valid over those bytes."))
	refuse("invalid-duplicate-header-alg", `{"alg":"EdDSA","alg":"none","typ":"`+TypWorkload+`","kid":"`+KidActive+`"}`, w.text(), active, workloadContractRefusal("duplicate_member", "alg appears twice in the header."))
	refuse("invalid-duplicate-header-kid", `{"alg":"EdDSA","typ":"`+TypWorkload+`","kid":"`+KidActive+`","kid":"`+KidOverlap+`"}`, w.text(), active, workloadContractRefusal("duplicate_member", "kid appears twice with two published kids."))
	refuse("invalid-duplicate-claim-aud", good, strings.Replace(w.text(), `"aud":"auth-service"`, `"aud":"dev-health-api","aud":"auth-service"`, 1), active, workloadContractRefusal("duplicate_member", "aud appears twice; the last is the verifier's audience and the signature is valid over those bytes."))
	refuse("invalid-duplicate-claim-exp", good, strings.Replace(w.text(), fmt.Sprintf(`"exp":%d`, w.exp), fmt.Sprintf(`"exp":%d,"exp":%d`, w.exp, w.exp+3600), 1), active, workloadContractRefusal("duplicate_member", "exp appears twice."))

	userAud := w
	userAud.aud = AudienceOther
	refuse("invalid-aud-user-api", good, userAud.text(), active, workloadContractRefusal("schema", "the user-token audience dev-health-api: not a workload audience."))
	unknownAud := w
	unknownAud.aud = "billing-edge"
	refuse("invalid-aud-unknown", good, unknownAud.text(), active, workloadContractRefusal("schema", "an audience no workload token carries."))
	arrayAud := w
	arrayAud.rawAud = `["auth-service","dev-health-api"]`
	refuse("invalid-aud-array", good, arrayAud.text(), active, workloadContractRefusal("schema", "two audiences: a workload token names exactly one service."))
	human := w
	human.ptype = "human"
	refuse("invalid-principal-type-human", good, human.text(), active, workloadContractRefusal("schema", "principal_type human: a workload token is workload only."))
	for _, c := range []struct{ name, member, raw, why string }{
		{"invalid-claim-org-id", "org_id", `"00000000-0000-4000-8000-000000000001"`, "org_id: a workload token acts for no organization."},
		{"invalid-claim-sid", "sid", `"ses_AAAAAAAAAAAAAAAAAAAAAQ"`, "sid: a workload token has no session."},
		{"invalid-claim-scope", "scope", `"auth:session"`, "scope: nothing in a workload token grants a permission."},
	} {
		x := w
		x.extra = []member{{c.member, c.raw}}
		refuse(c.name, good, x.text(), active, workloadContractRefusal("schema", "a "+c.why))
	}
	userSub := w
	userSub.sub = "usr_AAAAAAAAAAAAAAAAAAAAAA"
	refuse("invalid-sub-user-prefix", good, userSub.text(), active, workloadContractRefusal("schema", "sub with the user-id prefix, not a principal id."))
	intSub := w
	intSub.rawSub = "12345"
	refuse("invalid-sub-integer", good, intSub.text(), active, workloadContractRefusal("schema", "sub as a JSON number."))
	httpIss := w
	httpIss.iss = "http://auth.example.test"
	refuse("invalid-iss-not-an-https-url", good, httpIss.text(), active, workloadContractRefusal("schema", "an http issuer."))

	over := w
	over.exp = over.iat + WorkloadCeiling + 1
	refuse("invalid-lifetime-over-the-ceiling", good, over.text(), active, workloadContractRefusal("client_rule", "exp - iat one second over the 300 s ceiling."))
	hour := w
	hour.exp = hour.iat + 3600
	refuse("invalid-lifetime-one-hour", good, hour.text(), active, workloadContractRefusal("client_rule", "exp - iat of 3600 s, the user access token's ceiling: a workload token never lives that long."))
	notAfter := w
	notAfter.exp = notAfter.nbf
	refuse("invalid-exp-not-after-nbf", good, notAfter.text(), active, workloadContractRefusal("client_rule", "exp equals nbf: an empty validity window."))

	userToken := `{"iss":"` + Issuer + `","aud":"` + AudienceAPI + `","sub":"` + WorkloadSub + `","principal_type":"human","sid":"ses_AAAAAAAAAAAAAAAAAAAAAQ","jti":"000000000000000000000001","iat":1760000000,"nbf":1760000000,"exp":1760000600,"org_id":"00000000-0000-4000-8000-000000000001","scope":"api:access"}`
	refuse("invalid-user-access-token-signed-by-the-active-key", headerText("EdDSA", "at+jwt", KidActive), userToken, active, workloadContractRefusal("schema", "a genuine user access token signed by the active key and presented as a workload token."))

	otherIss := w
	otherIss.iss = "https://other.example.test"
	refuse("invalid-iss-other-issuer", good, otherIss.text(), active, reject(now, AudienceAuth, ReasonIssuer, "A well-formed issuer that is not the one the verifier pins, signed by a published key."))
	refuse("invalid-aud-wrong-service", good, w.text(), active, reject(now, AudienceOther, ReasonAudience, "A valid auth-service workload token presented to dev-health-api: the audience is exactly one service."))
	refuse("invalid-exp-equals-now", good, w.text(), active, reject(w.exp, AudienceAuth, ReasonExpired, "The clock equals exp: expired from exp inclusive."))
	refuse("invalid-exp-one-second-ago", good, w.text(), active, reject(w.exp+1, AudienceAuth, ReasonExpired, "One second past exp."))
	refuse("invalid-nbf-one-second-ahead", good, w.text(), active, reject(w.nbf-1, AudienceAuth, ReasonNotYetValid, "One second before nbf."))
	future := w
	future.iat, future.nbf, future.exp = now+1, now+1, now+1+WorkloadLife
	refuse("invalid-iat-one-second-ahead", good, future.text(), active, reject(now, AudienceAuth, ReasonNotYetValid, "iat and nbf one second after the clock."))
	refuse("invalid-delayed-nbf-before-nbf", good, delayed.text(), active, reject(delayed.nbf-1, AudienceAuth, ReasonNotYetValid, "nbf later than iat: the clock is after iat and one second before nbf."))

	refuse("invalid-kid-unknown", headerText("EdDSA", TypWorkload, KidUnknown), w.text(), edSigner(strangerKey), reject(now, AudienceAuth, ReasonUnknownKid, "A kid that was never published."))
	refuse("invalid-kid-removed", headerText("EdDSA", TypWorkload, KidRemoved), w.text(), edSigner(removedKey),
		withRemovedKey(reject(now, AudienceAuth, ReasonUnknownKid, "Valid claims signed by a key that was published and has been removed. The row runs with that key in the verifier's key set (a stale cache): it must still refuse the kid.")))
	refuse("invalid-signature-wrong-key", good, w.text(), edSigner(strangerKey), reject(now, AudienceAuth, ReasonSignature, "The active kid, signed by a key outside the set."))
	refuse("invalid-signature-wrong-published-key", good, w.text(), edSigner(overlapKey), reject(now, AudienceAuth, ReasonSignature, "The active kid, signed by the other published key."))
	refuse("invalid-signature-bit-flip", good, w.text(), func(in []byte) []byte {
		sig := ed25519.Sign(activeKey, in)
		sig[len(sig)-1] ^= 0x01
		return sig
	}, reject(now, AudienceAuth, ReasonSignature, "A valid signature with its last bit flipped."))
	refuse("invalid-signature-truncated", good, w.text(), func(in []byte) []byte {
		return ed25519.Sign(activeKey, in)[:ed25519.SignatureSize-1]
	}, reject(now, AudienceAuth, ReasonSignature, "A 63-byte signature."))
	refuse("invalid-signature-empty", good, w.text(), none, reject(now, AudienceAuth, ReasonSignature, "An empty signature under alg EdDSA."))
	tampered := strings.Replace(w.text(), `"principal_type":"workload"`, `"principal_type":"workload" `, 1)
	refuse("invalid-signature-payload-tampered", good, tampered, func([]byte) []byte {
		return ed25519.Sign(activeKey, Vector{HeaderJSON: good, PayloadJSON: w.text()}.SigningInput())
	}, reject(now, AudienceAuth, ReasonSignature, "A payload changed by one byte after signing."))

	refuse("invalid-order-contract-over-unknown-kid", headerText("EdDSA", "at+jwt", KidUnknown), w.text(), edSigner(strangerKey), workloadContractRefusal("schema", "Two failures: typ at+jwt and a kid never published. The contract refusal comes first."))
	refuse("invalid-order-signature-over-issuer", good, otherIss.text(), edSigner(strangerKey), reject(now, AudienceAuth, ReasonSignature, "Two failures: another issuer and a signature by a key outside the set. The signature refusal comes first."))
	refuse("invalid-order-issuer-over-audience", good, otherIss.text(), active, reject(now, AudienceOther, ReasonIssuer, "Two failures: another issuer and the wrong service. The issuer refusal comes first."))
	refuse("invalid-order-audience-over-not-yet-valid", good, w.text(), active, reject(w.nbf-1, AudienceOther, ReasonAudience, "Two failures: the wrong service and a clock before nbf. The audience refusal comes first."))
	refuse("invalid-order-audience-over-expiry", good, w.text(), active, reject(w.exp, AudienceOther, ReasonAudience, "Two failures: the wrong service and an expired token. The audience refusal comes first."))

	sort.Slice(b.entries, func(i, j int) bool { return b.entries[i].File < b.entries[j].File })
	x := func(p ed25519.PublicKey) string { return base64.RawURLEncoding.EncodeToString(p) }
	return &Corpus{
		Manifest: Manifest{
			Description:   "The signed golden corpus for workload-token.v1. Each vector is a header text, a payload text and a hex signature; the signing input is base64url(header) + \".\" + base64url(payload). A verifier decides every vector at the fixed clock and audience (the service it is) of its row, in decision_order, and must reach the row's decision. Only the public JWKS is committed.",
			Surface:       SurfaceWorkload,
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
