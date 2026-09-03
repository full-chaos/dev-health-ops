package contracts

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

// PrincipalSurface is the wire surface name for a resolved principal; it is
// also the stem of contracts/auth/v1/jsonschema/principal.v1.schema.json.
const PrincipalSurface = "principal.v1"

// PrincipalSchemaVersion is the only schema_version this package accepts.
//
// TRD section 21: contract evolution is additive within v1; breaking
// semantics require v2. Distinct from the three other version-shaped values
// in this system -- the effective-principal envelope's `v` claim, a
// principal's credential generation counter, and the "v1" path segment --
// which move independently.
const PrincipalSchemaVersion = "principal.v1"

// Revision is a monotonic revision counter decoded leniently enough to match
// what the CONTRACT actually permits.
//
// JSON Schema draft 2020-12 defines "type": "integer" as any number with a
// zero fractional part, so `1.0` IS a valid integer by the specification and
// the schema is right to accept it. Plain `int64` is not: encoding/json
// refuses `1.0` with "cannot unmarshal number 1.0 into ... of type int64".
//
// That produced a real cross-language split, found by codex round 1 and
// re-executed here: `{"membership_revision": 1.0}` validated in BOTH planes,
// then Python built a principal carrying a float and Go failed to decode the
// same bytes. The disagreement lives DOWNSTREAM of a validation both passed,
// which is exactly why the golden corpus could not see it -- every fixture
// stops at "does it validate".
//
// Rejecting `1.0` in the schema was the wrong fix: it would make the contract
// stricter than the specification it declares, and a conforming producer
// emitting `1.0` would be turned away by us and accepted by every other
// draft-2020-12 validator. Normalising in the decoders is the fix, so both
// languages land on the same integer.
type Revision int64

// UnmarshalJSON accepts an integer or an integral decimal and rejects a
// genuinely fractional value.
//
// A fractional revision is refused rather than truncated: silently turning
// 1.5 into 1 would make two different wire documents produce one in-memory
// principal, and a revision is a cache-invalidation key (G-31) where two
// inputs collapsing to one value is precisely the failure mode.
func (r *Revision) UnmarshalJSON(data []byte) error {
	number := json.Number(strings.TrimSpace(string(data)))
	if integer, err := number.Int64(); err == nil {
		*r = Revision(integer)
		return nil
	}
	asFloat, err := number.Float64()
	if err != nil {
		return fmt.Errorf("revision %s is not a number: %w", number, err)
	}
	if asFloat != math.Trunc(asFloat) {
		return fmt.Errorf("revision %s has a fractional part; a revision is a whole counter", number)
	}
	// Compare against the exact powers of two, NOT against math.MaxInt64.
	//
	// float64(math.MaxInt64) rounds UP to 2^63, so `asFloat > math.MaxInt64`
	// is FALSE for asFloat == 2^63 and the conversion below then SILENTLY
	// CLAMPS to MaxInt64. Measured: input 9223372036854775808 decoded to
	// 9223372036854775807, so 2^63 and 2^63-1 -- two different wire
	// documents -- produced the SAME revision. That is precisely the
	// collapse the fractional check above exists to prevent, reintroduced by
	// a lossy comparison in the guard meant to prevent it (codex round 2 P2,
	// sharpened by the lane's own re-execution: the round reported a
	// rejection, the real behaviour was a silent clamp).
	//
	// 2^63 and -2^63 are both exactly representable as float64, so these
	// comparisons are exact.
	// FAIL CLOSED past float64's exact-integer range.
	//
	// Reaching this branch means the token did NOT parse as an integer, i.e.
	// it arrived in decimal form. float64 represents every integer up to 2^53
	// exactly and only every SECOND one above it, so beyond that the value has
	// already been rounded to the nearest representable double before this
	// code sees it -- in either direction. Measured on both clients:
	// 9007199254740993.0 decoded to ...992, and 9007199254740995.0 decoded to
	// ...996, so a revision can come back LARGER than it was sent and two
	// distinct revisions can collapse to one. A revision is a G-31 cache key;
	// a collision there is the failure the key exists to prevent, and a value
	// that never existed is worse than a rejection.
	//
	// This is the THIRD defect found in this function, after representation
	// (round 1) and magnitude (round 2), and the Float64 fallback added for
	// the first is what produced the third. The full repair is to parse the
	// raw JSON token as a decimal string and never touch float64 -- tracked
	// as a follow-up, because Python cannot implement it at its current
	// dict-taking entry point (json.load has already destroyed the precision
	// before the client runs). Until then this refuses what it cannot
	// represent faithfully. Integer-form tokens are unaffected: they take the
	// Int64 path above and stay exact to 2^63.
	// The bound is >=, not >, and the reason is the finding itself.
	//
	// 2^53 IS exactly representable, so a decimal token of exactly
	// "9007199254740992.0" is faithful -- but "9007199254740993.0" ROUNDS DOWN
	// to the same double, and by the time this code holds a float64 the two
	// are indistinguishable. Verified while writing this guard: a first
	// version used > and did not fire, because the very value it was meant to
	// catch had already become 2^53. Refusing AT the boundary is the only
	// honest position, since the question "was this faithful?" cannot be
	// answered from the value that survived. It costs the exact-2^53 decimal,
	// which an integer token expresses without loss.
	const maxExactFloatInteger = 9007199254740992.0 // 2^53
	if asFloat >= maxExactFloatInteger || asFloat <= -maxExactFloatInteger {
		return fmt.Errorf(
			"revision %s arrived in decimal form with a magnitude past 2^53, where float64 "+
				"cannot represent every integer; refused rather than silently rounded, "+
				"because a rounded revision breaks the G-31 cache key. Send it as an "+
				"integer token instead, which stays exact", number)
	}
	const maxInt64AsFloat = 9223372036854775808.0  // 2^63, one past MaxInt64
	const minInt64AsFloat = -9223372036854775808.0 // -2^63, exactly MinInt64
	if asFloat >= maxInt64AsFloat || asFloat < minInt64AsFloat {
		return fmt.Errorf(
			"revision %s is outside the int64 range this contract can represent "+
				"in both languages; refused rather than clamped, because two wire "+
				"values collapsing to one revision breaks the G-31 cache key", number)
	}
	*r = Revision(int64(asFloat))
	return nil
}

// Timestamp is an RFC 3339 instant that MARSHALS within the contract.
//
// The schema caps fractional seconds at six digits, because Python's datetime
// holds microseconds and Go's time.Time holds nanoseconds, and a value past
// six digits means different instants in the two clients. Capping the schema
// fixed the READER and left the WRITER broken: encoding/json renders a
// time.Time with RFC3339Nano, so a service stamping time.Now() emitted nine
// digits and produced a document THIS PACKAGE'S OWN SCHEMA REJECTS. Measured
// before the fix:
//
//	Go emitted issued_at  = 2026-09-02T23:15:00.123456789Z   -> pattern violation
//	Go emitted expires_at = 2026-09-02T23:25:00.0000005Z     -> pattern violation
//
// A contract whose reference client cannot produce a conforming document is
// not a contract anybody can implement, and no golden fixture would have
// caught it: every fixture is hand-written text that goes only through the
// READ path. The round trip is the only place the write path is exercised.
//
// Marshalling TRUNCATES to microsecond resolution rather than refusing. That
// is a deliberate lossy narrowing at a declared boundary, not a silent repair:
// the contract states microsecond resolution, so precision below it is data
// the wire format cannot carry, and refusing would fail every caller that
// stamps time.Now(). Truncation rather than rounding, so a timestamp never
// moves forward past an expiry it was checked against.
type Timestamp struct{ time.Time }

// MarshalJSON emits at most six fractional digits.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Truncate(time.Microsecond).Format(time.RFC3339Nano))
}

// UnmarshalJSON accepts what the schema accepts; the schema is the gate.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return fmt.Errorf("timestamp %q is not RFC 3339: %w", raw, err)
	}
	t.Time = parsed
	return nil
}

// Credential is the credential that authenticated a principal.
//
// Present because TRD section 2 principle 1 makes credential class part of
// the route contract, and G-2/G-3 forbid treating every bearer value as one
// class or discovering the class by validator fan-out. The credential
// authenticates a principal but IS NOT the principal (TRD section 8 rule 2).
type Credential struct {
	// Class is a value from the closed vocabulary: the Wave 0 frozen 30 in
	// contracts/auth/v1/credential-classes.json plus the three new platform
	// credentials TRD section 11 defines. Unknown denies (G-26). The schema
	// enforces it, so a value here has already been checked.
	Class string `json:"class"`
	// CredentialID identifies the credential INSTANCE. Never the secret:
	// G-16 forbids a credential value in any fixture, log, trace, metric
	// label or audit row, and G-17 makes plaintext secrets one-time output.
	CredentialID string `json:"credential_id"`
	Issuer       string `json:"issuer"`
	Audience     string `json:"audience"`
	// Scopes bound what the credential may be used for. A DIFFERENT
	// vocabulary from authorization actions (G-5: authentication is not
	// authorization). Empty is meaningful and never means unrestricted.
	Scopes []string `json:"scopes"`
}

// Authentication records how and when the principal authenticated.
//
// Assurance is an authorization input, not decoration: G-31 names it as part
// of an allow-cache key, and G-51 requires step-up gating for high-risk
// actions during impersonation.
type Authentication struct {
	// Methods are RFC 8176 amr names, restricted by the schema to the subset
	// this platform's described flows can produce.
	Methods []string `json:"methods"`
	// AuthenticatedAt is when the principal actually authenticated, which is
	// NOT Principal.IssuedAt: one authentication event can produce many
	// resolved principal documents, and a step-up decision needs the age of
	// the authentication rather than the age of this document.
	AuthenticatedAt Timestamp `json:"authenticated_at"`
	// Assurance is a NIST SP 800-63B level: aal1, aal2 or aal3.
	Assurance string `json:"assurance"`
}

// Delegation is one hop in the actor chain.
//
// ACP-ADR-03 bounds a delegated session at 15 minutes; G-52 requires an
// independent id, a reason, an explicit permitted action set, and independent
// revocability. G-50: while delegated, permissions derive from the effective
// subject -- real-actor platform authority does not implicitly authorize
// target actions.
type Delegation struct {
	// ActorPrincipalID is the REAL actor acting as the effective subject.
	ActorPrincipalID string    `json:"actor_principal_id"`
	DelegationID     string    `json:"delegation_id"`
	Reason           string    `json:"reason"`
	StartedAt        Timestamp `json:"started_at"`
	ExpiresAt        Timestamp `json:"expires_at"`
	// PermittedActions is the bounded set this delegation may exercise.
	// Empty means the delegation authorizes nothing by itself -- a real
	// state, never "unrestricted".
	PermittedActions []string `json:"permitted_actions"`
}

// Principal is a resolved principal.v1 document (TRD section 8).
//
// FOUR DELIBERATE OMISSIONS, none an oversight to repair:
//
//   - no Tier / LicensedFeatures -- ACP-ADR-07 decision 2, G-14, and TRD
//     section 8's own rule "Grants and entitlements are not identity claims".
//   - no Permissions -- TRD section 11 and G-14 forbid embedding a complete
//     mutable permission set in a credential; ask the authorization surface,
//     which also binds entitlement as an independent gate.
//   - no Role -- ACP-ADR-05 decision 4 and G-27 require an explicit action at
//     the call site, never a broad role check as the final decision.
//   - no IsSuperuser -- TRD section 3 and G-23 move platform authority to a
//     separate role/action namespace resolved against CURRENT state.
//
// The existing effective-principal envelope
// (cmd/query-api/internal/principal/claims.go) carries all four today. It is
// the compatibility implementation this migration retires; a reader diffing
// against it will find four things "missing", and the absence is the design.
type Principal struct {
	SchemaVersion string `json:"schema_version"`
	// PrincipalID is the stable authorization subject (TRD section 8 rule 1,
	// G-20) -- never an email, username or token prefix.
	PrincipalID   string `json:"principal_id"`
	PrincipalType string `json:"principal_type"`
	// SubjectID is the underlying subject record, SEPARATE from PrincipalID
	// by design: authorization keys on PrincipalID, never on this. nil where
	// the principal has no distinct underlying subject.
	SubjectID *string `json:"subject_id"`
	// OrganizationID is nil only for a genuinely org-less principal. A
	// relying party that scopes by organization must treat nil as "no
	// organization" and DENY, never as "any organization" (G-22).
	OrganizationID *string        `json:"organization_id"`
	Credential     Credential     `json:"credential"`
	Authentication Authentication `json:"authentication"`
	// ActorChain is server-derived and append-only (TRD section 8 rule 4).
	// Empty means acting as itself. A chain rather than a single impersonator
	// because G-49 requires the real actor and the effective subject both to
	// survive downstream, and delegation nests.
	ActorChain []Delegation `json:"actor_chain"`
	// All four revisions are required by G-31's allow-cache key. TRD section
	// 8's example shows only membership and policy; that example is
	// under-specified against the guardrail, which is stated as an acceptance
	// criterion rather than a suggestion.
	MembershipRevision  Revision  `json:"membership_revision"`
	PolicyRevision      Revision  `json:"policy_revision"`
	GrantRevision       Revision  `json:"grant_revision"`
	EntitlementRevision Revision  `json:"entitlement_revision"`
	IssuedAt            Timestamp `json:"issued_at"`
	// ExpiresAt is enforced by the relying party. ACP-ADR-03 removes the
	// envelope's per-call TTL override outright: a security bound any call
	// site may widen is not a bound.
	ExpiresAt Timestamp `json:"expires_at"`
}

// MaxDelegationDuration is ACP-ADR-03's bound on a delegated/impersonation
// session, and G-52's requirement that it be shorter than the base session and
// independently revocable.
//
// Enforced HERE, in the client, because JSON Schema cannot subtract two
// timestamps. The schema requires an expires_at on every actor-chain hop, and
// codex round 1 demonstrated that requiring the FIELD is not a bound: a
// 30-minute delegated session validated cleanly and EffectiveDeadline reported
// it happily, because taking the earliest supplied deadline says nothing about
// how far away that deadline is.
//
// This is the "a property the code CLAIMED but did not ENFORCE" class, so the
// bound is a named constant with its ADR beside it rather than a literal
// buried in a comparison.
const MaxDelegationDuration = 15 * time.Minute

// checkDelegationBounds refuses an actor chain the schema cannot police.
//
// TWO rules, not one. A hop must end after it starts, and it must not last
// longer than MaxDelegationDuration. The ordering check is not redundant: a
// negative duration satisfies any maximum, so a bound written only as
// "expires_at - started_at <= 15m" accepts a session that ends before it
// begins. Both have fixtures in the manifest's reject_by_client list.
func checkDelegationBounds(chain []Delegation) error {
	for index, hop := range chain {
		// THREE rules now, not two. The chain is documented append-only
		// (TRD section 8 rule 4) and RealActorPrincipalID reads index zero as
		// the originating actor, but nothing enforced the order -- so
		// reversing a two-hop chain validated cleanly and reported the LATER
		// delegator as the real actor (codex round 2 P1, re-executed in both
		// languages). G-49 requires the real actor to survive downstream;
		// it does not survive being silently relabelled.
		if index > 0 {
			previous := chain[index-1]
			if hop.StartedAt.Before(previous.StartedAt.Time) {
				return fmt.Errorf(
					"%s: /actor_chain/%d starts at %s, before the preceding hop's %s; the "+
						"chain is append-only and index zero is read as the originating "+
						"actor, so an out-of-order chain reports the wrong real actor",
					PrincipalSurface, index, hop.StartedAt.Format(time.RFC3339),
					previous.StartedAt.Format(time.RFC3339))
			}
			if hop.ExpiresAt.After(previous.ExpiresAt.Time) {
				return fmt.Errorf(
					"%s: /actor_chain/%d expires at %s, after the delegation it descends "+
						"from (%s); a sub-delegation that outlives its parent is not bounded "+
						"by it (G-52)",
					PrincipalSurface, index, hop.ExpiresAt.Format(time.RFC3339),
					previous.ExpiresAt.Format(time.RFC3339))
			}
		}
		if !hop.ExpiresAt.After(hop.StartedAt.Time) {
			return fmt.Errorf(
				"%s: /actor_chain/%d expires_at (%s) is not after started_at (%s); a delegated "+
					"session that ends before it begins is not a bounded session",
				PrincipalSurface, index, hop.ExpiresAt.Format(time.RFC3339), hop.StartedAt.Format(time.RFC3339))
		}
		if duration := hop.ExpiresAt.Sub(hop.StartedAt.Time); duration > MaxDelegationDuration {
			return fmt.Errorf(
				"%s: /actor_chain/%d lasts %s, exceeding the %s bound ACP-ADR-03 sets for a "+
					"delegated session (G-52 also requires it shorter than the base session). "+
					"JSON Schema cannot express a duration, so this is enforced here; the "+
					"fixture proving it is in the manifest's reject_by_client list",
				PrincipalSurface, index, duration, MaxDelegationDuration)
		}
	}
	return nil
}

// PrincipalFromWire validates raw against principal.v1 and then decodes it.
//
// The order is the point. Validation runs against the DECODED JSON VALUE, not
// against this struct, so the contract is checked on the bytes that actually
// arrived rather than on Go's rendering of them -- decoding into the struct
// first would silently drop any field the struct does not declare, which is
// exactly the divergence additionalProperties:false exists to catch.
func PrincipalFromWire(root string, raw []byte) (*Principal, error) {
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("%s: not valid JSON: %w", PrincipalSurface, err)
	}
	if err := Validate(root, PrincipalSurface, document); err != nil {
		return nil, err
	}
	var principal Principal
	if err := json.Unmarshal(raw, &principal); err != nil {
		return nil, fmt.Errorf("%s: decoding validated document: %w", PrincipalSurface, err)
	}
	if err := checkDelegationBounds(principal.ActorChain); err != nil {
		return nil, err
	}
	return &principal, nil
}

// IsDelegated reports whether at least one actor is acting as this principal.
func (p *Principal) IsDelegated() bool { return len(p.ActorChain) > 0 }

// RealActorPrincipalID returns the originating real actor, or the principal
// itself when not delegated.
//
// G-49 requires the real actor and the effective subject to stay
// distinguishable through token, decision, audit and downstream execution.
// One named accessor so that question has one answer instead of being
// re-derived, and got wrong, at each call site.
func (p *Principal) RealActorPrincipalID() string {
	if len(p.ActorChain) > 0 {
		return p.ActorChain[0].ActorPrincipalID
	}
	return p.PrincipalID
}

// EffectiveDeadline returns the earliest of the principal's own expiry and
// every delegation's.
//
// Takes the minimum rather than trusting ACP-ADR-03's requirement that a
// delegated session be strictly shorter than the base session: a bound that
// relies on an invariant it does not check is only a bound while the
// invariant holds.
func (p *Principal) EffectiveDeadline() Timestamp {
	deadline := p.ExpiresAt
	for _, hop := range p.ActorChain {
		if hop.ExpiresAt.Before(deadline.Time) {
			deadline = hop.ExpiresAt
		}
	}
	return deadline
}

// CacheKeyDimensions returns every dimension G-31 requires an allow-cache key
// to bind, from the principal's side.
//
// G-31: "An allow cache key includes principal, actor chain,
// credential/session, organization, action, resource, policy revision,
// membership/grant revisions, entitlement revision when applicable, and
// relevant assurance." Action and resource belong to the decision rather than
// the principal, so a caller appends those; everything the PRINCIPAL
// contributes is produced here, in one place, so a consumer cannot bind a
// subset by accident. A key built from a subset is a cache a revision bump
// cannot invalidate.
func (p *Principal) CacheKeyDimensions() []string {
	organization := "<nil>"
	if p.OrganizationID != nil {
		organization = *p.OrganizationID
	}
	dimensions := []string{p.PrincipalID}
	for _, hop := range p.ActorChain {
		dimensions = append(dimensions, "dlg="+hop.DelegationID)
	}
	return append(dimensions,
		"cred="+p.Credential.CredentialID,
		"org="+organization,
		fmt.Sprintf("policy=%d", p.PolicyRevision),
		fmt.Sprintf("membership=%d", p.MembershipRevision),
		fmt.Sprintf("grant=%d", p.GrantRevision),
		fmt.Sprintf("entitlement=%d", p.EntitlementRevision),
		"assurance="+p.Authentication.Assurance,
	)
}
