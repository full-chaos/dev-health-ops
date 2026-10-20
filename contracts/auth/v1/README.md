# Dev Health Auth Control Plane wire contracts v1

This directory is the language-neutral source of truth for the Auth Control
Plane's wire format (TRD §21). The JSON Schemas, the OpenAPI document that
`$ref`s them, and the golden fixtures are authoritative; the Go, Python and
TypeScript types are implementations of them, never the other way round.

Seven surfaces are planned (CHAOS-3270). Shipped so far: `principal.v1`
(CHAOS-4884), `error.v1` (CHAOS-4929), `jwks.v1` and `discovery.v1`
(CHAOS-4930), `provisioning-event.v1` and `audit-event.v1` (CHAOS-4933),
`credential-entry.v1`, `credential-list.v1` and `credential-receipt.v1`
(CHAOS-4934), `endpoint-auth-profile.v1` (CHAOS-4931), `authorization_request.v1`,
`authorization_decision.v1` and the two batch forms (CHAOS-4932). All seven
planned surfaces have shipped, and the Wave 2 session surfaces `session.v1`,
`refresh-request.v1`, `refresh-response.v1` and `auth-outcome.v1` (CHAOS-10155)
follow them, with `transport-error.v1` (CHAOS-10392), the body auth-service's
shared transport writes when it answers before a route (400, 404, 405, 413,
429, 500).

Distinct from `credential-classes.json` and `endpoint-profiles.ops.json`, which
are the Wave-0 **inventory** contract describing what authentication exists
today. These describe what the new service emits.

## Conventions every surface inherits

Written down because each was paid for once. A new surface that re-derives one
of these instead of reusing it will rediscover the same defect — that has
already happened once, between the two shipped surfaces.

### Identifiers are server-minted and never echoed

**Any identifier a response carries is minted server-side and is never copied
from caller input.** This is a hard rule, not a default.

An earlier version of `error.v1` allowed a caller-supplied `request_id` to be
echoed once it passed a pattern check. Codex refuted that as a P1: a character
class cannot distinguish an opaque correlation ID from a secret, so a caller
could put a credential fragment in the one field documented as safe and have it
reflected into a response body — the disclosure TRD §18 forbids, arriving
through the front door.

Pin identifiers to a **fixed length** as a defence in depth, and state it as
partial: a fixed length makes common token shapes unrepresentable, it does not
make smuggling impossible, and no pattern could. The producer obligation is the
control. A caller's own correlation ID, if accepted at all, belongs in
authorized server-side audit data keyed to the response ID — never in the
response.

### Integer fields need a client guard in every language

JSON Schema's `integer` means "a number with no fractional part", so `403.0`
validates. Go clients that decode into `int` refuse the identical document that
a Python client accepts as a float — the same wire bytes, opposite outcomes.

Every integer field needs an explicit client-side check, and the fixtures for it
belong in `reject_by_client`, not `reject`: the schema genuinely accepts these
documents, and filing them as `reject` would assert a check that does not exist.

**The guard decides on the raw number TOKEN, never on a parsed value.** A token
containing `.`, `e` or `E` is refused even when its value is integral (`1.0`,
`1e0`): a float64 reader has already rounded `9007199254740990.1` and
`1.0000000000000000001` to integers, so no value-based or magnitude-based check
can tell them from the integers they round to. Integer tokens are exact and are
the only accepted form (CHAOS-4940 withdrew the earlier normalisation of
integral decimals such as `1.0`). Python's `json` returns a `float` for exactly
the decimal-form tokens, so a `float` at the guard is a decimal-form token; Go's
custom unmarshalers hold the token text.

**Reuse these; do not re-derive them** (CHAOS-4940 extracted them, after the
class bit `error.v1` once because no shared guard existed):

| concern | Go | Python |
| --- | --- | --- |
| wire-integer TYPE: is this an integer token, not `403.0` or `1e0`? | an ordinary field: decode into an integer-typed field (`encoding/json` refuses `403.0`, pinned by `TestDecodingIntoAnIntegerFieldRefusesAZeroFractionDecimal`); a custom unmarshaler: `contracts.RefuseDecimalForm(token)` | `authclient.contracts.require_wire_int`, or `refuse_decimal_form` for a field with its own range rule |
| a boolean where a number is required | the decoder refuses it | `authclient.contracts.refuse_bool` (before any `int` check: `bool` subclasses `int`) |

`error.v1`'s `status` and `retry_after_seconds` and `principal.v1`'s revisions
use the same rule; revisions add the int64 range. Fixtures for these rules
belong in `reject_by_client`.

### Constraints live in the cross-language dialect intersection

Six divergences are known. **They are enforced by four different mechanisms,
and one of them is not enforced at all** — an earlier version of this table
attributed all of them to one Go test, which was false for two.

| divergence | consequence | actually enforced by |
| --- | --- | --- |
| `format` is recorded and ignored by `github.com/google/jsonschema-go` | assert with `pattern`, never `format` | `TestEveryWireSchemaStaysInTheCrossLanguageDialect` |
| `\d` `\w` `\s` `\b` are Unicode in Python `re`, ASCII in RE2 and ECMA-262 | no shorthand classes | same test |
| `(?i)` is valid in RE2 and Python, a SyntaxError in ECMA-262 | no inline flag groups | same test |
| a trailing `$` matches before a final newline in Python only | the Python client rewrites it to `\Z`, failing closed on shapes it cannot rewrite | **the client, not a schema guard** — `authclient/contracts.py` `strictly_anchored` |
| ajv strict mode refuses `required` in a subschema that does not define the property | an ajv lint, not a spec rule; declare the property in the subschema | **the ajv runner** — `ci/auth-contracts-ajv/run.mjs` (strict mode; `--self-test` proves it is on), run by the `Auth Contracts ajv` workflow |
| `maxLength` counts CODE POINTS; Go's `len()` counts BYTES | to mirror a Go byte bound, restrict the alphabet to one-byte characters so the two counts coincide | the `jwks.v1` corpus — `invalid-kid-non-ascii.json` plus `TestRejectedJWKSFixturesAreAlsoRefusedByTheRealConsumer` |

The ECMA-262 leg is committed: `ci/auth-contracts-ajv/run.mjs` validates every
`examples/*/manifest.json` corpus with ajv 2020-12 in **strict mode** (ajv is
pinned by `package-lock.json`, because `strictRequired` is a lint and can move
between releases) and runs in CI on any change under `contracts/auth/v1/**`.
It reads the same manifests as the Go and Python runners and enumerates nothing
itself. Exit codes: 1 = a fixture disagrees, 2 = a schema failed to compile.
Run it locally: `cd ci/auth-contracts-ajv && npm ci && node run.mjs`.

**Redundant keywords are not free.** A `maxLength` beside a pattern that already
bounds the same range makes Go report `maxLength` (first violation only) and
Python report `pattern` (all violations) for one fixture, so the manifest cannot
name a single expected keyword. One constraint per property.

### Timestamps are bounded to their semantic range, not to a digit count

Every component: year 0001-9999, month 01-12, day 01-31, hour 00-23, minute
00-59, second 00-59, offset hours 00-23 and minutes 00-59.

**This is written as a class rule because doing it one component at a time
failed twice.** Round 2 of `error.v1` found `+24:00` and the offset alone was
bounded; round 3 then found year `0000` — the same defect one component over.
Enumerating the class at that point turned up a **second** split no reviewer had
reported, running the opposite way: Go refuses `T24:00:00Z` while Python accepts
it. A permissive digit-count pattern produces a cross-language split at every
component independently, so bound them all or expect to be back.

**Use an alternation for the year bound, never a lookahead.** The natural
spelling is `(?!0000)`, and **RE2 has no lookahead** — that form compiles in
Python and ECMA and fails in Go, which is divergence #2 reproduced inside a fix
for divergence #1.

**Leap seconds are REJECTED.** `23:59:60` is legal RFC 3339 and this contract
refuses it. **Both clients reject `:60`** — Go's `time.Parse` says *second out of
range*, Python's `fromisoformat` says *second must be in 0..59* — so the pattern
matches them and all three stay aligned. The server mints these timestamps and
never emits `:60`. Ruled by team-lead and listed for chris with this default;
accepting a leap second would mean changing both clients, not just the pattern.

*An earlier version of this section justified the same decision wrongly*, and the
correction is kept rather than edited away because the error is the instructive
part: it claimed permitting `:60` "would manufacture a third cross-language
split", reasoning from Python's refusal without checking Go's. Go refuses it too,
so no such split existed. **The decision was right and the stated reason was
false** — which is the harder defect to catch, because a wrong justification
sitting beside a correct rule reads as settled, and whoever revisits the rule
inherits the wrong model of why it is there.

**Component bounds do not bound the COMPOSITION, and this is the subtlest one.**
The pattern bounds year 0001-9999 of the *local* fields, but the instant is
(fields × offset) and the offset can carry it outside that range:
`0001-01-01T00:00:00+23:59` normalises to UTC year 0000. A regex cannot see it —
the composition is arithmetic, not lexical.

Both clients therefore **normalise to UTC at parse and refuse an instant outside
0001-01-01..9999-12-31**, with `reject_by_client` fixtures at both boundaries.
Before that check, the low boundary parsed cleanly and then raised
`OverflowError` on the caller's first `astimezone` — an uncaught stdlib
exception on an accepted document, while `.timestamp()` succeeded on the same
value.

**The range check runs BEFORE the clock-skew check, and the order is
load-bearing.** The high boundary is also far-future, so the skew bound would
refuse it anyway — but that is a coincidence: the skew bound is deliberately
one-directional and is about clocks, not representability. Ordering the range
check first means each boundary fixture is refused by the rule that is actually
about it, and a future symmetric skew bound cannot quietly become the only thing
holding the 9999 case. Proven: making the skew bound symmetric leaves that
fixture refused **by range**.

Go has no such limit — `time.Time` represents both instants happily — and
mirrors the check anyway, because a wire contract's job is that the two clients
agree about which documents are acceptable.

That makes a **third category of constraint**, beside the two already
documented. Name it at each site that meets it:

| category | example | why it exists |
| -- | -- | -- |
| the language needs it | Python's `_require_wire_int` refusing `403.0` | Go's decoder would reject the same document |
| the contract needs it | `additionalProperties: false` | the wire format forbids the field |
| **only agreement needs it** | Go's UTC range check | neither language requires the bound; the two clients must not disagree |

A reader who asks only whether Go needs the check will call it dead code, which
is why the comment at each such site says the reason is the *other* client. Any
constraint where the runtimes differ in representable range, precision or
tolerance produces one. `principal.v1` carries the same rule (Go
`principalInstantInRange`, Python `_require_utc_instant_in_range`) with no
clock-skew check to order it against.

**A regex cannot know February has 28 days.** `2026-02-30` satisfies the
pattern; both clients refuse it at parse. That is a genuine client-enforced
rule and it has a `reject_by_client` fixture stating so — the alternative is a
gap that reads as an oversight rather than as a boundary.

For `principal.v1` the schema layer itself disagrees on that day, and the
disagreement is declared rather than hidden: Go's `Validate` ACCEPTS
`2026-02-30` and `2026-04-31` (google/jsonschema-go ignores `format`), while
Python and ajv refuse them by `format`. Those fixtures live in the manifest's
`go_schema_gap` group, which is neither `reject` (that asserts Go's `Validate`
refuses) nor `reject_by_client` (that asserts every validator accepts). Every
typed client refuses the document. `TestGoSchemaValidateAcceptsACalendarImpossibleDay`
pins the Go acceptance, so a library that starts asserting `format` turns it red
and the fixtures move to `reject`. Decided in CHAOS-10135: the typed client is
the floor, as `ParseWireTimestamp` is on the event surfaces.

### A contract with one real consumer is pinned in BOTH directions

Most surfaces here are validated by two clients this repository owns. `jwks.v1`
is not: dev-health-go's `authverify.Ed25519JWKSVerifier` **consumes** it, from
another module, and nothing in this repository parses it. That changes what the
corpus has to prove.

A schema for a surface like this can fail in two opposite ways, and only one of
them is habitually tested:

* **looser than the consumer** — the schema certifies a document the only
  reader refuses. This is the familiar direction, and the `accept` fixtures are
  run through the real `Keys()` to close it.
* **stricter than the consumer** — the schema refuses a document the reader
  would have accepted. Nobody writes this test, because a rejection always
  looks like the schema working. It is the more dangerous direction: the false
  alarm eventually gets "fixed" by loosening whichever rule was load-bearing.
  `TestRejectedJWKSFixturesAreAlsoRefusedByTheRealConsumer` closes it — **but
  only against the cases the corpus actually contains**; see "State a narrowing
  as a FIXTURE" below for how that test stayed green while four narrowings went
  undeclared.

Where the schema is deliberately narrower than the consumer, give it a fixture
in `narrower_than_consumer` — never a `reject` entry, which would force the
second test above to be weakened, and never prose alone, which cannot fail.

**Pin the producer when one exists.** No JWKS producer exists in this
repository yet. When the Go auth-service publishes `/.well-known/jwks.json`, its
real output must be validated by a test beside the Go runner, or the schema can
describe an idealised document, pass its own corpus forever, and never touch
what production writes.

### Provisioning and audit events: the consumer is the deliverable (CHAOS-4933)

`provisioning-event.v1` is one `auth_outbox_events` row as a consumer receives
it; `audit-event.v1` is one `security_audit_events` row crossing a service
boundary (TRD §9, §13).

* **The envelope is the producer's row, derived not hand-picked.**
  `provisioning-event.v1`'s members are exactly the `auth_outbox_events` columns
  that cross a service boundary (`aggregate_type`, `aggregate_id`, `event_type`,
  `idempotency_key`, `created_at`, `payload`); `audit-event.v1`'s are the
  `security_audit_events` columns. `TestWireEnvelopesMatchTheirProducersColumnsInBothDirections`
  parses the 0005 migration and fails if a column is neither a member nor
  excluded with a reason, or a member has no column. Excluded: surrogate ids,
  delivery bookkeeping (`available_at`, `published_at`, `attempts`,
  `last_error`) and `client_ip`.
* **The idempotency key is `(aggregate_type, aggregate_id, payload.revision)`,
  and `revision` is per aggregate.** Nothing implies an ordering across
  aggregates, so a consumer that sorts the whole stream is wrong. The
  `idempotency_key` column (UNIQUE) must equal
  `aggregate_type:aggregate_id:revision`; a pattern cannot compare two members,
  so the clients enforce it (`reject_by_client`).
* **Consumer rule, with golden vectors** (`consumer_vectors` in the
  provisioning manifest, replayed by Go and Python): apply when the revision is
  last+1 (1 for an aggregate never seen); ignore a revision at or below the last
  applied one; a larger one is a gap, which is not applied and means resync.
* **`payload.revision` is 1..2^53-1** and guarded on the raw token like every wire
  integer (above): `1.0`, `1e0` and `9007199254740990.1` are `reject_by_client`;
  2^53 and past are refused by the schema.
* **`change` and audit `attributes` are allowlists.** A schema cannot assert
  "no secrets", so the members that would carry one are unrepresentable: the
  provisioning `change` object names, per aggregate type, exactly the members it
  may carry (`propertyNames` in a per-aggregate conditional; a `user` event
  carries none), and audit `attributes` is a closed object. Emails, provider
  subjects, display names and client addresses have no member. An arbitrary key
  fails, and a test fails if an aggregate type lacks its conditional branch.
* **Each aggregate type pins its id prefix and event vocabulary**, so an
  `organization` event with a `prn_` id disagrees in shape and fails. `idn_`,
  `mbr_`, `pra_`, `grt_` and `rol_` are introduced here; `prn_` and `org_` are
  `principal.v1`'s. A grant event carries `expires_at` (TRD §9; Q2 default keep).
* **Scope, decided:** entitlement snapshots are a consumed projection, not a
  provisioning event (ACP-ADR-07); ACR and External Push lifecycle are
  metadata projections owned by the credential surface and are not in this
  stream in v1 (ACP-ADR-08, ACP-ADR-09); mutation request bodies are caller
  hints, never authority, and belong with each route when it is mounted.

### Credential metadata: secrets are returned once (CHAOS-4934)

TRD §12: secret values are returned once. The contract makes that asymmetry
visible in the file layout, not in a conditionally present field:

* `credential-receipt.v1` (the response of issue and rotate) is the ONLY schema
  that declares a `secret`. `credential-entry.v1` (registry metadata, also the
  response of revoke) and `credential-list.v1` are closed objects with no secret,
  hash or token member, so a secret in an entry or a list item fails
  (`additionalProperties`). A test walks every declared property name and fails
  if a secret-bearing name appears anywhere but the receipt.
* **The entry is derived from its producer, two ways.** Its members are the
  columns of `credential_registry` (migration 0003 plus 0007's `ADD COLUMN`s)
  minus `secret_hash` and `hash_algorithm` (secret material), plus one declared
  derived member, `status`.
  `TestCredentialEntryMatchesTheRegistryColumnsInBothDirections` parses both
  migrations (every line at the table's indentation is a column or a constraint,
  or the test fails) and fails on any difference.
* **`status` is derived, not stored:** `revoked_at` set means revoked; else
  `expires_at <= now` means expired; else active. The schema pins status to the
  timestamps' shape (revoked requires `revoked_at` and a reason code; expired
  requires `expires_at`; active forbids the revocation members). Comparing with
  the clock is a client rule (`clock_now` in the manifest).
* **Revocation is monotonic:** `CheckCredentialTransition` /
  `check_credential_transition` refuse active-after-revoked, a changed
  `revoked_at` or reason, a changed identity, lineage, prefix or fingerprint, and
  widened scopes or audiences. A `reject_by_client` manifest entry with `after`
  is a transition from that accepted entry. Migration 0007 enforces the same
  rules in PostgreSQL.
* **Rotation lineage:** `kind=rotate` requires `credential.rotation_parent_id`;
  `kind=issue` forbids it. The secret begins with the credential's
  `display_prefix` (a client rule), and `display_prefix` is the only fragment of
  a secret that appears anywhere else, pinned at 16 characters by the schema.
  A receipt names only its parent id, so narrowing is checked by a client that
  holds the parent entry: `CheckCredentialRotation` / `check_credential_rotation`
  refuse widened scopes or audiences, a different principal, class or domain
  owner, a wrong parent id and a revoked parent (migration 0007's rules).
* `scopes` use `principal.v1`'s shape and are sets; `credential_class` is the
  same enum as `principal.v1`'s (a test fails on drift); `revoked_reason` is a
  value of a closed vocabulary (`vocabulary/reason-codes.json`: `compromised`,
  `owner_request`), never the free text the database column allows; the audit
  event's `attributes.reason_code` is a closed list in the same file, and a new
  value arrives with the producer that emits it and its own migration.
* The list items, the receipt's credential and the entry are copies of one body
  (no cross-file `$ref`: the three runners differ on external refs); tests keep
  the list items equal to the entry and the receipt's credential a stricter
  subset. `crd_` is a new id prefix introduced here.
* ACR and External Push appear only as metadata projections (`domain_owner`,
  `external_reference`), never as validators (ACP-ADR-08/09). No pagination is
  specified for the list (TRD §15 names no parameters).

### JWKS and discovery: one issuer, one key use (CHAOS-4930)

`jwks.v1` and `discovery.v1` are the two documents behind TRD §15's
`/.well-known/jwks.json` and `/.well-known/openid-configuration`.

* **Private members are absent from the schema, not prohibited.** `d p q dp dq
  qi k` are not declared and `additionalProperties` is false; each has a reject
  fixture and a test reads the schema to confirm none is declared.
* **`alg=none`, symmetric `alg` and symmetric `kty` are reject fixtures.**
* **Rotation is the normal case:** `valid-two-keys-during-rotation.json`. A
  consumer that meets an unknown `kid` refreshes once, boundedly, then denies;
  a schema cannot express that, so it is a consumer rule (TRD §12).
* **A JWKS has no issuer member.** The binding is made by discovery: a verifier
  reaches the JWKS only through the `jwks_uri` of a discovery document from the
  issuer it has pinned for that credential class. ACR Web-assertion keys are
  another issuer (TRD §12); keeping them apart is the pin, and token-class
  confusion is settled by the token's own `typ`/`aud`/`iss`, not by discovery.
  No custom discovery member is used.
* **Two rules are client rules:** `jwks_uri == issuer + "/.well-known/jwks.json"`
  and `issuer == the pinned issuer`. They need a cross-member and a
  configuration comparison JSON Schema cannot make. `ParseDiscovery` (Go) and
  `parse_discovery` (Python) take the trusted issuer as a required argument, and
  the `reject_by_client` fixtures are valid against the schema on purpose.
* **Discovery-endpoint errors use the shared `error.v1` envelope.** OpenAPI
  `paths` stays empty until the service mounts routes (see the OpenAPI
  description); the `Jwks` and `Discovery` components are the deliverable now.
* **Known gaps, owned elsewhere:** duplicate members and trailing content in a
  JWKS file are accepted by `authverify` (CHAOS-4951); the property-based sweep
  of the `kid` and member-name narrowings is CHAOS-4958.

### A wrong length: what is true, and what was overstated

`ed25519.Verify` **panics** on a 31- or 33-byte public key rather than returning
an error, and *both* languages' base64 decoders accept those lengths without
complaint — the decode never checks. Those two facts are real and were executed.

**The conclusion drawn from them was not.** An earlier version of this section
said the length "has to be pinned in the schema or it is pinned nowhere". The
consumer checks it itself: `Keys()` compares the decoded length against
`ed25519.PublicKeySize` before returning a key, so the panic is unreachable
through it. The schema pin is defence-in-depth and a statement of the wire form.

Keeping the correction visible because of *how* it happened: the disproving
line was in a function this lane had read and quoted in its own review context
minutes earlier. Two verified facts were joined into a claim about a third
thing, and the third thing was never executed. That is the house failure mode,
committed inside the document that names it.

`^[A-Za-z0-9_-]{43}$` is still the right pattern, for the reasons that survive:
43 characters of the URL-safe alphabet always decode to exactly 32 bytes
(verified across 200000 random such strings), the alphabet excludes `+` and `/`,
and the fixed count excludes `=` padding. Python's `urlsafe_b64decode` accepts
padding *and* the standard alphabet; Go's `RawURLEncoding` rejects both.

### State a narrowing as a FIXTURE, never as a sentence

The rule this surface paid for. Where a schema is deliberately stricter than the
client it describes, that gap needs a fixture in a category of its own —
`narrower_than_consumer` — with a test asserting **both** halves: the client
accepts it, the schema refuses it.

A prose list cannot be executed, and this one was wrong: it declared two
narrowings and there were five. The missing ones were case-variant member names (Go's `encoding/json` matches
field names **case-insensitively**, and `DisallowUnknownFields` compares *after*
that match), `use: null` decoding to the zero string, and a `kid` carrying NUL or
surrounding spaces — `unicode.IsSpace` does not consider NUL a space, so
`TrimSpace` leaves it.

**Round 2 then blocked it again for a FIFTH**, `use: ""`, which had a predicate
in the harness and a sentence in the schema and no fixture. A predicate without a
fixture is worse than prose: prose merely fails to test the narrowing, while a
predicate actively *excuses* the disagreement, so the differential stays green
over an asymmetry nothing asserts. The correspondence between predicates and
fixtures is now asserted in both directions — remembering to add both is what
failed twice.

**And the test built to catch exactly this passed the whole time.**
`TestRejectedJWKSFixturesAreAlsoRefusedByTheRealConsumer` runs every reject
fixture through the real client for precisely this purpose. It was green,
because not one of the twenty reject fixtures happened to exercise any of the
four. A correct instrument fed a corpus that cannot trip it reports coverage it
does not have — which is worse than no test, because it is cited as evidence.
Adding the category is what converts each sentence into an assertion that fails
when it stops being true.

### The manifest is the single inventory

One `manifest.json` per surface, read by every runner. A runner that enumerates
fixtures itself is a review defect (G-70): three independent lists over one
directory is the drift the cross-language goldens exist to catch.

Three categories, and the third is load-bearing:

- `accept` — must validate. Carries equal weight to `reject`: a validator that
  is too strict passes every rejection test while breaking every real caller.
- `reject` — must fail, at a declared `expect_instance_location` and
  `expect_keyword`. Each fixture violates exactly **one** instance location, so
  Go (which stops at the first violation) and Python (which reports all) agree
  on which rule fired.
- `reject_by_client` — **validates against the schema** and every client must
  refuse it. For rules JSON Schema cannot express: a duration bound, a
  cross-field consistency check, anything needing context the validator never
  sees.

### Prefer absence to prohibition

A schema cannot assert "this string contains no secret". Where a field would
carry something a contract forbids, **omit the field** and set
`additionalProperties: false`; then a reject fixture proves the absence bites.

State what the schema cannot enforce, in the schema. A contract that implies
more enforcement than it has is worse than one that admits its boundary.

### Fixtures are synthetic, and the scanner is not advisory

`TestNoFixtureCarriesARealLookingSecret` walks the whole `examples/` tree by
prefix, pattern and Shannon entropy. Identifiers are unmistakably synthetic
(fixed `EXAMPLE` bodies, zero-padded counters, no plausible entropy) per G-73.

It scans **prose as well as values** — a manifest note explaining why a
credential prefix was removed will trip on naming that prefix. Describe the
shape instead. This is correct behaviour and has caught two authors.

## Proof obligation

Every surface ships an executed cross-language run on one tip — Go, Python and
ajv-2020 validating the same corpus, output pasted, not described (G-70) — the
ajv leg is `ci/auth-contracts-ajv/run.mjs`, so the pasted run is reproducible —
and a mutation table showing the corpus can fail: delete each guard, confirm exactly
the predicted fixtures go red, restore.
