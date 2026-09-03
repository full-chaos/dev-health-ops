# Dev Health Auth Control Plane wire contracts v1

This directory is the language-neutral source of truth for the Auth Control
Plane's wire format (TRD §21). The JSON Schemas, the OpenAPI document that
`$ref`s them, and the golden fixtures are authoritative; the Go, Python and
TypeScript types are implementations of them, never the other way round.

Seven surfaces are planned (CHAOS-3270). Shipped so far: `principal.v1`
(CHAOS-4884), `error.v1` (CHAOS-4929). The remaining five are CHAOS-4930–4934.

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

`principal.v1` also carries a `2**53` bound for revision fields, where the
concern is exact float representability rather than type. Reuse the guard in
`internal/auth/contracts`; do not re-derive it.

### Constraints live in the cross-language dialect intersection

Five divergences are known. All are enforced by
`TestEveryWireSchemaStaysInTheCrossLanguageDialect` rather than by review.

| divergence | consequence |
| --- | --- |
| `format` is recorded and ignored by `github.com/google/jsonschema-go` | assert with `pattern`, never `format` |
| `\d` `\w` `\s` are Unicode in Python `re`, ASCII in RE2 and ECMA-262 | no shorthand classes |
| `(?i)` is valid in RE2 and Python, a SyntaxError in ECMA-262 | no inline flag groups |
| a trailing `$` matches before a final newline in Python only | the Python client rewrites it to `\Z`, failing closed on shapes it cannot rewrite |
| ajv strict mode refuses `required` in a subschema that does not define the property | an ajv lint, not a spec rule; declare the property in the subschema |

**Redundant keywords are not free.** A `maxLength` beside a pattern that already
bounds the same range makes Go report `maxLength` (first violation only) and
Python report `pattern` (all violations) for one fixture, so the manifest cannot
name a single expected keyword. One constraint per property.

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
ajv-2020 validating the same corpus, output pasted, not described (G-70) — and
a mutation table showing the corpus can fail: delete each guard, confirm exactly
the predicted fixtures go red, restore.
