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
concern is exact float representability rather than type.

**There is no shared guard yet, and an earlier version of this file told you to
reuse one.** That instruction was unactionable and worse than silence: a remedy
that does not exist looks like the problem is solved, so the next author
searches, finds nothing, re-derives, and does not know they re-derived. That is
not hypothetical — it is what produced `error.v1`'s integral-decimal P1, in a
surface written by the author of the note telling them to reuse it.

What actually exists today, to copy from:

| | where | covers |
| --- | --- | --- |
| Go | `principal.go` — a function-local `const` inside `Revision.UnmarshalJSON` | 2^53 only, and not callable from outside that method |
| Python | `error_envelope.py` `_require_wire_int` | zero-fraction decimals, and `bool` before `int` (order matters — `bool` subclasses `int`) |
| Python | `principal.py` `_parse_revision` | a separate copy, plus the 2^53 bound |

`contracts.py`, the shared module every surface already imports, contains
neither. **CHAOS-4940 tracks extracting one guard per language**; until it
lands, copy from the table above and expect to delete your copy afterwards.

### Constraints live in the cross-language dialect intersection

Five divergences are known. **They are enforced by three different mechanisms,
and one of them is not enforced at all** — an earlier version of this table
attributed all five to one Go test, which was false for two of them.

| divergence | consequence | actually enforced by |
| --- | --- | --- |
| `format` is recorded and ignored by `github.com/google/jsonschema-go` | assert with `pattern`, never `format` | `TestEveryWireSchemaStaysInTheCrossLanguageDialect` |
| `\d` `\w` `\s` `\b` are Unicode in Python `re`, ASCII in RE2 and ECMA-262 | no shorthand classes | same test |
| `(?i)` is valid in RE2 and Python, a SyntaxError in ECMA-262 | no inline flag groups | same test |
| a trailing `$` matches before a final newline in Python only | the Python client rewrites it to `\Z`, failing closed on shapes it cannot rewrite | **the client, not a schema guard** — `authclient/contracts.py` `strictly_anchored` |
| ajv strict mode refuses `required` in a subschema that does not define the property | an ajv lint, not a spec rule; declare the property in the subschema | **review only — there is no ajv runner in this repository** |

That last row is a real gap, not a formality. **No committed code or CI job runs
ajv against these contracts**, so the ECMA-262 leg of every "three languages
agree" claim rests on a runner the author supplied locally. A schema edit that
breaks ECMA compatibility passes everything in this repo. Treat the ECMA column
of any proof table as unreproducible until that changes. **CHAOS-4941 tracks
committing a runner and wiring it into CI.**

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

**A regex cannot know February has 28 days.** `2026-02-30` satisfies the
pattern; both clients refuse it at parse. That is a genuine client-enforced
rule and it has a `reject_by_client` fixture stating so — the alternative is a
gap that reads as an oversight rather than as a boundary.

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
ajv-2020 validating the same corpus, output pasted, not described (G-70) — with
the standing caveat above that **the ajv leg is run from a local runner and is
not reproducible from this repository** — and
a mutation table showing the corpus can fail: delete each guard, confirm exactly
the predicted fixtures go red, restore.
