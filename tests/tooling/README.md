# Mutation-testing harness

**Use `scripts/mutation_harness.py`. Do not write your own.** Three ad-hoc per-lane
harnesses produced false results on 2026-07-26 — one left a mutation on disk while
reporting a restore, one reverted unrelated uncommitted edits with `git checkout`, one
waited on itself forever — and every failure was the same shape: *the harness could not
detect its own failure*. The rationale for each guard is in the module docstring; read it
before changing the tool.

## Commands

```sh
# Is this tree trustworthy? Runs first in ci/local_validate.sh.
python3 scripts/mutation_harness.py verify

# Repair a mutation left applied by a killed run.
python3 scripts/mutation_harness.py restore

# Execute a plan; fail on any survivor that is not declared.
python3 scripts/mutation_harness.py run --plan <plan.json> --assert-all-killed
python3 scripts/mutation_harness.py run --plan <plan.json> --only M3,M4
```

`verify` fails both when a mutation leaked from a dead run **and** while a run is live, but
the two messages are different on purpose: one says repair, the other says wait. Confusing
them cost real time.

### What `verify` does not prove

It answers exactly one question — *did this harness leave a mutation applied?* — by reading
its own record. Both of these were measured, not assumed:

- A branch with **no plan and no state passes trivially.** So a branch that predates this
  tool does not go red when the tool lands; there is no "registered plan" requirement.
- A mutation applied by a **hand edit or another tool leaves no record, and `verify` reports
  the tree clean.**

Proving a tree is unmutated would need a pristine digest for every source file, which
nothing here has. `verify` passing means "this harness is not mid-run and did not leak" —
the correct scope, but a narrower claim than "the source is pristine". Do not quote it as
the wider one; believing a check proves more than it does is the failure this tool exists
to prevent.

## Plan format

```json
{
  "schema_version": 1,
  "name": "scheduled-report producer",
  "mutations": [
    {
      "id": "M22",
      "file": "internal/scheduler/fixed/reports.go",
      "find": "if linkedRunID == nil || runStatus == nil {",
      "replace": "if false && (linkedRunID == nil || runStatus == nil) {",
      "expect_occurrences": 1,
      "rationale": "the occurrence-links-run guard is also a nil-dereference guard",
      "proof": [
        ["go", "test", "-tags", "integration", "-run",
         "^TestReplayFailsWhenTheOccurrenceNoLongerLinksItsRun$",
         "./internal/scheduler/fixed/"]
      ]
    }
  ]
}
```

| field | why it is required |
| --- | --- |
| `proof` | argv **arrays**, not shell strings — the plan is data, not an execution surface. Every command must pass on the clean tree and at least one must fail mutated. A mutation nothing observes is not a measurement. |
| `rationale` | State the property under test. Without it a verdict is uninterpretable six weeks later. |
| `build` | argv array, plan-level or per-mutation. Must exit 0 **after** the mutation is applied. Without it, a mutation that fails to compile records `KILLED` — a build break and a failing assertion are the same exit code, and a build break runs no test at all. Its absence is reported as a warning on every mutation. **Match the proof's build configuration exactly**: if the proofs run `-tags integration`, so must the build. A plain `go build` passed a mutation whose orphaned variable only broke the tagged configuration, so the check would have passed while the proof could not run — a narrower version of the false confidence it exists to prevent. |
| `expect_occurrences` | Defaults to 1 and is enforced. One real mutation landed in a doc comment because the anchor appeared twice, and a mutation in prose reads exactly like a coverage gap. |
| `expected_survivor_reason` | Declare a survivor you have judged acceptable — a genuinely unobservable change, or redundancy with no reachable state left to assert. Undeclared survivors fail `--assert-all-killed`. |
| `allow_comment_anchor` | Comment lines are refused by default for the reason above. |

## What a verdict means

`KILLED` — the proof went green → red → green. `SURVIVED` — nothing noticed, and it is
**your** job to say which of three things that is: a missing test, an invalid mutation
(no-op, wrong target, or a test asserting the constant being mutated), or genuine
redundancy where no reachable state remains. Only the first is a coverage gap; reporting
redundancy as a gap invites deleting a real guard.

**Check the kill site against the mutation's `rationale`.** A verdict says a test noticed; the
site says *which* test noticed *what*. Real cases: one mutation died in a seeding helper with a
count mismatch rather than at the invariant written for it, and one had a rationale claiming to
pin tenant *isolation* while the mutation actually swallowed the error and pinned *detection* —
both real properties, but only one of them covered, and a green tick could not tell them apart.
The rationale is a claim; the kill site is what checks it.

`BASELINE_FAILED` — the proof was already red, so the file was never touched. `INVALID` —
the mutation measured nothing: the anchor was rejected before any write, or the mutated source
did not build. `STALE_DECLARATION` — a mutation declared to survive was killed, so the
declaration describes code that has since changed and must be re-derived, not trusted.

## Two rules the tool cannot enforce for you

**Mutate compound predicates clause by clause, never as a unit.** A three-clause condition
mutated wholesale reported `KILLED` while one clause inside it was both unasserted and
*wrong* — that is how the Python-authored-occurrence coexistence bug was found. Unit
mutation measures the condition; clause mutation measures the clauses.

**Never verify a restore with a build or a git check.** And note the mirror image: a build is
not a *kill* either. Both directions of that mistake cost real time today.

**Never restate a plan's anchors anywhere else.** A copy is a second source of truth and it will
drift — one lane's shell harness restated its anchors space-indented against tab-indented Go
sources, so every anchor matched zero times and the run measured nothing, while a sibling checker
reading the plan directly stayed green the whole time. Drive from the plan. `go build` and `go vet` both pass on
`if false && (guard)`, and `git diff` reports an untracked file clean whatever it contains.
The harness asserts a content digest for exactly this reason.

## Self-check

The harness's own guards are mutated by the harness:

```sh
python3 scripts/mutation_harness.py run \
  --plan tests/tooling/mutation-plans/mutation_harness.json --assert-all-killed
```

Eleven guards, eleven kills. The plan's `$limitation` field names what it does *not* cover — the
record-before-apply ordering, the lock, and the schema validators are pinned by
`test_mutation_harness.py` instead. A plan that looks complete is how a coverage gap
survives, so the gaps are stated where the coverage is claimed.
