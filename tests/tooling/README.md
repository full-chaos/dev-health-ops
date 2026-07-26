# Mutation-testing harness

> **Scope of the mandate — CHAOS-3165.** Requiring a mutation plan for new work is scoped to the
> **Go port (CHAOS-3033)** and will be **removed when the port finalises**; CHAOS-3165 tracks
> retiring it, blocked by CHAOS-3033. This is a statement about scope, not about confidence: the
> requirement is **load-bearing until the port closes** and applies in full until then. The tool
> itself, `verify` in the gate, and this runbook outlive the mandate — only the obligation to
> author a plan for every change goes away.

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

# Clear the record after undoing a mutation BY HAND, when restore cannot.
python3 scripts/mutation_harness.py accept --digest $(shasum -a 256 <file> | cut -d' ' -f1)

# Execute a plan; fail on any survivor that is not declared.
python3 scripts/mutation_harness.py run --plan <plan.json> --assert-all-killed
python3 scripts/mutation_harness.py run --plan <plan.json> --only M3,M4
```

`verify` distinguishes three states, and the messages are different on purpose because the
instructions are opposite: a mutation **leaked** from a dead run says repair; a **live** run
holding the lock says wait; a lock with **no pid file** says neither — there is no pid to
wait for and none to check, so it says break the stale lock. Confusing the first two cost
real time; phrasing the third around a pid nobody has left the gate red behind advice no one
could act on.

### When `restore` refuses: `accept`

Several of `restore`'s refusals are correct *and* terminal. The snapshot is gone because a
proof command ran `git clean -fdX` (`.mutation-harness/` is gitignored). Or the file holds
neither the original nor the mutation, because you reconciled a foreign edit by hand — and
writing the snapshot would destroy it. In those states the right move is to fix the file
yourself, and until `accept` existed there was then **no way to clear the record**: `verify`
stayed red, the gate stayed red, and the only exit was deleting the record blind. A safe path
that dead-ends is what teaches people to take the unsafe one.

`accept` is that exit and it is **not** "ignore the record" — leak detection is the whole
point of the record, so clearing it is earned, and every piece of evidence is checked rather
than trusted:

| it demands | because |
| --- | --- |
| the file still resolves to the recorded `device:inode` | otherwise the file actually holding the mutation stays broken *and* unrecorded |
| `--digest` naming the file's **current** content | pins the decision to bytes. If anything writes the file between your inspection and the acceptance, it fails rather than clearing against content nobody approved |
| the file is **not** the recorded mutation | that state is the leak itself, not a repair. Use `restore` |
| the replacement text appears **nowhere an intact anchor explains** | this is what makes it a measurement. A file merely edited near the mutation, or reformatted while still mutated, fails it |

It never writes to your source. The only thing it changes is the record.

**What it deliberately does not check: that the original text came back.** An earlier version
counted anchor occurrences, and counting is location-blind — with a deleted-clause mutation, two
*comments* containing the anchor satisfy any count while every code site stays mutated. A check the
file's own prose can satisfy is not a check; it is the doc-comment failure this harness refuses
anchors for, arriving through the command that clears the harness's own record. Absence of the
replacement is what actually answers *is a mutation still applied*, so that is the property tested.
The cost is a conservative refusal: where the replacement text legitimately occurs elsewhere in the
file, the tool cannot tell that from a survivor and refuses both, naming the manual last resort
rather than pretending to be sharper than it is.

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
| `build` | argv array, plan-level or per-mutation. Must exit 0 **before** the mutation (or the run reports `BASELINE_FAILED`, because a build command that cannot pass on a clean tree would report every mutation `INVALID` and blame the mutations for its own defect — measured while writing this repo's self-check plan) and again **after** it. Without it, a mutation that fails to compile records `KILLED` — a build break and a failing assertion are the same exit code, and a build break runs no test at all. Its absence is reported as a warning on every mutation. **Match the proof's build configuration exactly**: if the proofs run `-tags integration`, so must the build. A plain `go build` passed a mutation whose orphaned variable only broke the tagged configuration, so the check would have passed while the proof could not run — a narrower version of the false confidence it exists to prevent. Interpreted languages have one too: for Python it is importing the mutated module, which catches a syntax error or an import-time `NameError`. |
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

**Nineteen guards, nineteen kills, zero `INVALID`** — measured on 2026-07-26 against this tree, not
claimed. Check the number against a real run rather than trusting this line: a previous version of
this sentence said eleven while two of its anchors had drifted to match zero lines, so the run
actually produced nine kills and two `INVALID`. A stale claim about mutation coverage is the same
false confidence the tool exists to prevent, wearing the tool's own badge.

The plan's `$limitation` field names what it does *not* cover — the record-before-apply ordering,
the restored-byte digest comparison, the lock, the schema validators, the build/`INVALID` routing,
`STALE_DECLARATION` routing, and kill-site extraction are pinned by `test_mutation_harness.py`
instead, and the declared `build` catches syntax and import errors only. A plan that looks complete
is how a coverage gap survives, so the gaps are stated where the coverage is claimed.

**A mutation whose proof asserts a MESSAGE is not a kill.** H9 originally disabled `restore`'s
specialised live-holder refusal — but a generic refusal further down still refused, so the tool
still did the right thing and only the wording changed. It reported `KILLED` purely because its
proof expected the specialised sentence. It now targets the `--force` guard, whose absence
genuinely writes the source, clears the record and evicts a live run's lock, and its proof asserts
that state. When a kill looks decorative, either give the proof a behaviour to assert or delete the
mutation and say so.
