# Codex PR-round prompt of record — 2026-10-30 14:1xZ (D6521; chris: "The cap has been 2 and anything above that means the prompt is wrong.")

Owned by pm. Lanes fill ONLY the bracketed fields. Max 2 rounds per PR (D6520). No round after a CLEAN verdict.
chris rulings 2026-10-30: round 2 sees the round 1 verdict + the delta (replaces "no round history"); round 1
reports findings by CLASS (root cause once, with all its cells).
Source: opus audit /var/lib/oci-cache/lane-scratch/pm-prompt-audit/REPORT.md (of 27 P1s on the 4-round PRs,
19 were the same class found again, 2 were pre-ruled items, 2 were out-of-contract input).
Previous version (all amendments and history): codex-review-prompt-of-record.md.pre-d6521-0482e063.

Until launcher v4.8.37 (copies .codex-review-r1.md and matches the SEVERITY bar): for round 2 the lane pastes
the round 1 verdict VERBATIM into the [r2 round 1 verdict] bracket (the reviewer's own text, no lane notes).

---- PROMPT (copy verbatim, fill brackets) ----

Review of PR #[N], round [1|2] of 2, at tip [full tip sha] against origin/main [full main sha].

What this change does, for whom, and its contract:
[2-6 sentences from the ticket and PR body: the change, its user, what "working" means. State the
contract as a bounded set: which inputs or shapes it must accept and which it must refuse, and the
threat model (who is trusted). No "every", "any" or "all" without the bound in the same sentence.
No seam list, no file list.]

Diff: `git diff [main sha]...[tip sha]` — [F] files, +[A]/-[D].
[r2 only] Delta: `git diff [r1 tip]...[tip sha]` — [F2] files, +[A2]/-[D2].
[r2 only] Round 1 verdict, verbatim:
[r2 round 1 verdict]

The PR body (## TEST-EVIDENCE, ## RISK-NOTES) is at .codex-review-context.md. Its test claims are
unverified until you execute them. Its ruled items are scope (see OUT OF SCOPE), not claims.

do not read outside the repository root

SCOPE: the lines this diff adds or changes, and the behaviour they control. Use the change the way its
user would, from scratch, and prove each finding by running it. Code the diff does not touch is in
scope only where a changed line calls it or relies on it for the contract above.

OUT OF SCOPE — at most one P3 line each under "Follow-ups"; never P1 or P2:
1. A RISK-NOTES item that cites a ruling (D<n>) or a ticket. If you think the ruling is wrong, give
   one P3 line with your repro; it does not block.
2. Host or run-time state the change cannot see: values set at deploy time, host files, secrets,
   other repositories, live services, CI-only architecture.
3. Input outside the stated contract: corrupt stored data, values past a wire or database limit, a
   trusted operator who builds a hostile layout on purpose.
4. Defects in code the diff does not touch and does not rely on.
5. A guard that reads static text when the contract says it reads static text.

SEVERITY (no executed repro, no P1):
P1 = executed repro, AND an in-scope input that a normal user, operator or CI run can produce, AND
     the contract breaks: a required refusal does not happen, a valid input is refused, wrong data,
     a crash, a credential reaches a party it must not, or behaviour that worked on main regresses.
P2 = an in-scope defect that needs an unusual but legal input, or one you could not reproduce
     (paste what you ran).
P3 = test strength, style, docs, and all OUT OF SCOPE items. Only a P1 makes the verdict NOT CLEAN.

ROUND 1: one full pass over the diff; report by CLASS. A class is one root cause (one missing check,
one wrong assumption) that can show at more than one input, site, field, form or nesting level. When
you find one instance, name the root cause, list the whole domain inside SCOPE that the same check
handles, execute every cell, and report the class ONCE with a table: cell | command | output |
accepted/refused. Then give the fix that closes the class, not the cell. Report every class now;
hold nothing for a later round.

ROUND 2: verify the fixes, then look for new P1s only in the delta.
(a) For each round 1 class: run its table again on this tip, add the cells the fix creates, and
    report CLOSED or OPEN with the failing cells.
(b) Review only the delta lines, for NEW P1s; a fix that refuses a valid input is a P1.
(c) Report no new P2 or P3. Do not review unchanged lines again. A P1 you see in unchanged code goes
    under "Missed in round 1" with its repro; the lead rules on it. There is no round 3.

Question, answered verbatim before the verdict: what does this change make observable, and what
regression in it would be invisible at Info?

Final line, alone, exactly one of: VERDICT: CLEAN | VERDICT: NOT CLEAN

---- END ----

Rules: diff counts come from `git diff --numstat`, never typed. The launcher writes the current PR body to
`<lane worktree>/.codex-review-context.md` (gitignored, untracked) and copies it into the review worktree.
The output contract (codex-output-contract.md) still binds the reviewer's final message.
On r2 NOT CLEAN the lead stops and sends pm both verdicts + the P-list (D6520). Hash this file at fill time.
