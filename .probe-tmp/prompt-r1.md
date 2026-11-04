Adversarial review of PR #5710 at tip fb9195d8f14b8123a463091580cce30b524832eb against origin/main 306ab602210e56b6268ed7f2d1f7ec2e13c4d046.

What this change is trying to accomplish, and for whom:
The investment materialize job (internal/jobs/investment) used to allocate a work unit that had no repository evidence to the repositories owned by the teams of its member issues (a final "team ownership fallback", allocation_source team_ownership, repo_source team:<ids>). chris ruled that fallback wrong and asked to drop it: a team's repositories are never a unit's repository. After this change such a unit gets no repository share and is shown as a separate unattributed share, using the rows the writer already produced (an 'empty' row, or an 'active_hours_unassigned' row with the unit's effort, repo_id NULL). Operators and the analytics readers (investment breakdowns by repository and team, the sankey coverage) must keep working: rows stay append-only, readers take each unit's newest allocation generation, persisted per-unit identity, categorization and organization totals must not change, and units that are not recomputed keep their older generation until they are. Docs for the changed behaviour are part of the change.

Diff: git diff 306ab602210e56b6268ed7f2d1f7ec2e13c4d046...fb9195d8f14b8123a463091580cce30b524832eb -- 25 files, +160/-3467.

The PR body (## TEST-EVIDENCE, ## RISK-NOTES) is at .codex-review-context.md in this worktree. Every claim in it is unverified until you execute it.

do not read outside the repository root

Find the ways this change fails to accomplish the above, or breaks something that worked, and prove each one by running it: a test case, a command, a bring-up, a request -- executed, with its output. Use the change the way its user would, from scratch, and go wherever the change can reach; nothing reachable is out of scope because it is not named here. A claim without executed evidence, a test that cannot fail, and a guard that checks text instead of behaviour are findings too. Report each finding with path:line, severity P1-P3, and the executed reproduction. A P1 or P2 carries its own `$ ` command line and its real output, run in this round; one you could not execute is P3 OPINION (not executed), or dropped.

Question, answered verbatim before the verdict: what does this change make observable, and what regression in it would be invisible at Info?

Final line, alone, exactly one of: CLEAN | NOT CLEAN | BLOCK | REQUEST CHANGES
