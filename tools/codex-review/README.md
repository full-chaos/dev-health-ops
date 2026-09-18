# Codex review wrapper

`codex-review.sh` runs the adversarial Codex review over a lane's diff.
`verify-round-repros.py` checks that a round's proof repros actually run.

## Install paths

`codex-review.sh` and `verify-round-repros.py` install differently; do not
conflate them.

`codex-review.sh`'s wrapper of record is the newest un-RETIRED
`codex-review.sh.v*` under `/var/lib/oci-cache/lane-scratch/_shared/` on
bigboy — the filename itself carries the version and a content hash, so
there is no single fixed installed path to keep in sync with this doc. A
`.RETIRED` marker beside an older version means a newer one has taken
over; never launch a `.RETIRED` version. `~/.local/lib/dev-health-review/
codex-review.sh` and `dev-health/scripts/codex-review.sh` are both retired
copies of the WRAPPER — do not launch either.

`verify-round-repros.py` is still installed at the one fixed path,
`~/.local/lib/dev-health-review/verify-round-repros.py` on bigboy, per the
"Install convention" and canonical hash below — that has not changed.

Launch `codex-review.sh` with `CODEX_REVIEW_PERMS=codex-review
CODEX_REVIEW_SANDBOX=workspace-write` set explicitly: an unset
`CODEX_REVIEW_PERMS` defaults to `legacy` mode (closed network, no
docker), and an unset/wrong `CODEX_REVIEW_SANDBOX` in `codex-review` mode
silently downgrades the round to read-only. The prompt of record is
filled from its own path, never retyped; see the `codex-review` skill for
the current pin and the fill rules.

## Install convention

Applies to `verify-round-repros.py`'s single fixed install path above
(`codex-review.sh` instead publishes a new versioned file under `_shared/`
with a `.RETIRED` marker on the version it supersedes, never an in-place
overwrite of an existing one). Never edit the installed file in place.
Write the new version as a sibling `.new` file, set the exec bit (`chmod
755 file.new`, or `install -m 755 file.new target`), then atomically `mv`
it over the installed path. This avoids a partial-write window for any
review running mid-copy, and a non-executable install.

After the `mv`, read back: `sha256sum` (must match the canonical hash
below), `stat -c %a` (must read `755`), and `bash -n` (must parse clean).

Canonical hash for `verify-round-repros.py` (installed on bigboy):
`4572dd0b5d8ae3cbd0e5054c50f69a75cdeec8f6a3f593258614fbb9c39e31c0`.

The in-repo `verify-round-repros.py` is ruff-formatted (AST-identical to
the installed copy above), so its committed bytes differ from that hash —
that is expected.

## Sandbox by OS

Linux picks `workspace-write`: a read-only landlock sandbox permits no
writes at all, so Codex needs write access to run. The wrapper warns on any
`Read-only file system` error instead of failing silently.

## Version check

Before trusting a copy, verify its sha256 against the version noted in the
handoff or PR that shipped it.
