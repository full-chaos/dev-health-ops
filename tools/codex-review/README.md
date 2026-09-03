# Codex review wrapper

`codex-review.sh` runs the adversarial Codex review over a lane's diff.
`verify-round-repros.py` checks that a round's proof repros actually run.

## Install paths

- Installed on bigboy: `~/.local/lib/dev-health-review/`
- Installed locally: `dev-health/scripts/codex-review.sh`

## Install convention

Never edit the installed file in place. Write the new version as a sibling
`.new` file, set the exec bit (`chmod 755 file.new`, or `install -m 755
file.new target`), then atomically `mv` it over the installed path. This
avoids a partial-write window for any review running mid-copy, and a
non-executable install.

After the `mv`, read back: `sha256sum` (must match the canonical hash
below), `stat -c %a` (must read `755`), and `bash -n` (must parse clean).

Canonical v4.8.1 hashes:

- `codex-review.sh`:
  `7eab082c157a48f3eb0960d729d6569b039ee671491a196fa0581e5c59db4cfb`
- `verify-round-repros.py` (installed on bigboy):
  `4572dd0b5d8ae3cbd0e5054c50f69a75cdeec8f6a3f593258614fbb9c39e31c0`

The in-repo `verify-round-repros.py` is ruff-formatted (AST-identical to
the installed copy above; installed sha `4572dd0b...`), so its committed
bytes differ from that hash — that is expected.

## Sandbox by OS

Linux picks `workspace-write`: a read-only landlock sandbox permits no
writes at all, so Codex needs write access to run. The wrapper warns on any
`Read-only file system` error instead of failing silently.

## Version check

Before trusting a copy, verify its sha256 against the version noted in the
handoff or PR that shipped it.
