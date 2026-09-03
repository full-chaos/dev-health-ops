# Codex review wrapper

`codex-review.sh` runs the adversarial Codex review over a lane's diff.
`verify-round-repros.py` checks that a round's proof repros actually run.

## Install paths

- Installed on bigboy: `~/.local/lib/dev-health-review/`
- Installed locally: `dev-health/scripts/codex-review.sh`

## Install convention

Never edit the installed file in place. Write the new version as a sibling
file, then atomically `mv` it over the installed path. This avoids a
partial-write window for any review running mid-copy.

## Sandbox by OS

Linux picks `workspace-write`: a read-only landlock sandbox permits no
writes at all, so Codex needs write access to run. The wrapper warns on any
`Read-only file system` error instead of failing silently.

## Version check

Before trusting a copy, verify its sha256 against the version noted in the
handoff or PR that shipped it.
