# Legacy GitHub and GitLab Connectors

`src/dev_health_ops/connectors/` is legacy and frozen.
Do not add new provider code here.

## Relationship to providers/

New development belongs in `src/dev_health_ops/providers/<provider>/`.
Use `src/dev_health_ops/providers/base.py` for provider contracts and
`src/dev_health_ops/providers/_base.py` for shared async REST/TestOps adapter
helpers.

See the provider boundary in `../../../AGENTS.md`.

## Current contents

This package is kept for connector compatibility only.

- `base.py`, `models.py`, `exceptions.py`: legacy shared connector types.
- `github.py`, `gitlab.py`, `teams.py`, `testops.py`: legacy connector
  implementations and adapters.

## Migration status

- Moved to `providers/`: `github/`, `gitlab/`, `jira/`, `linear/`,
  `identity.py`, `teams.py`, `registry.py`, `status_mapping.py`,
  `team_bridge.py`, `normalize_common.py`, `normalize_helpers.py`, `pr_state.py`,
  `utils.py`, `_base.py`, `_ratelimit.py`.
- Still legacy here: the connector entrypoints for GitHub, GitLab, Teams, and
  TestOps, plus the connector base/model/exceptions layer.

The remaining connector files stay only until the compatibility cleanup lands.
