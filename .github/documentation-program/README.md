# Documentation remediation — Phase 1 baseline

This directory holds internal program evidence for the **User Guides & Development Documentation** remediation. Nothing here is public product documentation.

## Phase 1 outputs

1. `scripts/docs_inventory_v2.py` — deterministic source and tooling inventory.
2. `reader-tasks.md` — representative reader tasks used for IA, search, and journey review.
3. `gitlab-pattern-audit.md` — explicit adopt/adapt/reject decisions for the GitLab benchmark.
4. `current-preview-baseline.md` — WIP preview and maintenance baseline.
5. Generated `.build/documentation-inventory.json` — one row per page or supporting artifact.

## Commands

```bash
python scripts/docs_inventory_v2.py
python -m pytest tests/docs/test_docs_inventory_v2.py -q
```

The inventory is a reporting input. It does not decide publication, navigation, or content disposition automatically.
