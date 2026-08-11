"""Live Python GitLab MR sync traversal oracle."""

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.oracle_pairs._gitlab_pr_family_traversal import (
    build_traversal,
    traversal_fields,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/prs/traversal",
        build_row=build_traversal,
        reflected_fields=traversal_fields,
    )
)
