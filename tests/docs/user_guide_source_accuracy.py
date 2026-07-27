"""Source-accuracy constants for the canonical user guides.

The archived-corpus helpers that lived here (``guide_source_accuracy_errors``,
``fixture_truth_errors``, ``work_graph_search_errors`` and their supporting tables) were
removed with the tests that consumed them: their inputs were the legacy user-guide
corpus, its fixture-capture metadata, and the search-acceptance manifest, none of which
survive in the canonical tree. Only the constants still asserted against live pages
remain.
"""

from typing import Final

PR_FLOW_UNSUPPORTED_CLAIMS: Final = (
    "review latency",
    "first review",
    "merge timing",
    "PR stages",
)
