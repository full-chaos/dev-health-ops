"""Per-domain mixins composed by :class:`dev_health_ops.fixtures.generator.SyntheticDataGenerator`.

This package splits the large :mod:`dev_health_ops.fixtures.generator` module
into domain-focused mixin classes. The composer in ``generator.py`` inherits
from all mixins so the public API (``SyntheticDataGenerator``) is unchanged.
"""

from dev_health_ops.fixtures.generators.ai_governance import (
    generate_ai_tool_allowlist_entries,
)
from dev_health_ops.fixtures.generators.ai_workflow import AiWorkflowGeneratorMixin
from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.fixtures.generators.commits import CommitsGeneratorMixin
from dev_health_ops.fixtures.generators.incidents import IncidentsGeneratorMixin
from dev_health_ops.fixtures.generators.interactions import InteractionsGeneratorMixin
from dev_health_ops.fixtures.generators.investments import InvestmentsGeneratorMixin
from dev_health_ops.fixtures.generators.pipelines import PipelinesGeneratorMixin
from dev_health_ops.fixtures.generators.prs import PrsGeneratorMixin
from dev_health_ops.fixtures.generators.teams import TeamsGeneratorMixin
from dev_health_ops.fixtures.generators.work_items import WorkItemsGeneratorMixin

__all__ = [
    "BaseGeneratorMixin",
    "AiWorkflowGeneratorMixin",
    "generate_ai_tool_allowlist_entries",
    "CommitsGeneratorMixin",
    "IncidentsGeneratorMixin",
    "InteractionsGeneratorMixin",
    "InvestmentsGeneratorMixin",
    "PipelinesGeneratorMixin",
    "PrsGeneratorMixin",
    "TeamsGeneratorMixin",
    "WorkItemsGeneratorMixin",
]
