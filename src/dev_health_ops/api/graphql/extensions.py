"""Strawberry schema extensions for the analytics GraphQL API.

Extensions run at the execution layer and apply cross-cutting concerns
(auth, org scoping) consistently across all resolvers without requiring
each resolver to repeat the same guard logic.
"""

from __future__ import annotations

import logging

from graphql.language.ast import (
    FieldNode,
    FragmentDefinitionNode,
    FragmentSpreadNode,
    InlineFragmentNode,
    OperationDefinitionNode,
    StringValueNode,
    VariableNode,
)
from strawberry.extensions import AddValidationRules, SchemaExtension

from .errors import AuthorizationError
from .security import get_graphql_validation_rules

logger = logging.getLogger(__name__)


def _operation_org_id(execution_context) -> str | None:
    document = getattr(execution_context, "graphql_document", None)
    if document is None:
        return None
    operation_name = getattr(execution_context, "operation_name", None)
    operation: OperationDefinitionNode | None = None
    fragments: dict[str, FragmentDefinitionNode] = {}
    for definition in document.definitions:
        if isinstance(definition, FragmentDefinitionNode):
            fragments[definition.name.value] = definition
        elif isinstance(definition, OperationDefinitionNode) and (
            operation_name is None
            or definition.name is not None
            and definition.name.value == operation_name
        ):
            operation = definition
    if operation is None:
        return None

    variables = getattr(execution_context, "variables", None) or {}
    requested_org_ids: list[str] = []
    visited_fragments: set[str] = set()

    def collect(selection_set) -> None:
        for selection in selection_set.selections:
            if isinstance(selection, FieldNode):
                for argument in selection.arguments or ():
                    if argument.name.value not in {"orgId", "org_id"}:
                        continue
                    value = argument.value
                    raw_value: str | None
                    if isinstance(value, VariableNode):
                        raw_value = variables.get(value.name.value)
                    elif isinstance(value, StringValueNode):
                        raw_value = value.value
                    else:
                        raw_value = None
                    if (
                        not isinstance(raw_value, str)
                        or not raw_value
                        or raw_value != raw_value.strip()
                    ):
                        raise AuthorizationError("A valid organization ID is required")
                    requested_org_ids.append(raw_value)
                if selection.selection_set is not None:
                    collect(selection.selection_set)
            elif isinstance(selection, InlineFragmentNode):
                collect(selection.selection_set)
            elif isinstance(selection, FragmentSpreadNode):
                fragment_name = selection.name.value
                if fragment_name in visited_fragments:
                    continue
                visited_fragments.add(fragment_name)
                fragment = fragments.get(fragment_name)
                if fragment is not None:
                    collect(fragment.selection_set)

    collect(operation.selection_set)
    if not requested_org_ids:
        return None
    if len(set(requested_org_ids)) != 1:
        raise AuthorizationError("Only one organization may be queried per operation")
    return requested_org_ids[0]


def _rebind_context_org_id(context, org_id: str) -> None:
    rebind_org_id = getattr(context, "rebind_org_id", None)
    if callable(rebind_org_id):
        rebind_org_id(org_id)
        return
    context.org_id = org_id


class ConfiguredValidationRules(AddValidationRules):
    """Strawberry validation rules pre-bound to the dev-health policy."""

    def __init__(self, *, execution_context: object | None = None) -> None:
        super().__init__(get_graphql_validation_rules())


class OrgIdAuthExtension(SchemaExtension):
    """Strawberry extension that enforces org-ID scoping on every operation.

    Resolves the org_id from the query argument (if present) and writes it
    into the GraphQL context so that:
    - ``context.org_id`` is always authoritative for downstream resolvers.
    - Cross-org access is blocked before any resolver runs.
    - The per-field ``if context.org_id and context.org_id != org_id`` guard
      in schema.py can be removed (extension runs first).

    Usage
    -----
    Register in the schema:

        schema = strawberry.Schema(
            query=Query,
            extensions=[OrgIdAuthExtension],
        )
    """

    def on_execute(self):
        """Called by Strawberry before field resolution begins.

        Resolves org_id from the operation's variable map and validates it
        against the authenticated organization before field resolution.
        """
        execution_context = self.execution_context
        context = getattr(execution_context, "context", None)
        if context is None:
            yield
            return

        from dev_health_ops.api.services.auth import (
            get_impersonation_context,
            reset_current_org_id,
            set_current_org_id,
        )

        original_org_id = context.org_id
        org_context_token = None
        try:
            requested_org_id = _operation_org_id(execution_context)
            impersonation = get_impersonation_context()
            is_impersonating = bool(
                impersonation is not None and getattr(impersonation, "is_active", False)
            )
            is_superuser = bool(
                getattr(context.user, "is_superuser", False)
                and getattr(context.user, "is_superuser_verified", False)
            )
            if requested_org_id is not None:
                if (not is_superuser or is_impersonating) and (
                    requested_org_id != original_org_id
                ):
                    raise AuthorizationError(
                        f"Access denied: cannot query org '{requested_org_id}'"
                    )
                if requested_org_id != original_org_id:
                    _rebind_context_org_id(context, requested_org_id)
                    org_context_token = set_current_org_id(requested_org_id)
            yield
        finally:
            if org_context_token is not None:
                reset_current_org_id(org_context_token)
            if context.org_id != original_org_id:
                _rebind_context_org_id(context, original_org_id)
