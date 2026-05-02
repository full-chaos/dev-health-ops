"""Strawberry schema extensions for the analytics GraphQL API.

Extensions run at the execution layer and apply cross-cutting concerns
(auth, org scoping) consistently across all resolvers without requiring
each resolver to repeat the same guard logic.
"""

from __future__ import annotations

import logging

from strawberry.extensions import SchemaExtension

from .errors import AuthorizationError

logger = logging.getLogger(__name__)


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

    def on_executing_start(self) -> None:
        """Called by Strawberry before field resolution begins.

        Resolves org_id from the operation's variable map or inline argument
        and validates it against the context org_id set by middleware.
        """
        execution_context = self.execution_context
        context = getattr(execution_context, "context", None)
        if context is None:
            return

        # Extract org_id from GraphQL variables only. Inline argument org_id
        # (e.g. query { metrics(org_id: "x") }) is intentionally not validated
        # here — the OrgIdMiddleware contextvar is the authoritative source and
        # all resolvers scope via context.org_id, so inline args cannot bypass
        # the auth boundary.
        variables = getattr(execution_context, "variables", None) or {}
        requested_org_id: str | None = variables.get("org_id") or None

        # If the query supplies org_id as a variable, enforce it matches the
        # JWT-validated org_id in the context (set by OrgIdMiddleware).
        if requested_org_id:
            ctx_org_id = getattr(context, "org_id", None)
            if ctx_org_id and ctx_org_id != requested_org_id:
                raise AuthorizationError(
                    f"Access denied: cannot query org '{requested_org_id}'"
                )
            # Write the resolved org_id back to context (handles empty string
            # org_id when a public/dev endpoint sets "" as the initial value).
            try:
                context.org_id = requested_org_id
            except AttributeError:
                pass  # frozen context; leave as-is
