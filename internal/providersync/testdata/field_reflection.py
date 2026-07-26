"""Static field-set reflection for CHAOS-3162's oracle_registry (codex
finding #1).

A pair's `reflected_fields` callable must answer "what is the COMPLETE set
of fields the production function is capable of emitting" without either
(a) hand-maintaining a second list that can drift from the function it
describes, or (b) executing the function itself (which would need the same
case data a comparison run does, and would only prove completeness for
whatever cases happen to be supplied -- exactly the coverage gap this
module exists to close independent of any particular case).

`dict_literal_keys` answers it by parsing the target function's source with
`ast` -- no import, no execution, so it works under the same stock
interpreter constraint as everything else in this directory -- and
collecting every string-literal key assigned into one of the named dict
variables anywhere in that function's body. This mirrors, structurally,
`build_git_pull_request`'s own shape (a `values` dict, unconditionally
populated, plus an `optional_values` dict merged in when non-None): the
reflected field set is derived from the SAME two dict literals the function
actually builds its result from, not from a parallel description of them.
"""

from __future__ import annotations

import ast

# The pseudo dict-var-name that matches a bare `return {...}` literal
# instead of an assignment. Used by pairs whose "production" function (or,
# for a pinned fallback like github_prs_window.py, whose pinned copy) builds
# its result as a single return expression rather than an intermediate
# named dict.
RETURN_LITERAL = "return"


def dict_literal_keys(
    source: str, function_name: str, dict_var_names: tuple[str, ...]
) -> frozenset[str]:
    tree = ast.parse(source)
    target_function: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function_name
        ):
            target_function = node
            break
    if target_function is None:
        raise ValueError(
            f"dict_literal_keys: no function named {function_name!r} found in "
            "the given source -- has it been renamed or moved?"
        )

    keys: set[str] = set()
    for node in ast.walk(target_function):
        # Three shapes matter: a plain assignment (`values = {...}`), an
        # annotated assignment (`values: dict[str, Any] = {...}` --
        # build_git_pull_request uses this form for `values`), and a bare
        # `return {...}` literal (matched when RETURN_LITERAL is requested).
        # Missing any of these silently under-reflects the function they
        # are meant to describe.
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
            targets = node.targets
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.value, ast.Dict)
            and node.target is not None
        ):
            targets = [node.target]
        elif (
            RETURN_LITERAL in dict_var_names
            and isinstance(node, ast.Return)
            and isinstance(node.value, ast.Dict)
        ):
            for key_node in node.value.keys:
                if isinstance(key_node, ast.Constant) and isinstance(
                    key_node.value, str
                ):
                    keys.add(key_node.value)
            continue
        else:
            continue
        if not any(
            isinstance(target, ast.Name) and target.id in dict_var_names
            for target in targets
        ):
            continue
        for key_node in node.value.keys:
            if isinstance(key_node, ast.Constant) and isinstance(key_node.value, str):
                keys.add(key_node.value)

    if not keys:
        raise ValueError(
            f"dict_literal_keys: found {function_name!r} but no string-keyed "
            f"dict literal assigned to (or returned via) any of "
            f"{dict_var_names!r} inside it -- the function's shape has "
            "likely changed and this reflector needs updating to match, "
            "not silently returning an empty/stale set"
        )
    return frozenset(keys)
