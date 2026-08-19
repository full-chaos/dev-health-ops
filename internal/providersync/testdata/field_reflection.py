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


def class_annotated_field_names(source: str, class_name: str) -> frozenset[str]:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        fields = {
            statement.target.id
            for statement in node.body
            if isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and not statement.target.id.startswith("_")
            and isinstance(statement.value, ast.Call)
            and isinstance(statement.value.func, ast.Name)
            and statement.value.func.id == "mapped_column"
        }
        if fields:
            return frozenset(fields)
        break
    raise ValueError(
        f"class_annotated_field_names: no annotated public fields found for {class_name!r}"
    )


def dataclass_field_names(source: str, class_name: str) -> frozenset[str]:
    """Return every public annotated field declared by a production dataclass.

    Provider work-item normalizers return dataclasses directly rather than a
    dict literal or ORM row. Reflecting the owning class keeps the oracle's
    completeness set tied to the real semantic batch contract: adding a field
    to the dataclass makes every pair fail until it compares or explicitly
    excludes that field.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        fields = {
            statement.target.id
            for statement in node.body
            if isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and not statement.target.id.startswith("_")
        }
        if fields:
            return frozenset(fields)
        break
    raise ValueError(
        f"dataclass_field_names: no annotated public fields found for {class_name!r}"
    )


def typed_dict_field_names(source: str, class_name: str) -> frozenset[str]:
    """Return every annotated field declared by one production TypedDict.

    TestOps producers construct rows through ``TypedDictName(...)`` calls rather
    than ORM constructors or dict literals. Reflecting the owning TypedDict is
    the same structural completeness boundary for that shape: adding a field to
    the production contract makes the oracle fail until its live row emits or
    explicitly excludes the field.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        fields = {
            statement.target.id
            for statement in node.body
            if isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and not statement.target.id.startswith("_")
        }
        if fields:
            return frozenset(fields)
        break
    raise ValueError(
        f"typed_dict_field_names: no annotated public fields found for {class_name!r}"
    )


def call_dict_literal_keys(source: str, method_name: str) -> frozenset[str]:
    """Return the string keys of the dict literal passed to ``x.method_name({...})``.

    Some contracts are stated by a CALL rather than by a type: the investment
    classifier's production call site builds its artifact as an inline dict
    literal argument, and which keys that literal does NOT contain is exactly
    what makes three config rules unreachable. ``dict_literal_keys`` cannot see
    it -- there is no named variable and no return statement to anchor on -- so
    a test wanting that premise had no choice but to transcribe it, which is how
    it rots the day the call site gains a key.

    Raises when the call is absent or its first positional argument is not a
    non-empty string-keyed dict literal, rather than returning an empty set: a
    silently empty premise makes every assertion built on it vacuously true.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if (
            not isinstance(node, ast.Call)
            or not isinstance(node.func, ast.Attribute)
            or node.func.attr != method_name
            or not node.args
            or not isinstance(node.args[0], ast.Dict)
        ):
            continue
        keys = {
            key.value
            for key in node.args[0].keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }
        if keys:
            return frozenset(keys)
    raise ValueError(
        f"call_dict_literal_keys: no `.{method_name}(...)` call taking a "
        "non-empty string-keyed dict literal was found in the given source -- "
        "the call site has moved or changed shape, and every premise derived "
        "from it must be re-derived rather than silently emptied"
    )


def dict_assigned_keys(
    source: str, function_name: str, dict_var_name: str
) -> frozenset[str]:
    """Reflect literal keys assigned through ``mapping[\"key\"] = value``."""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if (
            not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            or node.name != function_name
        ):
            continue
        keys: set[str] = set()
        for statement in ast.walk(node):
            if not isinstance(statement, ast.Assign):
                continue
            for target in statement.targets:
                if (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == dict_var_name
                    and isinstance(target.slice, ast.Constant)
                    and isinstance(target.slice.value, str)
                ):
                    keys.add(target.slice.value)
        if keys:
            return frozenset(keys)
        break
    raise ValueError(
        f"dict_assigned_keys: no literal subscript assignments to {dict_var_name!r} "
        f"inside {function_name!r}"
    )
