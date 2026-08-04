"""CHAOS-3377 defect 3: structural JSON-tail sanitization for model prose.

A live run showed a model-authored ``direct_summary`` ending in a stray
``}}}{`` -- leftover structural JSON characters, not prose.

Where the leak can and cannot originate (verified, not assumed): the raw
provider completion is parsed with a single strict ``json.loads`` call
(``dev_health_ops/llm/agent/openai_compatible.py``, ``_normalize_response``,
``payload = json.loads(str(message.content or ""))``). That call either
succeeds -- producing a syntactically valid JSON object, whose ``value``
dict becomes ``AgentFinalAnswer.value`` unchanged -- or raises
``json.JSONDecodeError``, which is handled well above this module (a
provider/schema-repair error, never silently reaching ``decision.value``).
There is no lenient fallback parse (no fenced-JSON extraction, no
best-effort regex) anywhere on that path. So a reported trailing artifact on
``direct_summary`` cannot be an ENVELOPE malformation -- the envelope, by
construction, parsed as valid JSON -- it can only be literal content the
model put INSIDE an otherwise well-formed string value. There is no separate
"envelope" boundary to validate; the anomaly is embedded in the prose itself,
which is exactly why this module operates on the text, not the parse layer.

That constrains the fix to a PRECISE text-level rule, not a blanket one.
Codex adversarial review (round 2) reproduced three cases where the first
revision's blanket "trailing run of >=2 JSON-structural characters" regex
corrupted legitimate content: ``"Expected payload: {}"``, ``"Valid
alternatives are []"``, ``"Use an empty object: { }"`` -- each ends in a
syntactically VALID, balanced, well-nested bracket literal that is plausible
real prose (an inline example), not leaked debris. The fix distinguishes the
two with a real bracket-matching pass over the trailing run: a candidate tail
is stripped only when it is NOT a validly balanced/nested bracket sequence
(the reported ``'}}}{'`` opens nothing and closes from empty -- structurally
invalid by construction), never merely because it is JSON-shaped.
"""

from __future__ import annotations

import re

__all__ = ["sanitize_model_text"]

#: The maximal trailing run of bare JSON-structural characters (braces,
#: brackets, quotes, commas, colons, whitespace) -- the CANDIDATE tail to
#: validate. Extracted once; validity (not shape) decides whether it is
#: stripped. See ``_is_structurally_invalid``.
_TRAILING_CANDIDATE = re.compile(r'[\s{}\[\]",:]+\Z')

_OPENERS = {"{": "}", "[": "]"}
_CLOSERS = {"}": "{", "]": "["}


def _is_structurally_invalid(candidate: str) -> bool:
    """Whether ``candidate`` (a run of only ``{ } [ ] " , :`` and whitespace)
    is NOT a validly nested, fully-closed bracket sequence.

    A real stack-based bracket match, not a character-count balance check:
    counting opens vs. closes alone would call ``']['`` valid (one of each),
    when a close before any matching open is exactly the "leftover fragment"
    shape a leaked JSON tail has. Quotes/commas/colons are ignored for
    nesting purposes -- they carry no structural relationship here -- but
    still count as part of the candidate (so ``'": }'`` is still scanned).
    """

    stack: list[str] = []
    for char in candidate:
        if char in _OPENERS:
            stack.append(char)
        elif char in _CLOSERS:
            if not stack or stack[-1] != _CLOSERS[char]:
                return True
            stack.pop()
    return bool(stack)


def sanitize_model_text(text: str) -> str:
    """Strip a trailing JSON-structural artifact from model-authored prose,
    but only when the trailing run is not itself validly bracketed.

    ``"...done.}}}{"`` -> ``"...done."`` (the tail opens nothing and closes
    from empty -- invalid, stripped). ``"Expected payload: {}"`` is returned
    UNCHANGED (the tail is a balanced, well-nested, plausible inline
    example). Text with no trailing structural run at all is returned
    unchanged. Never touches the interior of the string -- the candidate
    pattern is anchored at the very end.
    """

    if not text:
        return text
    match = _TRAILING_CANDIDATE.search(text)
    if match is None:
        return text
    candidate = match.group(0)
    if not _is_structurally_invalid(candidate):
        return text
    return text[: match.start()].rstrip()
