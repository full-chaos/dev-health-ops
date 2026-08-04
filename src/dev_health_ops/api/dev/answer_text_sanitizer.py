"""CHAOS-3377 defect 3: structural JSON-tail sanitization for model prose.

A live run showed a model-authored ``direct_summary`` ending in a stray
``}}}{`` -- leftover structural JSON characters, not prose. The narrative
provider boundary (``answer_frames/narrative_fallback.py``) already validates
its own output strictly, but the legacy v1 path (``dict(decision.value)`` in
``orchestrator.py``) takes the model's free-text fields as-is.

This is fixed structurally, not by replacing the literal string ``'}}}{'``:
that would fix the one reported string and nothing else shaped like it. The
rule instead is general -- strip a TRAILING run of bare JSON-structural
characters (``{ } [ ] " ,``) once real prose has ended, since ordinary English
text does not end in a bare run of those characters with no surrounding word
content. It is applied to every model-authored free-text field
(``direct_summary``, each claim's ``text``, each warning) at the seam where
the model's raw decision is turned into an answer candidate, before that
candidate is validated or persisted.
"""

from __future__ import annotations

import re

__all__ = ["sanitize_model_text"]

#: A trailing run of two or more bare JSON-structural characters, optionally
#: preceded/interleaved with whitespace -- e.g. ``'}}}{'``, ``' }] '``,
#: ``'"}'``. Two or more, not one: a single trailing brace/bracket is not by
#: itself distinguishable from legitimate prose that happens to end a
#: sentence inside a quotation of code or JSON ("the response was `{}`."),
#: and one is a much weaker signal of a parser-boundary leak than a bare run
#: of two or more with nothing else around them.
_TRAILING_JSON_ARTIFACT = re.compile(r'[\s{}\[\]",:]{2,}\Z')


def sanitize_model_text(text: str) -> str:
    """Strip a trailing JSON-structural artifact from model-authored prose.

    Repeats until stable (a single pass can uncover a second artifact
    directly behind the first, e.g. ``'... done.} }'``), then trims
    surrounding whitespace. Text with no such tail is returned unchanged.
    Never touches the interior of the string -- a legitimate brace pair
    inside a sentence ("the payload is `{}`  by default") is left alone,
    since the pattern only ever matches anchored at the very end.
    """

    if not text:
        return text
    previous = None
    cleaned = text
    while previous != cleaned:
        previous = cleaned
        cleaned = _TRAILING_JSON_ARTIFACT.sub("", cleaned)
    return cleaned.rstrip()
