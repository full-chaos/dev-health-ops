"""Production-representative per-role readiness probes (CHAOS-3285).

Each probe is built exclusively from the real production producers
(``PromptComposer``, ``AskDevToolRegistry``, ``DevRunLimits``, the
``DevAnswer``/``DevToolResult`` contracts) -- never a hand-authored
miniature -- so a passing probe genuinely demonstrates the provider can
handle the real wire shape, not a reduced synthetic echo.
"""

from __future__ import annotations
