"""Provider-agnostic LLM client for the trial's extraction candidate arm.

Step 2 of the bring-up plan (harness-design.md §7): plumbing only. This
module exists so :mod:`harness.arms.extraction` never talks to a provider
SDK directly -- exactly the same discipline every other arm adapter holds
(one seam, one place a provider swap touches).
"""

from __future__ import annotations
