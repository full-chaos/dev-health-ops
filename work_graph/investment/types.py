"""Shared datatypes for investment materialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class TextBundle:
    source_block: str
    source_texts: Dict[str, Dict[str, str]]
    input_hash: str
    text_source_count: int
    text_char_count: int
