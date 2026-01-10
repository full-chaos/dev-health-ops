from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

from analytics.work_units import CANONICAL_SUBCATEGORIES, normalize_scores

from .llm_providers import get_provider

logger = logging.getLogger(__name__)

MAX_TEXT_ITEMS_PER_SOURCE = 4
MAX_TEXT_CHARS = 240


@dataclass(frozen=True)
class InvestmentCategorization:
    subcategories: Dict[str, float]
    textual_evidence: List[Dict[str, object]]
    uncertainty: List[Dict[str, object]]


CANONICAL_PROMPT = """You are categorizing work unit text into canonical investment subcategories.

Rules:
- Output JSON only. No markdown, no explanations.
- Use ONLY these subcategories as keys: {subcategories}
- Provide a probability distribution across all subcategories (values 0-1, sum to 1).
- Cite exact phrases from the input text in textual_evidence and tie each to a subcategory.
- Provide uncertainty statements per subcategory cluster.
- Do not invent facts or categories.

Output schema:
{{
  "subcategories": {{
    "feature_delivery.customer": 0.0,
    "feature_delivery.roadmap": 0.0,
    "feature_delivery.enablement": 0.0,
    "operational.incident_response": 0.0,
    "operational.on_call": 0.0,
    "operational.support": 0.0,
    "maintenance.refactor": 0.0,
    "maintenance.upgrade": 0.0,
    "maintenance.debt": 0.0,
    "quality.testing": 0.0,
    "quality.bugfix": 0.0,
    "quality.reliability": 0.0,
    "risk.security": 0.0,
    "risk.compliance": 0.0,
    "risk.vulnerability": 0.0
  }},
  "textual_evidence": [
    {{ "phrase": "...", "source": "issue_title", "subcategory": "operational.incident_response" }}
  ],
  "uncertainty": [
    {{ "subcategory": "operational.incident_response", "statement": "..." }}
  ]
}}
"""


def _truncate_text(value: str) -> str:
    compact = " ".join(str(value or "").split())
    if len(compact) <= MAX_TEXT_CHARS:
        return compact
    return f"{compact[:MAX_TEXT_CHARS].rstrip()}…"


def _format_texts(texts_by_source: Dict[str, List[str]]) -> str:
    sections: List[str] = []
    for source, texts in texts_by_source.items():
        if not texts:
            continue
        trimmed = [_truncate_text(text) for text in texts[:MAX_TEXT_ITEMS_PER_SOURCE]]
        lines = "\n".join(f"- {text}" for text in trimmed if text)
        sections.append(f"[{source}]\n{lines}")
    return "\n\n".join(sections)


def _extract_json(raw: str) -> Optional[Dict[str, object]]:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def _normalize_subcategories(payload: Dict[str, object]) -> Dict[str, float]:
    raw_scores = {
        category: float(payload.get(category, 0.0) or 0.0)
        for category in CANONICAL_SUBCATEGORIES
    }
    return normalize_scores(raw_scores, CANONICAL_SUBCATEGORIES)


def _normalize_textual_evidence(
    evidence: object,
    texts_by_source: Dict[str, List[str]],
) -> List[Dict[str, object]]:
    if not isinstance(evidence, list):
        return []
    allowed_sources = set(texts_by_source.keys())
    cleaned: List[Dict[str, object]] = []
    for entry in evidence:
        if not isinstance(entry, dict):
            continue
        phrase = str(entry.get("phrase") or "").strip()
        source = str(entry.get("source") or "").strip()
        subcategory = str(entry.get("subcategory") or "").strip()
        if (
            not phrase
            or not source
            or source not in allowed_sources
            or subcategory not in CANONICAL_SUBCATEGORIES
        ):
            continue
        cleaned.append(
            {
                "type": "text_phrase",
                "phrase": phrase,
                "source": source,
                "subcategory": subcategory,
            }
        )
    return cleaned


def _normalize_uncertainty(
    payload: object,
    subcategories: Dict[str, float],
) -> List[Dict[str, object]]:
    if isinstance(payload, str):
        payload = [{"subcategory": "", "statement": payload}]
    if not isinstance(payload, list):
        return []
    cleaned: List[Dict[str, object]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        subcategory = str(entry.get("subcategory") or "").strip()
        statement = str(entry.get("statement") or "").strip()
        if not statement:
            continue
        if subcategory and subcategory not in CANONICAL_SUBCATEGORIES:
            continue
        cleaned.append(
            {
                "type": "uncertainty_statement",
                "subcategory": subcategory,
                "statement": statement,
            }
        )
    if not cleaned and subcategories:
        top_subcategory = max(subcategories, key=subcategories.get)
        cleaned.append(
            {
                "type": "uncertainty_statement",
                "subcategory": top_subcategory,
                "statement": "Text evidence is limited; categorization suggests an initial interpretation.",
            }
        )
    return cleaned


async def categorize_investment_texts(
    texts_by_source: Dict[str, List[str]],
    *,
    llm_provider: str = "auto",
) -> Optional[InvestmentCategorization]:
    if not any(texts_by_source.values()):
        return None

    prompt = CANONICAL_PROMPT.format(
        subcategories=", ".join(CANONICAL_SUBCATEGORIES)
    )
    input_block = _format_texts(texts_by_source)
    full_prompt = f"{prompt}\n\nInput text:\n{input_block}"

    provider = get_provider(llm_provider)
    raw_response = await provider.complete(full_prompt)
    parsed = _extract_json(raw_response)
    if not isinstance(parsed, dict):
        logger.warning("Investment categorization failed to parse LLM response.")
        return None

    subcategories = _normalize_subcategories(parsed.get("subcategories", {}))
    textual_evidence = _normalize_textual_evidence(
        parsed.get("textual_evidence"), texts_by_source
    )
    uncertainty = _normalize_uncertainty(parsed.get("uncertainty"), subcategories)

    return InvestmentCategorization(
        subcategories=subcategories,
        textual_evidence=textual_evidence,
        uncertainty=uncertainty,
    )
