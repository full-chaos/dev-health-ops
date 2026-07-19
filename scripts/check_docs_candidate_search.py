#!/usr/bin/env python3
"""Validate natural-language documentation queries against the built search index.

The check intentionally uses the generated MkDocs search index instead of exact page
headings or a browser screenshot. It verifies that a canonical destination is among
the highest-scoring unique URLs for a small, reader-oriented query set.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

TOKEN_RE = re.compile(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*")
TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class SearchDocument:
    location: str
    title: str
    text: str


@dataclass(frozen=True)
class SearchCase:
    query: str
    expected: str
    max_rank: int


def _normalize_url(value: str) -> str:
    parsed = urlsplit(value)
    path = parsed.path or "/"
    path = path.removesuffix("index.html")
    if not path.startswith("/"):
        path = f"/{path}"
    if not path.endswith("/") and Path(path).suffix == "":
        path = f"{path}/"
    return re.sub(r"/{2,}", "/", path)


def _plain_text(value: str) -> str:
    return html.unescape(TAG_RE.sub(" ", value or "")).lower()


def _stem(token: str) -> str:
    for suffix in ("ing", "ied", "ed", "es", "s"):
        if token.endswith(suffix) and len(token) - len(suffix) >= 4:
            if suffix == "ied":
                return f"{token[:-3]}y"
            return token[: -len(suffix)]
    return token


def _tokens(value: str) -> list[str]:
    return [_stem(token) for token in TOKEN_RE.findall(_plain_text(value))]


def _score(query: str, document: SearchDocument) -> float:
    query_text = _plain_text(query)
    query_tokens = list(dict.fromkeys(_tokens(query)))
    title = _plain_text(document.title)
    text = _plain_text(document.text)
    location = _plain_text(document.location)
    title_tokens = set(_tokens(title))
    text_tokens = set(_tokens(text))
    location_tokens = set(_tokens(location))

    score = 0.0
    if query_text and query_text in title:
        score += 30.0
    elif query_text and query_text in text:
        score += 10.0

    matched = 0
    for token in query_tokens:
        if token in title_tokens:
            score += 7.0
            matched += 1
        elif token in location_tokens:
            score += 4.0
            matched += 1
        elif token in text_tokens:
            score += 1.5
            matched += 1

    if query_tokens and matched == len(query_tokens):
        score += 12.0
    elif query_tokens:
        score += 4.0 * (matched / len(query_tokens))

    return score


def _load_documents(path: Path) -> list[SearchDocument]:
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    raw_documents = payload.get("docs")
    if not isinstance(raw_documents, list):
        raise ValueError(f"search index has no docs array: {path}")

    documents: list[SearchDocument] = []
    for raw in raw_documents:
        if not isinstance(raw, dict):
            continue
        location = str(raw.get("location") or "")
        title = str(raw.get("title") or "")
        text = str(raw.get("text") or "")
        if location:
            documents.append(SearchDocument(location=location, title=title, text=text))
    return documents


def _load_cases(path: Path) -> list[SearchCase]:
    payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    raw_cases = payload.get("queries")
    if not isinstance(raw_cases, list):
        raise ValueError(f"query file has no queries array: {path}")

    cases: list[SearchCase] = []
    for raw in raw_cases:
        if not isinstance(raw, dict):
            continue
        query = str(raw.get("query") or "").strip()
        expected = _normalize_url(str(raw.get("expected") or ""))
        max_rank = int(raw.get("max_rank") or 5)
        if not query or expected == "/":
            raise ValueError(f"invalid search case: {raw}")
        cases.append(SearchCase(query=query, expected=expected, max_rank=max_rank))
    return cases


def _rank(query: str, documents: list[SearchDocument]) -> list[tuple[str, float]]:
    best_by_url: dict[str, float] = {}
    for document in documents:
        canonical = _normalize_url(document.location)
        score = _score(query, document)
        best_by_url[canonical] = max(best_by_url.get(canonical, 0.0), score)
    return sorted(best_by_url.items(), key=lambda item: (-item[1], item[0]))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Validate task-based queries against a built MkDocs search index."
    )
    parser.add_argument("--site-dir", type=Path, required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(".build/docs-search-acceptance.tsv"),
    )
    args = parser.parse_args(argv)

    index_path = args.site_dir / "search" / "search_index.json"
    if not index_path.is_file():
        print(f"ERROR: search index not found: {index_path}", file=sys.stderr)
        return 1

    try:
        documents = _load_documents(index_path)
        cases = _load_cases(args.queries)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    args.report.parent.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    with args.report.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(
            ["query", "expected", "rank", "max_rank", "top_result", "status"]
        )
        for case in cases:
            ranked = _rank(case.query, documents)
            urls = [url for url, _ in ranked]
            rank = urls.index(case.expected) + 1 if case.expected in urls else None
            top_result = ranked[0][0] if ranked else ""
            passed = rank is not None and rank <= case.max_rank
            status = "pass" if passed else "fail"
            writer.writerow(
                [
                    case.query,
                    case.expected,
                    rank or "missing",
                    case.max_rank,
                    top_result,
                    status,
                ]
            )
            if not passed:
                failures.append(
                    f"{case.query!r}: expected {case.expected} at rank "
                    f"<= {case.max_rank}, found {rank or 'missing'}; top={top_result}"
                )

    if failures:
        for failure in failures:
            print(f"ERROR: {failure}")
        print(f"Search acceptance failed; report: {args.report}")
        return 1

    print(f"Search acceptance passed for {len(cases)} task queries")
    print(f"Report: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
