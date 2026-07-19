#!/usr/bin/env python3
"""Run lean, deterministic accessibility checks against the built docs candidate.

This is not a substitute for keyboard, screen-reader, zoom, contrast, or mobile
human review. It protects structural invariants that are cheap and reliable to
check on every pull request.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

GENERIC_LINK_TEXT = {"click here", "here", "learn more", "more", "read more"}


class PageAuditParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.errors: list[str] = []
        self.html_lang = ""
        self.main_count = 0
        self.main_depth = 0
        self.content_region_count = 0
        self.content_depth = 0
        self.h1_count = 0
        self.skip_link_found = False
        self.title_parts: list[str] = []
        self._in_title = False
        self._in_h1 = False
        self._h1_parts: list[str] = []
        self._article_stack: list[bool] = []
        self._link_stack: list[dict[str, Any]] = []
        self._button_stack: list[dict[str, Any]] = []
        self._table_stack: list[dict[str, int]] = []

    @staticmethod
    def _attrs(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        return {name: value or "" for name, value in attrs}

    @staticmethod
    def _classes(attrs: dict[str, str]) -> set[str]:
        return {item for item in attrs.get("class", "").split() if item}

    def handle_starttag(
        self, tag: str, attrs_list: list[tuple[str, str | None]]
    ) -> None:
        attrs = self._attrs(attrs_list)
        classes = self._classes(attrs)
        if tag == "html":
            self.html_lang = attrs.get("lang", "").strip()
        elif tag == "title":
            self._in_title = True
        elif tag == "main":
            self.main_count += 1
            self.main_depth += 1
        elif tag == "article":
            is_content_region = "md-content__inner" in classes
            self._article_stack.append(is_content_region)
            if is_content_region:
                self.content_region_count += 1
                self.content_depth += 1
        elif self.content_depth > 0 and tag == "h1":
            self.h1_count += 1
            self._in_h1 = True
            self._h1_parts = []
        elif self.content_depth > 0 and tag == "img":
            alt = attrs.get("alt")
            decorative = (
                attrs.get("role") == "presentation"
                or attrs.get("aria-hidden", "").lower() == "true"
            )
            if alt is None:
                self.errors.append("content image is missing an alt attribute")
            elif not alt.strip() and not decorative:
                self.errors.append(
                    "content image has empty alt text without being marked decorative"
                )
        elif tag == "a":
            self._link_stack.append(
                {
                    "audit": self.content_depth > 0,
                    "text": [],
                    "href": attrs.get("href", ""),
                    "aria": attrs.get("aria-label", ""),
                    "title": attrs.get("title", ""),
                    "classes": classes,
                }
            )
        elif tag == "button":
            self._button_stack.append(
                {
                    "audit": self.content_depth > 0,
                    "text": [],
                    "aria": attrs.get("aria-label", ""),
                    "title": attrs.get("title", ""),
                }
            )
        elif self.content_depth > 0 and tag == "input":
            input_type = attrs.get("type", "text").lower()
            if input_type not in {"hidden", "submit", "button", "reset"}:
                name = (
                    attrs.get("aria-label", "")
                    or attrs.get("aria-labelledby", "")
                    or attrs.get("title", "")
                )
                if not name.strip():
                    self.errors.append(
                        "content input has no aria-label, aria-labelledby, or title"
                    )
        elif self.content_depth > 0 and tag == "table":
            self._table_stack.append({"th": 0})
        elif self.content_depth > 0 and tag == "th" and self._table_stack:
            self._table_stack[-1]["th"] += 1

        tabindex = attrs.get("tabindex")
        if tabindex:
            try:
                if int(tabindex) > 0:
                    self.errors.append(f"positive tabindex is not allowed: {tabindex}")
            except ValueError:
                self.errors.append(f"invalid tabindex value: {tabindex}")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        elif tag == "main" and self.main_depth > 0:
            self.main_depth -= 1
        elif tag == "article" and self._article_stack:
            if self._article_stack.pop():
                self.content_depth -= 1
        elif tag == "h1" and self._in_h1:
            self._in_h1 = False
            if not "".join(self._h1_parts).strip():
                self.errors.append("content h1 has no accessible text")
        elif tag == "a" and self._link_stack:
            record = self._link_stack.pop()
            text = " ".join(record["text"]).strip()
            if "md-skip" in record["classes"] and record["href"].startswith("#"):
                self.skip_link_found = True
            if record["audit"]:
                accessible_name = text or record["aria"] or record["title"]
                if not accessible_name:
                    self.errors.append(
                        f"link has no accessible name: {record['href'] or '[no href]'}"
                    )
                if text.lower() in GENERIC_LINK_TEXT:
                    self.errors.append(f"generic link text is not descriptive: {text!r}")
        elif tag == "button" and self._button_stack:
            record = self._button_stack.pop()
            text = " ".join(record["text"]).strip()
            if record["audit"] and not (text or record["aria"] or record["title"]):
                self.errors.append("button has no accessible name")
        elif tag == "table" and self._table_stack:
            table = self._table_stack.pop()
            if table["th"] == 0:
                self.errors.append("content table has no header cells")

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)
        if self._in_h1:
            self._h1_parts.append(data)
        if self._link_stack:
            self._link_stack[-1]["text"].append(data)
        if self._button_stack:
            self._button_stack[-1]["text"].append(data)


def _audit_html(path: Path) -> list[str]:
    parser = PageAuditParser()
    try:
        parser.feed(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as exc:
        return [f"could not parse HTML: {exc}"]

    errors = list(parser.errors)
    if not parser.html_lang:
        errors.append("html element has no lang attribute")
    if not "".join(parser.title_parts).strip():
        errors.append("document title is empty")
    if parser.main_count != 1:
        errors.append(f"expected one main landmark, found {parser.main_count}")
    if parser.content_region_count != 1:
        errors.append(
            f"expected one primary content article, found {parser.content_region_count}"
        )
    if parser.h1_count != 1:
        errors.append(f"expected one content h1, found {parser.h1_count}")
    if path.name != "404.html" and not parser.skip_link_found:
        errors.append("Material skip link was not found")
    return errors


def _audit_css(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    errors: list[str] = []
    if ":focus-visible" not in text:
        errors.append("custom CSS has no visible-focus rule")
    if "prefers-reduced-motion" not in text:
        errors.append("custom CSS has no reduced-motion handling")
    if re.search(r"outline\s*:\s*none", text) and "box-shadow" not in text:
        errors.append("outline is removed without an alternative visible focus style")
    return errors


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Run deterministic accessibility checks against built docs HTML."
    )
    parser.add_argument("--site-dir", type=Path, required=True)
    parser.add_argument("--css", type=Path, required=True)
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(".build/docs-accessibility-audit.tsv"),
    )
    args = parser.parse_args(argv)

    if not args.site_dir.is_dir():
        print(f"ERROR: built site directory not found: {args.site_dir}")
        return 1
    if not args.css.is_file():
        print(f"ERROR: CSS file not found: {args.css}")
        return 1

    rows: list[tuple[str, str]] = []
    for html_path in sorted(args.site_dir.rglob("*.html")):
        for error in _audit_html(html_path):
            rows.append((str(html_path.relative_to(args.site_dir)), error))
    for error in _audit_css(args.css):
        rows.append((str(args.css), error))

    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["page", "finding"])
        writer.writerows(rows)

    if rows:
        for page, finding in rows:
            print(f"ERROR: {page}: {finding}")
        print(f"Accessibility structural audit failed; report: {args.report}")
        return 1

    page_count = len(list(args.site_dir.rglob("*.html")))
    print(f"Accessibility structural audit passed for {page_count} HTML pages")
    print(f"Report: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
