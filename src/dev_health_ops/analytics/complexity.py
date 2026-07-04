import fnmatch
import importlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import lizard
from radon.complexity import cc_visit

logger = logging.getLogger(__name__)
yaml = importlib.import_module("yaml")

DEFAULT_COMPLEXITY_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "complexity.yaml"
)

#: Extensions the scanner can analyze, mapped to the persisted ``language``
#: value. Python goes through radon (kept for trend continuity with existing
#: ``repo_complexity_daily`` rows, CHAOS-2850); everything else goes through
#: lizard. Keep this map aligned with ``config/complexity.yaml`` include_globs:
#: the same globs also gate provider file-content ingestion
#: (``_fetch_scannable_contents``), so an extension missing there can never
#: produce complexity data.
LANGUAGE_BY_EXTENSION: dict[str, str] = {
    ".py": "python",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".js": "javascript",
    ".jsx": "javascript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".go": "go",
    ".rs": "rust",
    ".java": "java",
    ".kt": "kotlin",
    ".kts": "kotlin",
    ".rb": "ruby",
    ".php": "php",
    ".c": "c",
    ".h": "c",
    ".cpp": "cpp",
    ".cc": "cpp",
    ".hpp": "cpp",
    ".cs": "csharp",
    ".swift": "swift",
    ".scala": "scala",
    ".m": "objective-c",
    ".mm": "objective-c",
    ".lua": "lua",
    ".vue": "vue",
}


@dataclass
class FileComplexity:
    file_path: str
    language: str
    loc: int
    functions_count: int
    cyclomatic_total: int
    cyclomatic_avg: float
    high_complexity_functions: int
    very_high_complexity_functions: int


class ComplexityScanner:
    def __init__(self, config_path: Path):
        self.config = self._load_config(config_path)
        self.high_threshold = self.config.get("high_complexity_threshold", 15)
        self.very_high_threshold = self.config.get("very_high_threshold", 25)
        self.include_globs = self.config.get("include_globs", ["**/*.py"])
        self.exclude_globs = self.config.get("exclude_globs", [])

    def _load_config(self, path: Path) -> dict:
        if not path.exists():
            logger.warning(f"Complexity config not found at {path}, using defaults")
            return {}
        with open(path) as f:
            return yaml.safe_load(f)

    def should_process(self, file_path: str) -> bool:
        # Check excludes first
        for pat in self.exclude_globs:
            if fnmatch.fnmatch(file_path, pat):
                return False
        # Check includes
        for pat in self.include_globs:
            if fnmatch.fnmatch(file_path, pat):
                return True
        return False

    def scan_repo(self, repo_root: Path) -> list[FileComplexity]:
        results = []
        repo_root = repo_root.resolve()

        for root, dirs, files in os.walk(repo_root):
            # Modify dirs in-place to skip hidden directories (e.g. .git)
            dirs[:] = [d for d in dirs if not d.startswith(".")]

            for file in files:
                full_path = Path(root) / file
                rel_path = str(full_path.relative_to(repo_root))

                if self.should_process(rel_path):
                    try:
                        metrics = self._analyze_file(full_path)
                        if metrics:
                            # Add path as relative
                            metrics.file_path = rel_path
                            results.append(metrics)
                    except Exception as e:
                        logger.warning(f"Failed to analyze {rel_path}: {e}")

        return results

    def scan_git_ref(self, repo_root: Path, ref: str) -> list[FileComplexity]:
        """Scan files at a specific git reference/commit using GitPython."""
        import git

        results = []
        try:
            repo = git.Repo(repo_root)
            commit = repo.commit(ref)

            # Walk the tree of the commit
            # stack of (tree, parent_path)
            stack = [(commit.tree, "")]

            while stack:
                tree, parent = stack.pop()
                for item in tree:
                    if item.type == "tree":
                        # Directory
                        stack.append((item, os.path.join(parent, item.name)))
                    elif item.type == "blob":
                        # File
                        rel_path = os.path.join(parent, item.name)
                        if self.should_process(rel_path):
                            try:
                                # Get content from blob
                                content = item.data_stream.read().decode(
                                    "utf-8", errors="replace"
                                )
                                metrics = self._analyze_content(content, rel_path)
                                if metrics:
                                    results.append(metrics)
                            except Exception as e:
                                logger.warning(
                                    f"Failed to analyze blob {rel_path} at {ref}: {e}"
                                )

        except Exception as e:
            logger.error(f"Failed to scan git ref {ref}: {e}")

        return results

    def scan_file_contents(self, files: list[tuple[str, str]]) -> list[FileComplexity]:
        results = []
        for file_path, contents in files:
            if not self.should_process(file_path):
                continue
            try:
                metrics = self._analyze_content(contents, file_path)
                if metrics:
                    results.append(metrics)
            except Exception as e:
                logger.warning(f"Failed to analyze {file_path}: {e}")

        return results

    def _analyze_file(self, file_path: Path) -> FileComplexity | None:
        if file_path.suffix.lower() not in LANGUAGE_BY_EXTENSION:
            return None

        try:
            with open(file_path, encoding="utf-8") as f:
                code = f.read()
            return self._analyze_content(code, str(file_path))
        except Exception:
            # Unreadable files (permissions, encoding, ...)
            return None

    def _analyze_content(self, code: str, file_path: str) -> FileComplexity | None:
        ext = os.path.splitext(file_path)[1].lower()
        language = LANGUAGE_BY_EXTENSION.get(ext)
        if language is None:
            return None

        if ext == ".py":
            return self._analyze_python(code, file_path)
        return self._analyze_with_lizard(code, file_path, language)

    def _analyze_python(self, code: str, file_path: str) -> FileComplexity | None:
        """Python via radon -- kept separate from lizard so historical
        ``repo_complexity_daily`` trends stay comparable (CHAOS-2850)."""
        try:
            blocks = cc_visit(code)
        except Exception:
            # Syntax errors or other parse issues
            return None

        complexities = [b.complexity for b in blocks]
        return self._build_result(code, file_path, "python", complexities)

    def _analyze_with_lizard(
        self, code: str, file_path: str, language: str
    ) -> FileComplexity | None:
        try:
            analysis = lizard.analyze_file.analyze_source_code(file_path, code)
        except Exception:
            # Malformed source the tokenizer cannot handle
            return None

        complexities = [f.cyclomatic_complexity for f in analysis.function_list]
        return self._build_result(code, file_path, language, complexities)

    def _build_result(
        self, code: str, file_path: str, language: str, complexities: list[int]
    ) -> FileComplexity:
        functions_count = len(complexities)
        cyclomatic_total = sum(complexities)
        cyclomatic_avg = (
            cyclomatic_total / functions_count if functions_count > 0 else 0.0
        )
        high_count = sum(1 for c in complexities if c > self.high_threshold)
        very_high_count = sum(1 for c in complexities if c > self.very_high_threshold)

        return FileComplexity(
            file_path=str(file_path),
            language=language,
            loc=len(code.splitlines()),
            functions_count=functions_count,
            cyclomatic_total=cyclomatic_total,
            cyclomatic_avg=cyclomatic_avg,
            high_complexity_functions=high_count,
            very_high_complexity_functions=very_high_count,
        )
