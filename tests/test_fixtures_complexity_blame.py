from pathlib import Path

from dev_health_ops.analytics.complexity import ComplexityScanner
from dev_health_ops.fixtures.generator import SyntheticDataGenerator


def test_fixture_blame_reconstructs_parseable_python():
    generator = SyntheticDataGenerator(repo_name="acme/demo-app", seed=1)
    commits = generator.generate_commits(days=1, commits_per_day=1)
    blame_rows = generator.generate_blame(commits)

    target_path = "src/main.py"
    lines = [row for row in blame_rows if getattr(row, "path", None) == target_path]
    assert lines

    lines.sort(key=lambda row: getattr(row, "line_no", 0))
    contents = "\n".join((getattr(row, "line", None) or "") for row in lines)

    scanner = ComplexityScanner(config_path=Path("src/dev_health_ops/config/complexity.yaml"))
    results = scanner.scan_file_contents([(target_path, contents)])

    assert results
