from __future__ import annotations

import logging

from dev_health_ops.processors.github import _BoundedBlameFailureLogger


def test_bounded_blame_failure_logger_warns_first_failures_and_summary(caplog) -> None:
    caplog.set_level(logging.WARNING)
    failure_logger = _BoundedBlameFailureLogger("full-chaos/dev-health")

    for index in range(6):
        failure_logger.record_failure(f"src/file_{index}.py", RuntimeError("boom"))
    failure_logger.log_summary(total_fetches=6)

    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 6
    assert messages[0].startswith(
        "Failed blame fetch for full-chaos/dev-health:src/file_0.py"
    )
    assert messages[4].startswith(
        "Failed blame fetch for full-chaos/dev-health:src/file_4.py"
    )
    assert "src/file_5.py" not in "\n".join(messages)
    assert messages[-1] == "6 of 6 blame fetches failed for full-chaos/dev-health"
