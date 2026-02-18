from __future__ import annotations

from datetime import timezone
from types import SimpleNamespace

import pytest

from dev_health_ops.api.graphql import subscriptions as sub


class _FakePubSub:
    def __init__(self, messages=None):
        self.messages = messages or []
        self.published = []

    async def publish(self, channel, data):
        self.published.append((channel, data))
        return 1

    def subscribe(self, channel):
        async def _gen():
            for message in self.messages:
                yield SimpleNamespace(channel=channel, data=message)

        return _gen()


def test_utc_now_is_timezone_aware():
    now = sub._utc_now()
    assert now.tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_publish_helpers_emit_expected_channels(monkeypatch):
    fake = _FakePubSub()

    async def get_pubsub():
        return fake

    monkeypatch.setattr(sub, "get_pubsub", get_pubsub)

    await sub.publish_metrics_update("org-1", "2026-02-18")
    await sub.publish_task_status("task-1", "running", 50.0, message="halfway")
    await sub.publish_sync_progress("org-1", "github", "syncing", 4, 10)

    assert fake.published[0][0] == sub.metrics_channel("org-1")
    assert fake.published[1][0] == sub.task_channel("task-1")
    assert fake.published[2][0] == sub.sync_channel("org-1")


@pytest.mark.asyncio
async def test_subscription_metrics_updated_yields_valid_messages(monkeypatch):
    fake = _FakePubSub(
        messages=[
            {"day": "2026-02-18", "updated_at": "2026-02-18T12:00:00+00:00", "message": "ok"},
            {"day": "bad", "updated_at": "not-a-date", "message": "skip"},
        ]
    )

    async def get_pubsub():
        return fake

    monkeypatch.setattr(sub, "get_pubsub", get_pubsub)

    s = sub.Subscription()
    gen = s.metrics_updated(info=None, org_id="org-1")
    first = await anext(gen)

    assert first.org_id == "org-1"
    assert first.day == "2026-02-18"
    assert first.message == "ok"


@pytest.mark.asyncio
async def test_subscription_task_status_parses_progress(monkeypatch):
    fake = _FakePubSub(
        messages=[
            {
                "status": "completed",
                "progress": "100",
                "result": "done",
                "updated_at": "2026-02-18T12:00:00+00:00",
            }
        ]
    )

    async def get_pubsub():
        return fake

    monkeypatch.setattr(sub, "get_pubsub", get_pubsub)

    s = sub.Subscription()
    gen = s.task_status(info=None, task_id="task-1")
    item = await anext(gen)

    assert item.task_id == "task-1"
    assert item.status == "completed"
    assert item.progress == pytest.approx(100.0)
    assert item.result == "done"
