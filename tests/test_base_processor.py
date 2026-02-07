from __future__ import annotations

import asyncio
import importlib
import threading

import pytest

from dev_health_ops.processors.base import BaseProcessor


class _TestProcessor(BaseProcessor[str, str]):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.processed_items: list[str] = []
        self.stored_results: list[str] = []

    async def process_single(self, item: str) -> str:
        self.processed_items.append(item)
        return f"result:{item}"

    async def store_result(self, result: str) -> None:
        self.stored_results.append(result)


class TestBaseProcessorContract:
    def test_cannot_instantiate_abc(self) -> None:
        with pytest.raises(TypeError):
            BaseProcessor(max_concurrent=1)

    def test_subclass_must_implement_process_single(self) -> None:
        class MissingProcessSingle(BaseProcessor[str, str]):
            async def store_result(self, result: str) -> None:
                return None

        with pytest.raises(TypeError):
            MissingProcessSingle()

    def test_subclass_must_implement_store_result(self) -> None:
        class MissingStoreResult(BaseProcessor[str, str]):
            async def process_single(self, item: str) -> str:
                return item

        with pytest.raises(TypeError):
            MissingStoreResult()


class TestProcessBatch:
    @pytest.mark.asyncio
    async def test_empty_batch(self) -> None:
        processor = _TestProcessor()
        count = await processor.process_batch([])
        assert count == 0
        assert processor.processed_items == []
        assert processor.stored_results == []

    @pytest.mark.asyncio
    async def test_single_item(self) -> None:
        processor = _TestProcessor()
        count = await processor.process_batch(["a"])
        assert count == 1
        assert processor.processed_items == ["a"]
        assert processor.stored_results == ["result:a"]

    @pytest.mark.asyncio
    async def test_multiple_items(self) -> None:
        processor = _TestProcessor()
        items = ["a", "b", "c"]
        count = await processor.process_batch(items)
        assert count == len(items)
        assert sorted(processor.processed_items) == sorted(items)
        assert sorted(processor.stored_results) == sorted(
            [f"result:{item}" for item in items]
        )

    @pytest.mark.asyncio
    async def test_respects_max_concurrent(self) -> None:
        class ConcurrencyProcessor(_TestProcessor):
            def __init__(self, **kwargs: object) -> None:
                super().__init__(**kwargs)
                self.lock = asyncio.Lock()
                self.in_flight = 0
                self.max_seen = 0
                self.started = asyncio.Event()
                self.release = asyncio.Event()

            async def process_single(self, item: str) -> str:
                async with self.lock:
                    self.in_flight += 1
                    self.max_seen = max(self.max_seen, self.in_flight)
                    if self.in_flight == self.max_concurrent:
                        self.started.set()

                await self.release.wait()
                result = await super().process_single(item)

                async with self.lock:
                    self.in_flight -= 1
                return result

        processor = ConcurrencyProcessor(max_concurrent=2)
        task = asyncio.create_task(processor.process_batch(["a", "b", "c", "d"]))

        await asyncio.wait_for(processor.started.wait(), timeout=1.0)
        assert processor.max_seen <= 2

        processor.release.set()
        count = await asyncio.wait_for(task, timeout=1.0)
        assert count == 4
        assert processor.max_seen == 2

    @pytest.mark.asyncio
    async def test_error_handling(self) -> None:
        class FailingProcessor(_TestProcessor):
            async def process_single(self, item: str) -> str:
                if item == "bad":
                    raise ValueError("boom")
                return await super().process_single(item)

        processor = FailingProcessor()
        items = ["ok1", "bad", "ok2"]
        count = await processor.process_batch(items)

        assert count == 2
        assert sorted(processor.stored_results) == ["result:ok1", "result:ok2"]

    @pytest.mark.asyncio
    async def test_store_result_called_for_each(self) -> None:
        processor = _TestProcessor()
        items = ["a", "b", "c", "d"]
        count = await processor.process_batch(items)
        assert count == len(items)
        assert len(processor.stored_results) == len(items)


class TestRunSyncInExecutor:
    @pytest.mark.asyncio
    async def test_runs_sync_func(self) -> None:
        processor = _TestProcessor()
        main_thread_id = threading.get_ident()

        def blocking_thread_id() -> int:
            return threading.get_ident()

        executor_thread_id = await processor.run_sync_in_executor(blocking_thread_id)
        assert executor_thread_id != main_thread_id

    @pytest.mark.asyncio
    async def test_returns_result(self) -> None:
        processor = _TestProcessor()

        def add(a: int, b: int) -> int:
            return a + b

        result = await processor.run_sync_in_executor(add, 2, 3)
        assert result == 5


class TestRunMethod:
    @pytest.mark.asyncio
    async def test_calls_hooks(self) -> None:
        class HookProcessor(_TestProcessor):
            def __init__(self) -> None:
                super().__init__()
                self.events: list[str] = []

            async def on_before_batch(self, items: list[str]) -> list[str]:
                self.events.append("before")
                return items

            async def on_after_batch(self, processed_count: int) -> None:
                self.events.append(f"after:{processed_count}")

        processor = HookProcessor()
        count = await processor.run(["a", "b"])

        assert count == 2
        assert processor.events == ["before", "after:2"]

    @pytest.mark.asyncio
    async def test_before_hook_can_filter(self) -> None:
        class FilterProcessor(_TestProcessor):
            async def on_before_batch(self, items: list[str]) -> list[str]:
                return [item for item in items if item != "skip"]

        processor = FilterProcessor()
        count = await processor.run(["a", "skip", "b"])

        assert count == 2
        assert sorted(processor.processed_items) == ["a", "b"]
        assert sorted(processor.stored_results) == ["result:a", "result:b"]


class TestSentinelIsolation:
    def test_sentinel_not_exported(self) -> None:
        module = importlib.import_module("dev_health_ops.processors.base")
        assert "_sentinel" in module.__dict__
        assert "sentinel" not in module.__dict__
        if hasattr(module, "__all__"):
            assert "_sentinel" not in module.__all__
