"""Unit coverage for ``scripts.acceptance.corpus.sse_client``."""

from __future__ import annotations

import pytest

from scripts.acceptance.corpus.sse_client import SseParseError, parse_sse_events


class TestParseSseEvents:
    def test_parses_a_single_frame(self) -> None:
        body = 'event: run.started\ndata: {"run_id": "abc"}\n\n'
        frames = parse_sse_events(body)
        assert len(frames) == 1
        assert frames[0].event == "run.started"
        assert frames[0].data == {"run_id": "abc"}

    def test_parses_multiple_frames_in_order(self) -> None:
        body = (
            'event: run.started\ndata: {"a": 1}\n\n'
            'event: progress\ndata: {"a": 2}\n\n'
            'event: done\ndata: {"a": 3}\n\n'
        )
        frames = parse_sse_events(body)
        assert [frame.event for frame in frames] == ["run.started", "progress", "done"]
        assert [frame.data["a"] for frame in frames] == [1, 2, 3]

    def test_joins_multi_line_data(self) -> None:
        body = 'event: answer.completed\ndata: {"a": 1,\ndata: "b": 2}\n\n'
        frames = parse_sse_events(body)
        assert frames[0].data == {"a": 1, "b": 2}

    def test_skips_leading_and_trailing_blank_frames(self) -> None:
        body = "\n\nevent: done\ndata: {}\n\n\n\n"
        frames = parse_sse_events(body)
        assert len(frames) == 1

    def test_missing_event_name_raises(self) -> None:
        body = 'data: {"a": 1}\n\n'
        with pytest.raises(SseParseError, match="omitted an event name"):
            parse_sse_events(body)

    def test_missing_data_raises(self) -> None:
        body = "event: done\n\n"
        with pytest.raises(SseParseError, match="omitted data"):
            parse_sse_events(body)

    def test_invalid_json_data_raises(self) -> None:
        body = "event: done\ndata: {not json}\n\n"
        with pytest.raises(SseParseError, match="not valid JSON"):
            parse_sse_events(body)

    def test_non_object_json_data_raises(self) -> None:
        body = "event: done\ndata: [1, 2, 3]\n\n"
        with pytest.raises(SseParseError, match="must decode to an object"):
            parse_sse_events(body)

    def test_empty_body_returns_no_frames(self) -> None:
        assert parse_sse_events("") == []
