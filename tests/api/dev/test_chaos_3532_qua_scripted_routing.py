"""CHAOS-3532: the acceptance stack must be able to answer a QUA call, and
must refuse to guess at one.

THE GAP. The scripted OpenAI service routes on question text, and it gets
that text by parsing the user message as JSON:
``scripted_openai_service._question_from_messages`` does
``json.loads(content)["question"]``. But ``qua_shadow._build_messages``
sends PLAIN TEXT (``Question: "..."`` followed by the named mentions). The
parse raises, the router returns ``None``, and the scripted engine is never
consulted -- so no script file of any name could answer a QUA call.

WHY THAT IS WORSE THAN A MISSING FEATURE. With no script consulted the
request falls through to the pre-CHAOS-3219 heuristic and comes back as a
``tool_calls`` response. That is not an ``AgentFinalAnswer``, so
``qua_shadow.evaluate`` records ``SKIPPED_UNEXPECTED_DECISION`` -- quietly,
with no error. An armed QUA run would report "skipped" and read as a
negative result about CHAOS-3525's commit path. CHAOS-3532 exists precisely
because "the boot would have faithfully reproduced the old dead-end and been
mistaken for a negative result about the fix"; a silent fall-through is that
same trap through a different door.

The compose overlay already records the last time this class bit: "all 19
scripted cases fell through to the unscripted default heuristic while
PASSING."

THE FIX. A QUA request self-identifies -- its ``response_format.json_schema``
carries the ``dev_question_understanding.v1`` contract. Detection pins that
contract ID string rather than guessing at structure, and needs no
production change. An identified QUA request is answered from
``qua-decisions.v1.json``, and one with no matching entry returns a loud
422 rather than a plausible guess.
"""

from __future__ import annotations

import json
import secrets
import threading
import urllib.request
from collections.abc import Iterator
from typing import Any, cast

import pytest

from dev_health_ops.api.dev.contracts_v2.question_understanding import (
    QUESTION_UNDERSTANDING_SCHEMA_VERSION,
)
from dev_health_ops.llm.agent.scripted_openai_service import ScriptedOpenAIServer

_ACCEPTANCE_KEY = secrets.token_hex(16)

#: A question the QUA script answers. Kept in one place so the script file
#: and these tests cannot drift.
_SCRIPTED_QUESTION = "What's the status of the ACR project?"


@pytest.fixture
def scripted_openai_server() -> Iterator[ScriptedOpenAIServer]:
    server = ScriptedOpenAIServer(_ACCEPTANCE_KEY)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _qua_request(question: str, *, mention_count: int = 1) -> dict[str, Any]:
    """A request shaped exactly as ``qua_shadow`` sends one.

    The user content is PLAIN TEXT, not JSON -- that is the whole point. If
    this helper ever starts sending a JSON object with a ``question`` key it
    stops reproducing the real call and these tests go vacuous.
    """

    return {
        "model": "ask-dev-scripted-v1",
        "messages": [
            {"role": "system", "content": "You are a question understanding agent."},
            {
                "role": "user",
                "content": (
                    f'Question: "{question}"\n'
                    "\n"
                    "Named mentions:\n"
                    '- "ACR" (requested kind: project):\n'
                    "    [0] project: ACR"
                ),
            },
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "ask_dev_decision",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "value": {
                            "type": "object",
                            "properties": {
                                "schema_version": {
                                    "const": QUESTION_UNDERSTANDING_SCHEMA_VERSION
                                },
                                "mentions": {
                                    "type": "array",
                                    "minItems": mention_count,
                                    "maxItems": mention_count,
                                },
                            },
                        }
                    },
                },
            },
        },
    }


def _post(server: ScriptedOpenAIServer, body: dict[str, Any]) -> tuple[int, Any]:
    host, port = cast(tuple[str, int], server.server_address)
    request = urllib.request.Request(
        f"http://{host}:{port}/v1/chat/completions",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {_ACCEPTANCE_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:  # noqa: PERF203 - the assertion surface
        return error.code, json.loads(error.read())


def test_a_scripted_qua_question_is_answered_deterministically(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """The capability this ticket exists to create.

    Asserts the shape the QUA adapter actually consumes: a ``final_answer``
    decision whose ``value`` validates as ``dev_question_understanding.v1``.
    Anything else -- notably the heuristic's ``tool_calls`` response -- makes
    ``qua_shadow.evaluate`` record SKIPPED_UNEXPECTED_DECISION instead.
    """

    status, body = _post(scripted_openai_server, _qua_request(_SCRIPTED_QUESTION))

    assert status == 200, body
    content = json.loads(body["choices"][0]["message"]["content"])
    assert content["kind"] == "final_answer", (
        "a QUA call must be answered with a final_answer decision -- a "
        "tool_calls response is what the unscripted heuristic returns, and "
        "it is recorded as SKIPPED_UNEXPECTED_DECISION rather than a result"
    )
    value = content["value"]
    assert value["schema_version"] == QUESTION_UNDERSTANDING_SCHEMA_VERSION
    assert len(value["mentions"]) == 1, (
        "the mention count must match the request's own schema bound -- "
        "qua_shadow rejects a response whose mention count differs"
    )


def test_an_unscripted_qua_question_fails_loudly_rather_than_guessing(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """THE negative control, and the reason this ticket is not just a script
    file.

    A QUA-shaped request with no matching script must return a distinct 422.
    It must NOT fall through to the heuristic, which would answer with a
    plausible ``tool_calls`` response that reads downstream as "the QUA path
    declined" rather than "the harness has no script for this".

    That distinction is the entire finding behind this ticket. Watch this
    test fail if anyone re-adds a fall-through for QUA-shaped requests.
    """

    status, body = _post(
        scripted_openai_server, _qua_request("A question no QUA script answers at all")
    )

    assert status == 422, (
        "an unscripted QUA request must fail loudly. Falling through to the "
        "heuristic is the '19 scripted cases passed while serving nothing' "
        f"incident with a new door. Got {status}: {body}"
    )
    assert body["error"]["type"] == "scripted_provider_unmapped_qua_question", (
        "and with its own error type, so a runner can tell 'no QUA script "
        "for this question' apart from a wire-protocol error or a scripted "
        f"fault. Got {body['error']!r}."
    )


def test_a_non_qua_request_is_untouched_by_the_qua_branch(
    scripted_openai_server: ScriptedOpenAIServer,
) -> None:
    """Preserved-behaviour control.

    Every existing corpus case flows through the ordinary question-JSON
    router and the heuristic behind it. If QUA detection ever widened to
    match those, the whole corpus would start 422ing -- so this asserts an
    ordinary request still gets its ordinary answer.
    """

    status, body = _post(
        scripted_openai_server,
        {
            "model": "ask-dev-scripted-v1",
            "messages": [
                {
                    "role": "user",
                    "content": json.dumps({"question": "How is meridian/web-app?"}),
                }
            ],
        },
    )

    assert status == 200, body
    assert "choices" in body
