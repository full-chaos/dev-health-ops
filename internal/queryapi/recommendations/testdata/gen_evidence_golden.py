"""Regenerates evidence_golden.json from the REAL production row mapper.

    PYTHONPATH=src .venv/bin/python internal/queryapi/recommendations/testdata/gen_evidence_golden.py \
        > internal/queryapi/recommendations/testdata/evidence_golden.json

Each case's evidence_json goes through resolvers.recommendations
._row_to_recommendation (the function the resolver maps every row with), so
"row dropped" and the parsed evidence are Python's own answers, never hand
authored.
"""

import json
import math
from datetime import date, datetime, timezone

from dev_health_ops.api.graphql.resolvers.recommendations import _row_to_recommendation

E1 = '{"team_id":"t","metric_table":"m","field":"f","window_start":"2026-04-01","window_end":"2026-04-07","value":14.0}'
RAWS = [
    "",
    "[]",
    "[" + E1 + "]",
    "[null," + E1 + "]",
    '[5,"x",[1],' + E1 + "]",
    '[{"team_id":"t","value":null}]',
    '[{"team_id":"t"}]',
    '[{"team_id":null,"metric_table":null,"field":null,"value":1}]',
    '[{"team_id":5,"metric_table":true,"field":1.5,"value":2}]',
    '[{"team_id":["a",1],"metric_table":{"k":null},"field":"f","value":3}]',
    '[{"value":true}]',
    '[{"value":false}]',
    '[{"value":"2.5"}]',
    '[{"value":" 7 "}]',
    '[{"value":"1_000"}]',
    '[{"value":"1__0"}]',
    '[{"value":"abc"}]',
    '[{"value":""}]',
    '[{"value":"inf"}]',
    '[{"value":"-Infinity"}]',
    '[{"value":"nan"}]',
    '[{"value":[1]}]',
    '[{"value":{"a":1}}]',
    '[{"value":1e999}]',
    '[{"value":NaN}]',
    '[{"value":Infinity}]',
    '[{"value":123456789012345678901234567890}]',
    '[{"value":1' + "0" * 400 + "}]",
    '[{"window_start":"2026-04-01"}]',
    '[{"window_start":"20260401","window_end":"2026-W14-3"}]',
    '[{"window_start":"nope"}]',
    '[{"window_start":null,"window_end":0}]',
    '[{"window_start":0}]',
    '[{"window_start":20260401}]',
    '[{"window_start":true}]',
    '[{"window_start":"","window_end":"2026-02-30"}]',
    '{"a":1}',
    '"abc"',
    "5",
    "null",
    "true",
    "0",
    "1.5",
    "not json",
    "[",
    '[{"value":1},{"value":null},{"value":2}]',
    "[" + E1 + ',{"window_start":"bad"},' + E1 + "]",
    "  [ " + E1 + " ]  ",
    '[{"value":1,"value":2}]',
    '[{"team_id":"\\u00e9\\ud83d\\ude00","value":1}]',
]


def encode(value):
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return {"nonfinite": repr(value)}
    return value


def run(raw):
    row = {
        "rule_id": "r",
        "team_id": "t",
        "org_id": "o",
        "latest_severity": "warning",
        "latest_window_start": date(2026, 4, 1),
        "latest_window_end": date(2026, 4, 7),
        "latest_computed_at": datetime(2026, 4, 8, tzinfo=timezone.utc),
        "latest_title": "T",
        "latest_rationale": "R",
        "latest_success_criterion": "S",
        "latest_evidence_json": raw,
    }
    try:
        rec = _row_to_recommendation(row)
    except Exception as exc:  # an exception the resolver would not catch
        return {"raw": raw, "python_exception": type(exc).__name__}
    if rec is None:
        return {"raw": raw, "keep_row": False, "evidence": []}
    return {
        "raw": raw,
        "keep_row": True,
        "evidence": [
            {
                "team_id": e.team_id,
                "metric_table": e.metric_table,
                "window_start": e.window_start.isoformat(),
                "window_end": e.window_end.isoformat(),
                "field": e.field,
                "value": encode(e.value),
            }
            for e in rec.evidence
        ],
    }


print(json.dumps([run(raw) for raw in RAWS], indent=1, ensure_ascii=True))
