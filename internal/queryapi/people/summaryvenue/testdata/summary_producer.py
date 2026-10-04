"""Runs the real Python person summary service and prints the timestamp fragments of the body FastAPI
writes for it (the response model's dump_json). argv[1] is a JSON object:
db_url, person_id, org_id, range_days, compare_days, today (ISO date)."""

import asyncio
import json
import re
import sys
from datetime import date

import dev_health_ops.api.services.people as svc
from dev_health_ops.api.models.schemas import PersonSummaryResponse
from pydantic import TypeAdapter

args = json.loads(sys.argv[1])
today = date.fromisoformat(args["today"])
svc.utc_today = lambda: today


async def main():
    response = await svc.build_person_summary_response(
        db_url=args["db_url"],
        person_id=args["person_id"],
        range_days=args["range_days"],
        compare_days=args["compare_days"],
        org_id=args["org_id"],
    )
    body = TypeAdapter(PersonSummaryResponse).dump_json(response).decode()
    # The rest of the body differs from run to run only in the engine-chosen row
    # order of unordered UNION ALL result sets, so a recording of it cannot be
    # pinned. What the Go test compares is the text of every timestamp-bearing
    # fragment (each deltas[].spark array and freshness.last_ingested_at): the
    # body's fragments, in order, joined with commas, are the answer.
    fragments = re.findall(r'"last_ingested_at":[^,}]*|"spark":\[[^\]]*\]', body)
    sys.stdout.write("BODY " + ",".join(fragments) + "\n")


asyncio.run(main())
