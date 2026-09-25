"""Runs the real Python person summary service and prints the body FastAPI
writes for it (the response model's dump_json). argv[1] is a JSON object:
db_url, person_id, org_id, range_days, compare_days, today (ISO date)."""

import asyncio
import json
import sys
from datetime import date

from pydantic import TypeAdapter

import dev_health_ops.api.services.people as svc
from dev_health_ops.api.models.schemas import PersonSummaryResponse

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
    sys.stdout.write(
        "BODY " + TypeAdapter(PersonSummaryResponse).dump_json(response).decode() + "\n"
    )


asyncio.run(main())
