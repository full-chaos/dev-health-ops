"""Venue-oracle CLI for the Go port of _list_field (CHAOS-6311).

Applies the REAL production _list_field
(dev_health_ops.api.services.configuration.clickhouse_team_drift_projector)
to each JSON value in argv[1] (a JSON list) and prints the results as JSON.
"""

import json
import sys

from dev_health_ops.api.services.configuration.clickhouse_team_drift_projector import (
    _list_field,
)


def main() -> None:
    values = json.loads(sys.argv[1])
    sys.stdout.write(json.dumps([_list_field(v) for v in values]))


if __name__ == "__main__":
    main()
