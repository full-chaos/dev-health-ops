"""Venue-oracle CLI for the Go port of _validate_external_url (CHAOS-6311).

Runs the REAL production dev_health_ops.api.admin.routers.credentials.
_validate_external_url over a JSON list of URLs (argv[1]) and prints a JSON
list of [ok, error] pairs, in order. Only IP-literal and blocked-name hosts
are used by the caller, so no DNS is involved and results are stable.
"""

import json
import sys

from dev_health_ops.api.admin.routers.credentials import _validate_external_url


def main() -> None:
    urls = json.loads(sys.argv[1])
    sys.stdout.write(json.dumps([list(_validate_external_url(u)) for u in urls]))


if __name__ == "__main__":
    main()
