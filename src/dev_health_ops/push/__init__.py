"""`dev-hops push` command group (CHAOS-2700): the customer-push CLI.

See ``docs/architecture/customer-push-cli-and-examples.md`` for the design
decisions behind this package (httpx + retry_with_backoff reuse, offline
validation via the server's own Pydantic models, exit-code contract, sample
payload sourcing, env var precedence).
"""

from __future__ import annotations
