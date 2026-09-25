"""Answers each (path, query) with the REAL Python api's own request
validation: the app is imported as it is, get_current_user is overridden so
the request reaches parameter validation, and the route's own signature
(date | None, datetime | None, int, ...) produces the status and body. A
request that fails validation is a 422 before any endpoint code runs."""
import json
import sys

from fastapi.testclient import TestClient

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.main import app

app.dependency_overrides[get_current_user] = lambda: None
client = TestClient(app, raise_server_exceptions=False)
out = []
for case in json.loads(sys.argv[1]):
    response = client.get(case["path"] + "?" + case["query"])
    out.append({"status": response.status_code, "body": response.text})
print("RESULT " + json.dumps(out))
