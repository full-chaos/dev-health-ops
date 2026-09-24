//go:build integration

package billing

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonDispatchProgram executes the real router.stripe_webhook once per
// candidate event type and reports which handlers each type reached. The
// candidates are every dotted string constant in stripe_webhook's compiled
// code (tuples and frozensets included: "in (...)" compiles to one), the
// types on stdin (the Go routes and gaps), and probes. The
// signature step returns the event, and every router-level function, the
// refund service and the session factory are replaced by recorders, so
// nothing is written; a type counts as applied when it reaches any handler
// other than the _invoice_has_org_id predicate (a type named only in a dead
// branch reaches none).
const pythonDispatchProgram = `
import asyncio, contextlib, importlib, inspect, json, sys, types
importlib.import_module("dev_health_ops.api.billing.router")
r = sys.modules["dev_health_ops.api.billing.router"]
target = r.stripe_webhook
def consts(values):
    for c in values:
        if isinstance(c, str):
            yield c
        elif isinstance(c, (tuple, frozenset)):
            yield from consts(c)
        elif hasattr(c, "co_consts"):
            yield from consts(c.co_consts)
candidates = {c for c in consts(target.__code__.co_consts) if "." in c and " " not in c and c.islower()}
candidates |= set(json.loads(sys.stdin.read()))
candidates |= {"invoice.a_type_stripe_adds_later", "customer.created", "a.type.stripe.adds.later"}
calls = []
def recorder(name, fn):
    if inspect.iscoroutinefunction(fn):
        async def record(*args, **kwargs):
            calls.append(name)
        return record
    def record(*args, **kwargs):
        calls.append(name)
        return True
    return record
for name, value in list(vars(r).items()):
    if inspect.isfunction(value) and value.__module__ == r.__name__ and value is not target and name.startswith("_"):
        setattr(r, name, recorder(name, value))
class RefundService:
    async def process_webhook(self, **kwargs):
        calls.append("refund_service.process_webhook")
r.refund_service = RefundService()
@contextlib.asynccontextmanager
async def session():
    yield None
r.get_postgres_session = session
def event_of(kind):
    obj = types.SimpleNamespace(metadata={}, customer="cus_x", id="obj_x")
    return types.SimpleNamespace(type=kind, id="evt_x", data=types.SimpleNamespace(object=obj))
class Client:
    def __init__(self, kind):
        self.kind = kind
    def construct_event(self, payload, sig, secret):
        return event_of(self.kind)
class Request:
    headers = {"stripe-signature": "t=1,v1=0"}
    async def body(self):
        return b"{}"
r.get_webhook_secret = lambda: "whsec_x"
out = {}
for kind in sorted(candidates):
    calls.clear()
    r.get_stripe_client = lambda kind=kind: Client(kind)
    asyncio.run(target(Request()))
    out[kind] = sorted(set(calls))
print(json.dumps(out))
`

// TestVenueOracleStripeEventDispatch holds the Go dispatch to the Python
// route by execution: every type the real stripe_webhook applies is routed
// here or named in stripeEventGaps, every gap is a type Python applies and
// Go does not route, and Go routes no type Python does not apply.
func TestVenueOracleStripeEventDispatch(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the dispatch oracle runs the Python api; it runs in the venue-oracles job")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	goTypes := make([]string, 0, len(stripeEventRoutes)+len(stripeEventGaps))
	for eventType := range stripeEventRoutes {
		goTypes = append(goTypes, eventType)
	}
	for eventType := range stripeEventGaps {
		goTypes = append(goTypes, eventType)
	}
	stdin, _ := json.Marshal(goTypes)
	command := exec.Command(python, "-c", pythonDispatchProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(stdin))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var reached map[string][]string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &reached); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	applied := map[string]bool{}
	for eventType, handlers := range reached {
		for _, handler := range handlers {
			if handler != "_invoice_has_org_id" {
				applied[eventType] = true
			}
		}
	}
	if len(reached) < 10 || len(applied) < 8 {
		t.Fatalf("python executed %d types, %d applied: the oracle measured too little\n%v", len(reached), len(applied), reached)
	}
	names := make([]string, 0, len(reached))
	for eventType := range reached {
		names = append(names, eventType)
	}
	sort.Strings(names)
	for _, eventType := range names {
		routed, gap := stripeEventRoute(eventType) != "", stripeEventGaps[eventType] != ""
		switch {
		case applied[eventType] && !routed && !gap:
			t.Errorf("%s: Python applies it (%v); Go answers 200 and drops it (route it or name it in stripeEventGaps)", eventType, reached[eventType])
		case applied[eventType] && routed && gap:
			t.Errorf("%s is routed and still listed as a gap", eventType)
		case !applied[eventType] && routed:
			t.Errorf("Go routes %s, which Python does not apply (%v)", eventType, reached[eventType])
		case !applied[eventType] && gap:
			t.Errorf("gap %s is not a type Python applies: a stale entry", eventType)
		}
	}
	t.Logf("python executed %d types, %d applied; go routes %d by name plus every invoice. type; named gaps: %d",
		len(reached), len(applied), len(stripeEventRoutes), len(stripeEventGaps))
	venueoracle.WriteProof(t)
}
