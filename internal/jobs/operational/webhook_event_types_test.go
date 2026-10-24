package operational

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// ingressEventTypes reads the eventType constants of the webhook intake, the
// only writer of webhook_deliveries.event_type, from its source.
func ingressEventTypes(t *testing.T) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "../../api/webhookintake/events.go", nil, 0)
	if err != nil {
		t.Fatalf("reading the intake's event types: %v", err)
	}
	var values []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			if id, ok := value.Type.(*ast.Ident); !ok || id.Name != "eventType" {
				continue
			}
			for _, expr := range value.Values {
				literal, ok := expr.(*ast.BasicLit)
				if !ok {
					t.Fatalf("an eventType constant is not a string literal")
				}
				unquoted, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, unquoted)
			}
		}
	}
	if len(values) == 0 {
		t.Fatal("no eventType constant found; the pin below would pass vacuously")
	}
	return values
}

func TestEveryEventTypeAHandlerRoutesIsKnown(t *testing.T) {
	routed := map[string]bool{"installation": true, "marketplace_purchase": true}
	for _, set := range []map[string]bool{githubSyncEventTypes, gitlabSyncEventTypes, jiraSyncEventTypes} {
		for eventType := range set {
			routed[eventType] = true
		}
	}
	var missing []string
	for eventType := range routed {
		if !knownWebhookEventTypes[eventType] {
			missing = append(missing, eventType)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("handlers route event types that knownWebhookEventTypes lacks: %v", missing)
	}
}

func TestEveryEventTypeTheIntakePersistsIsKnown(t *testing.T) {
	var missing []string
	for _, eventType := range ingressEventTypes(t) {
		if eventType == "unknown" {
			continue
		}
		if !knownWebhookEventTypes[eventType] {
			missing = append(missing, eventType)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("the intake persists event types that knownWebhookEventTypes lacks: %v", missing)
	}
}

// The only known type the intake does not persist is workflow_run, named by
// the github sync routing; any other extra entry is a type nothing produces.
func TestTheKnownSetHoldsNoTypeNothingProduces(t *testing.T) {
	produced := map[string]bool{}
	for _, eventType := range ingressEventTypes(t) {
		produced[eventType] = true
	}
	for _, set := range []map[string]bool{githubSyncEventTypes, gitlabSyncEventTypes, jiraSyncEventTypes} {
		for eventType := range set {
			produced[eventType] = true
		}
	}
	produced["installation"], produced["marketplace_purchase"] = true, true
	var extra []string
	for eventType := range knownWebhookEventTypes {
		if !produced[eventType] {
			extra = append(extra, eventType)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Fatalf("known event types no intake mapping or handler produces or routes: %v", extra)
	}
	if !knownWebhookEventTypes["workflow_run"] || !githubSyncEventTypes["workflow_run"] {
		t.Fatal("workflow_run must be known because the github sync routing names it")
	}
	for _, eventType := range ingressEventTypes(t) {
		if eventType == "workflow_run" {
			t.Fatal("the intake now persists workflow_run: drop the exception comment in webhook_event_types.go")
		}
	}
}

func TestOtherIsNotItselfAnEventType(t *testing.T) {
	if knownWebhookEventTypes[otherWebhookEventType] {
		t.Fatal(`"other" is in the known set: it would be indistinguishable from the collapse value`)
	}
	for _, eventType := range ingressEventTypes(t) {
		if eventType == otherWebhookEventType {
			t.Fatal(`the intake defines an event type "other"`)
		}
	}
	for _, set := range []map[string]bool{githubSyncEventTypes, gitlabSyncEventTypes, jiraSyncEventTypes} {
		if set[otherWebhookEventType] {
			t.Fatal(`a handler routes an event type "other"`)
		}
	}
}

func TestTheEventTypeLabelKeepsKnownTypesAndCollapsesTheRest(t *testing.T) {
	for eventType := range knownWebhookEventTypes {
		if got := webhookEventTypeLabel(eventType); got != eventType {
			t.Errorf("known %q became %q", eventType, got)
		}
	}
	for _, eventType := range []string{
		"", "unknown", "other", "Push", "push ", " push", "push\n", "pull_request.opened", "jira:issue_created",
		"synthetic_event_secret_shaped_0123456789abcdef", strings.Repeat("a", 4096), "événement", "check_run\x00",
	} {
		if got := webhookEventTypeLabel(eventType); got != otherWebhookEventType {
			t.Errorf("%q became %q, want %q", eventType, got, otherWebhookEventType)
		}
	}
}
