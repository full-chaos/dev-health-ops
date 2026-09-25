package apiservice

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// withScratchTable swaps responseModelRoutes for a copy for the duration of a
// test, so a registration under test never leaks into the real table.
func withScratchTable(t *testing.T) {
	t.Helper()
	real := responseModelRoutes
	scratch := make(map[string]bool, len(real))
	for key, model := range real {
		scratch[key] = model
	}
	responseModelRoutes = scratch
	t.Cleanup(func() { responseModelRoutes = real })
}

func panicMessage(t *testing.T, call func()) (message string) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			message = fmt.Sprint(recovered)
		}
	}()
	call()
	return ""
}

func TestRegisterResponseModelRoutesAddsEntriesFromAnotherFile(t *testing.T) {
	withScratchTable(t)
	before := len(responseModelRoutes)
	registerResponseModelRoutes(map[string]bool{
		"GET /api/v1/zz-family/things":  true,
		"POST /api/v1/zz-family/things": false,
	})
	if len(responseModelRoutes) != before+2 {
		t.Fatalf("table has %d entries, want %d", len(responseModelRoutes), before+2)
	}
	if !responseModelRoutes["GET /api/v1/zz-family/things"] || responseModelRoutes["POST /api/v1/zz-family/things"] {
		t.Fatalf("registered values were not kept: %#v", responseModelRoutes)
	}
	// markResponseModels reads a registered entry exactly like a literal one.
	routes := markResponseModels([]httpapi.Route{
		{Method: "GET", Pattern: "/api/v1/zz-family/things"},
		{Method: "POST", Pattern: "/api/v1/zz-family/things"},
	})
	if !routes[0].ResponseModel || routes[1].ResponseModel {
		t.Fatalf("markResponseModels ignored the registered entries: %#v", routes)
	}
}

func TestRegisterResponseModelRoutesRefusesADuplicateOfTheSharedTable(t *testing.T) {
	withScratchTable(t)
	var key string
	for candidate := range responseModelRoutes {
		key = candidate
		break
	}
	if key == "" {
		t.Fatal("the shared table is empty")
	}
	message := panicMessage(t, func() { registerResponseModelRoutes(map[string]bool{key: true}) })
	if !strings.Contains(message, key) || !strings.Contains(message, "registered twice") {
		t.Fatalf("a duplicate of a shared-table key must panic naming it; got %q", message)
	}
}

func TestRegisterResponseModelRoutesRefusesADuplicateFromAnotherFile(t *testing.T) {
	withScratchTable(t)
	registerResponseModelRoutes(map[string]bool{"GET /api/v1/zz-family/dup": true})
	message := panicMessage(t, func() {
		registerResponseModelRoutes(map[string]bool{"GET /api/v1/zz-family/dup": false})
	})
	if !strings.Contains(message, "GET /api/v1/zz-family/dup") || !strings.Contains(message, "registered twice") {
		t.Fatalf("a second registration of the same key must panic naming it; got %q", message)
	}
	if !responseModelRoutes["GET /api/v1/zz-family/dup"] {
		t.Fatal("the losing registration overwrote the first value")
	}
}

func TestRegisterResponseModelRoutesRefusesAMalformedKey(t *testing.T) {
	withScratchTable(t)
	for _, key := range []string{"", "get /api/v1/x", "GET api/v1/x", "GET  /api/v1/x", "/api/v1/x", "FETCH /api/v1/x", "GET /api/v1/x y"} {
		message := panicMessage(t, func() { registerResponseModelRoutes(map[string]bool{key: true}) })
		if !strings.Contains(message, "not \"METHOD /path\"") {
			t.Errorf("key %q was accepted or refused with the wrong message: %q", key, message)
		}
		if _, added := responseModelRoutes[key]; added {
			t.Errorf("malformed key %q was added before the panic", key)
		}
	}
}

// Every key of the table -- the literal in responsemodel.go and any per-family
// file registered later -- has the shape the lookups build ("METHOD /path").
func TestEveryResponseModelKeyIsWellFormed(t *testing.T) {
	if len(responseModelRoutes) < 100 {
		t.Fatalf("the table has only %d entries; it is not the real table", len(responseModelRoutes))
	}
	for key := range responseModelRoutes {
		if !responseModelKeyPattern.MatchString(key) {
			t.Errorf("response model key %q is not \"METHOD /path\"", key)
		}
	}
}
