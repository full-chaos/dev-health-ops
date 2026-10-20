package requestid

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/google/uuid"
)

// errorV1RequestID is error.v1's request_id pattern.
var errorV1RequestID = regexp.MustCompile(`^req_[A-Za-z0-9_-]{26}$`)

func TestWireFormSatisfiesErrorV1AndIsOneToOneWithTheAuditForm(t *testing.T) {
	seen := map[string]string{}
	for range 1000 {
		id := uuid.New()
		wire := WireOf(id)
		if !errorV1RequestID.MatchString(wire) {
			t.Fatalf("wire form %q does not satisfy error.v1 request_id", wire)
		}
		decoded, err := wireEncoding.DecodeString(wire[len(WirePrefix):])
		if err != nil || uuid.UUID(decoded) != id {
			t.Fatalf("wire form %q does not decode back to %s (err %v)", wire, id, err)
		}
		if prior, dup := seen[wire]; dup && prior != id.String() {
			t.Fatalf("two ids share wire form %q", wire)
		}
		seen[wire] = id.String()
	}
}

func TestMiddlewareMintsAServerIDAndIgnoresTheClientHeader(t *testing.T) {
	var audit *string
	var wire string
	handler := Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		audit, wire = Audit(r.Context()), Wire(r.Context())
	}))
	client := uuid.New().String()
	request := httptest.NewRequest(http.MethodGet, "/v1/x", nil)
	request.Header.Set("X-Request-ID", client)
	handler.ServeHTTP(httptest.NewRecorder(), request)

	if audit == nil {
		t.Fatal("no audit id was bound")
	}
	if *audit == client {
		t.Fatal("the audit id copied the client's X-Request-ID")
	}
	id, err := uuid.Parse(*audit)
	if err != nil {
		t.Fatalf("audit id %q is not a UUID: %v", *audit, err)
	}
	if wire != WireOf(id) {
		t.Fatalf("wire %q and audit %q are not the same id", wire, *audit)
	}

	var second *string
	Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { second = Audit(r.Context()) })).
		ServeHTTP(httptest.NewRecorder(), request)
	if second == nil || *second == *audit {
		t.Fatal("two requests got the same id")
	}
}

func TestNoBoundIDGivesNoAuditID(t *testing.T) {
	if Audit(context.Background()) != nil {
		t.Fatal("Audit made up an id")
	}
	if Wire(context.Background()) != "" {
		t.Fatal("Wire made up an id")
	}
}

// The wire form is echoed in X-Auth-Request-ID on every answer, so the go-api
// proxy can log the id the audit rows carry; the client's own header is not
// touched and never becomes it.
func TestMiddlewareEchoesTheWireFormInXAuthRequestID(t *testing.T) {
	var wire string
	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire = Wire(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodPost, "/v1/x", nil)
	request.Header.Set(Header, "req_AAAAAAAAAAAAAAAAAAAAAAAAAA")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	got := response.Header().Get("X-Auth-Request-ID")
	if got == "" || got != wire || !errorV1RequestID.MatchString(got) {
		t.Fatalf("X-Auth-Request-ID = %q, want the bound wire id %q", got, wire)
	}
	if got == "req_AAAAAAAAAAAAAAAAAAAAAAAAAA" {
		t.Fatal("the client's X-Auth-Request-ID was echoed")
	}
}
