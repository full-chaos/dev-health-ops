package investment

import (
	"context"
	"testing"
)

func jevWithHeader(id string) func(int, []byte) jevReply {
	return func(int, []byte) jevReply {
		if id == "" {
			return jevReply{}
		}
		return jevReply{headers: map[string]string{"X-Typesafe-Request-Id": id}}
	}
}

// The platform jev served rows name the response header as the source of their request id; without the header the
// id and the source are both empty (unknown).
func TestPlatformJevServedRowsNameTheHeaderAsTheRequestIDSource(t *testing.T) {
	for name, tc := range map[string]struct{ header, wantID, wantSource string }{
		"header present": {"req-served-1", "req-served-1", "header"},
		"header absent":  {"", "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			served := newTestServed(t, newFakeJev(t, jevWithHeader(tc.header)), &syncBuffer{})
			for _, row := range servedRowsOf(t, served) {
				if row.RequestID != tc.wantID || row.RequestIDSource != tc.wantSource {
					t.Fatalf("row id %q source %q, want %q and %q", row.RequestID, row.RequestIDSource, tc.wantID, tc.wantSource)
				}
			}
		})
	}
}

func TestPlatformJevShadowRowsNameTheHeaderAsTheRequestIDSource(t *testing.T) {
	for name, tc := range map[string]struct{ header, wantID, wantSource string }{
		"header present": {"req-shadow-1", "req-shadow-1", "header"},
		"header absent":  {"", "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			phase := newTestShadowPhase(t, newFakeJev(t, jevWithHeader(tc.header)), shadowTestSettings(), debugLogger(&syncBuffer{}))
			store := &memoryShadowStore{}
			phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1"))
			if len(store.attempts) == 0 {
				t.Fatal("no shadow attempt row was written")
			}
			for _, row := range store.attempts {
				if row.RequestID != tc.wantID || row.RequestIDSource != tc.wantSource {
					t.Fatalf("row id %q source %q, want %q and %q", row.RequestID, row.RequestIDSource, tc.wantID, tc.wantSource)
				}
			}
		})
	}
}

// The other backends keep what they had: a BYO text provider has no request id (empty, unknown), and a clef row
// without a gateway or ray header carries the id the client minted.
func TestOtherBackendsKeepTheirRequestIDSources(t *testing.T) {
	stub := &stubOrgModel{model: "org-model-1", reply: answerAllQuestions}
	byo, err := newBYOSystemOne(stub, "openai", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = byo.Close() }()
	for _, row := range servedRowsOf(t, byo) {
		if row.RequestID != "" || row.RequestIDSource != "" {
			t.Fatalf("BYO openai row id %q source %q, want both empty", row.RequestID, row.RequestIDSource)
		}
	}
	clefStub := newClefBYO(clefAnswers("clef"))
	clef, err := clefStub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = clef.Close() }()
	for _, row := range servedRowsOf(t, clef) {
		if row.RequestID == "" || row.RequestIDSource != "client" {
			t.Fatalf("clef row id %q source %q, want a client id", row.RequestID, row.RequestIDSource)
		}
	}
}

func TestShadowRequestIDSourceAcceptsTheJevHeaderSource(t *testing.T) {
	if got := shadowRequestIDSource("header"); got != "header" {
		t.Fatalf("shadowRequestIDSource(header) = %q, want it kept", got)
	}
	for _, dropped := range []string{"Header", "headers", "header ", "header\n", "jev"} {
		if got := shadowRequestIDSource(dropped); got != "" {
			t.Fatalf("shadowRequestIDSource(%q) = %q, want an empty string", dropped, got)
		}
	}
}
