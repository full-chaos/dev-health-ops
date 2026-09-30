package streamhandlers

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// Round 1 of the decoder fix found the same leak in the sibling consumer that shares the
// ClickHouse pool: internal ingest opened a batch, then refused an invalid commit (or failed
// an append/send) without aborting it, so four such entries exhausted the default pool and
// stalled every valid entry, telemetry included. Every exit after PrepareBatch that does not
// send must abort, for every entity.
func TestInternalIngestReleasesItsBatchOnEveryUnsentExit(t *testing.T) {
	validCommit := `{"hash":"h","message":"m","author_name":"n","author_email":"e@x","author_when":"2026-01-01T00:00:00Z"}`
	env := func(entity, items string) (string, string) {
		return "ingest:org:" + entity, `{"org_id":"org","repo_url":"https://example.test/r","items":[` + items + `]}`
	}
	cases := map[string]struct {
		entity, items string
		sendErr       error
		wantPermanent bool
		wantAbort     bool
	}{
		"invalid commit after a valid one": {"commits", validCommit + `,{"hash":""}`, nil, true, true},
		"invalid commit first":             {"commits", `{"hash":""}`, nil, true, true},
		"commit send fails":                {"commits", validCommit, errors.New("boom"), false, true},
		"commit sent":                      {"commits", validCommit, nil, false, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			batch := &productBatch{sendErr: tc.sendErr}
			handler, _ := NewInternalIngestHandler(&productSink{batch: batch})
			stream, payload := env(tc.entity, tc.items)
			err := handler.Handle(context.Background(), streamrunner.Message{Stream: stream, Fields: map[string]string{"payload": payload}})
			if tc.wantPermanent != streamrunner.IsPermanent(err) {
				t.Fatalf("err = %v, permanent want %v", err, tc.wantPermanent)
			}
			if (batch.aborts > 0) != tc.wantAbort {
				t.Fatalf("aborts = %d, want abort=%v (err=%v)", batch.aborts, tc.wantAbort, err)
			}
		})
	}
}
