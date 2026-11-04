package apiservice

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
)

type stateReaderStub struct{}

func (stateReaderStub) State(context.Context, uuid.UUID) (orgreactivate.State, error) {
	return orgreactivate.State{}, nil
}

// With the switch off there is no organization client, so nothing runs and auth sees no read.
func TestOrgStateReconcilerRunsOnlyWithTheOrganizationClientAndADatabase(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	pool := &pgxpool.Pool{}
	if got := orgStateReconciler(Deps{Pool: pool}, logger); len(got) != 0 {
		t.Fatalf("switch off: %d components, want 0", len(got))
	}
	if got := orgStateReconciler(Deps{OrgState: stateReaderStub{}}, logger); len(got) != 0 {
		t.Fatalf("no database: %d components, want 0", len(got))
	}
	got := orgStateReconciler(Deps{Pool: pool, OrgState: stateReaderStub{}}, logger)
	if len(got) != 1 {
		t.Fatalf("switch on: %d components, want 1", len(got))
	}
	if err := got[0].Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := got[0].Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
