package syncdispatchruntime

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

type noopInvalidator struct{}

func (noopInvalidator) InvalidateOrg(context.Context, string) error { return nil }

func TestUseCoverageCacheInvalidatorRejectsNilAndReportsConfiguration(t *testing.T) {
	t.Parallel()
	service, err := NewNativeFinalizeSyncRunService(&pgxpool.Pool{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if service.CoverageCacheInvalidatorConfigured() {
		t.Fatal("fresh service reports an invalidator")
	}
	if err := service.UseCoverageCacheInvalidator(nil); err == nil {
		t.Fatal("nil invalidator accepted")
	}
	if err := service.UseCoverageCacheInvalidator(noopInvalidator{}); err != nil {
		t.Fatal(err)
	}
	if !service.CoverageCacheInvalidatorConfigured() {
		t.Fatal("configured invalidator not reported")
	}
	var nilService *NativeFinalizeSyncRunService
	if err := nilService.UseCoverageCacheInvalidator(noopInvalidator{}); err == nil {
		t.Fatal("nil service accepted an invalidator")
	}
}
