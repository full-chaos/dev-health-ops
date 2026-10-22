package providersync

import (
	"errors"
	"testing"
)

func TestChunkStoreErrorKeepsBothErrors(t *testing.T) {
	driver := errors.New("driver failure")
	err := NewChunkStoreError(driver)
	if !errors.Is(err, ErrChunkCheckpointConflict) || !errors.Is(err, driver) {
		t.Fatalf("NewChunkStoreError(%v) = %v; want both errors in the chain", driver, err)
	}
	var store *ChunkStoreError
	if !errors.As(err, &store) || store.Cause != driver {
		t.Fatalf("errors.As = %v; want the cause %v", store, driver)
	}
	if errors.As(ErrChunkCheckpointConflict, &store) {
		t.Fatal("the bare sentinel must not look like a store error")
	}
}
