package oidc

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// storedRow is one version of a public.sso_providers row as the fake keeps it.
type storedRow struct {
	ID, OrgID        uuid.UUID
	Protocol, Status string
	Config           string
}

// fakeProviders serves before for the first flipAfter reads and after for every
// later read (flipAfter 0 = never flips); reads counts every read of any kind.
type fakeProviders struct {
	mu            sync.Mutex
	before, after storedRow
	flipAfter     int
	reads         int
}

func (f *fakeProviders) read() storedRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if f.flipAfter > 0 && f.reads > f.flipAfter {
		return f.after
	}
	return f.before
}

func (f *fakeProviders) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

func (row storedRow) stored() StoredProvider {
	return StoredProvider{ID: row.ID, OrgID: row.OrgID, Protocol: row.Protocol, Status: row.Status, Config: row.Config}
}

func (f *fakeProviders) ProviderRow(_ context.Context, id uuid.UUID) (StoredProvider, error) {
	row := f.read()
	if row.ID != id {
		return StoredProvider{}, ErrProviderNotFound
	}
	return row.stored(), nil
}

func (f *fakeProviders) DefaultProviderRow(_ context.Context, orgID uuid.UUID, protocol string) (StoredProvider, error) {
	row := f.read()
	if row.OrgID != orgID || row.Protocol != protocol || row.Status != "active" {
		return StoredProvider{}, ErrProviderNotFound
	}
	return row.stored(), nil
}
