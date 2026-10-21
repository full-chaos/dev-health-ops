package oidc

import (
	"context"
	"fmt"
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

func (f *fakeProviders) ProviderHead(_ context.Context, id uuid.UUID) (ProviderHead, error) {
	row := f.read()
	if row.ID != id {
		return ProviderHead{}, ErrProviderNotFound
	}
	return ProviderHead{ID: row.ID, OrgID: row.OrgID, Protocol: row.Protocol, Status: row.Status}, nil
}

func (f *fakeProviders) DefaultProviderHead(_ context.Context, orgID uuid.UUID, protocol string) (ProviderHead, error) {
	row := f.read()
	if row.OrgID != orgID || row.Protocol != protocol || row.Status != "active" {
		return ProviderHead{}, ErrProviderNotFound
	}
	return ProviderHead{ID: row.ID, OrgID: row.OrgID, Protocol: row.Protocol, Status: row.Status}, nil
}

func (f *fakeProviders) Provider(_ context.Context, id uuid.UUID) (Provider, error) {
	row := f.read()
	if row.ID != id {
		return Provider{}, ErrProviderNotFound
	}
	out := Provider{ID: row.ID, OrgID: row.OrgID, Protocol: row.Protocol, Status: row.Status}
	var err error
	if _, oauth := oauthProviderOf(row.Protocol); oauth {
		if out.OAuth, err = decodeOAuthConfig(row.Config); err == nil {
			err = checkOAuthBase(row.Protocol, out.OAuth)
		}
	} else {
		out.Config, err = decodeProviderConfig(row.Config)
	}
	if err != nil {
		return Provider{}, fmt.Errorf("%w: config: %w", ErrProviderConfig, err)
	}
	return out, nil
}
