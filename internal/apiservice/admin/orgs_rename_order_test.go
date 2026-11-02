package admin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

type fakeOrderRenamer struct {
	log *orderLog
	err error
}

func (f *fakeOrderRenamer) Rename(context.Context, uuid.UUID, uuid.UUID, string) error {
	f.log.add("rename")
	return f.err
}

func orderHandlers(log *orderLog, current string, renamer orgreactivate.Renamer) *handlers {
	db := &reactivationDB{log: log, found: true, active: current == "active"}
	h := &handlers{store: pgStore{Pool: db}, orgReactivator: &fakeAuthReactivator{log: log}, orgSuspender: &fakeAuthSuspender{log: log},
		orgState: &fakeAuthState{log: log, seq: []orgreactivate.State{{Status: current, Revision: 1}}},
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil))}
	if renamer != nil {
		h.orgRenamer = renamer
	}
	return h
}

// While organizations are owned a PATCH that renames and suspends (or only renames) renames first: a refused or failed
// rename, a name that is blank after the strip, or no rename client answers before any suspend reaches auth-service.
// A PATCH that reactivates must reactivate first (auth refuses a rename of a suspended organization), so a name blank
// after the strip and a missing client are refused before the reactivate.
func TestOrgPatchRefusedRenameChangesNoAuthStateWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	refusals := map[string]error{"name": orgreactivate.ErrNameInvalid, "suspended": orgreactivate.ErrSuspended,
		"not found": orgreactivate.ErrNotFound, "forbidden": orgreactivate.ErrRefused, "unavailable": orgreactivate.ErrUnavailable}
	for name, err := range refusals {
		log := &orderLog{}
		code, _ := patchOrg(orderHandlers(log, "active", &fakeOrderRenamer{log: log, err: err}), `{"name": "New", "is_active": false}`)
		if code < 400 || log.count("auth-suspend") != 0 || log.count("rename") != 1 {
			t.Errorf("%s: %d, steps %v; want a refusal with one rename and no suspend", name, code, log.steps)
		}
	}
	for _, current := range []string{"active", "suspended"} {
		for name, tc := range map[string]struct {
			body    string
			renamer bool
			code    int
		}{
			"blank name":             {`{"name": "   ", "is_active": false}`, true, http.StatusUnprocessableEntity},
			"no client":              {`{"name": "New", "is_active": false}`, false, http.StatusInternalServerError},
			"blank name, reactivate": {`{"name": "   ", "is_active": true}`, true, http.StatusUnprocessableEntity},
			"no client, reactivate":  {`{"name": "New", "is_active": true}`, false, http.StatusInternalServerError},
		} {
			log := &orderLog{}
			var renamer orgreactivate.Renamer
			if tc.renamer {
				renamer = &fakeOrderRenamer{log: log}
			}
			code, _ := patchOrg(orderHandlers(log, current, renamer), tc.body)
			if code != tc.code || log.count("auth-suspend") != 0 || log.count("auth") != 0 || log.count("auth-state") != 0 || log.count("rename") != 0 {
				t.Errorf("%s (%s): %d, steps %v; want %d before any auth call", name, current, code, log.steps, tc.code)
			}
		}
	}
	log := &orderLog{}
	if code, _ := patchOrg(orderHandlers(log, "suspended", &fakeOrderRenamer{log: log}), `{"name": "New", "is_active": true}`); code == http.StatusConflict ||
		log.index("auth") < 0 || log.index("rename") < log.index("auth") {
		t.Errorf("reactivate and rename: %d, steps %v; want the reactivate before the rename", code, log.steps)
	}
	log = &orderLog{}
	patchOrg(orderHandlers(log, "active", &fakeOrderRenamer{log: log}), `{"name": "New", "is_active": false}`)
	if log.index("rename") < 0 || log.index("auth-suspend") < log.index("rename") {
		t.Errorf("suspend and rename: steps %v; want the rename before the suspend", log.steps)
	}
	if !strings.Contains(strings.Join(log.steps, ","), "rename") {
		t.Fatal("no rename step")
	}
}
