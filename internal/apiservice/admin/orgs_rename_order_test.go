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

// While organizations are owned a PATCH that renames renames first: a refused or failed rename, a name that is blank
// after the strip, or no rename client answers before any suspend or reactivate reaches auth-service.
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
	patchOrg(orderHandlers(log, "active", &fakeOrderRenamer{log: log}), `{"name": "New", "is_active": false}`)
	if log.index("rename") < 0 || log.index("auth-suspend") < log.index("rename") {
		t.Errorf("suspend and rename: steps %v; want the rename before the suspend", log.steps)
	}
	if !strings.Contains(strings.Join(log.steps, ","), "rename") {
		t.Fatal("no rename step")
	}
}

// Rename always goes first (answers-12275-2): a PATCH {name, is_active: true} on a suspended organization is refused
// by auth's rename (409), and neither auth state nor go-api's row changes. On an active one the rename still precedes
// the reactivate call.
func TestOrgPatchRenameBeforeReactivateWhileOrgsOwned(t *testing.T) {
	t.Setenv(usersowned.OrgsEnv, "true")
	log := &orderLog{}
	code, body := patchOrg(orderHandlers(log, "suspended", &fakeOrderRenamer{log: log, err: orgreactivate.ErrSuspended}), `{"name": "New", "is_active": true}`)
	if code != http.StatusConflict || !strings.Contains(body, "Organization is suspended") {
		t.Errorf("suspended: %d %s, steps %v; want 409 Organization is suspended", code, body, log.steps)
	}
	if log.count("rename") != 1 || log.count("auth") != 0 || log.count("auth-suspend") != 0 || log.count("db:begin") != 0 || log.count("db:exec") != 0 {
		t.Errorf("suspended: steps %v; want one rename and no reactivate, suspend or go-api write", log.steps)
	}
	log = &orderLog{}
	patchOrg(orderHandlers(log, "active", &fakeOrderRenamer{log: log}), `{"name": "New", "is_active": true}`)
	if log.index("rename") < 0 || log.index("auth") < log.index("rename") {
		t.Errorf("active: steps %v; want the rename before the reactivate", log.steps)
	}
}
