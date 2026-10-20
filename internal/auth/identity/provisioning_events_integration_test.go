//go:build integration

package identity

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

type wireEvent struct {
	Verb     string
	Change   map[string]any
	Revision int64
}

// eventsOf reads one aggregate's provisioning events, oldest first.
func (e *env) eventsOf(t *testing.T, ctx context.Context, aggregateType, aggregateID string) []wireEvent {
	t.Helper()
	rows, err := e.admin.Query(ctx, `SELECT event_type, payload->'change', (payload->>'revision')::bigint
FROM auth.auth_outbox_events WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY id`, aggregateType, aggregateID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []wireEvent
	for rows.Next() {
		var w wireEvent
		var change []byte
		if err := rows.Scan(&w.Verb, &change, &w.Revision); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(change, &w.Change); err != nil {
			t.Fatal(err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (e *env) wantEvents(t *testing.T, ctx context.Context, aggregateType, aggregateID string, want ...wireEvent) {
	t.Helper()
	for i := range want {
		want[i].Revision = int64(i + 1)
		if want[i].Change == nil {
			want[i].Change = map[string]any{}
		}
	}
	if got := e.eventsOf(t, ctx, aggregateType, aggregateID); !reflect.DeepEqual(got, want) {
		t.Errorf("%s %s events:\n got  %+v\n want %+v", aggregateType, aggregateID, got, want)
	}
}

func status(s string) map[string]any { return map[string]any{"status": s} }

// TestEachProducerWritesItsMappedEvent: every producer writes the contract
// event the mapping names, with the change it names, and every aggregate's
// revision 1 is its created (or linked) event, so a consumer meets each
// principal, user, organization and identity at its creation.
func TestEachProducerWritesItsMappedEvent(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()

	// Projection of a go-api user: created, then each changed field.
	user := e.seedUser(t, ctx, uniqueEmail("map"), true, true)
	e.project(t, ctx, user)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false, is_verified = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, user)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = true WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, user)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET email = $2 WHERE id = $1`, user, uniqueEmail("map-new")); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, user)
	p := "prn_" + user.String()
	e.wantEvents(t, ctx, "principal", p,
		wireEvent{Verb: "principal.created", Change: status("active")},
		wireEvent{Verb: "principal.disabled", Change: status("disabled")},
		wireEvent{Verb: "principal.enabled", Change: status("active")})
	e.wantEvents(t, ctx, "user", p,
		wireEvent{Verb: "user.created"}, wireEvent{Verb: "user.email_verified"},
		wireEvent{Verb: "user.email_unverified"}, wireEvent{Verb: "user.email_changed"})

	// An inactive, unverified source is created as it is.
	inactive := e.seedUser(t, ctx, uniqueEmail("map-off"), false, false)
	e.project(t, ctx, inactive)
	e.wantEvents(t, ctx, "principal", "prn_"+inactive.String(), wireEvent{Verb: "principal.created", Change: status("disabled")})
	e.wantEvents(t, ctx, "user", "prn_"+inactive.String(), wireEvent{Verb: "user.created"})

	// Link and unlink.
	key := googleKey("map-" + uuid.NewString())
	// The link is made by another principal: the event names the linked
	// principal, never the actor.
	link, err := e.store.LinkIdentity(ctx, key, user, Actor{PrincipalID: &inactive})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.UnlinkIdentity(ctx, key, user, Actor{}); err != nil {
		t.Fatal(err)
	}
	e.wantEvents(t, ctx, "external_identity", "idn_"+link.String(),
		wireEvent{Verb: "external_identity.linked", Change: map[string]any{"provider": "google", "principal_id": p}},
		wireEvent{Verb: "external_identity.unlinked", Change: map[string]any{"provider": "google", "principal_id": p}})

	// Local sign-up, verification, password change.
	hash := testBcrypt
	local, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: "map-" + uuid.NewString()[:8] + "@example.com", PasswordHash: &hash, Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.owned.SetEmailVerified(ctx, local, time.Now(), Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.owned.SetPasswordHash(ctx, local, testBcrypt, Actor{}); err != nil {
		t.Fatal(err)
	}
	e.wantEvents(t, ctx, "principal", "prn_"+local.String(), wireEvent{Verb: "principal.created", Change: status("active")})
	e.wantEvents(t, ctx, "user", "prn_"+local.String(),
		wireEvent{Verb: "user.created"}, wireEvent{Verb: "user.email_verified"}, wireEvent{Verb: "user.credentials_changed"})

	// Federated sign-up: principal, user (verified by the provider) and identity.
	fedKey := googleKey("map-fed-" + uuid.NewString())
	fed, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: fedKey, Email: "map-fed-" + uuid.NewString()[:8] + "@example.com", Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatal(err)
	}
	e.wantEvents(t, ctx, "principal", "prn_"+fed.String(), wireEvent{Verb: "principal.created", Change: status("active")})
	e.wantEvents(t, ctx, "user", "prn_"+fed.String(), wireEvent{Verb: "user.created"}, wireEvent{Verb: "user.email_verified"})
	identities, err := e.owned.ListIdentities(ctx, fed)
	if err != nil || len(identities) != 1 {
		t.Fatalf("federated identities = %v, %v", identities, err)
	}
	e.wantEvents(t, ctx, "external_identity", "idn_"+identities[0].ID.String(),
		wireEvent{Verb: "external_identity.linked", Change: map[string]any{"provider": "google", "principal_id": "prn_" + fed.String()}})

	// Organizations: created, renamed, suspended, reactivated.
	org := e.seedOrg(t, ctx)
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET name = 'map-renamed' WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = true WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	e.wantEvents(t, ctx, "organization", "org_"+org.String(),
		wireEvent{Verb: "organization.created", Change: map[string]any{"status": "active", "slug": "org-" + org.String()}},
		wireEvent{Verb: "organization.updated"},
		wireEvent{Verb: "organization.suspended", Change: status("suspended")},
		wireEvent{Verb: "organization.reactivated", Change: status("active")})

	// A go-api slug the wire cannot carry is left out, never rewritten.
	unicodeOrg := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'café')`, unicodeOrg, "café_"+unicodeOrg.String()[:8]); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, unicodeOrg); err != nil {
		t.Fatal(err)
	}
	e.wantEvents(t, ctx, "organization", "org_"+unicodeOrg.String(), wireEvent{Verb: "organization.created", Change: status("active")})
}

// TestCreatedPrincipalWritesNoFalseState: a create writes the principal as
// it is, once. No producer inserts an opposite state and corrects it with a
// follow-up UPDATE, so updated_at equals created_at after every create.
func TestCreatedPrincipalWritesNoFalseState(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	hash := testBcrypt
	local, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: "nofalse-" + uuid.NewString()[:8] + "@example.com", PasswordHash: &hash, Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatal(err)
	}
	fed, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: googleKey("nofalse-" + uuid.NewString()),
		Email: "nofalse-fed-" + uuid.NewString()[:8] + "@example.com", Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatal(err)
	}
	active := e.seedUser(t, ctx, uniqueEmail("nofalse-on"), true, true)
	e.project(t, ctx, active)
	inactive := e.seedUser(t, ctx, uniqueEmail("nofalse-off"), false, false)
	e.project(t, ctx, inactive)
	for name, id := range map[string]uuid.UUID{"ProvisionLocal": local, "ProvisionFederated": fed, "Project active": active, "Project inactive": inactive} {
		var same bool
		if err := e.admin.QueryRow(ctx, `SELECT updated_at = created_at FROM auth.principals WHERE id = $1`, id).Scan(&same); err != nil {
			t.Fatal(err)
		}
		if !same {
			t.Errorf("%s: the principal row was updated after its insert (a false state written first)", name)
		}
	}
}
