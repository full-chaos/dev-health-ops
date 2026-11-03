package customerpush

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
)

var fingerprintShape = regexp.MustCompile(`^[0-9a-f]{16}$`)

func TestCredentialFingerprintIsTheSchemaShapeAndHoldsNoTokenByte(t *testing.T) {
	id := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	got := credentialFingerprint(id)
	if !fingerprintShape.MatchString(got) {
		t.Fatalf("fingerprint %q is not 16 lowercase hex characters", got)
	}
	if again := credentialFingerprint(id); again != got {
		t.Fatalf("not stable: %q then %q", got, again)
	}
	token, err := generateToken()
	if err != nil {
		t.Fatal(err)
	}
	hash := hashToken(token)
	for name, secret := range map[string]string{"token": token, "token hash": hash, "display prefix": token[:tokenPrefixDisplay]} {
		if strings.Contains(got, strings.ToLower(secret)) || strings.Contains(strings.ToLower(secret), got) {
			t.Errorf("the fingerprint and the %s overlap", name)
		}
	}
	if other := credentialFingerprint(uuid.New()); other == got {
		t.Fatal("two ids share a fingerprint")
	}
}

func TestLifecycleEventsAreBuiltFromTheTokenAndPassTheRuntimeCheck(t *testing.T) {
	org, source, id, parent := "9f1c2b3a-0000-4000-8000-000000000001", uuid.New(), uuid.New(), uuid.New()
	expires := pytime.DateTime{Time: time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)}
	next := newToken{ID: id, OrgID: org, SourceID: &source, Scopes: []string{"ingest:write", "ingest:write"}, ExpiresAt: &expires}

	created, err := createdEvents(next)
	if err != nil || len(created) != 1 {
		t.Fatalf("created = %+v, %v", created, err)
	}
	if !slices.Equal(created[0].Change.Scopes, []string{"ingest:write"}) || created[0].OrganizationID != "org_"+org || created[0].Binding != externalpush.BindingSource || created[0].SourceID != source.String() {
		t.Fatalf("created = %+v", created[0])
	}

	oldExpiry := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	old := tokenRow{ID: parent, OrgID: org, SourceID: &source, Scopes: []byte(`["ingest:write"]`), ExpiresAt: &oldExpiry}
	events, err := rotatedEvents(old, next)
	if err != nil || len(events) != 2 {
		t.Fatalf("rotated = %+v, %v", events, err)
	}
	if events[0].EventType != externalpush.Rotated || events[0].Change.RotationParentID != parent.String() ||
		events[1].EventType != externalpush.Revoked || events[1].AggregateID != parent.String() || events[1].Snapshot == nil {
		t.Fatalf("rotated events = %+v", events)
	}
	for _, e := range append(created, events...) {
		if err := externalpush.Check(e.EventType, e.AggregateID, e.OrganizationID, e.Binding, e.SourceID, e.Change); err != nil {
			t.Errorf("%s: %v", e.EventType, err)
		}
	}
	if err := externalpush.Check(externalpush.Created, parent.String(), "org_"+org, externalpush.BindingSource, source.String(), *events[1].Snapshot); err != nil {
		t.Errorf("the revoked snapshot: %v", err)
	}
}

func TestAnOrganizationWideTokenHasOrganizationBoundLifecycleEvents(t *testing.T) {
	org := "9f1c2b3a-0000-4000-8000-000000000001"
	next := newToken{ID: uuid.New(), OrgID: org, Scopes: []string{"ingest:status"}}
	created, err := createdEvents(next)
	if err != nil || len(created) != 1 {
		t.Fatalf("created = %+v, %v", created, err)
	}
	old := tokenRow{ID: uuid.New(), OrgID: org, Scopes: []byte(`["ingest:status"]`)}
	rotated, err := rotatedEvents(old, next)
	if err != nil || len(rotated) != 2 {
		t.Fatalf("rotated = %+v, %v", rotated, err)
	}
	revoked, err := revokedEvent(old)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range append(append(created, rotated...), revoked) {
		if e.Binding != externalpush.BindingOrganization || e.SourceID != "" {
			t.Errorf("%s: binding %q source %q, want organization and no source", e.EventType, e.Binding, e.SourceID)
		}
		if err := externalpush.Check(e.EventType, e.AggregateID, e.OrganizationID, e.Binding, e.SourceID, e.Change); err != nil {
			t.Errorf("%s: %v", e.EventType, err)
		}
	}
	if revoked.Snapshot == nil {
		t.Fatal("the revoked event carries no snapshot")
	}
	if err := externalpush.Check(externalpush.Created, old.ID.String(), "org_"+org, externalpush.BindingOrganization, "", *revoked.Snapshot); err != nil {
		t.Errorf("the revoked snapshot: %v", err)
	}
}

func TestAnOffSwitchWritesNothingAndTouchesNoTransaction(t *testing.T) {
	var off lifecycle
	source := uuid.New()
	bad := newToken{ID: uuid.New(), OrgID: "has a space", SourceID: &source}
	if err := off.created(context.Background(), nil, bad); err != nil {
		t.Fatalf("created with the switch off: %v", err)
	}
	if err := off.rotated(context.Background(), nil, tokenRow{}, bad); err != nil {
		t.Fatalf("rotated with the switch off: %v", err)
	}
	if err := off.revoked(context.Background(), nil, tokenRow{OrgID: "has a space", SourceID: &source}); err != nil {
		t.Fatalf("revoked with the switch off: %v", err)
	}
	if _, err := createdEvents(bad); err == nil {
		t.Fatal("an organization id the wire cannot carry was accepted")
	}
	on := newLifecycle(true, "Not-A-Schema")
	if on.err == nil {
		t.Fatal("an invalid auth schema name was accepted")
	}
}
