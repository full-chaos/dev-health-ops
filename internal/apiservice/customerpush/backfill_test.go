package customerpush

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
)

func TestStoredLifecycleEventsAreTheCreatedEventOfTheStoredToken(t *testing.T) {
	org := "9f1c2b3a-0000-4000-8000-000000000001"
	source := uuid.New()
	expiry := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := map[string]tokenRow{
		"source bound":        {ID: uuid.New(), OrgID: org, SourceID: &source, Scopes: []byte(`["ingest:write","ingest:status"]`)},
		"organization wide":   {ID: uuid.New(), OrgID: org, Scopes: []byte(`["ingest:status"]`)},
		"expired not revoked": {ID: uuid.New(), OrgID: org, SourceID: &source, Scopes: []byte(`["ingest:write"]`), ExpiresAt: &expiry},
	}
	for name, row := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := storedLifecycleEvents(row)
			if err != nil || len(got) != 1 {
				t.Fatalf("events = %+v, %v", got, err)
			}
			names, _ := scopeNames(row.Scopes)
			var at *pytime.DateTime
			if row.ExpiresAt != nil {
				at = &pytime.DateTime{Time: *row.ExpiresAt}
			}
			want, err := createdEvents(newToken{ID: row.ID, OrgID: row.OrgID, SourceID: row.SourceID, Scopes: names, ExpiresAt: at})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("events differ from the route's created event:\n got %+v\nwant %+v", got, want)
			}
			if got[0].EventType != externalpush.Created {
				t.Fatalf("event type %q", got[0].EventType)
			}
		})
	}
	t.Run("organization wide carries no source id", func(t *testing.T) {
		got, _ := storedLifecycleEvents(cases["organization wide"])
		if len(got) != 1 || got[0].Binding != externalpush.BindingOrganization || got[0].SourceID != "" {
			t.Fatalf("events = %+v", got)
		}
	})
}

func TestStoredLifecycleEventsOfARevokedTokenAreTheRouteRevokedEvent(t *testing.T) {
	source := uuid.New()
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	row := tokenRow{ID: uuid.New(), OrgID: "9f1c2b3a-0000-4000-8000-000000000001", SourceID: &source, Scopes: []byte(`["ingest:write"]`), RevokedAt: &at}
	got, err := storedLifecycleEvents(row)
	if err != nil {
		t.Fatal(err)
	}
	want, err := revokedEvent(row)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []audit.PushLifecycleEvent{want}) {
		t.Fatalf("events = %+v, want the revoked event with its snapshot %+v", got, want)
	}
	if got[0].Snapshot == nil || got[0].Snapshot.Status != "active" {
		t.Fatalf("snapshot = %+v", got[0].Snapshot)
	}
}
