package streamhandlers

import (
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

func teamV1Values(t *testing.T, system string, payload map[string]any, scope *ExternalRecomputeScope) ([]any, error) {
	t.Helper()
	return externalRecordValues(externalSinkBatch{
		Pointer: externalPointer{
			OrgID: "org-1", SourceSystem: system, SourceInstance: "instance",
			IngestionID: uuid.MustParse("11111111-2222-4333-8444-555555555555"),
		},
		SourceID: uuid.MustParse("22222222-2222-4333-8444-555555555555"),
	}, externalSinkRecord{Kind: "team.v1", ExternalID: "t", Payload: payload},
		time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC), scope, nil)
}

func TestTeamV1WritesTheSystemPrefixedTeamID(t *testing.T) {
	cases := []struct {
		system, id, want string
	}{
		{"github", "platform", "gh:platform"},
		{"gitlab", "acme/ops", "gl:acme/ops"},
		{"jira", "9b1c2d3e", "jira:9b1c2d3e"},
		{"linear", "ENG", "linear:ENG"},
		{"linear", "linear:ENG", "linear:ENG"},
		{"custom", "squad-7", "custom:squad-7"},
		{"pagerduty", "P1", "pagerduty:P1"},
		{"atlassian", "x", "atlassian:x"},
	}
	for _, c := range cases {
		t.Run(c.system+" "+c.id, func(t *testing.T) {
			scope := &ExternalRecomputeScope{}
			values, err := teamV1Values(t, c.system, map[string]any{
				"id": c.id, "name": "Team", "updatedAt": "2026-10-08T11:00:00Z", "parentTeamId": "parent",
			}, scope)
			if err != nil {
				t.Fatal(err)
			}
			wantUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+c.want))
			if values[0] != c.want || values[1] != wantUUID {
				t.Fatalf("id, team_uuid = %v, %v; want %q, %v", values[0], values[1], c.want, wantUUID)
			}
			if values[12] != c.system {
				t.Fatalf("provider = %v, want %q", values[12], c.system)
			}
			if values[13] != c.id {
				t.Fatalf("native_team_key = %v, want the pushed id %q", values[13], c.id)
			}
			if values[14] != teamid.Of(c.system, "parent") {
				t.Fatalf("parent_team_id = %v, want %q", values[14], teamid.Of(c.system, "parent"))
			}
			if !slices.Equal(scope.TeamIDs, []string{c.want}) {
				t.Fatalf("recompute scope team ids = %v, want [%s]", scope.TeamIDs, c.want)
			}
		})
	}
}

func TestTeamV1KeepsAPushedNativeTeamKeyAndNoParent(t *testing.T) {
	values, err := teamV1Values(t, "linear", map[string]any{
		"id": "ENG", "name": "Team", "updatedAt": "2026-10-08T11:00:00Z", "nativeTeamKey": "ENG-NATIVE",
	}, &ExternalRecomputeScope{})
	if err != nil {
		t.Fatal(err)
	}
	if values[13] != "ENG-NATIVE" || values[14] != nil {
		t.Fatalf("native_team_key, parent_team_id = %v, %v; want ENG-NATIVE, nil", values[13], values[14])
	}
}

func TestTeamV1RefusesATeamIDWithNoSystem(t *testing.T) {
	_, err := teamV1Values(t, "", map[string]any{
		"id": "ENG", "name": "Team", "updatedAt": "2026-10-08T11:00:00Z",
	}, &ExternalRecomputeScope{})
	if !errors.Is(err, teamid.ErrBareTeamID) {
		t.Fatalf("err = %v, want the bare team id refused", err)
	}
}

func TestIdentityV1PrefixesItsTeamIDs(t *testing.T) {
	values, err := externalRecordValues(externalSinkBatch{
		Pointer: externalPointer{OrgID: "org-1", SourceSystem: "linear", SourceInstance: "instance"},
	}, externalSinkRecord{Kind: "identity.v1", ExternalID: "u", Payload: map[string]any{
		"canonicalId": "ada@example.test", "updatedAt": "2026-10-08T11:00:00Z",
		"teamIds": []any{"ENG", "linear:OPS"},
	}}, time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC), &ExternalRecomputeScope{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := values[6]; !reflect.DeepEqual(got, []string{"linear:ENG", "linear:OPS"}) {
		t.Fatalf("team_ids = %v, want [linear:ENG linear:OPS]", got)
	}
}
