package externalingest

import "testing"

func TestValidateRecordsUnknownKind(t *testing.T) {
	errs := validateRecords([]Record{{Kind: "nope.v1", ExternalID: "x", Payload: map[string]any{}}})
	if len(errs) != 1 || errs[0].Code != "unknown_kind" || errs[0].Path != "records[0].kind" {
		t.Fatalf("%+v", errs)
	}
}

func TestValidateRecordsMissingRequiredField(t *testing.T) {
	// repository.v1 requires externalId and sourceSystem.
	errs := validateRecords([]Record{{Kind: "repository.v1", ExternalID: "x", Payload: map[string]any{}}})
	codes := map[string]bool{}
	for _, e := range errs {
		codes[e.Code+":"+e.Path] = true
	}
	if !codes["missing_required_field:records[0].payload.externalId"] {
		t.Errorf("missing externalId not reported: %+v", errs)
	}
	if !codes["missing_required_field:records[0].payload.sourceSystem"] {
		t.Errorf("missing sourceSystem not reported: %+v", errs)
	}
}

func TestValidateRecordsInvalidLiteral(t *testing.T) {
	errs := validateRecords([]Record{{
		Kind: "repository.v1", ExternalID: "x",
		Payload: map[string]any{"externalId": "acme/repo", "sourceSystem": "bitbucket"},
	}})
	if len(errs) != 1 || errs[0].Code != "invalid_literal" {
		t.Fatalf("%+v", errs)
	}
}

func TestValidateRecordsExtraFieldForbidden(t *testing.T) {
	errs := validateRecords([]Record{{
		Kind: "repository.v1", ExternalID: "x",
		Payload: map[string]any{"externalId": "acme/repo", "sourceSystem": "github", "notAField": true},
	}})
	if len(errs) != 1 || errs[0].Code != "invalid_field" || errs[0].Path != "records[0].payload.notAField" {
		t.Fatalf("%+v", errs)
	}
}

func TestValidateRecordsAcceptsAWellFormedRecordOfEveryKind(t *testing.T) {
	fixtures := map[string]map[string]any{
		"repository.v1": {"externalId": "acme/repo", "sourceSystem": "github"},
		"identity.v1":   {"canonicalId": "u1", "updatedAt": "2026-01-01T00:00:00Z"},
		"team.v1":       {"id": "t1", "name": "Team", "updatedAt": "2026-01-01T00:00:00Z"},
		"work_item.v1": {
			"externalKey": "ABC-1", "provider": "jira", "title": "t", "status": "todo",
			"createdAt": "2026-01-01T00:00:00Z",
		},
		"work_item_transition.v1": {
			"externalKey": "ABC-1", "provider": "jira", "occurredAt": "2026-01-01T00:00:00Z",
			"fromStatus": "todo", "toStatus": "done",
		},
		"work_item_dependency.v1": {
			"sourceExternalKey": "ABC-1", "targetExternalKey": "ABC-2", "relationshipType": "blocks",
		},
		"pull_request.v1": {
			"repositoryExternalId": "acme/repo", "number": float64(1), "state": "open",
			"createdAt": "2026-01-01T00:00:00Z",
		},
		"review.v1": {
			"repositoryExternalId": "acme/repo", "pullRequestNumber": float64(1), "reviewId": "r1",
			"reviewer": "u1", "state": "APPROVED", "submittedAt": "2026-01-01T00:00:00Z",
		},
		"commit.v1": {
			"repositoryExternalId": "acme/repo", "hash": "abcdef1", "authorWhen": "2026-01-01T00:00:00Z",
		},
		"operational_service.v1": {
			"externalId": "svc-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "name": "svc",
		},
		"operational_incident.v1": {
			"externalId": "inc-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "title": "down",
		},
		"operational_alert.v1": {
			"externalId": "al-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "title": "alert",
		},
		"incident_timeline_event.v1": {
			"externalId": "ev-1", "sourceVersionAt": "2026-01-01T00:00:00Z",
			"incidentExternalId": "inc-1", "eventType": "note",
		},
		"incident_note.v1": {
			"externalId": "n-1", "sourceVersionAt": "2026-01-01T00:00:00Z",
			"incidentExternalId": "inc-1", "body": "note body",
		},
		"incident_responder.v1": {
			"externalId": "r-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "incidentExternalId": "inc-1",
		},
		"escalation_policy.v1":  {"externalId": "ep-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "name": "ep"},
		"on_call_schedule.v1":   {"externalId": "sc-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "name": "sched"},
		"on_call_assignment.v1": {"externalId": "as-1", "sourceVersionAt": "2026-01-01T00:00:00Z"},
		"operational_team.v1":   {"externalId": "ot-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "name": "team"},
		"operational_user.v1": {
			"externalId": "ou-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "displayName": "User",
		},
		"service_repository_mapping.v1": {
			"externalId": "sm-1", "sourceVersionAt": "2026-01-01T00:00:00Z", "serviceExternalId": "svc-1",
		},
	}
	if len(fixtures) != len(recordModels) {
		t.Fatalf("fixture set covers %d kinds, recordModels has %d -- add the missing fixture(s)",
			len(fixtures), len(recordModels))
	}
	for kind, payload := range fixtures {
		t.Run(kind, func(t *testing.T) {
			errs := validateRecords([]Record{{Kind: kind, ExternalID: "x", Payload: payload}})
			if len(errs) != 0 {
				t.Fatalf("unexpected errors: %+v", errs)
			}
		})
	}
}

func TestEntityFamilyForRecordKinds(t *testing.T) {
	cases := []struct {
		kinds []string
		want  string
	}{
		{[]string{"repository.v1", "commit.v1"}, "legacy"},
		{[]string{"operational_incident.v1", "operational_alert.v1"}, "operational"},
		{[]string{"repository.v1", "operational_incident.v1"}, ""},
		{nil, ""},
	}
	for _, c := range cases {
		if got := entityFamilyForRecordKinds(c.kinds); got != c.want {
			t.Errorf("%v: got %q, want %q", c.kinds, got, c.want)
		}
	}
}
