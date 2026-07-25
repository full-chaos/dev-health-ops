package providerfoundation

import (
	"testing"
	"time"
)

func TestNormalizeWorkItemDerivesSinkEnvelope(t *testing.T) {
	t.Parallel()
	observedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	statusRaw := "open"
	projectID := "acme/api"
	envelope, err := NormalizeWorkItem(
		NormalizationContext{
			IntegrationID: "integration-github",
			Provenance:    Provenance{Source: "native", Confidence: "1.0", EvidenceID: "github:issue:42"},
		},
		WorkItemRecord{
			Provider: "github", OrgID: "org-acme", WorkItemID: "gh:acme/api#42",
			Title: "Bound retry waits", Type: "issue", Status: "in_progress",
			StatusRaw: &statusRaw, ProjectID: &projectID, UpdatedAt: observedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.SourceID != "gh:acme/api#42" || envelope.DedupeKey != "github:work_item:gh:acme/api#42" || envelope.ObservedAt != observedAt {
		t.Fatalf("unexpected identity envelope: %+v", envelope)
	}
	if got := envelope.Attributes["work_scope_id"]; got != projectID {
		t.Fatalf("work scope=%q, want %q", got, projectID)
	}
	if got := envelope.Attributes["title"]; got != "Bound retry waits" {
		t.Fatalf("title=%q", got)
	}
}

func TestNormalizeFeatureFlagDerivesOptionalAttributes(t *testing.T) {
	t.Parallel()
	observedAt := time.Date(2026, 7, 23, 12, 4, 0, 0, time.UTC)
	projectKey := "payments"
	flagType := "boolean"
	envelope, err := NormalizeFeatureFlag(
		NormalizationContext{
			IntegrationID: "integration-launchdarkly",
			Provenance:    Provenance{Source: "native", Confidence: "1.0"},
		},
		FeatureFlagRecord{
			Provider: "launchdarkly", OrgID: "org-acme", FlagKey: "checkout-v2",
			ProjectKey: &projectKey, Environment: "production", FlagType: &flagType,
			LastSynced: observedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.EntityType != "feature_flag" || envelope.Attributes["project_key"] != projectKey || envelope.Attributes["flag_type"] != flagType {
		t.Fatalf("unexpected feature-flag envelope: %+v", envelope)
	}
}

func TestNormalizeSourceRecordRestrictsEntityFamilyAndCopiesAttributes(t *testing.T) {
	t.Parallel()
	observedAt := time.Date(2026, 7, 23, 12, 3, 0, 0, time.UTC)
	attributes := map[string]string{"name": "acme/api"}
	envelope, err := NormalizeSourceRecord(
		NormalizationContext{
			IntegrationID: "integration-github",
			Provenance:    Provenance{Source: "github_rest", Confidence: "1.0"},
		},
		SourceRecord{
			Provider: "github", OrgID: "org-acme", EntityType: "repository",
			SourceID: "github:repo:acme/api", ObservedAt: observedAt, Attributes: attributes,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	attributes["name"] = "mutated"
	if envelope.DedupeKey != "github:repository:github:repo:acme/api" ||
		envelope.Attributes["name"] != "acme/api" {
		t.Fatalf("unexpected source envelope: %+v", envelope)
	}
	if _, err := NormalizeSourceRecord(
		NormalizationContext{IntegrationID: "integration-github", Provenance: Provenance{Source: "native", Confidence: "1.0"}},
		SourceRecord{Provider: "github", OrgID: "org-acme", EntityType: "arbitrary", SourceID: "1", ObservedAt: observedAt},
	); err == nil {
		t.Fatal("arbitrary source entity family was accepted")
	}
}

func TestNormalizeOperationalServiceUsesPythonCanonicalIdentity(t *testing.T) {
	t.Parallel()
	observedAt := time.Date(2026, 7, 23, 12, 5, 0, 0, time.UTC)
	status := "active"
	envelope, err := NormalizeOperationalService(
		NormalizationContext{
			IntegrationID: "integration-pagerduty",
			Provenance:    Provenance{Source: "native", Confidence: "1.0"},
		},
		OperationalServiceRecord{
			Provider: "pagerduty", OrgID: "org-acme", ProviderInstanceID: "pd-acme",
			SourceEntityType: "service", ExternalID: "svc-payments",
			SourceVersionAt: observedAt, ObservedAt: observedAt,
			Name: "Payments", NormalizedStatus: &status,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	const expectedID = "340306bf375308bf0bbdf43c4b7cc12e0c7d6ed55c48f022c7c5c95ad6bf435f"
	if envelope.SourceID != expectedID || envelope.DedupeKey != "pagerduty:operational_service:"+expectedID {
		t.Fatalf("unexpected operational identity: %+v", envelope)
	}
}

func TestCanonicalOperationalIDMatchesPythonEnsureASCII(t *testing.T) {
	t.Parallel()
	got, err := CanonicalOperationalID("org-☃", "pager&duty", "pd<1>", "operational_service", "svc😀")
	if err != nil {
		t.Fatal(err)
	}
	const expected = "514ae0f189e7485a3b7cea416c6769206705b4cb77ad34f129a2a71930eb13f3"
	if got != expected {
		t.Fatalf("canonical ID=%s, want %s", got, expected)
	}
}
