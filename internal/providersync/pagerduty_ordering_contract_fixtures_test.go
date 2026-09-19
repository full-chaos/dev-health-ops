package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type pagerDutyContractEffects interface {
	EffectSink
	EffectReadback
}

// pagerDutyContractSinkCase is one PagerDuty sink writing one operational
// table: a valid effect for it and a constructor for the sink.
type pagerDutyContractSinkCase struct {
	name   string
	table  string
	claim  Claim
	effect EffectBatch
	build  func(driver.Conn) pagerDutyContractEffects
}

// pagerDutyContractSinkCases returns every PagerDuty sink x destination pair
// for one tenant. TestEveryPagerDutySinkTypeIsInTheContractSweep pins that no
// sink type is missing.
func pagerDutyContractSinkCases(t *testing.T, orgID string) []pagerDutyContractSinkCase {
	t.Helper()
	at := time.Date(2026, 8, 9, 19, 0, 0, 123456000, time.UTC)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	claimFor := func(dataset string) Claim {
		claim := nativeTestClaim("pagerduty", dataset)
		claim.OrgID = orgID
		return claim
	}
	must := func(err error) {
		if err != nil {
			t.Helper()
			t.Fatal(err)
		}
	}
	var cases []pagerDutyContractSinkCase

	servicesClaim := claimFor("services")
	service, err := normalizePagerDutyService(servicesClaim, "acme", pagerDutyServicePayload{ID: "PS1", Name: "Payments"}, at)
	must(err)
	mapping, err := pagerDutyServiceMappingFromReference(service,
		pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"},
		pagerDutyMappingMetadata, at, "")
	must(err)
	servicesSink := func(conn driver.Conn) pagerDutyContractEffects {
		return PagerDutyServicesClickHouseEffects{
			Conn: conn, Lease: lease, ProviderInstanceID: "acme", Now: func() time.Time { return at },
			Entitlement: allowIncidentEntitlement,
		}
	}
	cases = append(cases,
		pagerDutyContractSinkCase{"services", "operational_services", servicesClaim,
			mustContractEffect(t, "operational_services", []pagerDutyServiceRow{service}), servicesSink},
		pagerDutyContractSinkCase{"services-mappings", "operational_service_repository_mappings", servicesClaim,
			mustContractEffect(t, "operational_service_repository_mappings", []pagerDutyServiceRepositoryMappingRow{mapping}), servicesSink},
	)

	businessClaim := claimFor("business-services")
	business, err := normalizePagerDutyBusinessService(businessClaim, "acme",
		pagerDutyBusinessServicePayload{ID: "PBS1", Name: "Checkout"}, at)
	must(err)
	cases = append(cases, pagerDutyContractSinkCase{"business-services", "operational_services", businessClaim,
		mustContractEffect(t, "operational_services", []pagerDutyBusinessServiceRow{business}),
		func(conn driver.Conn) pagerDutyContractEffects {
			return PagerDutyBusinessServicesClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: "acme", Now: func() time.Time { return at },
				Entitlement: allowIncidentEntitlement,
			}
		}})

	escalationClaim := claimFor("escalation-policies")
	escalation, err := normalizePagerDutyEscalationPolicy(escalationClaim, "acme",
		pagerDutyEscalationPolicyPayload{ID: "PE1", Name: "Primary", UpdatedAt: "2026-08-01T10:00:00Z"}, at)
	must(err)
	cases = append(cases, pagerDutyContractSinkCase{"escalation-policies", "operational_escalation_policies", escalationClaim,
		mustContractEffect(t, "operational_escalation_policies", []pagerDutyEscalationPolicyRow{escalation}),
		func(conn driver.Conn) pagerDutyContractEffects {
			return PagerDutyEscalationPoliciesClickHouseEffects{
				Conn: conn, Lease: lease, Entitlement: allowIncidentEntitlement,
			}
		}})

	schedulesClaim := claimFor("schedules")
	schedule, err := normalizePagerDutySchedule(schedulesClaim, "acme",
		pagerDutySchedulePayload{ID: "PSC1", Name: "Primary"}, at)
	must(err)
	cases = append(cases, pagerDutyContractSinkCase{"schedules", "operational_on_call_schedules", schedulesClaim,
		mustContractEffect(t, "operational_on_call_schedules", []pagerDutyScheduleRow{schedule}),
		func(conn driver.Conn) pagerDutyContractEffects {
			return PagerDutySchedulesClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: "acme", Now: func() time.Time { return at },
				Entitlement: allowIncidentEntitlement,
			}
		}})

	onCallsClaim := claimFor("on-calls")
	level := int32(1)
	onCall, err := normalizePagerDutyOnCall(onCallsClaim, "acme", pagerDutyOnCallPayload{
		ID: "OC1", Start: "2026-08-01T10:00:00Z", End: "2026-08-01T18:00:00Z",
		EscalationLevel: &level, User: &pagerDutyOnCallReference{ID: "PU1"},
		Schedule: &pagerDutyOnCallReference{ID: "PSC1"}, EscalationPolicy: &pagerDutyOnCallReference{ID: "PE1"},
	}, at)
	must(err)
	cases = append(cases, pagerDutyContractSinkCase{"on-calls", "operational_on_call_assignments", onCallsClaim,
		mustContractEffect(t, "operational_on_call_assignments", []pagerDutyOnCallRow{onCall}),
		func(conn driver.Conn) pagerDutyContractEffects {
			return PagerDutyOnCallsClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: "acme",
				Entitlement: allowIncidentEntitlement,
			}
		}})

	usersClaim := claimFor("users")
	user, err := normalizePagerDutyUser(usersClaim, "acme", pagerDutyUserPayload{ID: "PU1", Name: "Alice"}, at)
	must(err)
	cases = append(cases, pagerDutyContractSinkCase{"users", "operational_users", usersClaim,
		mustContractEffect(t, "operational_users", []pagerDutyUserRow{user}),
		func(conn driver.Conn) pagerDutyContractEffects {
			return PagerDutyUsersClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: "acme", Now: func() time.Time { return at },
				Entitlement: allowIncidentEntitlement,
			}
		}})

	teamsClaim := claimFor("teams")
	team, err := normalizePagerDutyTeam(teamsClaim, "acme", pagerDutyTeamPayload{ID: "PT1", Name: "Platform"}, at)
	must(err)
	cases = append(cases, pagerDutyContractSinkCase{"teams", "operational_teams", teamsClaim,
		mustContractEffect(t, "operational_teams", []pagerDutyTeamRow{team}),
		func(conn driver.Conn) pagerDutyContractEffects {
			return PagerDutyTeamsClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: "acme", Now: func() time.Time { return at },
				Entitlement: allowIncidentEntitlement,
			}
		}})

	familySink := func(conn driver.Conn) pagerDutyContractEffects {
		return PagerDutyIncidentFamilyClickHouseEffects{
			Conn: conn, Lease: lease, ProviderInstanceID: "acme",
			Entitlement: allowIncidentEntitlement,
		}
	}
	incidentClaim := claimFor("incidents")
	incident, err := normalizePagerDutyIncident(incidentClaim, "acme", pagerDutyIncidentPayload{
		ID: "PI1", Title: pagerDutyStringPtr("Database outage"), Status: pagerDutyStringPtr("triggered"),
		CreatedAt: pagerDutyStringPtr("2026-07-17T12:00:00Z"), UpdatedAt: pagerDutyStringPtr("2026-07-17T12:01:00Z"),
	}, at)
	must(err)
	alertClaim := claimFor("incident-alerts")
	alert, err := normalizePagerDutyAlert(alertClaim, "acme", pagerDutyAlertPayload{
		ID: "PA1", Summary: pagerDutyStringPtr("Disk alert"), Status: pagerDutyStringPtr("triggered"),
		Severity:  pagerDutyStringPtr("critical"),
		CreatedAt: pagerDutyStringPtr("2026-07-17T12:02:00Z"), UpdatedAt: pagerDutyStringPtr("2026-07-17T12:03:00Z"),
	}, incident.ID, at)
	must(err)
	logClaim := claimFor("incident-log-entries")
	entry, err := normalizePagerDutyLogEntry(logClaim, "acme", pagerDutyLogEntryPayload{
		ID: "PL1", Type: pagerDutyStringPtr("status_change"), Summary: pagerDutyStringPtr("Triggered"),
		CreatedAt: pagerDutyStringPtr("2026-07-17T12:04:00Z"),
	}, incident.ID, at)
	must(err)
	noteClaim := claimFor("incident-notes")
	note, err := normalizePagerDutyNote(noteClaim, "acme", pagerDutyNotePayload{
		ID: "PN1", Content: pagerDutyStringPtr("Investigating"), CreatedAt: pagerDutyStringPtr("2026-07-17T12:05:00Z"),
	}, incident.ID, at)
	must(err)
	cases = append(cases,
		pagerDutyContractSinkCase{"incidents", "operational_incidents", incidentClaim,
			mustContractEffect(t, "operational_incidents", []pagerDutyIncidentRow{incident}), familySink},
		pagerDutyContractSinkCase{"incident-alerts", "operational_alerts", alertClaim,
			mustContractEffect(t, "operational_alerts", []pagerDutyAlertRow{alert}), familySink},
		pagerDutyContractSinkCase{"incident-log-entries", "operational_incident_timeline_events", logClaim,
			mustContractEffect(t, "operational_incident_timeline_events", []pagerDutyLogEntryRow{entry}), familySink},
		pagerDutyContractSinkCase{"incident-notes", "operational_incident_notes", noteClaim,
			mustContractEffect(t, "operational_incident_notes", []pagerDutyNoteRow{note}), familySink},
	)

	responderClaim, err := pagerDutyWebhookClaim(pagerDutyWebhookTestClaim(orgID), "incidents")
	must(err)
	responder := pagerDutyResponderRow{
		OrgID: orgID, Provider: "pagerduty", ProviderInstanceID: "acme",
		SourceEntityType: "responder", ExternalID: "evt-1", SourceVersionAt: at,
		ObservedAt: at, LastSynced: at, IncidentID: incident.ID,
	}
	must(fillPagerDutyResponderOrdering(&responder))
	cases = append(cases, pagerDutyContractSinkCase{"responders", "operational_incident_responders", responderClaim,
		mustContractEffect(t, "operational_incident_responders", []pagerDutyResponderRow{responder}),
		func(conn driver.Conn) pagerDutyContractEffects {
			return PagerDutyWebhookRespondersClickHouseEffects{
				Conn: conn, Lease: lease, ProviderInstanceID: "acme",
				Entitlement: allowIncidentEntitlement,
			}
		}})
	return cases
}

func mustContractEffect[T any](t *testing.T, table string, rows []T) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(table, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
