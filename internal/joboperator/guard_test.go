package joboperator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
	"github.com/riverqueue/river/rivertype"
)

// An operator must still be able to read, cancel and retry a queued job whose
// arguments carry a credential-shaped value; its summary holds the row's own
// fields and nothing taken from the arguments.
func TestSummaryOfAJobWithACredentialShapedArgumentOmitsTheArguments(t *testing.T) {
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	sentinel := probes[0]
	args := `{"contract_version":1,"correlation_id":"` + sentinel + `","idempotency_key":"heartbeat:2026-07-21T12:00:00Z",` +
		`"domain":{"type":"schedule_occurrence","id":"00000000-0000-4000-8000-000000000001"},"payload":{"scheduled_for":"2026-07-21T12:00:00Z"}}`
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	summary, err := summaryFromRiverRow(&rivertype.JobRow{
		ID: 7, Kind: jobcontract.KindHeartbeat, Queue: "heartbeat", State: rivertype.JobStateAvailable,
		MaxAttempts: 1, CreatedAt: now, ScheduledAt: now, EncodedArgs: []byte(args),
	})
	if err != nil {
		t.Fatalf("summary of a refused job failed: %v", err)
	}
	if summary.ID != 7 || summary.Kind != jobcontract.KindHeartbeat || summary.Queue != "heartbeat" ||
		summary.State != JobState(rivertype.JobStateAvailable) {
		t.Fatalf("summary lost the row fields: %+v", summary)
	}
	if summary.CorrelationID != "" || summary.OrganizationID != nil || summary.Domain != (jobcontract.DomainLink{}) || !summary.ArgumentsRefused {
		t.Fatalf("summary carries argument fields or is not flagged: %+v", summary)
	}
	if strings.Contains(strings.Join([]string{summary.CorrelationID, summary.Domain.Type, summary.Domain.ID}, " "), sentinel) {
		t.Fatal("summary carries the value")
	}
}

type projectedRow struct {
	correlation string
	org         *string
	domainType  string
	domainID    string
}

func (row projectedRow) Scan(dest ...any) error {
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	*dest[0].(*int64) = 7
	*dest[1].(*string) = jobcontract.KindHeartbeat
	*dest[2].(*string) = "heartbeat"
	*dest[3].(*string) = "available"
	*dest[4].(*int) = 0
	*dest[5].(*int) = 1
	*dest[6].(*time.Time) = now
	*dest[7].(*time.Time) = now
	*dest[10].(*string) = row.correlation
	*dest[11].(**string) = row.org
	*dest[12].(*string) = row.domainType
	*dest[13].(*string) = row.domainID
	return nil
}

// jobs inspect and jobs list read the argument fields straight from the stored
// row; a credential-shaped value in any of them must not reach the summary.
func TestProjectedSummaryOmitsACredentialShapedArgumentField(t *testing.T) {
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	sentinel := probes[0]
	benignOrg := "00000000-0000-4000-8000-000000000002"
	benign := projectedRow{correlation: "job-heartbeat-0001", org: &benignOrg, domainType: "schedule_occurrence", domainID: "00000000-0000-4000-8000-000000000001"}
	summary, err := scanProjectedSummary(benign)
	if err != nil || summary.CorrelationID != benign.correlation || summary.Domain.Type != benign.domainType ||
		summary.OrganizationID == nil || *summary.OrganizationID != benignOrg {
		t.Fatalf("benign row changed: %+v, %v", summary, err)
	}
	places := map[string]func(*projectedRow){
		"correlation_id":  func(row *projectedRow) { row.correlation = sentinel },
		"organization_id": func(row *projectedRow) { row.org = &sentinel },
		"domain.type":     func(row *projectedRow) { row.domainType = sentinel },
		"domain.id":       func(row *projectedRow) { row.domainID = sentinel },
	}
	for name, plant := range places {
		t.Run(name, func(t *testing.T) {
			row := benign
			plant(&row)
			summary, err := scanProjectedSummary(row)
			if err != nil {
				t.Fatalf("a refused job must stay readable: %v", err)
			}
			encoded, err := json.Marshal(summary)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), sentinel) || strings.Contains(string(encoded), sentinel[len(sentinel)/2:]) {
				t.Fatalf("summary carries the value: %s", encoded)
			}
			if summary.ID != 7 || summary.Kind != jobcontract.KindHeartbeat || summary.Queue != "heartbeat" {
				t.Fatalf("summary lost the row fields: %+v", summary)
			}
			if summary.CorrelationID != "" || summary.OrganizationID != nil || summary.Domain != (jobcontract.DomainLink{}) {
				t.Fatalf("summary keeps argument fields of a refused job: %+v", summary)
			}
		})
	}
}

func refusedSummary() JobSummary {
	summary := jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 0)
	summary.CorrelationID, summary.OrganizationID, summary.Domain = "", nil, jobcontract.DomainLink{}
	summary.ArgumentsRefused = true
	return summary
}

func TestScanProjectedSummaryFlagsAJobWhoseArgumentsAreRefused(t *testing.T) {
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	summary, err := scanProjectedSummary(projectedRow{correlation: probes[0], domainType: "schedule_occurrence", domainID: "00000000-0000-4000-8000-000000000001"})
	if err != nil || !summary.ArgumentsRefused {
		t.Fatalf("summary = %+v, %v", summary, err)
	}
	clean, err := scanProjectedSummary(projectedRow{correlation: "job-heartbeat-0001", domainType: "schedule_occurrence", domainID: "00000000-0000-4000-8000-000000000001"})
	if err != nil || clean.ArgumentsRefused {
		t.Fatalf("benign summary flagged: %+v, %v", clean, err)
	}
}

// A job whose arguments were refused stays listable and inspectable: one such
// row must not fail the page or the inspect call.
func TestInspectAndListKeepAJobWhoseArgumentsAreRefused(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	refused := refusedSummary()
	fixture.backend.jobs = []JobSummary{refused, jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 0)}
	fixture.backend.job = refused
	got, err := fixture.service.Inspect(context.Background(), testPrincipal(), refused.ID)
	if err != nil || !got.ArgumentsRefused || got.CorrelationID != "" {
		t.Fatalf("Inspect = %+v, %v", got, err)
	}
	jobs, err := fixture.service.List(context.Background(), testPrincipal(), ListFilter{States: []JobState{StateAvailable}, Limit: 25})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("List = %d jobs, %v", len(jobs), err)
	}
}

func TestRefusedFlagCannotSmuggleArgumentFields(t *testing.T) {
	t.Parallel()
	fixture := newServiceFixture(t)
	smuggled := refusedSummary()
	smuggled.CorrelationID = "corr-safe"
	fixture.backend.job = smuggled
	if _, err := fixture.service.Inspect(context.Background(), testPrincipal(), smuggled.ID); err == nil {
		t.Fatal("a refused summary that carries a correlation id was accepted")
	}
	empty := jobSummary(StateAvailable, jobcontract.KindHeartbeat, "heartbeat", 0)
	empty.CorrelationID = ""
	fixture.backend.job = empty
	if _, err := fixture.service.Inspect(context.Background(), testPrincipal(), empty.ID); err == nil {
		t.Fatal("an unflagged summary with no correlation id was accepted")
	}
}
