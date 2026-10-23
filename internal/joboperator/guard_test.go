package joboperator

import (
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
	if summary.CorrelationID != "" || summary.OrganizationID != nil || summary.Domain != (jobcontract.DomainLink{}) {
		t.Fatalf("summary carries argument fields: %+v", summary)
	}
	if strings.Contains(strings.Join([]string{summary.CorrelationID, summary.Domain.Type, summary.Domain.ID}, " "), sentinel) {
		t.Fatal("summary carries the value")
	}
}
