package joboperator

import (
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
	if summary.CorrelationID != "" || summary.OrganizationID != nil || summary.Domain != (jobcontract.DomainLink{}) {
		t.Fatalf("summary carries argument fields: %+v", summary)
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
