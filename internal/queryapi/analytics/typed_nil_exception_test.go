package analytics

import (
	"context"
	"fmt"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

// wrapsCause is the shape of the ClickHouse client's own operation error: a fixed text and the driver error behind Unwrap.
type wrapsCause struct{ cause error }

func (e wrapsCause) Error() string { return "ClickHouse query failed" }
func (e wrapsCause) Unwrap() error { return e.cause }

// A typed-nil *clickhouse.Exception behind the client's wrapper is "found" by the bounded errors.As with a nil target: neither
// recorder may dereference it (CHAOS-7936 r1).
func TestRecordersSurviveATypedNilClickHouseException(t *testing.T) {
	var nilException *clickhousedriver.Exception
	for name, err := range map[string]error{
		"bare":    error(nilException),
		"wrapped": wrapsCause{cause: nilException},
		"fmt":     fmt.Errorf("query: %w", wrapsCause{cause: nilException}),
	} {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("%s: a recorder panicked: %v", name, recovered)
				}
			}()
			captureSlog(t)
			defaultRecordDegradation(context.Background(), "sankey", err)
			defaultRecordInvestmentCoverageFailure(context.Background(), "org-1", MeasureCount, true, coverageStageQuery, "", err)
		}()
	}
}
