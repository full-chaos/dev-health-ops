package sync

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestExplicitDatasetsWithoutRowCounterIsScraped(t *testing.T) {
	scrape := func() string {
		var out bytes.Buffer
		if err := (&NativeMaterializer{}).WritePrometheus(&out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	before := explicitDatasetsWithoutRowTotal.Load()
	recordExplicitDatasetsWithoutRow(2)
	want := "dev_health_scheduler_explicit_datasets_without_row_total " + strconv.FormatUint(before+2, 10) + "\n"
	text := scrape()
	if !strings.Contains(text, "# TYPE dev_health_scheduler_explicit_datasets_without_row_total counter\n") || !strings.Contains(text, want) {
		t.Fatalf("scrape lacks %q:\n%s", want, text)
	}
}
