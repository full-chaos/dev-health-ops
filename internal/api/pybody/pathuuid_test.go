package pybody

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestPathUUIDReportsAPathLocation(t *testing.T) {
	var problems Errors
	if _, ok := problems.PathUUID("ingestion_id", "not-a-uuid"); ok || len(problems) != 1 {
		t.Fatalf("invalid id: ok=%v problems=%v", ok, problems)
	}
	text, err := pyjson.Marshal(Detail(problems))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"detail":[{"type":"uuid_parsing","loc":["path","ingestion_id"],"msg":"Input should be a valid UUID, invalid character: found ` + "`n`" + ` at 1","input":"not-a-uuid","ctx":{"error":"invalid character: found ` + "`n`" + ` at 1"}}]}`
	if string(text) != want {
		t.Fatalf("got %s\nwant %s", text, want)
	}
	problems = nil
	if value, ok := problems.PathUUID("ingestion_id", "{2DC94E6C-B35D-4B0F-839D-20720D48D7FA}"); !ok || value.String() != "2dc94e6c-b35d-4b0f-839d-20720d48d7fa" {
		t.Fatalf("braced upper-case form: %v %v", value, ok)
	}
}
