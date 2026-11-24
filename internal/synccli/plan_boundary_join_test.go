package synccli

import "testing"

// The sink's escaped login "svc:" ends where the database's password "A7kZ2"
// starts: one union over both credentials hides the password whole, where one
// Boundary after the other left its tail.
func TestPlanBoundaryRedactsTheUnionOfTheSinkAndTheDatabase(t *testing.T) {
	db := "postgres://acct:A7kZ2@h/db"
	plan := Plan{SinkURI: "clickhouse://svc%3A:zz9q@c/d", DB: &db}
	if got := planBoundary(plan)("echo svc%3A7kZ2 end"); got != "echo [REDACTED] end" {
		t.Errorf("got %q", got)
	}
	if got := planBoundary(Plan{SinkURI: plan.SinkURI})("echo svc%3A7kZ2 end"); got != "echo [REDACTED]7kZ2 end" {
		t.Errorf("sink only: got %q", got)
	}
}
