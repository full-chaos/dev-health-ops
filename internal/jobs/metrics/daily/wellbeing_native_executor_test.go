package daily

import (
	"context"
	stddriver "database/sql/driver"
	"errors"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// stubDriverConn is an unimplemented clickhouse driver.Conn. Every method
// panics, which is deliberate: a test that reaches one has left the path it
// meant to exercise (construction-only tests below never issue a query), and
// a silently-zero return would let it pass anyway. Mirrors
// internal/jobs/metrics/remaining's driverConnStub.
type stubDriverConn struct{}

func (stubDriverConn) Contributors() []string { panic("stub: Contributors") }
func (stubDriverConn) ServerVersion() (*chdriver.ServerVersion, error) {
	panic("stub: ServerVersion")
}
func (stubDriverConn) Select(context.Context, any, string, ...any) error { panic("stub: Select") }
func (stubDriverConn) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	panic("stub: Query")
}
func (stubDriverConn) QueryRow(context.Context, string, ...any) chdriver.Row {
	panic("stub: QueryRow")
}
func (stubDriverConn) PrepareBatch(context.Context, string, ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	panic("stub: PrepareBatch")
}
func (stubDriverConn) Exec(context.Context, string, ...any) error { panic("stub: Exec") }
func (stubDriverConn) AsyncInsert(context.Context, string, bool, ...any) error {
	panic("stub: AsyncInsert")
}
func (stubDriverConn) Ping(context.Context) error                  { panic("stub: Ping") }
func (stubDriverConn) Stats() chdriver.Stats                       { panic("stub: Stats") }
func (stubDriverConn) Close() error                                { panic("stub: Close") }
func (stubDriverConn) CheckNamedValue(*stddriver.NamedValue) error { panic("stub: CheckNamedValue") }

func mustParseDay(t *testing.T, value string) time.Time {
	t.Helper()
	day, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatal(err)
	}
	return day
}

func TestNewTeamWellbeingExecutorRejectsNilConn(t *testing.T) {
	if _, err := NewTeamWellbeingExecutor(nil); !errors.Is(err, errTeamWellbeingUnavailable) {
		t.Fatalf("err=%v, want errTeamWellbeingUnavailable", err)
	}
}

func TestNewTeamWellbeingExecutorRejectsUnparseableTimezone(t *testing.T) {
	t.Setenv("BUSINESS_TIMEZONE", "Not/A_Real_Zone")
	if _, err := NewTeamWellbeingExecutor(&stubDriverConn{}); !errors.Is(err, errTeamWellbeingUnavailable) {
		t.Fatalf("err=%v, want errTeamWellbeingUnavailable", err)
	}
}

func TestNewTeamWellbeingExecutorRejectsUnparseableBusinessHours(t *testing.T) {
	t.Setenv("BUSINESS_HOURS_START", "not-a-number")
	if _, err := NewTeamWellbeingExecutor(&stubDriverConn{}); !errors.Is(err, errTeamWellbeingUnavailable) {
		t.Fatalf("err=%v, want errTeamWellbeingUnavailable", err)
	}
}

func TestNewTeamWellbeingExecutorDefaultsToUTCNineToSeventeen(t *testing.T) {
	executor, err := NewTeamWellbeingExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	if executor.businessTZ.String() != "UTC" || executor.businessHoursStart != 9 || executor.businessHoursEnd != 17 {
		t.Fatalf("defaults: tz=%s start=%d end=%d", executor.businessTZ, executor.businessHoursStart, executor.businessHoursEnd)
	}
}

func TestComputeFamilyRejectsMissingOrganizationOrDay(t *testing.T) {
	executor, err := NewTeamWellbeingExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(context.Background(), Run{}, Partition{ID: testPartitionID}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestComputeFamilyRejectsUnparseablePartitionRepoIDs(t *testing.T) {
	executor, err := NewTeamWellbeingExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: testOrgID, TargetDay: mustParseDay(t, "2026-08-24")}
	partition := Partition{ID: testPartitionID, RepoIDs: []RepositoryID{"not-a-uuid"}}
	if _, err := executor.ComputeFamily(context.Background(), run, partition); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}
