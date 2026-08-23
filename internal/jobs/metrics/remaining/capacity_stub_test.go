package remaining

import (
	"context"
	"database/sql/driver"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// driverConnStub is an unimplemented clickhouse driver.Conn. Every method
// panics, which is deliberate: a test that reaches one has left the path it
// meant to exercise, and a silently-zero return would let it pass anyway.
type driverConnStub struct{}

func (driverConnStub) Contributors() []string { panic("stub: Contributors") }
func (driverConnStub) ServerVersion() (*chdriver.ServerVersion, error) {
	panic("stub: ServerVersion")
}
func (driverConnStub) Select(context.Context, any, string, ...any) error { panic("stub: Select") }
func (driverConnStub) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	panic("stub: Query")
}
func (driverConnStub) QueryRow(context.Context, string, ...any) chdriver.Row {
	panic("stub: QueryRow")
}
func (driverConnStub) PrepareBatch(context.Context, string, ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	panic("stub: PrepareBatch")
}
func (driverConnStub) Exec(context.Context, string, ...any) error { panic("stub: Exec") }
func (driverConnStub) AsyncInsert(context.Context, string, bool, ...any) error {
	panic("stub: AsyncInsert")
}
func (driverConnStub) Ping(context.Context) error               { panic("stub: Ping") }
func (driverConnStub) Stats() chdriver.Stats                    { panic("stub: Stats") }
func (driverConnStub) Close() error                             { panic("stub: Close") }
func (driverConnStub) CheckNamedValue(*driver.NamedValue) error { panic("stub: CheckNamedValue") }
