//go:build integration

package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// paramOutcome is what a real server answers for one parameter value: the
// value read back, or the code of the exception it raised.
type paramOutcome struct {
	got  string
	code int32
}

// The server parses a {name:Type} parameter value as the text form of Type;
// the native protocol adds one quoted layer around it. Every Array(String)
// cell must round-trip byte-exact on both protocols. A top-level String
// value is sent raw, so the server's escape reading still decodes a
// backslash and stops at a tab or a newline: those cells name today's
// answer, so a driver change that alters them fails here instead of in a
// query.
var parameterCharacterCases = []struct {
	name  string
	value string
	known *paramOutcome
}{
	{name: "plain", value: "gh:abc"},
	{name: "apostrophe", value: "gh:doesn't"},
	{name: "backslash", value: `a\b`, known: &paramOutcome{got: "a\b"}},
	{name: "backslash n", value: `a\nb`, known: &paramOutcome{got: "a\nb"}},
	{name: "trailing backslash", value: `ab\`, known: &paramOutcome{code: 25}},
	{name: "backslash apostrophe", value: `a\'b`, known: &paramOutcome{got: "a'b"}},
	{name: "tab", value: "a\tb", known: &paramOutcome{code: 457}},
	{name: "newline", value: "a\nb", known: &paramOutcome{code: 457}},
	{name: "carriage return", value: "a\rb"},
	{name: "nul", value: "a\x00b"},
	{name: "unicode", value: "é-日本-😀"},
	{name: "double quote", value: `a"b`},
	{name: "quote comma bracket", value: "a','b]"},
	{name: "braces", value: "{x:String}"},
	{name: "quoted whole", value: "'abc'"},
	{name: "field dump shape", value: "UInt64_42"},
	{name: "empty", value: ""},
}

func TestServerSideParameterCharactersOnBothProtocols(t *testing.T) {
	ctx, native, http := parameterConns(t)
	for _, protocol := range []struct {
		name string
		conn driver.Conn
	}{{"native", native}, {"http", http}} {
		t.Run(protocol.name, func(t *testing.T) {
			for _, tc := range parameterCharacterCases {
				t.Run(tc.name, func(t *testing.T) {
					var array []string
					if err := protocol.conn.QueryRow(ctx, "SELECT {ids:Array(String)}", clickhouse.Named("ids", []string{tc.value, "z"})).Scan(&array); err != nil {
						t.Errorf("Array(String): %v", err)
					} else if len(array) != 2 || array[0] != tc.value || array[1] != "z" {
						t.Errorf("Array(String) = %q, want %q", array, []string{tc.value, "z"})
					}

					want := paramOutcome{got: tc.value}
					if tc.known != nil {
						want = *tc.known
					}
					if got := stringParamOutcome(t, ctx, protocol.conn, tc.value); got != want {
						t.Errorf("String = %+v, want %+v", got, want)
					}
				})
			}
		})
	}
}

func stringParamOutcome(t *testing.T, ctx context.Context, conn driver.Conn, value string) paramOutcome {
	t.Helper()
	var got string
	err := conn.QueryRow(ctx, "SELECT {s:String}", clickhouse.Named("s", value)).Scan(&got)
	if err == nil {
		return paramOutcome{got: got}
	}
	var exception *clickhouse.Exception
	if !errors.As(err, &exception) {
		t.Fatalf("String: %v is not a server exception", err)
	}
	return paramOutcome{code: exception.Code}
}

// Every Go type production code passes to clickhouse.Named (the census in
// param_types_census_test.go keeps it to this set) round-trips on both
// protocols.
func TestServerSideParameterProductionTypesOnBothProtocols(t *testing.T) {
	ctx, native, http := parameterConns(t)
	id := uuid.MustParse("6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b")
	cases := []struct {
		typ   string
		value any
		want  string
	}{
		{"String", "o'brien", "o'brien"},
		{"UUID", id.String(), id.String()},
		{"Array(String)", []string{"a'b", "c"}, "['a\\'b','c']"},
		{"Array(String)", []string{}, "[]"},
		{"Array(UUID)", []uuid.UUID{id}, "['" + id.String() + "']"},
		{"Array(UInt32)", []uint32{1, 4294967295}, "[1,4294967295]"},
		{"UInt32", uint32(4294967295), "4294967295"},
		{"UInt64", uint64(18446744073709551615), "18446744073709551615"},
	}
	for _, protocol := range []struct {
		name string
		conn driver.Conn
	}{{"native", native}, {"http", http}} {
		for _, tc := range cases {
			var got string
			err := protocol.conn.QueryRow(ctx, "SELECT toString({v:"+tc.typ+"})", clickhouse.Named("v", tc.value)).Scan(&got)
			if err != nil || got != tc.want {
				t.Errorf("%s %s %T = %q, %v; want %q", protocol.name, tc.typ, tc.value, got, err, tc.want)
			}
		}
	}
}

// The dev-health-go query client encodes its own parameter text and sends
// it through the driver. Its array encoding doubles a quote and refuses a
// backslash, so it round-trips; its NULL marker is written for the older
// driver that dropped one backslash on the native protocol, and now
// arrives as the two characters \N.
func TestQueryClientParametersOnTheNativeProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance := startParameterClickHouse(t, ctx)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	at := time.Date(2026, 10, 9, 12, 34, 56, 789000000, time.UTC)
	cases := []struct {
		typ   string
		value any
		want  string
	}{
		{"String", "o'brien", "o'brien 0"},
		{"Array(String)", []string{"doesn't", "a,b]", "a\tb"}, "['doesn\\'t','a,b]','a\\tb'] 0"},
		{"Array(String)", []string{}, "[] 0"},
		{"DateTime64(3)", at, "2026-10-09 12:34:56.789 0"},
		{"UInt32", uint32(7), "7 0"},
		{"Int64", -5, "-5 0"},
		{"Nullable(String)", nil, `\N 0`},
	}
	for _, tc := range cases {
		rows, err := client.Query(ctx, "SELECT concat(toString({v:"+tc.typ+"}), ' ', toString(isNull({v:"+tc.typ+"})))", []dhclickhouse.Binding{{Name: "v", Value: tc.value}})
		if err != nil {
			t.Errorf("%s %T: %v", tc.typ, tc.value, err)
			continue
		}
		var got string
		for rows.Next() {
			if err := rows.Scan(&got); err != nil {
				t.Errorf("%s %T: scan: %v", tc.typ, tc.value, err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Errorf("%s %T: %v", tc.typ, tc.value, err)
		}
		_ = rows.Close()
		if got != tc.want {
			t.Errorf("%s %T = %q, want %q", tc.typ, tc.value, got, tc.want)
		}
	}
}

func startParameterClickHouse(t *testing.T, ctx context.Context) *containers.Instance {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	return instance
}

func parameterConns(t *testing.T) (context.Context, driver.Conn, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance := startParameterClickHouse(t, ctx)
	httpDSN, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	open := func(dsn string, protocol clickhouse.Protocol) driver.Conn {
		options, err := clickhouse.ParseDSN(dsn)
		if err != nil {
			t.Fatal(err)
		}
		options.Protocol = protocol
		conn, err := clickhouse.Open(options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if err := conn.Ping(ctx); err != nil {
			t.Fatalf("ping %v: %v", protocol, err)
		}
		return conn
	}
	return ctx, open(instance.URI, clickhouse.Native), open(httpDSN, clickhouse.HTTP)
}
