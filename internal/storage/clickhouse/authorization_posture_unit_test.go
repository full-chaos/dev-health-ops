package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type grantsConn struct {
	driver.Conn
	lines []string
}

func (c *grantsConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if query != "SHOW GRANTS FOR CURRENT_USER" {
		return nil, errors.New("unexpected query: " + query)
	}
	return &grantsRows{lines: c.lines, index: -1}, nil
}

type grantsRows struct {
	driver.Rows
	lines []string
	index int
}

func (r *grantsRows) Next() bool { r.index++; return r.index < len(r.lines) }
func (r *grantsRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.lines[r.index]
	return nil
}
func (r *grantsRows) Close() error { return nil }
func (r *grantsRows) Err() error   { return nil }

const telemetryGrantLine = "GRANT INSERT ON default.telemetry_signal_bucket TO dho_api_ch"

// liveGrants renders what the posture's own statements grant, minus every line that names the
// telemetry table, so each cell adds back exactly the telemetry line it needs.
func liveGrants(posture Posture) []string {
	var lines []string
	for _, statement := range GrantStatements("dho_api_ch", posture) {
		if !strings.Contains(statement, "telemetry_signal_bucket") {
			lines = append(lines, statement)
		}
	}
	return lines
}

func checkWith(t *testing.T, posture Posture, lines []string) error {
	t.Helper()
	return CheckPosture(context.Background(), &grantsConn{lines: lines}, posture)
}

// CHAOS-12878: the retired telemetry insert grant is gone from prod's users.d, so a login that still holds it is refused.
func TestTelemetryGrantPresentIsRefused(t *testing.T) {
	posture := APIPosture("default")
	err := checkWith(t, posture, append(liveGrants(posture), telemetryGrantLine))
	if !errors.Is(err, ErrPostureMismatch) || !strings.Contains(err.Error(), "default.telemetry_signal_bucket") {
		t.Fatalf("a held telemetry insert grant must be refused by name, got %v", err)
	}
}

func TestTelemetryGrantAbsentIsAccepted(t *testing.T) {
	posture := APIPosture("default")
	if err := checkWith(t, posture, liveGrants(posture)); err != nil {
		t.Fatalf("absent grant must pass: %v", err)
	}
}

func TestTelemetryGrantAbsentIsAcceptedWithTheProjectScopeGrant(t *testing.T) {
	posture := APIPostureFor("default", true)
	if err := checkWith(t, posture, liveGrants(posture)); err != nil {
		t.Fatalf("absent grant must pass with project scope: %v", err)
	}
	err := checkWith(t, posture, append(liveGrants(posture), telemetryGrantLine))
	if !errors.Is(err, ErrPostureMismatch) || !strings.Contains(err.Error(), "default.telemetry_signal_bucket") {
		t.Fatalf("a held telemetry insert grant must be refused with project scope, got %v", err)
	}
}

func TestTelemetryGrantWithAnotherPrivilegeIsRefused(t *testing.T) {
	posture := APIPosture("default")
	for _, line := range []string{
		"GRANT SELECT, INSERT ON default.telemetry_signal_bucket TO dho_api_ch",
		"GRANT SELECT ON default.telemetry_signal_bucket TO dho_api_ch",
		"GRANT INSERT, ALTER DELETE ON default.telemetry_signal_bucket TO dho_api_ch",
		"GRANT INSERT ON default.telemetry_signal_bucket TO dho_api_ch WITH GRANT OPTION",
	} {
		err := checkWith(t, posture, append(liveGrants(posture), line))
		if !errors.Is(err, ErrPostureMismatch) {
			t.Errorf("%q: want ErrPostureMismatch, got %v", line, err)
		}
	}
}

func TestOtherTablesStillCountWhenTheTelemetryGrantIsAbsent(t *testing.T) {
	posture := APIPosture("default")
	lines := liveGrants(posture)

	err := checkWith(t, posture, lines[1:])
	if !errors.Is(err, ErrPostureMismatch) || !strings.Contains(err.Error(), "missing") {
		t.Errorf("a dropped required grant must be refused as missing, got %v", err)
	}
	err = checkWith(t, posture, append(append([]string{}, lines...), "GRANT SELECT ON default.other_table TO dho_api_ch"))
	if !errors.Is(err, ErrPostureMismatch) || !strings.Contains(err.Error(), "default.other_table") {
		t.Errorf("a grant outside the manifest must be refused, got %v", err)
	}
}

func TestGrantStatementsDoNotRenderTheTelemetryTable(t *testing.T) {
	for _, projectScope := range []bool{false, true} {
		for _, statement := range GrantStatements("dho_api_ch", APIPostureFor("default", projectScope)) {
			if strings.Contains(statement, "telemetry_signal_bucket") {
				t.Errorf("projectScope=%v renders %q", projectScope, statement)
			}
		}
	}
}
