package licensing

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonTierLimitsProgram runs each case through the api's own
// TierLimitService over the real models on an in-memory SQLite database:
// the organization, an optional org_licenses row whose limits_override is
// stored as the case's raw JSON text, and the case's tier_limits rows. It
// reports get_limit's value as repr and check_limit for each current value,
// or the exception class either raised.
const pythonTierLimitsProgram = `
import json, sys, uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from dev_health_ops.models.licensing import OrgLicense, TierLimit
from dev_health_ops.models.users import Organization
from dev_health_ops.api.services.licensing import TierLimitService
out = []
for case in json.loads(sys.stdin.read()):
    engine = create_engine("sqlite://")
    for model in (Organization, OrgLicense, TierLimit):
        model.__table__.create(engine)
    org = uuid.UUID("c6a38355-0000-4000-8000-000000000001")
    with engine.begin() as conn:
        if case["org_tier"] is not None:
            conn.exec_driver_sql("INSERT INTO organizations (id, slug, name, settings, tier, managed_by, is_active, created_at, updated_at) VALUES (?, 's', 'n', '{}', ?, 'x', 1, '2026-01-01', '2026-01-01')", (org.hex, case["org_tier"]))
        if case["license"] is not None:
            conn.exec_driver_sql("INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, limits_override, created_at, updated_at) VALUES (?, ?, ?, 1, 'x', 'x', ?, '2026-01-01', '2026-01-01')", (uuid.uuid4().hex, org.hex, case["license"]["tier"], case["license"]["override"]))
        for key, value in case["rows"] or []:
            conn.exec_driver_sql("INSERT INTO tier_limits (id, tier, limit_key, limit_value, created_at, updated_at) VALUES (?, ?, ?, ?, '2026-01-01', '2026-01-01')", (uuid.uuid4().hex, case["row_tier"], key, value))
    result = {}
    with Session(engine) as session:
        service = TierLimitService(session)
        try:
            result["limit"] = repr(service.get_limit(org, case["key"]))
        except Exception as exc:
            result["limit"] = "raise " + type(exc).__name__
        checks = []
        for current in case["currents"] or []:
            try:
                allowed, reason = service.check_limit(org, case["key"], current)
                checks.append([allowed, reason])
            except Exception as exc:
                checks.append("raise " + type(exc).__name__)
        result["checks"] = checks
        backfills = []
        for requested in case.get("requested") or []:
            try:
                allowed, reason = service.check_backfill_limit(org, int(requested))
                backfills.append([allowed, reason])
            except Exception as exc:
                backfills.append("raise " + type(exc).__name__)
        result["backfills"] = backfills
    out.append(result)
print(json.dumps(out))
`

type tierLimitCase struct {
	OrgTier  *string           `json:"org_tier"`
	License  *tierLimitLicense `json:"license"`
	RowTier  string            `json:"row_tier"`
	Rows     [][2]*string      `json:"rows"`
	Key      string            `json:"key"`
	Currents []int64           `json:"currents"`
	// Requested are check_backfill_limit's requested_days, as decimal text
	// so an int past 64 bits reaches both planes exactly.
	Requested []string `json:"requested,omitempty"`
}

type tierLimitLicense struct {
	Tier     string  `json:"tier"`
	Override *string `json:"override"`
}

func text(value string) *string { return &value }

// tierLimitCases crosses the stored shapes get_limit reads: license and
// organization tiers (valid, other case, unknown, missing), overrides of
// every JSON shape and number form, and tier_limits text float() reads,
// refuses or overflows on, for the key asked and for another key.
func tierLimitCases() []tierLimitCase {
	currents := []int64{0, 1, 2, 3, 4, 9, 10, 11, 1000}
	var cases []tierLimitCase
	for _, orgTier := range []*string{nil, text("community"), text("team"), text("enterprise"), text("Team"), text("gold")} {
		cases = append(cases, tierLimitCase{OrgTier: orgTier, RowTier: "team", Key: "max_repos", Currents: currents})
	}
	overrides := []*string{nil, text(`{}`), text(`[]`), text(`null`), text(`0`), text(`"x"`), text(`{"max_users": 1}`),
		text(`{"max_repos": null}`), text(`{"max_repos": true}`), text(`{"max_repos": false}`), text(`{"max_repos": 2.5}`),
		text(`{"max_repos": 5}`), text(`{"max_repos": -1}`), text(`{"max_repos": "7"}`), text(`{"max_repos": [1]}`),
		text(`{"max_repos": 1e400}`), text(`{"max_repos": -1e400}`), text(`{"max_repos": NaN}`), text(`{"max_repos": Infinity}`),
		text(`{"max_repos": 123456789012345678901234567890}`), text(`{"max_repos": 3.0}`), text(`{"max_repos": 1e2}`),
		text(`{"max_repos": 1, "max_repos": 9}`), text(`{"max_repos": 0}`), text(`{"max_repos": 1e-7}`)}
	for _, tier := range []string{"community", "team", "enterprise", "TEAM", "bogus"} {
		for _, override := range overrides {
			cases = append(cases, tierLimitCase{OrgTier: text("team"), License: &tierLimitLicense{Tier: tier, Override: override},
				RowTier: tier, Key: "max_repos", Currents: currents})
		}
	}
	values := []*string{nil, text("5"), text("5.0"), text(" 5 "), text("1_0"), text("2.5"), text("-3"), text("-0"), text("abc"), text(""),
		text("nan"), text("inf"), text("-Infinity"), text("1e300"), text("1e-300"), text("١٢"), text("0x10"), text("1e3"), text("+7")}
	for _, value := range values {
		cases = append(cases,
			tierLimitCase{OrgTier: text("team"), RowTier: "team", Rows: [][2]*string{{text("max_repos"), value}}, Key: "max_repos", Currents: currents},
			tierLimitCase{OrgTier: text("team"), RowTier: "team", Rows: [][2]*string{{text("max_users"), value}}, Key: "max_repos", Currents: currents},
			tierLimitCase{OrgTier: text("team"), RowTier: "community", Rows: [][2]*string{{text("max_repos"), value}}, Key: "max_repos", Currents: currents},
		)
	}
	// check_backfill_limit: backfill_days from the tier defaults, from
	// every override number shape, and from tier_limits text.
	requested := []string{"-1", "0", "1", "29", "30", "31", "90", "91", "365", "366", "3650", "123456789012345678901234567890"}
	for _, tier := range []*string{nil, text("community"), text("team"), text("enterprise"), text("gold")} {
		cases = append(cases, tierLimitCase{OrgTier: tier, RowTier: "team", Key: "backfill_days", Requested: requested})
	}
	for _, override := range []*string{text(`{"backfill_days": null}`), text(`{"backfill_days": true}`), text(`{"backfill_days": false}`),
		text(`{"backfill_days": 30}`), text(`{"backfill_days": 30.5}`), text(`{"backfill_days": 1e400}`), text(`{"backfill_days": NaN}`),
		text(`{"backfill_days": 123456789012345678901234567890}`), text(`{"backfill_days": "30"}`), text(`{"backfill_days": 0}`)} {
		cases = append(cases, tierLimitCase{OrgTier: text("team"), License: &tierLimitLicense{Tier: "team", Override: override},
			RowTier: "team", Key: "backfill_days", Requested: requested})
	}
	for _, value := range values {
		cases = append(cases, tierLimitCase{OrgTier: text("team"), RowTier: "team", Rows: [][2]*string{{text("backfill_days"), value}},
			Key: "backfill_days", Requested: requested})
	}
	// A row for a key the defaults do not carry, and asking for it.
	cases = append(cases,
		tierLimitCase{OrgTier: text("enterprise"), RowTier: "enterprise", Rows: [][2]*string{{text("custom"), text("4")}}, Key: "custom", Currents: currents},
		tierLimitCase{OrgTier: text("enterprise"), RowTier: "enterprise", Key: "custom", Currents: currents},
		tierLimitCase{OrgTier: text("enterprise"), RowTier: "enterprise", Key: "min_sync_interval_hours", Currents: currents},
		tierLimitCase{License: &tierLimitLicense{Tier: "team", Override: text(`{"max_repos": 4}`)}, RowTier: "team", Key: "max_repos", Currents: currents},
	)
	return cases
}

// TestTierLimitsVenueOracleMatchesLivePython requires GetLimitFrom and
// CheckLimitFrom to answer as the api's TierLimitService.get_limit and
// check_limit do for the same stored rows: the same limit (int, float,
// bool or None, as repr), the same allowed flag and refusal text for each
// current value, and a failure exactly where Python raises.
func TestTierLimitsVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the tier-limit oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	cases := tierLimitCases()
	input, _ := json.Marshal(cases)
	command := exec.Command(python, "-c", pythonTierLimitsProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct {
		Limit     string            `json:"limit"`
		Checks    []json.RawMessage `json:"checks"`
		Backfills []json.RawMessage `json:"backfills"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(cases) {
		t.Fatalf("decode: %v (%d of %d)\n%s", err, len(want), len(cases), output)
	}
	raised := 0
	for index, c := range cases {
		inputs := caseInputs(c)
		limit, err := GetLimitFrom(inputs, c.Key)
		got := pyjson.Repr(limit)
		if err != nil {
			got = "raise " + map[bool]string{true: "OverflowError", false: err.Error()}[err == ErrTierLimitOverflow]
			raised++
		}
		if got != want[index].Limit {
			t.Errorf("case %d %+v: get_limit go %q, python %q", index, describe(c), got, want[index].Limit)
		}
		for position, current := range c.Currents {
			allowed, reason, err := CheckLimitFrom(inputs, c.Key, current)
			var goCheck string
			if err != nil {
				goCheck = `"raise ` + map[bool]string{true: "OverflowError", false: err.Error()}[err == ErrTierLimitOverflow] + `"`
			} else {
				encoded, _ := json.Marshal([]any{allowed, map[bool]any{true: nil, false: reason}[reason == "" && allowed]})
				goCheck = string(encoded)
			}
			var pythonCheck any
			_ = json.Unmarshal(want[index].Checks[position], &pythonCheck)
			compact, _ := json.Marshal(pythonCheck)
			if goCheck != string(compact) {
				t.Errorf("case %d %+v current %d: check_limit go %s, python %s", index, describe(c), current, goCheck, want[index].Checks[position])
			}
		}
	}
	if raised == 0 {
		t.Error("no case reached the OverflowError path")
	}
	backfillRefusals := 0
	for index, c := range cases {
		inputs := caseInputs(c)
		for position, text := range c.Requested {
			requested, ok := new(big.Int).SetString(text, 10)
			if !ok {
				t.Fatalf("requested %q", text)
			}
			allowed, reason, err := CheckBackfillLimitFrom(inputs, requested)
			var goCheck string
			if err != nil {
				goCheck = `"raise ` + map[bool]string{true: "OverflowError", false: err.Error()}[err == ErrTierLimitOverflow] + `"`
			} else {
				encoded, _ := json.Marshal([]any{allowed, map[bool]any{true: nil, false: reason}[reason == "" && allowed]})
				goCheck = string(encoded)
				if !allowed {
					backfillRefusals++
				}
			}
			var pythonCheck any
			_ = json.Unmarshal(want[index].Backfills[position], &pythonCheck)
			compact, _ := json.Marshal(pythonCheck)
			if goCheck != string(compact) {
				t.Errorf("case %d %+v requested %s: check_backfill_limit go %s, python %s", index, describe(c), text, goCheck, want[index].Backfills[position])
			}
		}
	}
	if backfillRefusals == 0 {
		t.Error("no case reached a backfill refusal")
	}
	t.Logf("%d cases compared, %d raising", len(cases), raised)
	venueoracle.WriteProof(t)
}

// caseInputs is a case's stored rows as GetLimitFrom reads them.
func caseInputs(c tierLimitCase) TierLimitInputs {
	inputs := TierLimitInputs{OrgTier: c.OrgTier, TierRows: func(tier string) ([]TierLimitRow, error) {
		if tier != c.RowTier {
			return nil, nil
		}
		var rows []TierLimitRow
		for _, row := range c.Rows {
			rows = append(rows, TierLimitRow{Key: *row[0], Value: row[1]})
		}
		return rows, nil
	}}
	if c.License != nil {
		inputs.License = &LicenseRow{Tier: text(c.License.Tier), LimitsOverride: c.License.Override}
		inputs.OrgTier = nil
	}
	return inputs
}

func describe(c tierLimitCase) string {
	encoded, _ := json.Marshal(c)
	return fmt.Sprint(string(encoded))
}
