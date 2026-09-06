// Package remaining owns the reviewed inventory and resource contracts for
// metrics that follow the daily core migration.
package remaining

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

//go:embed families.json
var rawFamilies []byte

var expectedFamilies = []string{
	"capacity",
	"complexity",
	"dora",
	"membership_backfill",
	"recommendations",
	"release_impact",
	"work_item_attribution",
}

type Inventory struct {
	SchemaVersion int      `json:"schema_version"`
	Owner         string   `json:"owner"`
	Families      []Family `json:"families"`
}

type Family struct {
	Name                  string   `json:"name"`
	PythonSources         []string `json:"python_sources"`
	Writes                []string `json:"writes"`
	MaxConcurrency        int      `json:"max_concurrency"`
	ClickHouseReadBudget  int      `json:"clickhouse_read_budget"`
	ClickHouseWriteBudget int      `json:"clickhouse_write_budget"`
	Replay                string   `json:"replay"`
	RouteKey              string   `json:"route_key"`
	Route                 string   `json:"route"`
	RollbackRoute         string   `json:"rollback_route"`
	HistoricalLimitation  string   `json:"historical_limitation"`
	ParityState           string   `json:"parity_state"`
	// Port mirrors internal/jobs/metrics/daily/families.json's own "port"
	// field ("go"/"pending"): whether cmd/dev-health-worker/daily.go's
	// per-kind switch (daily.go:571-634) passes this family's partitions to
	// a native Go executor ("go") or the Python compatibility bridge
	// ("pending"). PythonSources deliberately stays populated for "go"
	// families too -- same as daily/families.json -- because it records
	// historical/pre-cutover lineage, not current routing; Port is the field
	// that answers "who runs this today." Added 2026-09-04 (CHAOS-5030):
	// before this field existed, every family here appeared Python-routed
	// from python_sources alone, which was stale for 5 of the 7 rows and is
	// exactly the "told done" drift class this contract exists to prevent.
	// Cross-checked against the mechanically Go-AST-derived
	// contracts/native-families/v1/native-families.json by
	// cmd/dev-health-worker/native_families_artifact_test.go -- that
	// artifact, not this hand-set field, is the actual source of truth if
	// the two ever disagree.
	//
	// CHAOS-4291: complexity is the first family with NO surviving Python
	// source at all (job_complexity_db.py deleted whole, unlike
	// release_impact's release_impact.py which kept a live non-production
	// caller) -- python_sources is an empty list for it, ParityState
	// "native_python_orchestrator_deleted" records why. Validate() permits
	// an empty PythonSources; TestInventoryIsExactBoundedAndSourceBacked's
	// existence check is then vacuous for that family (an empty range
	// stats nothing), which is the correct behavior, not a gap.
	Port string `json:"port"`
}

func Load() (Inventory, error) {
	var inventory Inventory
	if err := json.Unmarshal(rawFamilies, &inventory); err != nil {
		return Inventory{}, err
	}
	if err := inventory.Validate(); err != nil {
		return Inventory{}, err
	}
	return inventory, nil
}

func (inventory Inventory) Validate() error {
	if inventory.SchemaVersion != 1 || inventory.Owner != "metrics.remaining" {
		return errors.New("remaining metrics inventory header is invalid")
	}
	names := make([]string, 0, len(inventory.Families))
	routes := make(map[string]struct{}, len(inventory.Families))
	for _, family := range inventory.Families {
		names = append(names, family.Name)
		// An empty PythonSources is valid ONLY when ParityState says so
		// explicitly (CHAOS-4291: complexity is the first family with no
		// surviving Python source at all) -- tying the two together means
		// an accidentally-emptied list for any other family still fails
		// closed, rather than this becoming a blanket "sources optional"
		// relaxation.
		noPythonSourcesLeft := family.ParityState == "native_python_orchestrator_deleted"
		if family.Name == "" || (len(family.PythonSources) == 0 && !noPythonSourcesLeft) || len(family.Writes) == 0 ||
			family.MaxConcurrency < 1 || family.MaxConcurrency > 4 ||
			family.ClickHouseReadBudget < 1 || family.ClickHouseReadBudget > 2 ||
			family.ClickHouseWriteBudget < 1 || family.ClickHouseWriteBudget > 2 ||
			!validRoutePair(family.Route, family.RollbackRoute) ||
			!strings.HasPrefix(family.RouteKey, "metrics.remaining.") ||
			family.Replay == "" || family.HistoricalLimitation == "" || family.ParityState == "" ||
			(family.Port != "go" && family.Port != "pending") {
			return fmt.Errorf("remaining metrics family %q is incomplete", family.Name)
		}
		if _, duplicate := routes[family.RouteKey]; duplicate {
			return fmt.Errorf("remaining metrics route %q is duplicated", family.RouteKey)
		}
		routes[family.RouteKey] = struct{}{}
	}
	slices.Sort(names)
	if !slices.Equal(names, expectedFamilies) {
		return fmt.Errorf("remaining metrics family set drift: %v", names)
	}
	return nil
}

func (family Family) Executable() bool {
	return family.Route == "shadow" || family.Route == "river_canary" || family.Route == "river"
}

func validRoutePair(route, rollback string) bool {
	switch route {
	case "celery":
		return rollback == "celery"
	case "shadow", "river_canary":
		return rollback == "celery"
	case "river":
		return rollback == "celery" || rollback == "none"
	default:
		return false
	}
}
