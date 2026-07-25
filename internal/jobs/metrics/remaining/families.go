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
	"extra_metrics",
	"membership_backfill",
	"recommendations",
	"release_impact",
	"team_metrics",
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
	Profile               string   `json:"profile"`
	MaxConcurrency        int      `json:"max_concurrency"`
	ClickHouseReadBudget  int      `json:"clickhouse_read_budget"`
	ClickHouseWriteBudget int      `json:"clickhouse_write_budget"`
	Replay                string   `json:"replay"`
	RouteKey              string   `json:"route_key"`
	Route                 string   `json:"route"`
	RollbackRoute         string   `json:"rollback_route"`
	HistoricalLimitation  string   `json:"historical_limitation"`
	ParityState           string   `json:"parity_state"`
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
		if family.Name == "" || len(family.PythonSources) == 0 || len(family.Writes) == 0 ||
			family.Profile != "heavy" || family.MaxConcurrency < 1 || family.MaxConcurrency > 4 ||
			family.ClickHouseReadBudget < 1 || family.ClickHouseReadBudget > 2 ||
			family.ClickHouseWriteBudget < 1 || family.ClickHouseWriteBudget > 2 ||
			!validRoutePair(family.Route, family.RollbackRoute) ||
			!strings.HasPrefix(family.RouteKey, "metrics.remaining.") ||
			family.Replay == "" || family.HistoricalLimitation == "" || family.ParityState == "" {
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
