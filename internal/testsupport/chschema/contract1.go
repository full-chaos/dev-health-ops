package chschema

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// contract1Head is what ordering contract 1 changes in the head: migration
// 067 is not applied and the operational tables keep their legacy shape. It
// was recorded from the real Python chain run with
// OPERATIONAL_ORDERING_CONTRACT=1 and holds exactly the objects that differ
// from the same chain run under contract 2. It is test data, not a second
// authoring site: TestContract1HeadDiffersOnlyInTheOperationalTables pins its
// relation to chmigrate's head.
//
//go:embed testdata/contract1_head.json
var contract1Head []byte

type contract1Overlay struct {
	Contract        int                `json:"operational_ordering_contract"`
	OmittedVersions []string           `json:"omitted_versions"`
	Objects         []chmigrate.Object `json:"objects"`
}

// LoadContract1Baseline returns chmigrate's head with the contract-1 overlay
// applied: the legacy operational tables, and no record of migration 067.
func LoadContract1Baseline() (chmigrate.Baseline, error) {
	var overlay contract1Overlay
	if err := json.Unmarshal(contract1Head, &overlay); err != nil {
		return chmigrate.Baseline{}, fmt.Errorf("decode the contract-1 overlay: %w", err)
	}
	if overlay.Contract != 1 || len(overlay.Objects) == 0 || len(overlay.OmittedVersions) == 0 {
		return chmigrate.Baseline{}, fmt.Errorf("the contract-1 overlay is malformed (contract %d, %d objects, %d omitted versions)",
			overlay.Contract, len(overlay.Objects), len(overlay.OmittedVersions))
	}
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		return chmigrate.Baseline{}, err
	}
	omitted := map[string]bool{}
	for _, version := range overlay.OmittedVersions {
		omitted[version] = true
	}
	baseline := chmigrate.Baseline{Contract: overlay.Contract, Rows: head.Rows}
	for _, version := range head.Versions {
		if !omitted[version] {
			baseline.Versions = append(baseline.Versions, version)
		}
	}
	if len(baseline.Versions) != len(head.Versions)-len(omitted) {
		return chmigrate.Baseline{}, fmt.Errorf("the head does not record every omitted version %v", overlay.OmittedVersions)
	}
	replaced := map[string]chmigrate.Object{}
	for _, object := range overlay.Objects {
		replaced[object.Name] = object
	}
	for _, object := range head.Objects {
		if legacy, ok := replaced[object.Name]; ok {
			object = legacy
			delete(replaced, object.Name)
		}
		baseline.Objects = append(baseline.Objects, object)
	}
	if len(replaced) != 0 {
		return chmigrate.Baseline{}, fmt.Errorf("the contract-1 overlay names objects the head does not hold: %v", replaced)
	}
	return baseline, nil
}
