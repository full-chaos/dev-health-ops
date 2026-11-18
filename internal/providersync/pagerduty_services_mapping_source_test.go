package providersync

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const legacyHeuristicMappingSource = "bounded_service_repository_heuristic"

func TestPagerDutyServiceMappingRefusesLegacyHeuristicSource(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "services")
	now := time.Date(2026, 8, 9, 19, 0, 0, 0, time.UTC)
	service, err := normalizePagerDutyService(claim, "acme", pagerDutyServicePayload{ID: "PS1", Name: "Payments"}, now)
	if err != nil {
		t.Fatal(err)
	}
	row, err := pagerDutyServiceMappingFromReference(
		service, pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"},
		pagerDutyMappingMetadata, now, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	row.SourceEntityType = legacyHeuristicMappingSource
	row.RelationshipProvenance = pagerDutyStringPtr(legacyHeuristicMappingSource)
	if err := fillPagerDutyServiceMappingOrdering(&row); err != nil {
		t.Fatal(err)
	}
	if err := validatePagerDutyServiceMappingRows(claim, []pagerDutyServiceRepositoryMappingRow{row}); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("legacy heuristic source must be refused, got %v", err)
	}
}

func TestPagerDutyServiceMappingKindsAreExact(t *testing.T) {
	for _, source := range []pagerDutyServiceMappingSource{pagerDutyMappingAdmin, pagerDutyMappingMetadata, pagerDutyMappingCompass} {
		if got, want := source.mappingKind(), string(source)+"_exact"; got != want {
			t.Fatalf("%s kind=%q want %q", source, got, want)
		}
	}
}

// pagerDutyReaderOnlyMappingSources lists declared sources with no producer.
// Legacy rows of these sources may still be stored and read.
var pagerDutyReaderOnlyMappingSources = map[string]string{}

func TestPagerDutyMappingSourceConstsHaveProducers(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "pagerduty_services_route.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		if id, ok := spec.Type.(*ast.Ident); ok && id.Name == "pagerDutyServiceMappingSource" {
			for _, name := range spec.Names {
				declared[name.Name] = false
			}
		}
		return true
	})
	if len(declared) == 0 {
		t.Fatal("no mapping source consts found")
	}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "pagerDutyServiceMappingFromReference" {
			return true
		}
		for _, arg := range call.Args {
			if id, ok := arg.(*ast.Ident); ok {
				if _, isConst := declared[id.Name]; isConst {
					declared[id.Name] = true
				}
			}
		}
		return true
	})
	for name, produced := range declared {
		if _, readerOnly := pagerDutyReaderOnlyMappingSources[name]; !produced && !readerOnly {
			t.Errorf("mapping source %s has no producer and is not on the reader-only list", name)
		}
	}
}
