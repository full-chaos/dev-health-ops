package providersync

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
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
		pagerDutyMappingMetadata, now,
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

func TestPagerDutyServiceMappingRefusesUndeclaredSource(t *testing.T) {
	claim := nativeTestClaim("pagerduty", "services")
	now := time.Date(2026, 8, 9, 19, 0, 0, 0, time.UTC)
	service, err := normalizePagerDutyService(claim, "acme", pagerDutyServicePayload{ID: "PS1", Name: "Payments"}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []pagerDutyServiceMappingSource{legacyHeuristicMappingSource, ""} {
		_, err := pagerDutyServiceMappingFromReference(
			service, pagerDutyServiceRepositoryReference{Provider: "github", FullName: "full-chaos/payments"},
			source, now,
		)
		if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
			t.Fatalf("source %q must be refused, got %v", source, err)
		}
	}
}

func TestPagerDutyMappingSourcesHaveExplicitRules(t *testing.T) {
	want := map[pagerDutyServiceMappingSource]pagerDutyMappingSourceRule{
		pagerDutyMappingAdmin:    {confidence: 1, ruleID: "service_repository_mapping.admin.v1"},
		pagerDutyMappingMetadata: {confidence: 0.95, ruleID: "pagerduty.service_metadata.repository_url_or_key.v1"},
		pagerDutyMappingCompass:  {confidence: 0.9, ruleID: "compass.service_repository_relationship.v1"},
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "pagerduty_services_route.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	declared := 0
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		if id, ok := spec.Type.(*ast.Ident); ok && id.Name == "pagerDutyServiceMappingSource" {
			for _, value := range spec.Values {
				lit, ok := value.(*ast.BasicLit)
				if !ok {
					t.Fatalf("mapping source const value is not a literal: %T", value)
				}
				unquoted, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				source := pagerDutyServiceMappingSource(unquoted)
				declared++
				if _, known := pagerDutyMappingSourceRules[source]; !known {
					t.Errorf("declared source %s has no explicit rule", source)
				}
				if _, pinned := want[source]; !pinned {
					t.Errorf("declared source %s has no pinned rule in this test", source)
				}
			}
		}
		return true
	})
	if declared == 0 {
		t.Fatal("no mapping source consts found")
	}
	if len(pagerDutyMappingSourceRules) != declared {
		t.Fatalf("rules=%d declared sources=%d", len(pagerDutyMappingSourceRules), declared)
	}
	for source, rule := range want {
		if got := pagerDutyMappingSourceRules[source]; got != rule {
			t.Errorf("%s rule=%+v want %+v", source, got, rule)
		}
		if !pagerDutyMappingSourceValid(string(source)) {
			t.Errorf("%s must be a valid stored source", source)
		}
	}
}
