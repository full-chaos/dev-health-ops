package routing

// `carry` and a registered document swapped with dual accept (CHAOS-8000).
//
// The oracle below reads the two artifacts the tools image carries, built
// the way docker/go-api-tools.Dockerfile builds them -- the registered-
// document dump from `registrydump` over the real query_route.go, and the
// checked-in edge catalog -- through the verb's own loaders, and asks the
// verb's own decision what happens to a routing row keyed to each legacy
// digest the image accepts. Nothing here is a hand-written registry: a
// legacy digest that the image, its dump and its catalog do not agree on
// fails this test whoever wrote it.

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// wave1SwappedDocuments are the three operations whose registered document
// was swapped with dual accept, and the digest of the text each one
// registered BEFORE the swap -- the digest the routing rows of a
// deployment enabled before the swap are keyed to.
//
// Pinned by value on purpose. When a cleanup retires one of these legacy
// texts, this test fails by name, and that is the moment to check the
// routing rows: a served row still keyed to a retired digest is dead at
// the image that retires it. Re-key the rows first, then remove the pin.
var wave1SwappedDocuments = map[string]string{
	"capacityForecast": "b4fb8f075aba9954714f10f7f4451242d969548d6392d778dae1177759f80780",
	"aiAttributedPrs":  "6ed54ebe5a535e28c16c1e903f64eda9fbbc32577465ccebe7fa30e2075f2f3e",
	"aiImpactSummary":  "7e84d73b35104dd2ec3ece19c163cd8247c4637aa93df3b5f1449394c4539328",
}

// realImageArtifacts returns the paths of the registered-document dump and
// of the edge catalog of THIS checkout, produced as the tools image
// produces them.
func realImageArtifacts(t *testing.T) (documentsPath, catalogPath string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dump := exec.Command("go", "run", "./cmd/registrydump", "-file", "internal/queryapi/server/query_route.go")
	dump.Dir = root
	var dumpErr bytes.Buffer
	dump.Stderr = &dumpErr
	documents, err := dump.Output()
	if err != nil {
		t.Fatalf("running registrydump over the real query_route.go: %v\n%s", err, dumpErr.String())
	}
	documentsPath = filepath.Join(t.TempDir(), goapiproof.DefaultDocumentsPath)
	if err := os.WriteFile(documentsPath, documents, 0o600); err != nil {
		t.Fatal(err)
	}
	return documentsPath, filepath.Join(root, goapiproof.DefaultCatalogPath)
}

func digestListed(digests []string, digest string) bool {
	for _, listed := range digests {
		if listed == digest {
			return true
		}
	}
	return false
}

func TestCarryDecisionOverTheRealImagesLegacyDocuments(t *testing.T) {
	documentsPath, catalogPath := realImageArtifacts(t)
	current, legacy, err := targetDocumentDigests(documentsPath)
	if err != nil {
		t.Fatalf("reading the real registered-document dump: %v", err)
	}
	catalog, catalogLegacy, err := goapiproof.LoadOperationCatalogWithLegacy(catalogPath)
	if err != nil {
		t.Fatalf("reading the real edge catalog: %v", err)
	}

	// The three real cases exist in the real image. A measurement over an
	// empty legacy list would pass and prove nothing.
	for operation, digest := range wave1SwappedDocuments {
		if !digestListed(legacy[operation], digest) {
			t.Fatalf("the image does not accept %s as a legacy text of %s any more (it lists %v).\n"+
				"  A served routing row still keyed to that digest is dead at this image: re-key the rows to %s first, then remove this pin.",
				digest, operation, legacy[operation], current[operation])
		}
	}

	// inputs is what `carry` gathers: the image's own two artifacts, and
	// the deployed process's /registry, which lists every operation under
	// its current digest. live overrides one operation's entry.
	inputs := func(operation, live string) goapiproof.CarryInputs {
		registry := make(map[string]string, len(current))
		for name, digest := range current {
			registry[name] = digest
		}
		registry[operation] = live
		return goapiproof.CarryInputs{
			LiveDocumentDigest:           registry,
			TargetDocumentDigest:         current,
			TargetLegacyDocumentDigests:  legacy,
			CatalogDocumentDigest:        catalog,
			CatalogLegacyDocumentDigests: catalogLegacy,
		}
	}
	row := func(operation, digest, mode string) goapiproof.CarryRow {
		return goapiproof.CarryRow{
			Operation: operation, DocumentDigest: digest, Mode: mode,
			Build: "0123456789abcdef0123456789abcdef01234567", Owner: "go", RolloutPercentage: 100,
			ReviewEvidence: "the original decision",
		}
	}
	modes := []string{goapiproof.TargetModeCanary, goapiproof.TargetModePrimary, goapiproof.TargetModeShadow}

	// LEGACY: every legacy digest the image accepts, in every mode a row is
	// carried in, against both deployed processes a roll can meet -- one
	// that still registers the old text, and one that already registers
	// the image's current text.
	measured := 0
	for operation, digests := range legacy {
		for _, digest := range digests {
			for _, mode := range modes {
				for liveName, live := range map[string]string{"old text": digest, "current text": current[operation]} {
					measured++
					got := goapiproof.DecideCarry(row(operation, digest, mode), inputs(operation, live))
					if got.Action != goapiproof.CarryActionCarry {
						t.Fatalf("%s mode=%s, deployed process registers the %s: %s (%s), want CARRY -- the image accepts %s as a legacy text, so the roll must not lose this row",
							operation, mode, liveName, got.Action, got.Reason, digest)
					}
					if got.DocumentDigest != digest || !got.LegacyDigest || got.TargetCurrentDigest != current[operation] {
						t.Fatalf("%s mode=%s: carried under %s (legacy=%t, current=%s), want its own digest %s named as a legacy digest of current %s",
							operation, mode, got.DocumentDigest, got.LegacyDigest, got.TargetCurrentDigest, digest, current[operation])
					}
				}
			}
		}
	}
	if want := len(wave1SwappedDocuments) * len(modes) * 2; measured < want {
		t.Fatalf("measured %d legacy rows, want at least %d", measured, want)
	}

	// CURRENT: a row keyed to the image's current digest is carried as
	// before, and is not named as a legacy-digest row.
	for operation := range wave1SwappedDocuments {
		for _, mode := range modes {
			got := goapiproof.DecideCarry(row(operation, current[operation], mode), inputs(operation, current[operation]))
			if got.Action != goapiproof.CarryActionCarry || got.LegacyDigest {
				t.Fatalf("%s mode=%s keyed to the current digest: %s legacy=%t (%s), want CARRY and not a legacy-digest row",
					operation, mode, got.Action, got.LegacyDigest, got.Reason)
			}
		}
	}

	// NEITHER: a digest the image does not accept for the operation at all
	// is refused with the unchanged text (a served row) or dropped (a
	// shadow row). The digest of ANOTHER operation's legacy text counts as
	// neither.
	neither := map[string]string{
		"a digest no image knows":                 strings.Repeat("0", 64),
		"the legacy digest of another operation":  wave1SwappedDocuments["aiImpactSummary"],
		"the current digest of another operation": current["aiImpactSummary"],
	}
	for name, digest := range neither {
		const operation = "capacityForecast"
		served := goapiproof.DecideCarry(row(operation, digest, goapiproof.TargetModeCanary), inputs(operation, digest))
		if served.Action != goapiproof.CarryActionRefuse || !errors.Is(served.Refusal, goapiproof.ErrCarryDocumentMoved) {
			t.Fatalf("%s: %s (%v: %s), want the refusal classified as a moved document", name, served.Action, served.Refusal, served.Reason)
		}
		want := "the registered document changed: this row is keyed to " + digest + ", the target image registers " + current[operation]
		if served.Reason != want {
			t.Fatalf("%s: reason = %q, want the unchanged text %q", name, served.Reason, want)
		}
		shadow := goapiproof.DecideCarry(row(operation, digest, goapiproof.TargetModeShadow), inputs(operation, digest))
		if shadow.Action != goapiproof.CarryActionSkip || !strings.Contains(shadow.Reason, "mode=shadow: the registered document changed") {
			t.Fatalf("%s: shadow %s (%s), want a SKIP naming the changed document", name, shadow.Action, shadow.Reason)
		}
	}
}

// The plan names a row carried under a legacy digest, on stdout for the
// operator and on the structured line for a log search, and names no
// other row that way.
func TestCarryPlanNamesARowCarriedUnderALegacyDigest(t *testing.T) {
	const (
		oldDigest = "b4fb8f075aba9954714f10f7f4451242d969548d6392d778dae1177759f80780"
		newDigest = "7ec8e919d784ed0dea1b87e85d64ffe18d05557379a397d07da0ab958694ffd1"
		build     = "0123456789abcdef0123456789abcdef01234567"
	)
	outcomes := []goapiproof.CarryOutcome{
		{Operation: "aiAttributedPrs", DocumentDigest: strings.Repeat("a", 64), Action: goapiproof.CarryActionUnchanged, Mode: "canary", RolloutPercentage: 100, Build: build, LegacyDigest: true, TargetCurrentDigest: strings.Repeat("b", 64)},
		{Operation: "aiImpactSummary", DocumentDigest: strings.Repeat("c", 64), Action: goapiproof.CarryActionRefuse, Reason: "the target digest already holds a row for this operation", Mode: "canary", RolloutPercentage: 100, Build: build, LegacyDigest: true, TargetCurrentDigest: strings.Repeat("d", 64)},
		{Operation: "capacityForecast", DocumentDigest: oldDigest, Action: goapiproof.CarryActionCarry, Mode: "canary", RolloutPercentage: 100, Build: build, LegacyDigest: true, TargetCurrentDigest: newDigest},
		{Operation: "hotspots", DocumentDigest: strings.Repeat("e", 64), Action: goapiproof.CarryActionCarry, Mode: "canary", RolloutPercentage: 100, Build: build},
	}
	for _, dryRun := range []bool{true, false} {
		var out, errOut bytes.Buffer
		savedOut, savedErr := stdout, stderr
		stdout, stderr = &out, &errOut
		printCarryPlan(outcomes, "sha256:live", "sha256:target", build, dryRun)
		stdout, stderr = savedOut, savedErr

		lines := strings.Split(out.String(), "\n")
		// noteAfter returns the line printed directly under an operation's
		// row line, or "" when the next line is another row or the end.
		noteAfter := func(operation string) string {
			for index, line := range lines {
				if strings.Contains(line, " "+operation+" ") && strings.Contains(line, "digest=") && index+1 < len(lines) {
					if next := lines[index+1]; strings.HasPrefix(next, "go-api-routing:              ") {
						return strings.TrimPrefix(next, "go-api-routing:              ")
					}
					return ""
				}
			}
			t.Fatalf("dry_run=%t: no row line for %s:\n%s", dryRun, operation, out.String())
			return ""
		}
		if note := noteAfter("capacityForecast"); !strings.HasPrefix(note, "carried (legacy digest): ") || !strings.Contains(note, newDigest) {
			t.Fatalf("dry_run=%t: capacityForecast note = %q, want `carried (legacy digest)` naming the image's current digest %s", dryRun, note, newDigest)
		}
		if note := noteAfter("aiAttributedPrs"); !strings.HasPrefix(note, "already carried (legacy digest): ") {
			t.Fatalf("dry_run=%t: aiAttributedPrs note = %q, want an unchanged row named as a legacy-digest row", dryRun, note)
		}
		if note := noteAfter("hotspots"); note != "" {
			t.Fatalf("dry_run=%t: hotspots is keyed to the current digest and prints %q", dryRun, note)
		}
		// A refused row prints the reason that decided it, and no carry note.
		if note := noteAfter("aiImpactSummary"); !strings.HasPrefix(note, "the target digest already holds a row") {
			t.Fatalf("dry_run=%t: aiImpactSummary note = %q, want its refusal reason", dryRun, note)
		}
		if got := strings.Count(out.String(), "carried (legacy digest)"); got != 2 {
			t.Fatalf("dry_run=%t: %d legacy-digest notes, want 2 (one carried, one already carried):\n%s", dryRun, got, out.String())
		}

		structured := errOut.String()
		if dryRun {
			if structured != "" {
				t.Fatalf("a dry run wrote structured lines:\n%s", structured)
			}
			continue
		}
		for operation, want := range map[string]string{"capacityForecast": "legacy_digest=true", "hotspots": "legacy_digest=false"} {
			found := false
			for _, line := range strings.Split(structured, "\n") {
				if strings.HasPrefix(line, "go_api_routing.carried operation="+operation+" ") {
					found = true
					if !strings.HasSuffix(line, " "+want) {
						t.Fatalf("structured line %q does not end with %s", line, want)
					}
				}
			}
			if !found {
				t.Fatalf("no structured line for %s:\n%s", operation, structured)
			}
		}
	}
}
