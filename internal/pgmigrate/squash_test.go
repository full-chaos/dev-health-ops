package pgmigrate

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// squashHead is the chain head D6430 moved the baseline to (CHAOS-11743): the baseline is the schema at this
// revision, and the files 0139..squashHead are history in archive/.
const squashHead = "0162"

func TestBaselineHeadsAreTheChainHead(t *testing.T) {
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"0066", squashHead}; !reflect.DeepEqual(baseline.Heads, want) {
		t.Errorf("baseline heads %v, want %v", baseline.Heads, want)
	}
	if !strings.Contains(baseline.Schema, "CREATE TABLE public.org_activity (") {
		t.Error("the baseline schema has no org_activity table (revision 0162)")
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range chain {
		if !revisionLess(squashHead, file.Revision) {
			t.Errorf("sql/%s is at or below the baseline %s: it belongs in archive/sql", file.Name, squashHead)
		}
	}
}

// Every revision between the old baseline and the squashed one is known to the build, so a database that records one
// is refused as below the baseline, never as ahead of the build.
func TestArchivedRevisionsAreKnown(t *testing.T) {
	history, err := LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	known := KnownRevisions(history, baseline, nil)
	for revision := 138; revision <= 162; revision++ {
		if id := fmt.Sprintf("%04d", revision); !known[id] {
			t.Errorf("revision %s is not known to the build", id)
		}
	}
}

func TestDownChainIsEmptyAfterTheSquash(t *testing.T) {
	down, err := LoadDownChain()
	if err != nil {
		t.Fatalf("LoadDownChain: %v", err)
	}
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range down {
		if !revisionLess(applicationHead(baseline), file.Revision) {
			t.Errorf("sql/down/%s reverts a revision at or below the baseline", file.Name)
		}
	}
}

func TestDowngradeBelowTheSquashFloorIsRefused(t *testing.T) {
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	down, err := LoadDownChain()
	if err != nil {
		t.Fatal(err)
	}
	r := NewDownRange(baseline, chain, down)
	refusal := r.CheckTarget(DowngradeTarget{Revision: "0161"})
	if refusal == nil || refusal.Code != "below_baseline_floor" || !refusal.PreDatabase {
		t.Errorf("downgrade to 0161 = %+v, want a below_baseline_floor refusal before any database", refusal)
	}
	if refusal := r.CheckTarget(DowngradeTarget{Revision: squashHead}); refusal != nil {
		t.Errorf("downgrade to the baseline %s (nothing to revert) is refused: %+v", squashHead, refusal)
	}
}

func TestDowngradeTextNamesTheBuildsRange(t *testing.T) {
	_, refusal := ParseDowngradeTarget("base", map[string]bool{})
	if refusal == nil {
		t.Fatal("base is accepted as a downgrade target")
	}
	if strings.Contains(refusal.Detail, "0138-0145") || !strings.Contains(refusal.Detail, squashHead) {
		t.Errorf("the unsupported-target text names %q, want the build's range from %s", refusal.Detail, squashHead)
	}
}

var embedDirective = regexp.MustCompile(`^//go:embed (.+)$`)

// The archive is history: nothing the binary embeds may be archived SQL, and the archive manifest names every archived
// file with its released sha256.
func TestNoEmbeddedSQLUnderArchive(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, err := os.Open(source)
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			match := embedDirective.FindStringSubmatch(strings.TrimSpace(scanner.Text()))
			if match == nil {
				continue
			}
			for _, pattern := range strings.Fields(match[1]) {
				if strings.HasPrefix(pattern, "archive") && pattern != "archive/chain_manifest.tsv" {
					t.Errorf("%s embeds %s: the binary may embed only archive/chain_manifest.tsv", source, pattern)
				}
			}
		}
		_ = file.Close()
	}
	data, err := os.ReadFile("archive/chain_manifest.tsv")
	if err != nil {
		t.Fatalf("read the archive manifest: %v", err)
	}
	listed := 0
	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			t.Fatalf("archive manifest line %q is not <revision><TAB><sha256><TAB><file name>", line)
		}
		for _, dir := range []string{"archive/sql/", "archive/sql/down/"} {
			body, err := os.ReadFile(dir + fields[2])
			if err != nil {
				t.Errorf("%s%s: %v", dir, fields[2], err)
				continue
			}
			if dir == "archive/sql/" {
				sum := sha256.Sum256(body)
				if hex.EncodeToString(sum[:]) != fields[1] {
					t.Errorf("archive/sql/%s changed after its release", fields[2])
				}
			}
		}
		listed++
	}
	if listed != 24 {
		t.Errorf("the archive manifest lists %d revisions, want 24 (0139..%s)", listed, squashHead)
	}
}
