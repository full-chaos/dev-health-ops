package pgmigrate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

// CHAOS-11634: LoadChain fails closed on a chain that Decide's chain[position+1:] would apply wrongly.

func plantedFiles(files map[string]string) fstest.MapFS {
	out := fstest.MapFS{}
	for name, data := range files {
		out[name] = &fstest.MapFile{Data: []byte(data)}
	}
	return out
}

func loadChainError(t *testing.T) string {
	t.Helper()
	if _, err := LoadChain(); err != nil {
		return err.Error()
	}
	return ""
}

func TestLoadChainRefusesAGap(t *testing.T) {
	UsePlantedChain(t, os.DirFS("testdata/postfreeze/gap"))
	got := loadChainError(t)
	gapAt, want := PlantedRevision(t, 165), PlantedRevision(t, 164)
	if !strings.Contains(got, "gap in the chain") || !strings.Contains(got, gapAt) || !strings.Contains(got, want) {
		t.Errorf("LoadChain error %q does not name the gap before %s (want %s)", got, gapAt, want)
	}
}

func TestLoadChainRefusesADuplicateRevision(t *testing.T) {
	UsePlantedChain(t, os.DirFS("testdata/postfreeze/duplicate"))
	got := loadChainError(t)
	if duplicate := PlantedRevision(t, 163); !strings.Contains(got, "more than one file") || !strings.Contains(got, duplicate) {
		t.Errorf("LoadChain error %q does not name the duplicate revision %s", got, duplicate)
	}
}

func TestLoadChainRefusesADownRevisionThatIsNotThePredecessor(t *testing.T) {
	UsePlantedChain(t, plantedFiles(map[string]string{
		"sql/0163_plant.sql": "-- Revision 0163 (down_revision 0160), planted.\nSELECT 1;\n",
	}))
	got := loadChainError(t)
	wrong, predecessor := PlantedRevision(t, 160), LiveHead(t)
	if !strings.Contains(got, "down_revision") || !strings.Contains(got, wrong) || !strings.Contains(got, predecessor) {
		t.Errorf("LoadChain error %q does not name down_revision %s against the predecessor %s", got, wrong, predecessor)
	}
}

func TestLoadChainRefusesAMissingDownRevisionHeader(t *testing.T) {
	for name, first := range map[string]string{
		"no comment":           "SELECT 1;\n",
		"no down_revision":     "-- Revision 0163, planted.\nSELECT 1;\n",
		"another revision":     "-- Revision 0164 (down_revision 0162), planted.\nSELECT 1;\n",
		"header not on line 1": "\n-- Revision 0163 (down_revision 0162), planted.\nSELECT 1;\n",
	} {
		t.Run(name, func(t *testing.T) {
			UsePlantedChain(t, plantedFiles(map[string]string{"sql/0163_plant.sql": first}))
			planted := PlantedRevision(t, 163) + "_plant.sql"
			if got := loadChainError(t); !strings.Contains(got, planted) || !strings.Contains(got, "header") {
				t.Errorf("LoadChain error %q does not refuse the header of sql/%s", got, planted)
			}
		})
	}
}

func TestLoadChainRefusesAFirstRevisionThatDoesNotContinueTheBaseline(t *testing.T) {
	UseChainFS(t, plantedFiles(map[string]string{
		"sql/0163_first.sql": "-- Revision 0163 (down_revision 0161), planted.\nSELECT 1;\n",
	}))
	got := loadChainError(t)
	if !strings.Contains(got, "down_revision") || !strings.Contains(got, "0161") || !strings.Contains(got, "0162") {
		t.Errorf("LoadChain error %q does not name down_revision 0161 against the baseline head 0162", got)
	}
}

func TestLoadChainRefusesAChainThatDoesNotStartAfterTheBaseline(t *testing.T) {
	UseChainFS(t, plantedFiles(map[string]string{
		"sql/0164_first.sql": "-- Revision 0164 (down_revision 0163), planted.\nSELECT 1;\n",
	}))
	if got := loadChainError(t); !strings.Contains(got, "gap in the chain") || !strings.Contains(got, "0163") {
		t.Errorf("LoadChain error %q does not name the missing 0163 after the baseline 0162", got)
	}
}

func TestLoadChainSortsRevisionsAsIntegers(t *testing.T) {
	files := map[string]string{}
	for n := 163; n <= 10001; n++ {
		files[fmt.Sprintf("sql/%04d_step.sql", n)] = fmt.Sprintf("-- Revision %04d (down_revision %04d), planted.\nSELECT 1;\n", n, n-1)
	}
	UseChainFS(t, plantedFiles(files))
	chain, err := LoadChain()
	if err != nil {
		t.Fatalf("LoadChain refused a chain that runs past 9999: %v", err)
	}
	if last := chain[len(chain)-1].Revision; last != "10001" {
		t.Errorf("the last revision is %s, want 10001", last)
	}
	for i := 1; i < len(chain); i++ {
		a, _ := strconv.Atoi(chain[i-1].Revision)
		b, _ := strconv.Atoi(chain[i].Revision)
		if a >= b {
			t.Fatalf("revision %s sorts before %s", chain[i-1].Revision, chain[i].Revision)
		}
	}
}

func TestRealChainPassesTheIntegrityRules(t *testing.T) {
	if _, err := LoadChain(); err != nil {
		t.Fatalf("the real chain breaks the integrity rules: %v", err)
	}
	archived := archivedChainFiles(t)
	if err := checkChainAfter(archived, "0138"); err != nil {
		t.Fatalf("the archived chain breaks the integrity rules: %v", err)
	}
	if len(archived) == 0 || archived[0].Revision != "0139" || archived[len(archived)-1].Revision != squashHead {
		t.Fatalf("the archived chain does not run 0139..%s", squashHead)
	}
}

// The manifest is the PR-time guard: sql/ may only grow by appending a manifest line, so two PRs that add
// the next revision conflict in git instead of merging below the head (CHAOS-11634).
func manifestProblems(chain []ChainFile, manifest string) []string {
	var problems []string
	listed := map[string]string{}
	last := -1
	for _, line := range strings.Split(strings.TrimSuffix(manifest, "\n"), "\n") {
		if line == "" && manifest == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		n, err := strconv.Atoi(fields[0])
		if len(fields) != 2 || err != nil {
			problems = append(problems, fmt.Sprintf("manifest line %q is not <revision><TAB><sha256>", line))
			continue
		}
		if n <= last {
			problems = append(problems, fmt.Sprintf("manifest revision %s is not after the line above it: append new revisions at the end", fields[0]))
		}
		last = n
		listed[fields[0]] = fields[1]
	}
	inChain := map[string]bool{}
	for _, file := range chain {
		inChain[file.Revision] = true
		sum := sha256.Sum256([]byte(file.SQL))
		want, ok := listed[file.Revision]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("sql/%s is not in the manifest: append %s<TAB>%s", file.Name, file.Revision, hex.EncodeToString(sum[:])))
		case want != hex.EncodeToString(sum[:]):
			problems = append(problems, fmt.Sprintf("sql/%s changed after its manifest line was written", file.Name))
		}
	}
	for revision := range listed {
		if !inChain[revision] {
			problems = append(problems, "the manifest lists "+revision+", which has no file in sql/")
		}
	}
	return problems
}

func TestTheChainMatchesTheReleasedManifest(t *testing.T) {
	data, err := os.ReadFile("chain_manifest.tsv")
	if err != nil {
		t.Fatalf("read the manifest: %v", err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if problems := manifestProblems(chain, string(data)); len(problems) > 0 {
		t.Errorf("the chain and chain_manifest.tsv disagree:\n%s", strings.Join(problems, "\n"))
	}
	archive, err := os.ReadFile("archive/chain_manifest.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var twoColumns strings.Builder
	archived := archivedChainFiles(t)
	for index, line := range strings.Split(strings.TrimSuffix(string(archive), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 3 || index >= len(archived) || fields[2] != archived[index].Name {
			t.Fatalf("archive manifest line %q does not name archive/sql/%s", line, archived[min(index, len(archived)-1)].Name)
		}
		twoColumns.WriteString(fields[0] + "\t" + fields[1] + "\n")
	}
	if problems := manifestProblems(archived, twoColumns.String()); len(problems) > 0 {
		t.Errorf("archive/sql and archive/chain_manifest.tsv disagree:\n%s", strings.Join(problems, "\n"))
	}
}

func TestTheManifestCheckRefusesADriftedManifest(t *testing.T) {
	sum := func(sql string) string { s := sha256.Sum256([]byte(sql)); return hex.EncodeToString(s[:]) }
	chain := []ChainFile{{Revision: "0139", Name: "0139_a.sql", SQL: "a"}, {Revision: "0140", Name: "0140_b.sql", SQL: "b"}}
	good := "0139\t" + sum("a") + "\n0140\t" + sum("b") + "\n"
	if problems := manifestProblems(chain, good); len(problems) > 0 {
		t.Fatalf("a matching manifest is refused: %v", problems)
	}
	for name, test := range map[string]struct{ manifest, want string }{
		"a file with no line":   {"0139\t" + sum("a") + "\n", "is not in the manifest"},
		"an edited file":        {"0139\t" + sum("a") + "\n0140\t" + sum("x") + "\n", "changed after"},
		"a line above the last": {"0140\t" + sum("b") + "\n0139\t" + sum("a") + "\n", "is not after the line above"},
		"a line with no file":   {good + "0141\t" + sum("c") + "\n", "has no file"},
		"a malformed line":      {good + "junk\n", "is not <revision>"},
	} {
		t.Run(name, func(t *testing.T) {
			if problems := manifestProblems(chain, test.manifest); !strings.Contains(strings.Join(problems, "\n"), test.want) {
				t.Errorf("problems %q do not name %q", problems, test.want)
			}
		})
	}
}
