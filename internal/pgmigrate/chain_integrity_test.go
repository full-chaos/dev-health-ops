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
	if !strings.Contains(got, "gap") || !strings.Contains(got, "0155") || !strings.Contains(got, "0154") {
		t.Errorf("LoadChain error %q does not name the gap before 0155 (want 0154)", got)
	}
}

func TestLoadChainRefusesADuplicateRevision(t *testing.T) {
	UsePlantedChain(t, os.DirFS("testdata/postfreeze/duplicate"))
	got := loadChainError(t)
	if !strings.Contains(got, "more than one file") || !strings.Contains(got, "0153") {
		t.Errorf("LoadChain error %q does not name the duplicate revision 0153", got)
	}
}

func TestLoadChainRefusesADownRevisionThatIsNotThePredecessor(t *testing.T) {
	UsePlantedChain(t, plantedFiles(map[string]string{
		"sql/0153_plant.sql": "-- Revision 0153 (down_revision 0150), planted.\nSELECT 1;\n",
	}))
	got := loadChainError(t)
	if !strings.Contains(got, "down_revision") || !strings.Contains(got, "0150") || !strings.Contains(got, "0152") {
		t.Errorf("LoadChain error %q does not name down_revision 0150 against the predecessor 0152", got)
	}
}

func TestLoadChainRefusesAMissingDownRevisionHeader(t *testing.T) {
	for name, first := range map[string]string{
		"no comment":           "SELECT 1;\n",
		"no down_revision":     "-- Revision 0153, planted.\nSELECT 1;\n",
		"another revision":     "-- Revision 0154 (down_revision 0152), planted.\nSELECT 1;\n",
		"header not on line 1": "\n-- Revision 0153 (down_revision 0152), planted.\nSELECT 1;\n",
	} {
		t.Run(name, func(t *testing.T) {
			UsePlantedChain(t, plantedFiles(map[string]string{"sql/0153_plant.sql": first}))
			if got := loadChainError(t); !strings.Contains(got, "0153_plant.sql") || !strings.Contains(got, "header") {
				t.Errorf("LoadChain error %q does not refuse the header of sql/0153_plant.sql", got)
			}
		})
	}
}

func TestLoadChainRefusesAFirstRevisionThatDoesNotContinueTheBaseline(t *testing.T) {
	UseChainFS(t, plantedFiles(map[string]string{
		"sql/0139_first.sql": "-- Revision 0139 (down_revision 0137), planted.\nSELECT 1;\n",
	}))
	got := loadChainError(t)
	if !strings.Contains(got, "down_revision") || !strings.Contains(got, "0137") || !strings.Contains(got, "0138") {
		t.Errorf("LoadChain error %q does not name down_revision 0137 against the baseline head 0138", got)
	}
}

func TestLoadChainRefusesAChainThatDoesNotStartAfterTheBaseline(t *testing.T) {
	UseChainFS(t, plantedFiles(map[string]string{
		"sql/0140_first.sql": "-- Revision 0140 (down_revision 0139), planted.\nSELECT 1;\n",
	}))
	if got := loadChainError(t); !strings.Contains(got, "0139") {
		t.Errorf("LoadChain error %q does not name the missing 0139 after the baseline 0138", got)
	}
}

func TestLoadChainSortsRevisionsAsIntegers(t *testing.T) {
	files := map[string]string{}
	for n := 139; n <= 10001; n++ {
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
	chain, err := LoadChain()
	if err != nil {
		t.Fatalf("the real chain breaks the integrity rules: %v", err)
	}
	if len(chain) == 0 || chain[0].Revision != "0139" {
		t.Fatalf("the real chain does not start at 0139")
	}
}

// The manifest is the PR-time guard: sql/ may only grow by appending a manifest line, so two PRs that add
// the next revision conflict in git instead of merging below the head (CHAOS-11634).
func manifestProblems(chain []ChainFile, manifest string) []string {
	var problems []string
	listed := map[string]string{}
	last := -1
	for _, line := range strings.Split(strings.TrimSuffix(manifest, "\n"), "\n") {
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
