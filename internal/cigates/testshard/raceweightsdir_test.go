package testshard_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Helpers for ci/go_race_weights.d: one file per package, named by race_weights_file_name in ci/check_go.sh, one
// `package<TAB>seconds` row each. The awks take one weights file, so tests merge the directory rows into a temp file;
// planted weights go to a temp directory of per-package files (GO_RACE_WEIGHTS_DIR).

const raceweightsRealDir = "ci/go_race_weights.d"

// raceweightsFileName mirrors race_weights_file_name: `_` becomes `_u`, `/` becomes `__`, the root package is `_root`.
func raceweightsFileName(pkg string) string {
	if pkg == "." {
		return "_root.tsv"
	}
	return strings.NewReplacer("_", "_u", "/", "__").Replace(pkg) + ".tsv"
}

// raceweightsRealFiles reads every *.tsv of the real directory: file name -> content.
func raceweightsRealFiles(t *testing.T) map[string]string {
	t.Helper()
	dir := filepath.Join(repoRoot(t), raceweightsRealDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".tsv") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[e.Name()] = string(b)
	}
	if len(files) == 0 {
		t.Fatalf("%s holds no .tsv file", raceweightsRealDir)
	}
	return files
}

// raceweightsMerge joins the rows of files (comments and blank lines dropped) in file-name order.
func raceweightsMerge(files map[string]string) string {
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		for _, line := range strings.Split(files[n], "\n") {
			if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
				continue
			}
			b.WriteString(line + "\n")
		}
	}
	return b.String()
}

// raceweightsRealText is every row of the real directory, one stream.
func raceweightsRealText(t *testing.T) string {
	t.Helper()
	return raceweightsMerge(raceweightsRealFiles(t))
}

// raceweightsRealMerged writes the real rows to one temp file, for the awks.
func raceweightsRealMerged(t *testing.T) string {
	t.Helper()
	return raceweightsWriteFile(t, "weights.tsv", raceweightsRealText(t))
}

func raceweightsWriteFile(t *testing.T, name, text string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// raceweightsDirFromFiles writes files into a fresh temp directory and returns it.
func raceweightsDirFromFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for n, text := range files {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// raceweightsDirFromRows writes one correctly named file per row of text. A package that appears twice gets its later
// rows in files named dupN_<name> so a duplicate stays visible to the census.
func raceweightsDirFromRows(t *testing.T, text string) string {
	t.Helper()
	files := map[string]string{}
	for i, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := raceweightsFileName(strings.Split(line, "\t")[0])
		if _, ok := files[name]; ok {
			name = fmt.Sprintf("dup%d_%s", i, name)
		}
		files[name] = line + "\n"
	}
	return raceweightsDirFromFiles(t, files)
}

// raceweightsEnv is the env entry that points check_go.sh at a planted directory.
func raceweightsEnv(dir string) []string {
	return []string{"GO_RACE_WEIGHTS_DIR=" + dir}
}
