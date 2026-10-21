package checkgo_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const weightsDir = "ci/go_race_weights.d"

// mergedWeights writes every row of ci/go_race_weights.d to one temp file, the one weights stream the shard awk reads.
func mergedWeights(t *testing.T, root string) string {
	t.Helper()
	var b strings.Builder
	for _, f := range rowFiles(t, filepath.Join(root, weightsDir)) {
		b.WriteString(strings.TrimRight(readText(t, f), "\n") + "\n")
	}
	path := filepath.Join(t.TempDir(), "weights.tsv")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
