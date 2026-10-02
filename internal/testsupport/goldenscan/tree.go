package goldenscan

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedfiles"
)

// TreeProblems is the unpacked secret scan of every golden of the tree at repo (a JSON file with a golden header under a manifest
// root) against rows: a hit with no row, a row that no longer matches, a golden that cannot be scanned.
func TreeProblems(repo string, rows []Row) ([]string, error) {
	roots, err := recordedfiles.Roots(repo)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, root := range roots {
		files, err := recordedfiles.Files(repo, root)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			path := root + "/" + file
			if strings.HasSuffix(path, ".json") && recordedfiles.HasGoldenHeader(repo, path) {
				paths = append(paths, path)
			}
		}
	}
	return CheckTree(paths, func(path string) ([]byte, error) {
		return os.ReadFile(filepath.Join(repo, filepath.FromSlash(path)))
	}, rows)
}
