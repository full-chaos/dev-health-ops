package venueoracle

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// GoldenTokenViolations walks root for golden files (a JSON file under a
// testdata directory with a golden header) and names each that holds a token.
func GoldenTokenViolations(root string) (checked int, violations []string, err error) {
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == ".git" || name == "node_modules" || name == ".venv" {
				return filepath.SkipDir
			}
			return nil
		}
		slash := filepath.ToSlash(path)
		gz := strings.HasSuffix(slash, ".json.gz")
		if !(strings.HasSuffix(slash, ".json") || gz) {
			return nil
		}
		if gz && !strings.Contains(slash, "/testdata/") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if gz {
			// A compressed golden (a recorded transport, a world file) is
			// scanned as text: it has no venue header to look for.
			reader, err := gzip.NewReader(bytes.NewReader(raw))
			if err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			if raw, err = io.ReadAll(reader); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			checked++
			if found := TokenShapesIn(string(raw)); len(found) > 0 {
				violations = append(violations, fmt.Sprintf("%s holds a token shape (%s)", path, strings.Join(found, ", ")))
			}
			return nil
		}
		if !strings.Contains(string(raw), `"python_build"`) {
			return nil
		}
		checked++
		if err := tokenShapeErr(path, raw); err != nil {
			violations = append(violations, err.Error())
		}
		return nil
	})
	return checked, violations, err
}
