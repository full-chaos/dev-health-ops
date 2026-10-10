package server

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every ClickHouse read client of the repository is built from the ONE
// constructor path (package chclient, CHAOS-9126, D5879): a client built from
// a bare Options literal gets dev-health-go's 1,000-row default. The census
// walks every non-test Go file: each NewClickHouseQueryClientWithOptions call
// must take options from chclient / newUnrestrictedReadClickHouseOptions, and
// no file outside chclient writes a dhclickhouse.Options literal.
func TestEveryQueryAPIClickHouseClientIsBuiltFromTheSharedConstructor(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	call := regexp.MustCompile(`NewClickHouseQueryClientWithOptions\(([^\n]*)`)
	literal := regexp.MustCompile(`(dhclickhouse|clickhouse)\.Options\{`)
	checked := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		src := string(raw)
		rel, _ := filepath.Rel(root, path)
		if filepath.ToSlash(rel) == "internal/queryapi/chclient/chclient.go" {
			return nil
		}
		lines := strings.Split(src, "\n")
		for index, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if literal.MatchString(line) && !strings.Contains(line, "newUnrestricted") {
				t.Errorf("%s writes a ClickHouse Options literal outside package chclient: %s", rel, trimmed)
			}
			m := call.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			checked++
			arg := m[1]
			if strings.Contains(arg, "newUnrestrictedReadClickHouseOptions(") || strings.Contains(arg, "chclient.Options(") {
				continue
			}
			if strings.TrimSpace(strings.TrimSuffix(arg, ")")) == "opts" {
				// a variable named opts counts only when the SAME function builds it from
				// the shared path: look back to the start of the function for its assignment
				fromShared := false
				for back := index - 1; back >= 0; back-- {
					prev := strings.TrimSpace(lines[back])
					if strings.HasPrefix(prev, "func ") {
						break
					}
					if strings.HasPrefix(prev, "opts := ") {
						fromShared = strings.HasPrefix(prev, "opts := newUnrestrictedReadClickHouseOptions(") || strings.HasPrefix(prev, "opts := chclient.Options(")
						break
					}
				}
				if fromShared {
					continue
				}
			}
			t.Errorf("%s builds a ClickHouse client without the shared options: %s", rel, trimmed)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 20 {
		t.Fatalf("only %d client constructions were measured, want the 20+ the REST routes hold: the walk found nothing", checked)
	}
}
