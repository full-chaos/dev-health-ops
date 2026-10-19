package cigates

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

var (
	affectedFullPrefixes = []string{"internal/testsupport/"}
	affectedFullFiles    = map[string]bool{"go.mod": true, "go.sum": true, "go.work": true, "go.work.sum": true}
)

type goListEntry struct {
	ImportPath      string
	Dir             string
	ForTest         string
	Deps            []string
	GoFiles         []string
	CgoFiles        []string
	TestGoFiles     []string
	XTestGoFiles    []string
	EmbedFiles      []string
	TestEmbedFiles  []string
	XTestEmbedFiles []string
	Module          *struct{ Path string }
}

func readGoList(p string) ([]goListEntry, error) {
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	var out []goListEntry
	for {
		var e goListEntry
		if err := dec.Decode(&e); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
}

func readChanged(p string) ([]string, error) {
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	sep := byte('\n')
	if bytes.IndexByte(raw, 0) >= 0 {
		sep = 0
	}
	var out []string
	for _, part := range bytes.Split(raw, []byte{sep}) {
		if len(bytes.TrimSpace(part)) > 0 {
			out = append(out, string(part))
		}
	}
	return out, nil
}

func stripVariant(p string) string {
	if i := strings.Index(p, " ["); i >= 0 {
		return p[:i]
	}
	return p
}

func ownerOfDataFile(p string, packages map[string]goListEntry) (string, bool) {
	dir := path.Dir(p)
	if dir == "." {
		dir = ""
	}
	for dir != "" && dir != "." {
		if owner, ok := packages[dir]; ok {
			rel, err := filepath.Rel(dir, p)
			if err != nil {
				return "", false
			}
			rel = filepath.ToSlash(rel)
			embedded := map[string]bool{}
			for _, list := range [][]string{owner.EmbedFiles, owner.TestEmbedFiles, owner.XTestEmbedFiles} {
				for _, f := range list {
					embedded[f] = true
				}
			}
			parts := strings.Split(rel, "/")
			for _, part := range parts[:len(parts)-1] {
				if part == "testdata" {
					return owner.ImportPath, true
				}
			}
			if embedded[rel] {
				return owner.ImportPath, true
			}
			return "", false
		}
		next := path.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	return "", false
}

func selectAffected(changed []string, golist []goListEntry, keys []string, root, base string) (string, []string, error) {
	packages := map[string]goListEntry{}
	binaries := map[string]map[string]bool{}
	modulePath := ""
	for _, e := range golist {
		if strings.HasSuffix(e.ImportPath, ".test") && !strings.Contains(e.ImportPath, " ") {
			owner := strings.TrimSuffix(e.ImportPath, ".test")
			set := map[string]bool{owner: true}
			for _, d := range e.Deps {
				set[stripVariant(d)] = true
			}
			binaries[owner] = set
			continue
		}
		if strings.Contains(e.ImportPath, " ") || e.ForTest != "" {
			continue
		}
		if e.Dir == "" {
			continue
		}
		rel, err := filepath.Rel(root, e.Dir)
		if err != nil {
			return "", nil, err
		}
		packages[filepath.ToSlash(rel)] = e
		if modulePath == "" && e.Module != nil {
			modulePath = e.Module.Path
		}
	}
	if len(packages) == 0 || modulePath == "" {
		return "", nil, errors.New("go list output holds no packages or no module path")
	}

	rules := &planRules{root: root, base: base, keys: map[string]bool{}, packages: packages, binaries: binaries, modulePath: modulePath}
	for _, k := range keys {
		rules.keys[k] = true
	}
	changedPackages := map[string]bool{}
	directKeys := map[string]bool{}
	for _, raw := range changed {
		p := path.Clean(raw)
		if affectedFullFiles[p] {
			return "FULL shared change: " + p, nil, nil
		}
		for _, pre := range affectedFullPrefixes {
			if strings.HasPrefix(p, pre) {
				return "FULL shared change: " + p, nil, nil
			}
		}
		if full, direct, handled := rules.classifyInput(p); handled {
			if full != "" {
				return full, nil, nil
			}
			for _, k := range direct {
				directKeys[k] = true
			}
			continue
		}
		if strings.HasSuffix(p, ".go") {
			parts := strings.Split(p, "/")
			inTestdata := false
			for _, part := range parts[:len(parts)-1] {
				if part == "testdata" {
					inTestdata = true
				}
			}
			if !inTestdata {
				dir := path.Dir(p)
				owner, ok := packages[dir]
				if !ok {
					return "FULL Go file outside any listed package: " + p, nil, nil
				}
				changedPackages[owner.ImportPath] = true
				continue
			}
		}
		owner, ok := ownerOfDataFile(p, packages)
		if ok {
			changedPackages[owner] = true
			continue
		}
		readers, scanned, err := rules.literalReaders(p)
		if err != nil {
			return "FULL could not scan Go string literals: " + err.Error(), nil, nil
		}
		if !scanned {
			return "FULL non-Go file with no owning package input: " + p, nil, nil
		}
		for _, k := range readers {
			directKeys[k] = true
		}
	}

	var affected []string
	for _, key := range keys {
		importPath := modulePath + "/" + key
		if key == "." {
			importPath = modulePath
		}
		closure, ok := binaries[importPath]
		if !ok {
			return "FULL integration package without a test binary: " + key, nil, nil
		}
		if directKeys[key] {
			affected = append(affected, key)
			continue
		}
		for p := range closure {
			if changedPackages[p] {
				affected = append(affected, key)
				break
			}
		}
	}
	sort.Strings(affected)
	return "AFFECTED", affected, nil
}

// AffectedIntegration selects the integration packages a change can affect. It returns the exit code.
func AffectedIntegration(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("affected-integration", flag.ContinueOnError)
	fs.SetOutput(stderr)
	changedP := fs.String("changed", "", "changed-file list")
	golistP := fs.String("golist", "", "go list -test -json output")
	keysP := fs.String("keys", "", "integration package keys, one per line")
	root := fs.String("root", "", "repository root")
	base := fs.String("base", "", "pull request base sha; go.yml and ci/check_go.sh are compared at its merge base with HEAD")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *changedP == "" || *golistP == "" || *keysP == "" || *root == "" {
		fmt.Fprintln(stderr, "affected-integration: --changed, --golist, --keys and --root are required")
		return 2
	}
	changed, err := readChanged(*changedP)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	keyRaw, err := os.ReadFile(*keysP)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	var keys []string
	for _, k := range strings.Split(string(keyRaw), "\n") {
		if k != "" {
			keys = append(keys, k)
		}
	}
	golist, err := readGoList(*golistP)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	verdict, affected, err := selectAffected(changed, golist, keys, *root, *base)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if verdict == "AFFECTED" {
		fmt.Fprintf(stdout, "AFFECTED %d\n", len(affected))
		for _, k := range affected {
			fmt.Fprintln(stdout, k)
		}
	} else {
		fmt.Fprintln(stdout, verdict)
	}
	return 0
}
