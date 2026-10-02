package goldenscan

import (
	_ "embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

//go:embed allowlist.tsv
var allowlistTSV string

// holdsShape reports whether a hit has the shape a row pins.
func holdsShape(row Row, hit Hit) bool { return hit.Shape == row.Shape }

// Row is one allowlist row: a golden file and a key, the exact shape every hit value there has, the exact count of hits, and the line
// of the triage record the row cites.
type Row struct {
	Path, Key, Shape string
	Count            int
	Triage           string
}

// Allowlist is the rows of the embedded allowlist.tsv.
func Allowlist() ([]Row, error) { return ParseAllowlist(allowlistTSV) }

// ParseAllowlist reads rows: path TAB key TAB shape TAB count TAB triage. A comment line starts with #. It refuses a wildcard, a
// shape other than uuid and hex64, a count below 1, a missing triage cite, an unsorted list and a duplicate (path, key).
func ParseAllowlist(text string) ([]Row, error) {
	var rows []Row
	seen := map[string]bool{}
	for number, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			return nil, fmt.Errorf("allowlist line %d: want 5 tab-separated fields, have %d", number+1, len(fields))
		}
		count, err := strconv.Atoi(fields[3])
		if err != nil || count < 1 {
			return nil, fmt.Errorf("allowlist line %d: count %q is not a positive number", number+1, fields[3])
		}
		row := Row{Path: fields[0], Key: fields[1], Shape: fields[2], Count: count, Triage: strings.TrimSpace(fields[4])}
		switch {
		case row.Path == "" || row.Key == "":
			return nil, fmt.Errorf("allowlist line %d: a row names one file and one key", number+1)
		case strings.ContainsAny(row.Path+row.Key, "*?[]"):
			return nil, fmt.Errorf("allowlist line %d: no wildcard in a path or a key", number+1)
		case row.Shape != "uuid" && row.Shape != "hex64":
			return nil, fmt.Errorf("allowlist line %d: shape %q is not uuid or hex64", number+1, row.Shape)
		case row.Triage == "":
			return nil, fmt.Errorf("allowlist line %d: a row cites the triage line that accepts it", number+1)
		case seen[row.Path+"\x00"+row.Key]:
			return nil, fmt.Errorf("allowlist line %d: a second row for the same file and key", number+1)
		}
		seen[row.Path+"\x00"+row.Key] = true
		rows = append(rows, row)
	}
	if !sort.SliceIsSorted(rows, func(i, j int) bool {
		return rows[i].Path+"\x00"+rows[i].Key < rows[j].Path+"\x00"+rows[j].Key
	}) {
		return nil, fmt.Errorf("allowlist rows are not sorted by path then key")
	}
	return rows, nil
}

// Check is what is wrong with one golden (its repository path and bytes) against the rows: a hit group (key) with no row, a hit whose
// value is not the row's shape, a count that differs from the row's. A message names the file, the key, the shape and the count,
// never a value. Rows of other files are ignored here; CheckRows finds a row whose file has no hit.
func Check(path string, golden []byte, rows []Row) ([]string, error) {
	leaves, err := Leaves(golden)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return checkHits(path, Hits(leaves), rows), nil
}

func checkHits(path string, hits []Hit, rows []Row) []string {
	byKey := map[string][]Hit{}
	for _, hit := range hits {
		byKey[hit.Key] = append(byKey[hit.Key], hit)
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var problems []string
	for _, key := range keys {
		group := byKey[key]
		var row *Row
		for i := range rows {
			if rows[i].Path == path && rows[i].Key == key {
				row = &rows[i]
			}
		}
		if row == nil {
			problems = append(problems, fmt.Sprintf("%s: %d value(s) under key %q look like a credential (shape %s) and the allowlist has no row for this file and key: a hit is a STOP, not a row to add without the triage record accepting the field", path, len(group), key, group[0].Shape))
			continue
		}
		for _, hit := range group {
			if !holdsShape(*row, hit) {
				problems = append(problems, fmt.Sprintf("%s: a value under key %q is not the shape %s its allowlist row pins (found shape %s)", path, key, row.Shape, hit.Shape))
				break
			}
		}
		if len(group) != row.Count {
			problems = append(problems, fmt.Sprintf("%s: key %q has %d hit(s), its allowlist row pins %d: the row is exact (a changed count is re-triaged, a row only shrinks)", path, key, len(group), row.Count))
		}
	}
	return problems
}

// CheckRows is a row whose file was scanned and has no hit under its key: the row is stale and goes.
func CheckRows(scanned map[string][]Hit, rows []Row) []string {
	var problems []string
	for _, row := range rows {
		hits, ok := scanned[row.Path]
		if !ok {
			problems = append(problems, fmt.Sprintf("allowlist row for %s key %q: the file is not a golden on the tree (delete the row)", row.Path, row.Key))
			continue
		}
		found := false
		for _, hit := range hits {
			if hit.Key == row.Key {
				found = true
			}
		}
		if !found {
			problems = append(problems, fmt.Sprintf("allowlist row for %s key %q has no hit any more: delete the row", row.Path, row.Key))
		}
	}
	return problems
}

// CheckTree scans every golden path of the tree (repository paths of golden files; read gives the bytes of one) against the rows and
// returns every problem, rows included.
func CheckTree(paths []string, read func(path string) ([]byte, error), rows []Row) ([]string, error) {
	scanned := map[string][]Hit{}
	var problems []string
	for _, path := range paths {
		raw, err := read(path)
		if err != nil {
			return nil, err
		}
		leaves, err := Leaves(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		hits := Hits(leaves)
		scanned[path] = hits
		problems = append(problems, checkHits(path, hits, rows)...)
	}
	return append(problems, CheckRows(scanned, rows)...), nil
}
