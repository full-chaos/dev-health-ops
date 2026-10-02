// Package recordedfiles keeps every file a test reads from disk under a
// digest: each file under a testdata directory and under tests/fixtures is a
// row (sha256, kind, path) of the manifest of its directory, and one guard
// (Problems, run by this package's test) fails on a file with no row, on a
// file whose bytes changed, on a row with no file and on a directory with no
// manifest.
//
// Why: a recorded answer of the Python producers is the only record of what
// Python did once Python is deleted. A test that compares the Go code with
// such a file fails when the file changes alone, but not when it changes
// together with the Go code, and not when it changes in a byte or a value
// the comparison does not see. With a row, any changed byte changes a line of
// a manifest, and a changed python-recorded file changes one more line: the
// digest of the directory's python-recorded set.
//
// A manifest is written by the verb in ./manifest and never by hand:
//
//	go run ./internal/testsupport/recordedfiles/manifest -kind <kind> <file>...
//
// There is one manifest per directory (beside it: <dir>.manifest.tsv), so two
// changes that add files to different directories never touch the same file.
//
// NOT covered: where the bytes came from. A row freezes the bytes a file has
// today; it does not prove that Python produced them. Only a golden recorded
// by the record verb (kind header) has that.
package recordedfiles

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// The kinds, a closed set.
const (
	// PythonRecorded is an answer of a Python producer, recorded once. It
	// changes only when it is recorded again.
	PythonRecorded = "python-recorded"
	// ProviderRecorded is a recorded answer of an outside service.
	ProviderRecorded = "provider-recorded"
	// HandWritten is a file a person wrote: an input, a case, a script.
	HandWritten = "hand-written"
	// GoGenerated is written by Go code of this repository.
	GoGenerated = "go-generated"
	// Header is a golden the record verb wrote: it holds a header with the
	// verb's stamp and is pinned by a digest in its own test, so its row
	// holds no digest.
	Header = "header"
	// HeaderBeforeStamp is a golden with a header and no stamp of the record
	// verb: it was recorded before the stamp existed. Its row holds its
	// digest. The set of these rows is a RECORD of how the goldens of that
	// time were made, not open work: it only shrinks, a golden leaves it
	// when another ticket has it recorded again, and emptying it gates
	// nothing (once the Python tree is deleted nothing can be recorded
	// again). No row of this kind is added after the first ones: a golden
	// with no stamp that is not in the set was made by hand.
	HeaderBeforeStamp = "header-before-stamp"
	// Unclassified is a file nobody classified when the manifests were first
	// written. It is allowed only for the files of the day-one list.
	Unclassified = "unclassified"
)

// Kinds is every kind a row may hold.
var Kinds = []string{PythonRecorded, ProviderRecorded, HandWritten, GoGenerated, Header, HeaderBeforeStamp, Unclassified}

// recordVerbStamp is what the record verb writes into a golden's header
// (venueoracle's recorded_by).
const recordVerbStamp = "goldenrecord"

// unclassifiedDayOne is how many files the day-one list holds. It only goes
// down: a file leaves the list when it gets a kind, and no file is added.
const unclassifiedDayOne = 974

// DayOneList is the list of the files that were unclassified when the
// manifests were first written, one repository path per line.
const DayOneList = "internal/testsupport/recordedfiles/unclassified_day_one.txt"

// headerBeforeStampCeiling is how many goldens may hold the kind
// header-before-stamp: the ones that were in the tree when the stamp became the
// verb's (CHAOS-7707). It only goes down.
const headerBeforeStampCeiling = 253

// BeforeStampList is the closed list of those goldens, one repository path per
// line. -day-one gives the kind header-before-stamp to a path on it and to no
// other: a new golden with no stamp was not recorded by the verb.
const BeforeStampList = "internal/testsupport/recordedfiles/header_before_stamp_ceiling.txt"

// BeforeStamp is the closed list of the goldens that may have no stamp.
func BeforeStamp(repo string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(BeforeStampList)))
	if err != nil {
		return nil, err
	}
	var list []string
	for _, line := range strings.Split(string(raw), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			list = append(list, line)
		}
	}
	return list, nil
}

// ManifestSuffix is what a manifest's name adds to its directory's.
const ManifestSuffix = ".manifest.tsv"

// Verb is the command that writes a manifest.
const Verb = "go run ./internal/testsupport/recordedfiles/manifest"

// noDigest is the digest column of a header row.
const noDigest = "-"

// candidateSuffix is the record verb's unpromoted candidate, never a file of
// the tree.
const candidateSuffix = ".recording"

// Row is one file of a manifest.
type Row struct {
	Digest, Kind, Path string
}

// Manifest is the rows of one directory, Root as a repository path.
type Manifest struct {
	Root string
	Rows []Row
}

// ManifestPath is the repository path of root's manifest.
func ManifestPath(root string) string { return root + ManifestSuffix }

// RepoRoot is the directory that holds go.mod, from dir upwards.
func RepoRoot(dir string) (string, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("no go.mod above this directory")
		}
		dir = parent
	}
}

// fixtures is the one root that is not named testdata.
const fixtures = "tests/fixtures"

// Roots is every directory that has a manifest of its own, as repository
// paths in order: each directory named testdata that is not inside another
// one, and tests/fixtures.
func Roots(repo string) ([]string, error) {
	var roots []string
	err := filepath.WalkDir(repo, func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(repo, file)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		name := entry.Name()
		if relative != "." && (strings.HasPrefix(name, ".") || name == "node_modules") {
			return filepath.SkipDir
		}
		if name == "testdata" || relative == fixtures {
			roots = append(roots, relative)
			return filepath.SkipDir
		}
		return nil
	})
	sort.Strings(roots)
	return roots, err
}

// Files is every file under root, as paths relative to root in order. The
// record verb's candidates are not files of the tree.
func Files(repo, root string) ([]string, error) {
	var files []string
	base := filepath.Join(repo, filepath.FromSlash(root))
	err := filepath.WalkDir(base, func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || strings.HasSuffix(entry.Name(), candidateSuffix) {
			return nil
		}
		relative, err := filepath.Rel(base, file)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(relative))
		return nil
	})
	sort.Strings(files)
	return files, err
}

// Digest is the sha256 of the file at the repository path.
func Digest(repo, file string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(file)))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// HasGoldenHeader tells whether the file at the repository path is a golden
// of the record verb: a JSON document whose header names its Python build.
func HasGoldenHeader(repo, file string) bool {
	golden, _ := goldenHeader(repo, file)
	return golden
}

// goldenHeader tells whether the file at the repository path is a golden with
// a header, and whether that header holds the record verb's stamp.
func goldenHeader(repo, file string) (golden, stamped bool) {
	raw, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(file)))
	if err != nil {
		return false, false
	}
	var document struct {
		Header struct {
			PythonBuild string `json:"python_build"`
			RecordedBy  string `json:"recorded_by"`
		} `json:"header"`
	}
	if json.Unmarshal(raw, &document) != nil || document.Header.PythonBuild == "" {
		return false, false
	}
	return true, document.Header.RecordedBy == recordVerbStamp
}

// SetDigest is the digest of the python-recorded rows as a set, and how many
// they are: any recorded file that changes, comes or goes changes it.
func SetDigest(rows []Row) (string, int) { return setDigest(rows, PythonRecorded) }

// setDigest is the digest of the rows of one kind as a set, and their number.
func setDigest(rows []Row, kind string) (string, int) {
	hash := sha256.New()
	count := 0
	for _, row := range sorted(rows) {
		if row.Kind == kind {
			fmt.Fprintf(hash, "%s  %s\n", row.Digest, row.Path)
			count++
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), count
}

func sorted(rows []Row) []Row {
	out := append([]Row(nil), rows...)
	sort.Slice(out, func(a, b int) bool { return out[a].Path < out[b].Path })
	return out
}

// Bytes is the manifest in the one form the verb writes.
func (m Manifest) Bytes() []byte {
	var out bytes.Buffer
	fmt.Fprintf(&out, "# Every file under %s: sha256, kind, path. Written by `%s`; never edit it by hand.\n", m.Root, Verb)
	digest, count := SetDigest(m.Rows)
	fmt.Fprintf(&out, "# python-recorded set: %s (%d files)\n", digest, count)
	// The goldens recorded before the record verb stamped its work: a record
	// that only shrinks, never open work (HeaderBeforeStamp).
	digest, count = setDigest(m.Rows, HeaderBeforeStamp)
	fmt.Fprintf(&out, "# %s set: %s (%d files)\n", HeaderBeforeStamp, digest, count)
	for _, row := range sorted(m.Rows) {
		fmt.Fprintf(&out, "%s\t%s\t%s\n", row.Digest, row.Kind, row.Path)
	}
	return out.Bytes()
}

// Read is root's manifest. A manifest that is not in the form the verb
// writes is an error: it was edited by hand.
func Read(repo, root string) (Manifest, error) {
	manifest, raw, err := parse(repo, root)
	if err != nil {
		return manifest, err
	}
	if !bytes.Equal(raw, manifest.Bytes()) {
		return manifest, fmt.Errorf("%s is not in the form the verb writes (edited by hand, or its python-recorded set line does not match its rows); write it again: %s -sync", ManifestPath(root), Verb)
	}
	return manifest, nil
}

// parse is the rows of root's manifest and its bytes, with no check of the
// comment lines.
func parse(repo, root string) (Manifest, []byte, error) {
	manifest := Manifest{Root: root}
	file := ManifestPath(root)
	raw, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(file)))
	if err != nil {
		return manifest, nil, err
	}
	seen := map[string]bool{}
	for number, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			return manifest, raw, fmt.Errorf("%s line %d is not a row (sha256, kind, path)", file, number+1)
		}
		row := Row{Digest: fields[0], Kind: fields[1], Path: fields[2]}
		switch {
		case !knownKind(row.Kind):
			return manifest, raw, fmt.Errorf("%s line %d: kind %q is not one of %s", file, number+1, row.Kind, strings.Join(Kinds, ", "))
		case seen[row.Path]:
			return manifest, raw, fmt.Errorf("%s line %d: %s has two rows", file, number+1, row.Path)
		case row.Path == "" || path.Clean(row.Path) != row.Path || strings.HasPrefix(row.Path, "../") || path.IsAbs(row.Path):
			return manifest, raw, fmt.Errorf("%s line %d: %q is not a path under %s", file, number+1, row.Path, root)
		case (row.Kind == Header) != (row.Digest == noDigest):
			return manifest, raw, fmt.Errorf("%s line %d: only a %s row holds %q for its digest", file, number+1, Header, noDigest)
		}
		seen[row.Path] = true
		manifest.Rows = append(manifest.Rows, row)
	}
	return manifest, raw, nil
}

func knownKind(kind string) bool {
	for _, known := range Kinds {
		if kind == known {
			return true
		}
	}
	return false
}

// Remaining says how many files are still unclassified, for the guard and the
// verb to print on every run: the number a reader watches go down.
func Remaining(repo string) (string, error) {
	dayOne, err := DayOne(repo)
	if err != nil {
		return "", err
	}
	byTop := map[string]int{}
	for _, file := range dayOne {
		parts := strings.SplitN(file, "/", 3)
		if len(parts) > 2 {
			byTop[parts[0]+"/"+parts[1]]++
		}
	}
	var tops []string
	for top := range byTop {
		tops = append(tops, top)
	}
	sort.Slice(tops, func(a, b int) bool {
		if byTop[tops[a]] != byTop[tops[b]] {
			return byTop[tops[a]] > byTop[tops[b]]
		}
		return tops[a] < tops[b]
	})
	var shown []string
	for _, top := range tops {
		shown = append(shown, fmt.Sprintf("%s %d", top, byTop[top]))
	}
	return fmt.Sprintf("%d files are still %s (their bytes are pinned, their kind is not known; %s): %s", len(dayOne), Unclassified, DayOneList, strings.Join(shown, ", ")), nil
}

// DayOne is the day-one list: the repository paths that may be unclassified.
func DayOne(repo string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(DayOneList)))
	if err != nil {
		return nil, err
	}
	var list []string
	for _, line := range strings.Split(string(raw), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			list = append(list, line)
		}
	}
	return list, nil
}

// Problems is everything the guard refuses in the tree at repo, each as one
// message that names the file and the command that puts it right.
func Problems(repo string) ([]string, error) {
	roots, err := Roots(repo)
	if err != nil {
		return nil, err
	}
	dayOne, err := DayOne(repo)
	if err != nil {
		return nil, err
	}
	found, err := problems(repo, roots, dayOne, unclassifiedDayOne)
	if err != nil {
		return nil, err
	}
	ceiling, err := ceilingProblems(repo, roots, headerBeforeStampCeiling)
	if err != nil {
		return nil, err
	}
	return append(found, ceiling...), nil
}

// ceilingProblems is what is wrong with the header-before-stamp rows against
// the closed list: a row whose path is not on it, and a list whose length is not
// the ceiling.
func ceilingProblems(repo string, roots []string, ceiling int) ([]string, error) {
	listed, err := BeforeStamp(repo)
	if err != nil {
		return nil, err
	}
	onList := map[string]bool{}
	for _, file := range listed {
		onList[file] = true
	}
	var out []string
	for _, root := range roots {
		manifest, err := Read(repo, root)
		if err != nil {
			continue // the other checks report an unreadable manifest
		}
		for _, row := range manifest.Rows {
			if full := root + "/" + row.Path; row.Kind == HeaderBeforeStamp && !onList[full] {
				out = append(out, fmt.Sprintf("%s has the kind %s and is not on the closed list (%s): a golden with no stamp that was not in the tree when the stamp became the verb's was not recorded by the verb; record it with the verb", full, HeaderBeforeStamp, BeforeStampList))
			}
		}
	}
	if len(listed) != ceiling {
		out = append(out, fmt.Sprintf("the closed list (%s) holds %d files and headerBeforeStampCeiling says %d: the list only shrinks, and the number goes down with it", BeforeStampList, len(listed), ceiling))
	}
	return out, nil
}

func problems(repo string, roots, dayOne []string, dayOneCount int) ([]string, error) {
	var out []string
	listed := map[string]bool{}
	for _, file := range dayOne {
		listed[file] = true
	}
	unclassified := map[string]bool{}
	// unread is the roots whose manifest could not be read: nothing is known
	// of their rows, so nothing is said of their day-one files.
	var unread []string
	for _, root := range roots {
		files, err := Files(repo, root)
		if err != nil {
			return nil, err
		}
		manifest, err := Read(repo, root)
		if errors.Is(err, fs.ErrNotExist) {
			out = append(out, fmt.Sprintf("%s has no manifest (%s): every file under a testdata directory and under %s is listed with its sha256 and its kind. Write it: %s -kind <%s> %s/<file>...",
				root, ManifestPath(root), fixtures, Verb, strings.Join(addable(), "|"), root))
			unread = append(unread, root)
			continue
		}
		if err != nil {
			out = append(out, err.Error())
			unread = append(unread, root)
			continue
		}
		rows := map[string]Row{}
		for _, row := range manifest.Rows {
			rows[row.Path] = row
		}
		for _, file := range files {
			full := root + "/" + file
			row, known := rows[file]
			delete(rows, file)
			if !known {
				out = append(out, fmt.Sprintf("%s is not in %s: every file a test reads from disk is listed with its sha256 and its kind, so that it cannot change unseen. Add it: %s -kind <%s> %s (a golden the record verb wrote needs no -kind)",
					full, ManifestPath(root), Verb, strings.Join(addable(), "|"), full))
				continue
			}
			if row.Kind == Unclassified {
				unclassified[full] = true
				if !listed[full] {
					out = append(out, fmt.Sprintf("%s is %s in %s and is not on the day-one list (%s): a new file gets a kind. Give it one: %s -kind <%s> %s", full, Unclassified, ManifestPath(root), DayOneList, Verb, strings.Join(addable(), "|"), full))
				}
			}
			golden, stamped := goldenHeader(repo, full)
			if row.Kind == Header {
				switch {
				case !golden:
					out = append(out, fmt.Sprintf("%s has the kind %s in %s and holds no golden header: only a golden the record verb wrote has that kind", full, Header, ManifestPath(root)))
				case !stamped:
					out = append(out, fmt.Sprintf("%s has the kind %s in %s and its header holds no stamp of the record verb (recorded_by): the verb did not make it. A golden is recorded by the verb, never by hand: record it (its test's recipe), then %s %s", full, Header, ManifestPath(root), Verb, full))
				}
				continue
			}
			if row.Kind == HeaderBeforeStamp && (!golden || stamped) {
				if stamped {
					out = append(out, fmt.Sprintf("%s is %s in %s and its header holds the record verb's stamp now: it was recorded again; write its row: %s %s", full, HeaderBeforeStamp, ManifestPath(root), Verb, full))
				} else {
					out = append(out, fmt.Sprintf("%s is %s in %s and holds no golden header", full, HeaderBeforeStamp, ManifestPath(root)))
				}
				continue
			}
			digest, err := Digest(repo, full)
			if err != nil {
				return nil, err
			}
			switch {
			case digest == row.Digest:
			case row.Kind == PythonRecorded:
				out = append(out, fmt.Sprintf("%s changed (sha256 %s, its row in %s holds %s). It is a recorded answer of a Python producer: it changes only when it is recorded again, never by an edit. If it was recorded again: %s -recorded-again -kind %s %s",
					full, digest, ManifestPath(root), row.Digest, Verb, PythonRecorded, full))
			case row.Kind == HeaderBeforeStamp:
				out = append(out, fmt.Sprintf("%s changed (sha256 %s, its row in %s holds %s). It is a golden recorded before the record verb stamped its work: it changes only when the verb records it again, which stamps it; then: %s %s",
					full, digest, ManifestPath(root), row.Digest, Verb, full))
			case row.Kind == Unclassified:
				out = append(out, fmt.Sprintf("%s changed (sha256 %s, its row in %s holds %s). It has no kind yet: if the change is meant, give it its kind, which also takes it off the day-one list: %s -kind <%s> %s",
					full, digest, ManifestPath(root), row.Digest, Verb, strings.Join(addable(), "|"), full))
			default:
				out = append(out, fmt.Sprintf("%s changed (sha256 %s, its row in %s holds %s). If the change is meant: %s -kind %s %s", full, digest, ManifestPath(root), row.Digest, Verb, row.Kind, full))
			}
		}
		for _, row := range sorted(valuesOf(rows)) {
			out = append(out, fmt.Sprintf("%s has a row for %s and the file is gone. If it was deleted on purpose: %s -sync", ManifestPath(root), row.Path, Verb))
		}
	}
	orphans, err := orphanManifests(repo, roots)
	if err != nil {
		return nil, err
	}
	for _, orphan := range orphans {
		out = append(out, fmt.Sprintf("%s is the manifest of a directory that is gone. If the directory was deleted on purpose: %s -sync", orphan, Verb))
	}
	for _, file := range dayOne {
		if !unclassified[file] && !under(unread, file) {
			out = append(out, fmt.Sprintf("%s is on the day-one list (%s) and is not an %s file any more: give it its kind with the verb, which writes the list (%s -kind <kind> %s), and lower unclassifiedDayOne by one", file, DayOneList, Unclassified, Verb, file))
		}
	}
	if len(dayOne) != dayOneCount {
		out = append(out, fmt.Sprintf("the day-one list (%s) holds %d files and unclassifiedDayOne says %d: the list only shrinks, and the number goes down with it", DayOneList, len(dayOne), dayOneCount))
	}
	return out, nil
}

func valuesOf(rows map[string]Row) []Row {
	var out []Row
	for _, row := range rows {
		out = append(out, row)
	}
	return out
}

// addable is the kinds a person may give a file.
func addable() []string {
	return []string{PythonRecorded, ProviderRecorded, HandWritten, GoGenerated}
}

// Write writes root's manifest in the verb's form.
func Write(repo string, manifest Manifest) error {
	return os.WriteFile(filepath.Join(repo, filepath.FromSlash(ManifestPath(manifest.Root))), manifest.Bytes(), 0o644)
}

// RootOf is the root that holds the repository path file, and file's path
// under it.
func RootOf(file string) (root, relative string, err error) {
	if strings.HasPrefix(file, fixtures+"/") {
		return fixtures, strings.TrimPrefix(file, fixtures+"/"), nil
	}
	parts := strings.Split(file, "/")
	for index, part := range parts[:len(parts)-1] {
		if part == "testdata" {
			return strings.Join(parts[:index+1], "/"), strings.Join(parts[index+1:], "/"), nil
		}
	}
	return "", "", fmt.Errorf("%s is not under a testdata directory or under %s", file, fixtures)
}

// orphanManifests is every manifest in the tree whose directory is not one
// of roots.
func orphanManifests(repo string, roots []string) ([]string, error) {
	known := map[string]bool{}
	for _, root := range roots {
		known[ManifestPath(root)] = true
	}
	var orphans []string
	err := filepath.WalkDir(repo, func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(repo, file)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		name := entry.Name()
		if entry.IsDir() {
			if relative != "." && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "testdata" || relative == fixtures) {
				return filepath.SkipDir
			}
			return nil
		}
		if (name == "testdata"+ManifestSuffix || relative == ManifestPath(fixtures)) && !known[relative] {
			orphans = append(orphans, relative)
		}
		return nil
	})
	sort.Strings(orphans)
	return orphans, err
}

// Change is what Set and Sync were asked to allow.
type Change struct {
	// Kind is the kind to give the files; "" for a golden with a header.
	Kind string
	// RecordedAgain allows a python-recorded row to change or to go.
	RecordedAgain bool
	// DayOne is for the first manifests only: it allows the kind unclassified
	// (and puts the file on the day-one list), and it gives a golden with no
	// stamp of the record verb the kind header-before-stamp.
	DayOne bool
}

// Set writes the rows of files (repository paths) with their digests of now,
// and returns a note for each thing the caller has left to do by hand.
func Set(repo string, files []string, change Change) ([]string, error) {
	var notes []string
	manifests := map[string]*Manifest{}
	dayOne, err := DayOne(repo)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	listed := map[string]bool{}
	for _, file := range dayOne {
		listed[file] = true
	}
	before := len(listed)
	for _, file := range files {
		file = path.Clean(filepath.ToSlash(file))
		root, relative, err := RootOf(file)
		if err != nil {
			return nil, err
		}
		if info, err := os.Stat(filepath.Join(repo, filepath.FromSlash(file))); err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%s is not a file of this tree", file)
		}
		kind := change.Kind
		golden, stamped := goldenHeader(repo, file)
		switch {
		case golden && kind != "" && kind != Header:
			return nil, fmt.Errorf("%s holds a golden header: the record verb wrote it, its kind is %s (give no -kind)", file, Header)
		case golden && stamped:
			kind = Header
		case golden && change.DayOne:
			// A golden recorded before the stamp existed: only the ones on the
			// closed list; any other golden with no stamp was made by hand.
			if err := onBeforeStampList(repo, file); err != nil {
				return nil, err
			}
			kind = HeaderBeforeStamp
		case golden:
			return nil, fmt.Errorf("%s holds a golden header and no stamp of the record verb (recorded_by): the verb did not make it. A golden is recorded by the verb, never by hand (its test's recipe names the command)", file)
		case kind == Header || kind == HeaderBeforeStamp:
			return nil, fmt.Errorf("%s holds no golden header: only a golden the record verb wrote has the kind %s", file, kind)
		case kind == "":
			return nil, fmt.Errorf("%s needs a kind: -kind <%s>", file, strings.Join(addable(), "|"))
		case !knownKind(kind):
			return nil, fmt.Errorf("kind %q is not one of %s", kind, strings.Join(addable(), ", "))
		case kind == Unclassified && !change.DayOne:
			return nil, fmt.Errorf("%s: %s is not a kind a file can be given; it is only for the files of the day-one list", file, Unclassified)
		}
		manifest := manifests[root]
		if manifest == nil {
			read, _, err := parse(repo, root)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
			manifest = &read
			manifests[root] = manifest
		}
		row := Row{Digest: noDigest, Kind: kind, Path: relative}
		if kind != Header {
			if row.Digest, err = Digest(repo, file); err != nil {
				return nil, err
			}
		}
		at := -1
		for index, existing := range manifest.Rows {
			if existing.Path == relative {
				at = index
			}
		}
		if at >= 0 {
			existing := manifest.Rows[at]
			if existing.Kind == HeaderBeforeStamp && kind == HeaderBeforeStamp && existing != row {
				return nil, fmt.Errorf("%s is %s and its bytes changed with no stamp of the record verb: a golden changes only when the verb records it again", file, HeaderBeforeStamp)
			}
			if existing.Kind == PythonRecorded && existing != row && !change.RecordedAgain {
				return nil, fmt.Errorf("%s is %s (sha256 %s in its row, %s now, kind asked %s): a recorded answer of a Python producer changes only when it is recorded again; if it was, add -recorded-again", file, PythonRecorded, existing.Digest, row.Digest, kind)
			}
			manifest.Rows[at] = row
		} else {
			manifest.Rows = append(manifest.Rows, row)
		}
		switch {
		case kind == Unclassified:
			listed[file] = true
		case listed[file]:
			delete(listed, file)
		}
	}
	for _, manifest := range manifests {
		if err := Write(repo, *manifest); err != nil {
			return nil, err
		}
	}
	if len(listed) != before || change.DayOne {
		if err := writeDayOne(repo, listed); err != nil {
			return nil, err
		}
		if len(listed) != unclassifiedDayOne {
			notes = append(notes, fmt.Sprintf("the day-one list now holds %d files: set unclassifiedDayOne to %d in internal/testsupport/recordedfiles/recordedfiles.go", len(listed), len(listed)))
		}
	}
	return notes, nil
}

func onBeforeStampList(repo, file string) error {
	listed, err := BeforeStamp(repo)
	if err != nil {
		return fmt.Errorf("%s holds a golden header and no stamp of the record verb, and the closed list of the goldens that may have none cannot be read: %w", file, err)
	}
	for _, entry := range listed {
		if entry == file {
			return nil
		}
	}
	return fmt.Errorf("%s holds a golden header and no stamp of the record verb (recorded_by), and it is not on the closed list of the goldens recorded before the stamp (%s): -day-one does not admit it. A golden is recorded by the verb, never by hand (its test's recipe names the command)", file, BeforeStampList)
}

func writeDayOne(repo string, listed map[string]bool) error {
	var files []string
	for file := range listed {
		files = append(files, file)
	}
	sort.Strings(files)
	text := "# The files that had no kind when the manifests were first written. The list only shrinks:\n" +
		"# a file leaves it when it gets a kind (" + Verb + " -kind <kind> <file>), and no file is added.\n"
	for _, file := range files {
		text += file + "\n"
	}
	file := filepath.Join(repo, filepath.FromSlash(DayOneList))
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}
	return os.WriteFile(file, []byte(text), 0o644)
}

// Sync drops the rows whose file is gone and the manifests whose directory
// is gone, writes every manifest in the verb's form, and returns a note for
// each thing left to do by hand.
func Sync(repo string, change Change) ([]string, error) {
	roots, err := Roots(repo)
	if err != nil {
		return nil, err
	}
	dayOne, err := DayOne(repo)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	listed := map[string]bool{}
	for _, file := range dayOne {
		listed[file] = true
	}
	before := len(listed)
	var notes []string
	for _, root := range roots {
		manifest, _, err := parse(repo, root)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		files, err := Files(repo, root)
		if err != nil {
			return nil, err
		}
		present := map[string]bool{}
		for _, file := range files {
			present[file] = true
		}
		var kept []Row
		for _, row := range manifest.Rows {
			switch {
			case present[row.Path]:
				kept = append(kept, row)
			case (row.Kind == PythonRecorded || row.Kind == HeaderBeforeStamp) && !change.RecordedAgain:
				return nil, fmt.Errorf("%s/%s is %s and the file is gone: a recorded answer of a Python producer goes only on purpose; if it was replaced or deleted on purpose, add -recorded-again", root, row.Path, row.Kind)
			default:
				delete(listed, root+"/"+row.Path)
			}
		}
		manifest.Rows = kept
		if err := Write(repo, manifest); err != nil {
			return nil, err
		}
	}
	orphans, err := orphanManifests(repo, roots)
	if err != nil {
		return nil, err
	}
	for _, orphan := range orphans {
		root := strings.TrimSuffix(orphan, ManifestSuffix)
		manifest, _, err := parse(repo, root)
		if err != nil {
			return nil, err
		}
		for _, row := range manifest.Rows {
			if (row.Kind == PythonRecorded || row.Kind == HeaderBeforeStamp) && !change.RecordedAgain {
				return nil, fmt.Errorf("%s/%s is %s and its directory is gone: a recorded answer of a Python producer goes only on purpose; if so, add -recorded-again", root, row.Path, row.Kind)
			}
			delete(listed, root+"/"+row.Path)
		}
		if err := os.Remove(filepath.Join(repo, filepath.FromSlash(orphan))); err != nil {
			return nil, err
		}
	}
	if len(listed) != before {
		if err := writeDayOne(repo, listed); err != nil {
			return nil, err
		}
		notes = append(notes, fmt.Sprintf("the day-one list now holds %d files: set unclassifiedDayOne to %d in internal/testsupport/recordedfiles/recordedfiles.go", len(listed), len(listed)))
	}
	return notes, nil
}

func under(roots []string, file string) bool {
	for _, root := range roots {
		if strings.HasPrefix(file, root+"/") {
			return true
		}
	}
	return false
}
