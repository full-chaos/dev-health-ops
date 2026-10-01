package recordedfiles

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The guard: in this repository every file under a testdata directory and
// under tests/fixtures is a row of its directory's manifest, with the bytes
// the row holds.
func TestEveryFileATestReadsIsInItsManifest(t *testing.T) {
	repo, err := RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	found, err := Problems(repo)
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range found {
		t.Error(problem)
	}
	remaining, err := Remaining(repo)
	if err != nil {
		t.Fatal(err)
	}
	// In the log of every run that shows test output (go test -v, a CI log, a
	// failure); the verb prints the same line on every run.
	t.Log(remaining)
}

func TestRemainingCountsTheDayOneListByTopLevelPackage(t *testing.T) {
	repo := t.TempDir()
	write(t, repo, DayOneList, "# note\ninternal/b/testdata/x\ninternal/a/testdata/y\ninternal/a/sub/testdata/z\ntests/fixtures/f.json\n")
	got, err := Remaining(repo)
	want := "4 files are still unclassified (their bytes are pinned, their kind is not known; " + DayOneList + "): internal/a 2, internal/b 1, tests/fixtures 1"
	if err != nil || got != want {
		t.Errorf("Remaining = %q, %v, want %q", got, err, want)
	}
}

const goldenText = `{"header": {"test": "TestX", "python_build": "0123456789012345678901234567890123456789"}, "requests": []}`

// tree builds a small repository whose manifests the verb's own code wrote:
// one testdata directory with a file of each kind, tests/fixtures, and a
// day-one list of one file.
func tree(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	for file, text := range map[string]string{
		"go.mod":                              "module example\n",
		"a/testdata/case.json":                `{"case": 1}`,
		"a/testdata/recorded.json":            `{"python": "said this"}`,
		"a/testdata/sub/second_recorded.txt":  "and this\n",
		"a/testdata/golden/TestX.json":        goldenText,
		"a/testdata/pages/page_0.json":        `{"page": 0}`,
		"a/testdata/old.sql":                  "select 1;\n",
		"tests/fixtures/x_python_golden.json": `{"x": 1.5}`,
	} {
		write(t, repo, file, text)
	}
	set(t, repo, Change{}, "a/testdata/golden/TestX.json")
	set(t, repo, Change{Kind: HandWritten}, "a/testdata/case.json")
	set(t, repo, Change{Kind: PythonRecorded}, "a/testdata/recorded.json", "a/testdata/sub/second_recorded.txt", "tests/fixtures/x_python_golden.json")
	set(t, repo, Change{Kind: ProviderRecorded}, "a/testdata/pages/page_0.json")
	set(t, repo, Change{Kind: Unclassified, DayOne: true}, "a/testdata/old.sql")
	return repo
}

func write(t *testing.T, repo, file, text string) {
	t.Helper()
	full := filepath.Join(repo, filepath.FromSlash(file))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, repo, file string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repo, filepath.FromSlash(file)))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func remove(t *testing.T, repo, file string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(repo, filepath.FromSlash(file))); err != nil {
		t.Fatal(err)
	}
}

func set(t *testing.T, repo string, change Change, files ...string) {
	t.Helper()
	if _, err := Set(repo, files, change); err != nil {
		t.Fatal(err)
	}
}

// found is the guard's problems in repo, with the day-one count the list has.
func found(t *testing.T, repo string, dayOneCount int) []string {
	t.Helper()
	roots, err := Roots(repo)
	if err != nil {
		t.Fatal(err)
	}
	dayOne, err := DayOne(repo)
	if err != nil {
		t.Fatal(err)
	}
	out, err := problems(repo, roots, dayOne, dayOneCount)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestTheGuardRefusesEachWayAFileCanChangeUnseen(t *testing.T) {
	for _, row := range []struct {
		name string
		do   func(t *testing.T, repo string)
		// want is, for each problem expected, texts it must hold; none = the tree is accepted.
		want [][]string
	}{
		{"nothing changed", func(*testing.T, string) {}, nil},
		{"a new file with no row", func(t *testing.T, repo string) { write(t, repo, "a/testdata/new.json", "{}") },
			[][]string{{"a/testdata/new.json is not in a/testdata.manifest.tsv", "-kind <python-recorded|provider-recorded|hand-written|go-generated> a/testdata/new.json"}}},
		{"one byte more in a hand-written file", func(t *testing.T, repo string) { write(t, repo, "a/testdata/case.json", `{"case": 1} `) },
			[][]string{{"a/testdata/case.json changed", "its row in a/testdata.manifest.tsv holds", "-kind hand-written a/testdata/case.json"}}},
		{"one byte more in a recorded Python answer", func(t *testing.T, repo string) {
			write(t, repo, "a/testdata/recorded.json", `{"python": "said this"} `)
		},
			[][]string{{"a/testdata/recorded.json changed", "its row in a/testdata.manifest.tsv holds", "recorded answer of a Python producer", "-recorded-again -kind python-recorded a/testdata/recorded.json"}}},
		{"one byte more in a recorded file in a subdirectory", func(t *testing.T, repo string) { write(t, repo, "a/testdata/sub/second_recorded.txt", "and this\n\n") },
			[][]string{{"a/testdata/sub/second_recorded.txt changed", "-recorded-again"}}},
		{"one byte more in a file under tests/fixtures", func(t *testing.T, repo string) { write(t, repo, "tests/fixtures/x_python_golden.json", `{"x": 1.50}`) },
			[][]string{{"tests/fixtures/x_python_golden.json changed", "its row in tests/fixtures.manifest.tsv holds", "-recorded-again"}}},
		{"one byte more in a provider's recorded page", func(t *testing.T, repo string) { write(t, repo, "a/testdata/pages/page_0.json", `{"page": 0} `) },
			[][]string{{"a/testdata/pages/page_0.json changed", "-kind provider-recorded"}}},
		{"one byte more in an unclassified file", func(t *testing.T, repo string) { write(t, repo, "a/testdata/old.sql", "select 2;\n") },
			[][]string{{"a/testdata/old.sql changed", "-kind unclassified"}}},
		{"a file deleted", func(t *testing.T, repo string) { remove(t, repo, "a/testdata/case.json") },
			[][]string{{"a/testdata.manifest.tsv has a row for case.json and the file is gone", "-sync"}}},
		{"a manifest deleted", func(t *testing.T, repo string) { remove(t, repo, "tests/fixtures.manifest.tsv") },
			[][]string{{"tests/fixtures has no manifest (tests/fixtures.manifest.tsv)"}}},
		{"a new testdata directory with no manifest", func(t *testing.T, repo string) { write(t, repo, "b/c/testdata/in.json", "{}") },
			[][]string{{"b/c/testdata has no manifest (b/c/testdata.manifest.tsv)", "b/c/testdata/<file>"}}},
		{"a directory deleted, its manifest left", func(t *testing.T, repo string) { remove(t, repo, "tests/fixtures") },
			[][]string{{"tests/fixtures.manifest.tsv is the manifest of a directory that is gone", "-sync"}}},
		{"a recorded file and its row's digest changed by hand, together", func(t *testing.T, repo string) {
			old, _ := Digest(repo, "a/testdata/recorded.json")
			write(t, repo, "a/testdata/recorded.json", `{"python": "said that"}`)
			now, _ := Digest(repo, "a/testdata/recorded.json")
			write(t, repo, "a/testdata.manifest.tsv", strings.Replace(read(t, repo, "a/testdata.manifest.tsv"), old, now, 1))
		}, [][]string{{"a/testdata.manifest.tsv is not in the form the verb writes", "python-recorded set line does not match its rows"}}},
		{"two rows swapped by hand", func(t *testing.T, repo string) {
			lines := strings.Split(read(t, repo, "a/testdata.manifest.tsv"), "\n")
			lines[2], lines[3] = lines[3], lines[2]
			write(t, repo, "a/testdata.manifest.tsv", strings.Join(lines, "\n"))
		}, [][]string{{"is not in the form the verb writes"}}},
		{"a kind that does not exist, typed by hand", func(t *testing.T, repo string) {
			write(t, repo, "a/testdata.manifest.tsv", strings.Replace(read(t, repo, "a/testdata.manifest.tsv"), "\thand-written\t", "\tfixture\t", 1))
		}, [][]string{{`kind "fixture" is not one of`}}},
		{"a recorded row turned into a hand-written row by hand", func(t *testing.T, repo string) {
			write(t, repo, "a/testdata.manifest.tsv", strings.Replace(read(t, repo, "a/testdata.manifest.tsv"), "\tpython-recorded\trecorded.json", "\thand-written\trecorded.json", 1))
		}, [][]string{{"is not in the form the verb writes"}}},
		{"the kind header on a file with no golden header", func(t *testing.T, repo string) {
			write(t, repo, "a/testdata/golden/TestX.json", `{"requests": []}`)
		}, [][]string{{"a/testdata/golden/TestX.json has the kind header", "holds no golden header"}}},
		{"a golden with a header recorded again (its own test pins its bytes)", func(t *testing.T, repo string) {
			write(t, repo, "a/testdata/golden/TestX.json", strings.Replace(goldenText, `"requests": []`, `"requests": [1]`, 1))
		}, nil},
		{"the record verb's candidate beside a golden", func(t *testing.T, repo string) { write(t, repo, "a/testdata/golden/TestX.json.recording", "{}") }, nil},
		{"an unclassified file that is not on the day-one list", func(t *testing.T, repo string) {
			write(t, repo, DayOneList, "# none\n")
		}, [][]string{{"a/testdata/old.sql is unclassified in a/testdata.manifest.tsv and is not on the day-one list", "-kind <"}, {"holds 0 files and unclassifiedDayOne says 1"}}},
		{"a file on the day-one list that has a kind now, the list left as it was", func(t *testing.T, repo string) {
			list := read(t, repo, DayOneList)
			set(t, repo, Change{Kind: HandWritten}, "a/testdata/old.sql")
			write(t, repo, DayOneList, list)
		}, [][]string{{"a/testdata/old.sql is on the day-one list", "-kind <kind> a/testdata/old.sql", "lower unclassifiedDayOne by one"}}},
		{"a line added to the day-one list for a new file", func(t *testing.T, repo string) {
			write(t, repo, "a/testdata/later.bin", "x")
			set(t, repo, Change{Kind: Unclassified, DayOne: true}, "a/testdata/later.bin")
		}, [][]string{{"holds 2 files and unclassifiedDayOne says 1", "the list only shrinks"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			repo := tree(t)
			if before := found(t, repo, 1); len(before) != 0 {
				t.Fatalf("the tree the verb wrote is refused: %q", before)
			}
			row.do(t, repo)
			got := found(t, repo, 1)
			if len(got) != len(row.want) {
				t.Fatalf("problems = %q, want %d", got, len(row.want))
			}
			for index, texts := range row.want {
				for _, text := range texts {
					if !strings.Contains(got[index], text) {
						t.Errorf("problem %d = %q, want it to hold %q", index, got[index], text)
					}
				}
			}
		})
	}
}

// The set line of a manifest: any recorded Python answer that changes, comes
// or goes changes it, and nothing else does.
func TestThePythonRecordedSetLineFollowsOnlyTheRecordedRows(t *testing.T) {
	repo := tree(t)
	line := func() string { return strings.Split(read(t, repo, "a/testdata.manifest.tsv"), "\n")[1] }
	first := line()
	if !strings.HasPrefix(first, "# python-recorded set: ") || !strings.HasSuffix(first, " (2 files)") {
		t.Fatalf("set line = %q", first)
	}
	write(t, repo, "a/testdata/case.json", `{"case": 2}`)
	set(t, repo, Change{Kind: HandWritten}, "a/testdata/case.json")
	write(t, repo, "a/testdata/more.json", "{}")
	set(t, repo, Change{Kind: GoGenerated}, "a/testdata/more.json")
	if line() != first {
		t.Errorf("the set line changed with files that are not recorded Python answers: %q, was %q", line(), first)
	}
	write(t, repo, "a/testdata/recorded.json", `{"python": "said that"}`)
	set(t, repo, Change{Kind: PythonRecorded, RecordedAgain: true}, "a/testdata/recorded.json")
	changed := line()
	if changed == first {
		t.Error("a recorded answer changed and the set line did not")
	}
	write(t, repo, "a/testdata/third_recorded.json", "{}")
	set(t, repo, Change{Kind: PythonRecorded}, "a/testdata/third_recorded.json")
	if added := line(); added == changed || !strings.HasSuffix(added, " (3 files)") {
		t.Errorf("a recorded answer was added and the set line is %q", added)
	}
	remove(t, repo, "a/testdata/third_recorded.json")
	if _, err := Sync(repo, Change{RecordedAgain: true}); err != nil {
		t.Fatal(err)
	}
	if line() != changed {
		t.Errorf("a recorded answer was removed and the set line is %q, want %q", line(), changed)
	}
}

func TestTheVerbRefusesWhatOnlyARecordingMayChange(t *testing.T) {
	refused := func(t *testing.T, err error, text string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), text) {
			t.Errorf("err = %v, want a refusal holding %q", err, text)
		}
	}
	t.Run("a recorded answer with other bytes", func(t *testing.T) {
		repo := tree(t)
		before := read(t, repo, "a/testdata.manifest.tsv")
		write(t, repo, "a/testdata/recorded.json", `{"python": "said that"}`)
		_, err := Set(repo, []string{"a/testdata/recorded.json"}, Change{Kind: PythonRecorded})
		refused(t, err, "changes only when it is recorded again")
		if read(t, repo, "a/testdata.manifest.tsv") != before {
			t.Error("the manifest was written on a refusal")
		}
		if _, err := Set(repo, []string{"a/testdata/recorded.json"}, Change{Kind: PythonRecorded, RecordedAgain: true}); err != nil {
			t.Errorf("with -recorded-again: %v", err)
		}
		if got := found(t, repo, 1); len(got) != 0 {
			t.Errorf("after -recorded-again: %q", got)
		}
	})
	t.Run("a recorded answer given another kind", func(t *testing.T) {
		repo := tree(t)
		_, err := Set(repo, []string{"a/testdata/recorded.json"}, Change{Kind: HandWritten})
		refused(t, err, "changes only when it is recorded again")
	})
	t.Run("a recorded answer deleted", func(t *testing.T) {
		repo := tree(t)
		remove(t, repo, "a/testdata/recorded.json")
		_, err := Sync(repo, Change{})
		refused(t, err, "goes only on purpose")
		if _, err := Sync(repo, Change{RecordedAgain: true}); err != nil {
			t.Fatal(err)
		}
		if got := found(t, repo, 1); len(got) != 0 {
			t.Errorf("after -sync -recorded-again: %q", got)
		}
	})
	t.Run("the directory of recorded answers deleted", func(t *testing.T) {
		repo := tree(t)
		remove(t, repo, "tests/fixtures")
		_, err := Sync(repo, Change{})
		refused(t, err, "its directory is gone")
		if _, err := Sync(repo, Change{RecordedAgain: true}); err != nil {
			t.Fatal(err)
		}
		if got := found(t, repo, 1); len(got) != 0 {
			t.Errorf("after -sync -recorded-again: %q", got)
		}
	})
	t.Run("the kind unclassified for a new file", func(t *testing.T) {
		repo := tree(t)
		write(t, repo, "a/testdata/new.bin", "x")
		_, err := Set(repo, []string{"a/testdata/new.bin"}, Change{Kind: Unclassified})
		refused(t, err, "is not a kind a file can be given")
	})
	t.Run("a kind that does not exist", func(t *testing.T) {
		repo := tree(t)
		_, err := Set(repo, []string{"a/testdata/case.json"}, Change{Kind: "fixture"})
		refused(t, err, `kind "fixture" is not one of`)
	})
	t.Run("no kind for a file that is not a golden", func(t *testing.T) {
		repo := tree(t)
		write(t, repo, "a/testdata/new.json", "{}")
		_, err := Set(repo, []string{"a/testdata/new.json"}, Change{})
		refused(t, err, "needs a kind")
	})
	t.Run("another kind for a golden with a header", func(t *testing.T) {
		repo := tree(t)
		_, err := Set(repo, []string{"a/testdata/golden/TestX.json"}, Change{Kind: HandWritten})
		refused(t, err, "holds a golden header")
	})
	t.Run("the kind header for a file with no header", func(t *testing.T) {
		repo := tree(t)
		_, err := Set(repo, []string{"a/testdata/case.json"}, Change{Kind: Header})
		refused(t, err, "holds no golden header")
	})
	t.Run("a file outside the roots", func(t *testing.T) {
		repo := tree(t)
		write(t, repo, "a/other/x.json", "{}")
		_, err := Set(repo, []string{"a/other/x.json"}, Change{Kind: HandWritten})
		refused(t, err, "is not under a testdata directory")
	})
}

// What the verb does for the ordinary cases: a file added, a file changed, a
// file deleted, an unclassified file given a kind.
func TestTheVerbWritesWhatTheGuardAccepts(t *testing.T) {
	repo := tree(t)
	write(t, repo, "a/testdata/new.json", "{}")
	write(t, repo, "b/testdata/first.json", "{}")
	set(t, repo, Change{Kind: HandWritten}, "a/testdata/new.json", "b/testdata/first.json")
	write(t, repo, "a/testdata/case.json", `{"case": 2}`)
	set(t, repo, Change{Kind: HandWritten}, "a/testdata/case.json")
	remove(t, repo, "a/testdata/pages/page_0.json")
	if _, err := Sync(repo, Change{}); err != nil {
		t.Fatal(err)
	}
	if got := found(t, repo, 1); len(got) != 0 {
		t.Fatalf("problems after the verb: %q", got)
	}
	rows := strings.Split(strings.TrimSpace(read(t, repo, "a/testdata.manifest.tsv")), "\n")[2:]
	var paths []string
	for _, row := range rows {
		paths = append(paths, strings.Split(row, "\t")[2])
	}
	if got, want := strings.Join(paths, " "), "case.json golden/TestX.json new.json old.sql recorded.json sub/second_recorded.txt"; got != want {
		t.Errorf("rows = %q, want %q (in order of path)", got, want)
	}
	if !strings.Contains(read(t, repo, "a/testdata.manifest.tsv"), "-\theader\tgolden/TestX.json\n") {
		t.Error("the golden's row holds a digest, or has another kind")
	}
	notes, err := Set(repo, []string{"a/testdata/old.sql"}, Change{Kind: HandWritten})
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || !strings.Contains(notes[0], "the day-one list now holds 0 files") {
		t.Errorf("notes = %q, want the new length of the day-one list", notes)
	}
	if got := found(t, repo, 0); len(got) != 0 {
		t.Errorf("after the last day-one file got a kind: %q", got)
	}
}

func TestRootsAreTheOutermostTestdataDirectoriesAndTheFixtures(t *testing.T) {
	repo := t.TempDir()
	for _, file := range []string{"go.mod", "a/testdata/x", "a/testdata/in/testdata/y", "a/b/testdata/z", "tests/fixtures/f", "tests/other/g",
		".hidden/testdata/h", "node_modules/p/testdata/i", "testdata/top"} {
		write(t, repo, file, "x")
	}
	roots, err := Roots(repo)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(roots, " "), "a/b/testdata a/testdata testdata tests/fixtures"; got != want {
		t.Errorf("roots = %q, want %q", got, want)
	}
	files, err := Files(repo, "a/testdata")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(files, " "), "in/testdata/y x"; got != want {
		t.Errorf("files = %q, want %q", got, want)
	}
	for file, want := range map[string]string{"a/testdata/in/testdata/y": "a/testdata|in/testdata/y", "tests/fixtures/d/f.json": "tests/fixtures|d/f.json", "testdata/top": "testdata|top"} {
		root, relative, err := RootOf(file)
		if err != nil || root+"|"+relative != want {
			t.Errorf("RootOf(%s) = %s|%s, %v, want %s", file, root, relative, err, want)
		}
	}
}
