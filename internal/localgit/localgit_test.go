package localgit

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

func TestModeText(t *testing.T) {
	for mode, want := range map[string]string{
		"100644": "33188", "100755": "33261", "120000": "40960", "160000": "57344",
		"000000": "000000", "": "000000", "0644": "420",
	} {
		if got := modeText(mode); got != want {
			t.Errorf("modeText(%q) = %q, want %q", mode, got, want)
		}
	}
}

func TestRepoIDIsTheDigestOfTheRemoteOrPath(t *testing.T) {
	if got := digestUUID("/tmp/some/repo").String(); got != "2ee1ea9e-70bb-62c6-d667-c117ed6ad99a" {
		t.Errorf("path digest = %s", got)
	}
	if got := digestUUID("https://example.com/acme/widgets.git").String(); got != "84803486-d353-a48d-6b0e-6a3eee4cd210" {
		t.Errorf("url digest = %s", got)
	}
}

func TestParseRawDiff(t *testing.T) {
	raw := ":100644 100644 aaa bbb M\x00a.txt\x00:100644 100755 aaa bbb R100\x00old.txt\x00new.txt\x00:000000 100644 0 bbb A\x00added.txt\x00"
	diffs, ok := parseRawDiff([]byte(raw))
	if !ok || len(diffs) != 3 {
		t.Fatalf("diffs = %+v, ok = %v", diffs, ok)
	}
	if diffs[1].APath != "old.txt" || diffs[1].BPath != "new.txt" || diffs[1].NewMode != "100755" {
		t.Errorf("rename = %+v", diffs[1])
	}
	if diffs[2].OldMode != "000000" || diffs[2].APath != "added.txt" {
		t.Errorf("add = %+v", diffs[2])
	}
	// A newline in a path cuts the entry there (readline chunks), and the rest is dropped.
	diffs, ok = parseRawDiff([]byte(":100644 100644 a b A\x00new\nline.txt\x00"))
	if !ok || len(diffs) != 1 || diffs[0].BPath != "new" {
		t.Errorf("newline path = %+v, ok = %v", diffs, ok)
	}
	// A path that is not UTF-8 raises in Python: the whole commit has no rows.
	if _, ok := parseRawDiff([]byte(":100644 100644 a b A\x00bad\xff.txt\x00")); ok {
		t.Error("invalid UTF-8 was accepted")
	}
	// A chunk that cannot be unpacked raises too.
	if _, ok := parseRawDiff([]byte(":100644 100644 a b\x00x\x00")); ok {
		t.Error("a short header was accepted")
	}
}

func TestPullRequestNumbers(t *testing.T) {
	cases := []struct {
		message string
		number  int
		ok      bool
		gitlab  bool
	}{
		{"Merge pull request #12 from a/b\n\nTitle", 12, true, false},
		{"Merge pull request #12abc from a/b", 0, false, false},
		{"Merge pull request #12é from a/b", 0, false, false},
		{"Merge pull request #12_ from a/b", 0, false, false},
		{"Merge pull request #９９ from a/b", 99, true, false},
		{"Title\n\nMerge pull request #7 later", 7, true, false},
		{"prefix Merge pull request #7", 0, false, false},
		{"x\nSee merge request g/p!41 and g/o!42", 42, true, true},
		{"See merge request g/p!41abc", 0, false, true},
		{"See merge requests !5", 0, false, true},
		{"aSee merge request !5", 0, false, true},
		{"See merge request\n!5", 0, false, true},
	}
	for _, tc := range cases {
		var number int
		var ok bool
		if tc.gitlab {
			number, ok = gitlabNumber(tc.message)
		} else {
			number, ok = prNumber(githubMergeRE, tc.message)
		}
		if number != tc.number || ok != tc.ok {
			t.Errorf("%q = %d, %v; want %d, %v", tc.message, number, ok, tc.number, tc.ok)
		}
	}
}

func TestFirstMeaningfulTitleLine(t *testing.T) {
	for message, want := range map[string]string{
		"Merge pull request #1 from a/b\n\n  Real title  \n":   "Real title",
		"Merge branch 'x'\nSee merge request g/p!2\n\t\nThird": "Third",
		"   spaced  ": "spaced",
	} {
		got := firstMeaningfulTitleLine(message)
		if got == nil || *got != want {
			t.Errorf("%q = %v, want %q", message, got, want)
		}
	}
	for _, message := range []string{"", "Merge branch 'a'\n", "  \n \n"} {
		if got := firstMeaningfulTitleLine(message); got != nil {
			t.Errorf("%q = %q, want nil", message, *got)
		}
	}
}

func TestParseActorAndDate(t *testing.T) {
	cases := []struct {
		line, name, email string
		hasEmail          bool
		epoch             int64
	}{
		{"author Jane Doe <jane@example.com> 1700000000 +0530", "Jane Doe", "jane@example.com", true, 1700000000},
		{"author Just A Name 1700000000 +0000", "Just A Name", "", false, 1700000000},
		{"author <only@example.com> 1700000000 +0000", "", "only@example.com", true, 1700000000},
		{"committer   Spaced   <c@example.com> 1700003600 -0700", "  Spaced", "c@example.com", true, 1700003600},
		{"author A <a@b> <c@d> 5 +0000", "A", "a@b", true, 5},
		{"author Name <a@b> １７００００００００ +0000", "Name", "a@b", true, 1700000000},
	}
	for _, tc := range cases {
		name, email, epoch := parseActorAndDate(tc.line)
		if name == nil || *name != tc.name || (email != nil) != tc.hasEmail || (tc.hasEmail && *email != tc.email) || epoch.value != tc.epoch {
			t.Errorf("%q = %v %v %d", tc.line, name, email, epoch.value)
		}
	}
}

func TestPythonJSONString(t *testing.T) {
	for in, want := range map[string]string{
		"plain": `"plain"`, `q"uote`: `"q\"uote"`, "tab\t": `"tab\t"`, "\x01": `"\u0001"`, "é✓": `"é✓"`, `back\slash`: `"back\\slash"`, "\x7f": "\"\x7f\"",
	} {
		if got := pythonJSONString(in); got != want {
			t.Errorf("pythonJSONString(%q) = %s, want %s", in, got, want)
		}
	}
}

// git in a temporary repository: numstat pairing, binary files, root commits.
func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=T", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=T", "GIT_COMMITTER_EMAIL=t@example.com",
		"GIT_AUTHOR_DATE=1700000000 +0000", "GIT_COMMITTER_DATE=1700000000 +0000")
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestCommitStatsOfARealRepository(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git binary")
	}
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a.txt", "1\n2\n")
	write("bin.dat", "\x00\x01")
	write("keep.txt", "same\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "root")
	write("a.txt", "1\n2\n3\n")
	write("bin.dat", "\x00\x02")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "second")

	repo, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	commits, err := repo.IterCommitsSince(context.Background(), nil)
	if err != nil || len(commits) != 2 {
		t.Fatalf("commits = %d, %v", len(commits), err)
	}
	stats := map[string]CommitStat{}
	for _, row := range repo.CommitStats(context.Background(), commits[0]) {
		stats[row.FilePath] = row
	}
	if got := stats["a.txt"]; got.Additions != 1 || got.Deletions != 0 || got.OldFileMode != "33188" || got.NewFileMode != "33188" {
		t.Errorf("a.txt = %+v", got)
	}
	if got := stats["bin.dat"]; got.Additions != 0 || got.Deletions != 0 {
		t.Errorf("a binary file counts 0/0, got %+v", got)
	}
	// The root commit is diffed against the WORKING TREE: its rows are the files
	// that differ from it now (keep.txt does not), and the counts are the ones
	// `git diff-tree --root` gives for the root commit itself.
	root := map[string]CommitStat{}
	for _, row := range repo.CommitStats(context.Background(), commits[1]) {
		root[row.FilePath] = row
	}
	if len(root) != 2 || root["a.txt"].Additions != 2 || root["bin.dat"].Additions != 0 {
		t.Errorf("root rows = %v", root)
	}
	if _, ok := root["keep.txt"]; ok {
		t.Errorf("an unchanged file must not show against the root commit, got %v", root)
	}
}

func TestIterCommitsSinceStopsAtTheFirstOldCommit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git binary")
	}
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	commit := func(date string, name string) {
		if err := os.WriteFile(filepath.Join(dir, "f"), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
		gitIn(t, dir, "add", "-A")
		command := exec.Command("git", "commit", "-q", "-m", name)
		command.Dir = dir
		command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=T", "GIT_AUTHOR_EMAIL=t@e", "GIT_COMMITTER_NAME=T", "GIT_COMMITTER_EMAIL=t@e", "GIT_AUTHOR_DATE="+date, "GIT_COMMITTER_DATE="+date)
		if out, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
	}
	commit("1600000000 +0000", "old")
	commit("1700000000 +0000", "new")
	commit("1600000100 +0000", "skewed")
	commit("1700100000 +0000", "newest")
	repo, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	since := time.Unix(1_650_000_000, 0).UTC()
	commits, err := repo.IterCommitsSince(context.Background(), &since)
	if err != nil {
		t.Fatal(err)
	}
	// Newest first: the walk stops at "skewed", which hides "new" behind it.
	if len(commits) != 1 || strings.TrimSpace(commits[0].Message) != "newest" {
		t.Fatalf("commits = %d, first %q", len(commits), commits[0].Message)
	}
}

func TestOpenRefusesAFolderWithoutGit(t *testing.T) {
	if _, err := Open(t.TempDir()); err == nil || !strings.Contains(err.Error(), "no git repository") {
		t.Fatalf("err = %v", err)
	}
}

func TestClassifyEpoch(t *testing.T) {
	for _, tc := range []struct {
		epoch int64
		fits  bool
		want  int
	}{
		{0, true, timeOK}, {1700000000, true, timeOK}, {253402300799, true, timeOK}, {253402300800, true, timeValueError},
		{1 << 55, true, timeValueError}, {67768036191676799, true, timeValueError}, {67768036191676800, true, timeOtherError},
		{1<<62 - 1, true, timeOtherError}, {1 << 62, true, timeOtherError}, {0, false, timeOtherError},
	} {
		if got := classifyEpoch(tc.epoch, tc.fits); got != tc.want {
			t.Errorf("classifyEpoch(%d, %v) = %d, want %d", tc.epoch, tc.fits, got, tc.want)
		}
	}
	if got := unicodeDigitsToInt64("９９９９９９９９９９９９９９９９９９９９９"); got.fits {
		t.Errorf("a value past int64 must not fit: %+v", got)
	}
	if got := unicodeDigitsToInt64("𝟏𝟐"); !got.fits || got.value != 12 {
		t.Errorf("mathematical digits = %+v", got)
	}
}

func TestRepresentableInstants(t *testing.T) {
	at := func(year int, month time.Month) time.Time { return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC) }
	for name, tc := range map[string]struct {
		when time.Time
		want bool
	}{
		"1600": {at(1600, 6), false}, "june 1677": {at(1677, 6), false}, "1678": {at(1678, 6), true}, "1900": {at(1900, 1), true},
		"2023": {at(2023, 1), true}, "january 2262": {at(2262, 1), true}, "june 2262": {at(2262, 6), false}, "2263": {at(2263, 1), false}, "5138": {at(5138, 1), false},
	} {
		if got := representable(tc.when); got != tc.want {
			t.Errorf("representable(%s) = %v, want %v", name, got, tc.want)
		}
	}
}

func TestOpenTreatsADanglingGitSymlinkAsNoRepository(t *testing.T) {
	dir := t.TempDir()
	if err := os.Symlink(filepath.Join(dir, "nowhere"), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "no git repository") {
		t.Fatalf("a dangling .git symlink does not exist to Path.exists(): err = %v", err)
	}
}

func refRepo(t *testing.T) (Repo, string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git binary")
	}
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "first")
	repo, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return repo, gitIn(t, dir, "rev-parse", "HEAD")
}

func setRef(t *testing.T, repo Repo, name, target string) {
	t.Helper()
	path := filepath.Join(repo.Root, ".git", filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(target+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestTagsAreReadLikeGitPythonReadsThem(t *testing.T) {
	repo, head := refRepo(t)
	for _, name := range []string{"plain", "ends ", " starts", "has space", "trail.", "nested/one", "Zeta"} {
		setRef(t, repo, "refs/tags/"+name, head)
	}
	got, err := repo.TagsJSON(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Sorted by code point; whitespace kept; names git itself would refuse are listed too.
	want := "[\"Zeta\",\"ends\u2003\",\"has space\",\"nested/one\",\"plain\",\"trail.\",\"\u2003starts\"]"
	if got != want {
		t.Errorf("tags = %s, want %s", got, want)
	}
	// A packed ref goes through GitPython's line.strip(): the trailing em space is gone.
	gitIn(t, repo.Root, "pack-refs", "--all")
	wantPacked := "[\"Zeta\",\"ends\",\"has space\",\"nested/one\",\"plain\",\"trail.\",\"\u2003starts\"]"
	if again, _ := repo.TagsJSON(context.Background()); again != wantPacked {
		t.Errorf("packed tags = %s, want %s", again, wantPacked)
	}
	setRef(t, repo, "refs/tags/bad\xffname", head)
	if _, err := repo.TagsJSON(context.Background()); err == nil {
		t.Error("a tag name that is not UTF-8 cannot be written and must fail the run")
	}
}

func TestRefNameIsThePathWithoutItsFirstTwoComponents(t *testing.T) {
	for path, want := range map[string]string{"refs/tags/v1": "v1", "refs/tags/a/b": "a/b", "refs/tags-old/x": "x", "refs/tags": "refs/tags"} {
		if got := refName(path); got != want {
			t.Errorf("refName(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestOpenPullRequestRefsResolveLikeGitPython(t *testing.T) {
	repo, head := refRepo(t)
	ctx := context.Background()
	tree := gitIn(t, repo.Root, "rev-parse", "HEAD^{tree}")
	blob := gitIn(t, repo.Root, "rev-parse", "HEAD:a.txt")
	tag := func(target, kind, name string) string {
		command := exec.Command("git", "hash-object", "-t", "tag", "-w", "--literally", "--stdin")
		command.Dir = repo.Root
		command.Stdin = strings.NewReader("object " + target + "\ntype " + kind + "\ntag " + name + "\ntagger T <t@e> 1700000000 +0000\n\nm\n")
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	inner := tag(head, "commit", "inner")
	setRef(t, repo, "refs/pull/1/head", head)                       // a commit
	setRef(t, repo, "refs/pull/2/head", tag(head, "commit", "t"))   // an annotated tag, peeled once
	setRef(t, repo, "refs/pull/3/head", tag(inner, "tag", "outer")) // a tag of a tag: not a commit
	setRef(t, repo, "refs/pull/4/head", tag(tree, "tree", "tt"))    // a tag of a tree
	setRef(t, repo, "refs/pull/5/head", blob)                       // a blob
	setRef(t, repo, "refs/pull/6/head", tree)                       // a tree
	setRef(t, repo, "refs/pull/7/head", strings.Repeat("ab", 20))   // a missing object
	setRef(t, repo, "refs/pull/８/head", head)                       // fullwidth digit
	setRef(t, repo, "refs/merge-requests/9/head", head)
	setRef(t, repo, "refs/pull/10/merge", head) // not a head ref
	rows, err := repo.InferOpenPullRequests(ctx, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int]string{}
	for _, row := range rows {
		got[row.Number] = *row.HeadBranch
	}
	want := map[int]string{1: "refs/pull/1/head", 2: "refs/pull/2/head", 8: "refs/pull/８/head", 9: "refs/merge-requests/9/head"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("open pull requests = %v, want %v", got, want)
	}
}

func TestPackedRefsHeadersAndLinesLikeGitPython(t *testing.T) {
	repo, head := refRepo(t)
	write := func(content string) {
		if err := os.WriteFile(filepath.Join(repo.Root, ".git", "packed-refs"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	write("# pack-refs with: other\n" + head + " refs/tags/x\n")
	if _, err := repo.refPaths(ctx, "refs/tags"); err == nil {
		t.Error("a packing scheme without `peeled` must fail")
	}
	write("# pack-refs with: peeled fully-peeled sorted \n" + head + "\n")
	if _, err := repo.refPaths(ctx, "refs/tags"); err == nil {
		t.Error("a line with no path cannot be unpacked")
	}
	write("# a comment\n\n" + head + " refs/tags/a\n^" + head + "\n" + head + " refs/tags/b\x1f\n" + head + " refs/tags-old/c\n" + head + " refs/heads/main\n")
	paths, err := repo.refPaths(ctx, "refs/tags")
	if err != nil || !reflect.DeepEqual(paths, []string{"refs/tags-old/c", "refs/tags/a", "refs/tags/b"}) {
		t.Errorf("paths = %q, err = %v (a name is stripped of control whitespace; a sibling that starts alike is listed)", paths, err)
	}
	write(head + " refs/heads/bad\xff\n")
	if _, err := repo.refPaths(ctx, "refs/tags"); !errors.Is(err, errPackedRefsNotUTF8) {
		t.Errorf("err = %v", err)
	}
}

// A rerun over a row another writer filled keeps what the local source has no field for
// (stored-version R1) and never un-merges (R3). `dev-hops` inserts blindly and CLEARS those
// columns: a named divergence, pinned here so it cannot drift silently.
func TestARerunKeepsWhatTheLocalSourceHasNoFieldFor(t *testing.T) {
	repoID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	merged := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	row, err := pullRequestRow(repoID, "org", PullRequest{Number: 7, State: "open", CreatedAt: merged}, merged)
	if err != nil {
		t.Fatal(err)
	}
	held := map[string]any{
		"body": "external body", "additions": uint32(7), "deletions": uint32(2), "changed_files": uint32(3),
		"first_review_at": merged, "first_comment_at": merged, "changes_requested_count": uint32(1), "reviews_count": uint32(4),
		"comments_count": uint32(5), "merged_at": merged,
	}
	rows := []storedversion.Row{{Values: row, Carry: pullRequestContract.Carry(func(string) bool { return false })}}
	if _, err := pullRequestContract.Fold(pullRequestInsert, rows, func([]any) (map[string]any, bool) { return held, true }); err != nil {
		t.Fatal(err)
	}
	positions, _ := storedversion.Positions(pullRequestInsert)
	for column, want := range held {
		if got := rows[0].Values[positions[column]]; !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %v, want the held %v", column, got, want)
		}
	}
	if rows[0].Values[positions["state"]] != "open" || rows[0].Values[positions["title"]] != nil {
		t.Errorf("stated columns are written as given: %v", rows[0].Values)
	}
}

func TestNumbersPastTheColumnsAreRefusedNotWrapped(t *testing.T) {
	for digits, wantSaturated := range map[string]bool{
		"4294967295": false, "4294967296": false, "9223372036854775807": false,
		"18446744073709551616": true, "18446744073709551617": true, "99999999999999999999999999": true,
	} {
		got, ok := decimalValue(digits)
		if !ok || (got == math.MaxInt) != wantSaturated && digits != "9223372036854775807" {
			t.Errorf("decimalValue(%s) = %d, %v", digits, got, ok)
		}
		if got < 0 {
			t.Errorf("decimalValue(%s) wrapped negative: %d", digits, got)
		}
		row := func() error {
			_, err := pullRequestRow(uuid.Nil, "o", PullRequest{Number: got, State: "open", CreatedAt: time.Now()}, time.Now())
			return err
		}
		if want := got > math.MaxUint32; (row() != nil) != want {
			t.Errorf("pull request number %s: refused = %v, want %v", digits, row() != nil, want)
		}
	}
	if numstatCount("99999999999999999999") != math.MaxInt || numstatCount("-") != 0 || numstatCount("x") != -1 || numstatCount("12") != 12 {
		t.Error("numstat counts saturate, `-` is 0, garbage is -1")
	}
	writer := Writer{}
	if err := writer.InsertCommitStats(context.Background(), uuid.Nil, []CommitStat{{CommitHash: "h", FilePath: "f", Additions: math.MaxInt32 + 1}}); err == nil {
		t.Error("a line count past Int32 must fail the insert")
	}
}

func TestTimezoneOffsetsPastATimedelta(t *testing.T) {
	for offset, want := range map[string]bool{
		"+0000": false, "+2359": false, "+2400": false, "+9999": false, "+99999999999": false, "-99999999999": false,
		"+240000000100": false, "+2399999999959": false, "+2400000000000": true, "+2400000000099": true, "+18446744073709551617": true,
	} {
		if got := tzOverflows(offset); got != want {
			t.Errorf("tzOverflows(%s) = %v, want %v", offset, got, want)
		}
	}
	commit := parseCommit("h", []byte("tree t\nauthor A <a@b> 1700000000 +0000\ncommitter C <c@b> 1700000000 +18446744073709551617\n\nm"))
	if commit.TimeClass != timeOtherError {
		t.Errorf("a committer offset past a timedelta is the OverflowError class, got %d", commit.TimeClass)
	}
	author := parseCommit("h", []byte("tree t\nauthor A <a@b> 1700000000 +18446744073709551617\ncommitter C <c@b> 1700000000 +0000\n\nm"))
	if author.TimeClass != timeOK {
		t.Errorf("the author offset is never read, got %d", author.TimeClass)
	}
}
