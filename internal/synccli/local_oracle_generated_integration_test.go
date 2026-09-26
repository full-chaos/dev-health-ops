//go:build integration

package synccli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// generatedScenarios is the class-wide corpus of the local-sync oracle: the
// git object model (refs, tags, paths, repository states) crossed with the value
// classes the Python producer (GitPython, pathlib, re, json) treats in its own
// way, so a divergence is found by the cross product and not by the four values
// a reviewer happened to try. Each family names its classes; a new class is one
// row, and every row runs against the real Python verb.
func generatedScenarios() []localScenario {
	var out []localScenario
	out = append(out, prRefNameScenarios()...)
	out = append(out, refTargetScenarios()...)
	out = append(out, tagNameScenarios()...)
	out = append(out, pathNameScenarios()...)
	out = append(out, repositoryStateScenarios()...)
	out = append(out, ambientGitEnvScenarios()...)
	out = append(out, tipTimeScenarios()...)
	out = append(out, linkedWorktreeScenarios()...)
	out = append(out, refValueScenarios()...)
	out = append(out, packedPullRefScenarios()...)
	out = append(out, packedRefsScenarios()...)
	out = append(out, mergeMessageScenarios()...)
	out = append(out, identityScenarios()...)
	out = append(out, blameTimeScenarios()...)
	out = append(out, blameBatchScenarios()...)
	return out
}

// twoCommits gives a repository with a merge target (`first`, `second`) for refs to point at.
func twoCommits(f *fixture) (first, second string) {
	f.write("a.txt", "a\n", 0o644)
	first = f.commit("first\n")
	f.write("b.txt", "b\n", 0o644)
	second = f.commit("second\n", commitAt{tz: "+0530"})
	return first, second
}

// digitClasses are runs of Unicode decimal digits and the shapes around them
// that Python's `\d+` / int() treat as a number or as no match.
var digitClasses = []string{
	"12", "007", "0", "00", "１２" /* fullwidth */, "١٢" /* Arabic-Indic */, "१२" /* Devanagari */, "১২", /* Bengali */
	"1２" /* ASCII + fullwidth */, "𝟏𝟐" /* mathematical bold, non-BMP */, "4294967295", "4294967296", "99999999999999999999",
	"1_2", "12a", "+12", "-1", "²" /* superscript two: isdigit, not isdecimal */, "①" /* circled one: not \d */, "½",
	// The numeric boundaries: 2**31, 2**63 and 2**64 and the values just past them, and a long run of leading zeros.
	"2147483647", "2147483648", "9223372036854775807", "9223372036854775808",
	"18446744073709551615", "18446744073709551616", "18446744073709551617", "0000000000000000000000000012",
}

// prRefNameScenarios: refs/pull and refs/merge-requests with every digit class,
// and the malformed shapes around a valid ref.
func prRefNameScenarios() []localScenario {
	var out []localScenario
	for _, family := range []string{"pull", "merge-requests"} {
		for _, digits := range digitClasses {
			family, digits := family, digits
			out = append(out, localScenario{
				name: fmt.Sprintf("gen ref: refs/%s/%s/head", family, digits), targets: []string{"prs"},
				build: func(f *fixture) {
					first, _ := twoCommits(f)
					f.setRef("refs/"+family+"/"+digits+"/head", first)
				},
			})
		}
	}
	for _, shape := range []string{
		"refs/pull/12/merge", "refs/pull/12", "refs/pull//head", "refs/pull/12/head/extra", "refs/pulls/12/head", "refs/PULL/12/head",
		"refs/pull/12/head.lock", "refs/merge-request/12/head", "refs/heads/pull/12/head", "refs/remotes/origin/pull/12/head",
	} {
		shape := shape
		out = append(out, localScenario{name: "gen ref shape: " + shape, targets: []string{"prs"}, build: func(f *fixture) {
			first, _ := twoCommits(f)
			f.setRef(shape, first)
		}})
	}
	return out
}

// refTargetScenarios: what refs/pull/5/head points at.
func refTargetScenarios() []localScenario {
	type target struct {
		name  string
		build func(f *fixture, first, second string) string
	}
	targets := []target{
		{"commit", func(f *fixture, first, second string) string { return first }},
		{"annotated tag of a commit", func(f *fixture, first, second string) string { return f.tagObject(second, "commit", "t1") }},
		{"tag of a tag of a commit", func(f *fixture, first, second string) string {
			inner := f.tagObject(second, "commit", "inner")
			return f.tagObject(inner, "tag", "outer")
		}},
		{"tag of a tree", func(f *fixture, first, second string) string {
			return f.tagObject(f.git("rev-parse", second+"^{tree}"), "tree", "ttree")
		}},
		{"tag of a blob", func(f *fixture, first, second string) string {
			return f.tagObject(f.git("rev-parse", second+":a.txt"), "blob", "tblob")
		}},
		{"tree", func(f *fixture, first, second string) string { return f.git("rev-parse", second+"^{tree}") }},
		{"blob", func(f *fixture, first, second string) string { return f.git("rev-parse", second+":a.txt") }},
		{"a missing object", func(f *fixture, first, second string) string { return strings.Repeat("ab", 20) }},
		{"a tag whose object is missing", func(f *fixture, first, second string) string {
			return f.tagObject(strings.Repeat("cd", 20), "commit", "dangling")
		}},
		{"a tag naming the wrong type", func(f *fixture, first, second string) string {
			return f.tagObject(second, "blob", "wrongtype")
		}},
	}
	var out []localScenario
	for _, target := range targets {
		target := target
		for _, family := range []string{"refs/pull/5/head", "refs/merge-requests/5/head"} {
			family := family
			out = append(out, localScenario{name: fmt.Sprintf("gen ref target: %s -> %s", family, target.name), targets: []string{"prs"}, build: func(f *fixture) {
				first, second := twoCommits(f)
				f.setRef(family, target.build(f, first, second))
			}})
		}
	}
	out = append(out, localScenario{name: "gen ref target: a symbolic ref", targets: []string{"prs"}, build: func(f *fixture) {
		twoCommits(f)
		f.git("symbolic-ref", "refs/pull/8/head", "refs/heads/main")
	}})
	out = append(out, localScenario{name: "gen ref target: two kinds, one number, tag and commit", targets: []string{"prs"}, build: func(f *fixture) {
		first, second := twoCommits(f)
		f.setRef("refs/pull/5/head", first)
		f.setRef("refs/merge-requests/5/head", f.tagObject(second, "commit", "t"))
	}})
	return out
}

// tagNameScenarios: repos.tags is the JSON list of tag names, so every class of
// character a ref name can carry is a tag.
func tagNameScenarios() []localScenario {
	names := []string{
		"plain", "v1.0.0-rc.1", "with space-free", "ends " /* em space */, " starts", "nbsp ", " nbsp-start", "ideographic　", "　lead",
		"line sep", "next\u0085line", "ogham ", "mid dle", "écombining", "🚀rocket", "non-BMP-𝟏𝟐", "שלום", "日本語",
		"upper", "Upper", "Zeta", "alpha", "Ålpha", "a/b/c", "rel/1.0", "quote\"d", "back\\slash-not-allowed", "at@sign", "hash#tag", "pct%", "semi;colon",
		strings.Repeat("long", 60), "​zero-width", "tab-not-allowed\t", "bad\xffbytes", "trail.dot.",
	}
	var out []localScenario
	for i, name := range names {
		name := name
		for _, packed := range []bool{false, true} {
			packed := packed
			out = append(out, localScenario{name: fmt.Sprintf("gen tag %02d packed=%v", i, packed), targets: []string{"git"}, build: func(f *fixture) {
				first, _ := twoCommits(f)
				f.setRef("refs/tags/"+name, first)
				if packed {
					f.git("pack-refs", "--all")
				}
			}})
		}
	}
	out = append(out, localScenario{name: "gen tag: many tags", targets: []string{"git"}, build: func(f *fixture) {
		first, _ := twoCommits(f)
		for _, name := range names {
			f.setRef("refs/tags/many-"+name, first)
		}
		f.git("pack-refs", "--all")
	}})
	return out
}

// pathNameScenarios: file names in commits (the numstat key is stripped, the raw
// diff path is not; both are text Python decodes as UTF-8).
func pathNameScenarios() []localScenario {
	names := []string{
		" lead.txt", "trail .txt", "nbsp .txt", " nbsp.txt", "ｆｕｌｌ.txt", "é.txt", "🚀.txt", "sep .txt", "next\u0085.txt",
		"dir with space/file.txt", "dir /inner.txt", "a /b .txt", "-dash.txt", "semi;colon.txt", "hash#.txt", "pct%20.txt", "back\\slash.txt", "quote\"d.txt",
		"tab\there.txt", "new\nline.txt", "bell\x07.txt", "del\x7f.txt", "bad\xffbytes.txt", strings.Repeat("l", 200) + ".txt", "..dots.txt", ".hidden", "trailing.", "UPPER.TXT",
	}
	var out []localScenario
	for i, name := range names {
		name := name
		out = append(out, localScenario{name: fmt.Sprintf("gen path %02d", i), targets: []string{"git"}, build: func(f *fixture) {
			f.write("seed.txt", "seed\n", 0o644)
			f.commit("seed\n")
			path := filepath.Join(f.dir, name)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				f.t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("one\ntwo\n"), 0o644); err != nil {
				f.t.Fatal(err)
			}
			f.commit("add the file\n")
			if err := os.WriteFile(path, []byte("one\ntwo\nthree\n"), 0o644); err != nil {
				f.t.Fatal(err)
			}
			f.commit("change the file\n")
			f.git("mv", "--", name, name+"-renamed")
			f.commit("rename the file\n")
		}})
	}
	return out
}

// repositoryStateScenarios: the shapes of `.git` and of the path Python resolves.
func repositoryStateScenarios() []localScenario {
	basic := func(f *fixture) {
		f.write("a.txt", "a\n", 0o644)
		f.commit("root\n")
		f.write("a.txt", "b\n", 0o644)
		f.commit("second\n")
	}
	move := func(f *fixture) string { // the real git dir, moved beside the working tree
		real := filepath.Join(filepath.Dir(f.dir), filepath.Base(f.dir)+"-gitdir")
		if err := os.Rename(filepath.Join(f.dir, ".git"), real); err != nil {
			f.t.Fatal(err)
		}
		return real
	}
	var out []localScenario
	add := func(name string, build func(f *fixture), pathForm string) {
		out = append(out, localScenario{name: "gen state: " + name, build: build, pathForm: pathForm, targets: []string{"git", "prs"}})
	}
	add(".git is a symlink to a valid git dir", func(f *fixture) {
		basic(f)
		real := move(f)
		if err := os.Symlink(real, filepath.Join(f.dir, ".git")); err != nil {
			f.t.Fatal(err)
		}
	}, "")
	add(".git is a dangling symlink", func(f *fixture) {
		basic(f)
		if err := os.RemoveAll(filepath.Join(f.dir, ".git")); err != nil {
			f.t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(f.dir, "nowhere"), filepath.Join(f.dir, ".git")); err != nil {
			f.t.Fatal(err)
		}
	}, "")
	add(".git is a gitfile to a valid dir", func(f *fixture) {
		basic(f)
		real := move(f)
		f.write(".git", "gitdir: "+real+"\n", 0o644)
	}, "")
	add(".git is a gitfile to a missing dir", func(f *fixture) {
		basic(f)
		if err := os.RemoveAll(filepath.Join(f.dir, ".git")); err != nil {
			f.t.Fatal(err)
		}
		f.write(".git", "gitdir: /nonexistent/gitdir\n", 0o644)
	}, "")
	add(".git is an empty directory", func(f *fixture) {
		basic(f)
		if err := os.RemoveAll(filepath.Join(f.dir, ".git")); err != nil {
			f.t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(f.dir, ".git"), 0o755); err != nil {
			f.t.Fatal(err)
		}
	}, "")
	add(".git has a garbage HEAD", func(f *fixture) {
		basic(f)
		f.write(".git/HEAD", "garbage\n", 0o644)
	}, "")
	add(".git is an empty file", func(f *fixture) {
		basic(f)
		if err := os.RemoveAll(filepath.Join(f.dir, ".git")); err != nil {
			f.t.Fatal(err)
		}
		f.write(".git", "", 0o644)
	}, "")
	add(".git is a gitfile without the gitdir prefix", func(f *fixture) {
		basic(f)
		real := move(f)
		f.write(".git", "notgitdir: "+real+"\n", 0o644)
	}, "")
	add(".git is a gitfile with a different prefix case", func(f *fixture) {
		basic(f)
		real := move(f)
		f.write(".git", "GITDIR: "+real+"\n", 0o644)
	}, "")
	add("a linked worktree ahead of the main tree", func(f *fixture) {
		basic(f)
		linked := filepath.Join(filepath.Dir(f.dir), filepath.Base(f.dir)+"-ahead")
		f.git("worktree", "add", "-q", "-b", "ahead", linked)
		f.dir = linked
		f.write("ahead.txt", "ahead\n", 0o644)
		f.commit("ahead\n")
	}, "")
	add("a linked worktree", func(f *fixture) {
		basic(f)
		linked := filepath.Join(filepath.Dir(f.dir), filepath.Base(f.dir)+"-linked")
		f.git("worktree", "add", "-q", "-b", "other", linked)
		f.dir = linked
	}, "")
	add("the path is a file", func(f *fixture) {
		basic(f)
		f.dir = filepath.Join(f.dir, "a.txt")
	}, "")
	add("the path does not exist", func(f *fixture) {
		basic(f)
		f.dir = filepath.Join(f.dir, "missing-dir")
	}, "")
	add("the path is a symlink to a file", func(f *fixture) {
		basic(f)
		link := filepath.Join(filepath.Dir(f.dir), "link-to-file")
		if err := os.Symlink(filepath.Join(f.dir, "a.txt"), link); err != nil {
			f.t.Fatal(err)
		}
		f.dir = link
	}, "")
	add("the path has a trailing space in the directory name", func(f *fixture) {
		basic(f)
		renamed := f.dir + " "
		if err := os.Rename(f.dir, renamed); err != nil {
			f.t.Fatal(err)
		}
		f.dir = renamed
	}, "")
	add("a unicode directory name", func(f *fixture) {
		basic(f)
		renamed := filepath.Join(filepath.Dir(f.dir), "ディレクトリ ")
		if err := os.Rename(f.dir, renamed); err != nil {
			f.t.Fatal(err)
		}
		f.dir = renamed
	}, "")
	add("a directory name starting with a dash", func(f *fixture) {
		basic(f)
		renamed := filepath.Join(filepath.Dir(f.dir), "-dashdir")
		if err := os.Rename(f.dir, renamed); err != nil {
			f.t.Fatal(err)
		}
		f.dir = renamed
	}, "")
	add("a very long directory name", func(f *fixture) {
		basic(f)
		renamed := filepath.Join(filepath.Dir(f.dir), strings.Repeat("d", 200))
		if err := os.Rename(f.dir, renamed); err != nil {
			f.t.Fatal(err)
		}
		f.dir = renamed
	}, "")
	add("a bare repository beside a working tree", func(f *fixture) {
		basic(f)
		bare := f.dir + "-bare"
		f.git("clone", "-q", "--bare", ".", bare)
		f.dir = bare
	}, "")
	return out
}

// mergeMessageScenarios: the merge-commit regexes and the title rule over every
// digit class, both marker formats, and the places a marker can sit.
func mergeMessageScenarios() []localScenario {
	var out []localScenario
	formats := []struct{ name, format string }{
		{"github", "Merge pull request #%s from acme/topic\n\nTitle line\n"},
		{"gitlab", "Merge branch 'x' into 'main'\n\nGitLab title\n\nSee merge request group/project!%s\n"},
		{"gitlab-inline", "See merge request g/p!%s and g/o!7\n"},
		{"github-after-blank", "Title first\n\nMerge pull request #%s later\n"},
		{"github-crlf", "Merge pull request #%s from a/b\r\n\r\nCRLF title\r\n"},
		{"github-unicode-sep", "Merge pull request #%s from a/b\u2028\u2028Separated title\u2029"},
		{"gitlab-glued", "See merge request!%s"},
		{"gitlab-spaced", "See merge request \u2003!%s"},
	}
	for _, format := range formats {
		for _, digits := range digitClasses {
			format, digits := format, digits
			out = append(out, localScenario{name: "gen merge " + format.name + " #" + digits, targets: []string{"prs", "git"}, build: func(f *fixture) {
				f.write("base.txt", "base\n", 0o644)
				f.commit("base\n")
				f.git("checkout", "-q", "-b", "topic")
				f.write("t.txt", "t\n", 0o644)
				f.commit("topic work\n")
				f.git("checkout", "-q", "main")
				f.merge("topic", fmt.Sprintf(format.format, digits))
			}})
		}
	}
	for i, title := range []string{
		"Merge pull request #1 from a/b\n\n\u2003  spaced  \u2003\n", "Merge pull request #1 from a/b\n\n\x1c\x1d\x1e\x1f\nafter separators\n",
		"Merge pull request #1 from a/b\n\n\u0085next line\n", "Merge pull request #1 from a/b\n\n\u00a0nbsp title\u00a0\n", "Merge pull request #1 from a/b\n\n\ufefffeff\n",
		"Merge pull request #1 from a/b\n\n\u200bzero width\n", "Merge pull request #1 from a/b\n\n\v\fcontrols\n", "Merge branch 'x'\nMerge pull request #2\nreal\n",
		"Merge pull request #1 from a/b\n\nSee merge request g/p!2\nafter\n",
	} {
		title := title
		out = append(out, localScenario{name: fmt.Sprintf("gen title %02d", i), targets: []string{"prs"}, build: func(f *fixture) {
			f.write("base.txt", "base\n", 0o644)
			f.commit("base\n")
			f.git("checkout", "-q", "-b", "topic")
			f.write("t.txt", "t\n", 0o644)
			f.commit("topic work\n")
			f.git("checkout", "-q", "main")
			f.merge("topic", title)
		}})
	}
	return out
}

// identityScenarios: author and committer lines (crafted, byte for byte) over
// the classes Actor.from_string and the regexes of parse_actor_and_date treat
// in their own way.
func identityScenarios() []localScenario {
	lines := []string{
		"Jane Doe <jane@example.com> 1700000000 +0000",
		"Just A Name 1700000000 +0000",
		"<only@example.com> 1700000000 +0000",
		"  Padded Name  <pad@example.com> 1700000000 +0000",
		"\u2003Em Space\u2003 <em@example.com> 1700000000 +0000",
		"Name With <Angle> <a@b> 1700000000 +0000",
		"Name <a@b> <c@d> 1700000000 +0000",
		"Name <a@b>trailing 1700000000 +0000",
		"Name <> 1700000000 +0000",
		"<> 1700000000 +0000",
		"Name <a@b 1700000000 +0000",
		"Name a@b> 1700000000 +0000",
		"Name <a@b> 1700000000",
		"Name <a@b> 1700000000 +0530",
		"Name <a@b> 1700000000 -0000",
		"Name <a@b> 00001700000000 +0000",
		"Name <a@b> 1700000000 +0000 extra words",
		"Name <a@b> 99999999999 +0000",
		"Name <a@b> 0 +0000",
		"日本語 <jp@example.com> 1700000000 +0900",
		"Ünï <u@example.com> 1700000000 +0000",
		"Name <a@b> １７００００００００ +0000",
		"Name <a@b> 1700000000 +２３５９",
		"Name <a@b> 18446744073709551617 +0000",
		"Name <a@b> 67768036191676799 +0000",
		"Name <a@b> 67768036191676800 +0000",
		"Name <a@b> 1700000000 +2400000000000",
		"Name <a@b> 1700000000 +2399999999959",
		"Name <a@b> 1700000000 +240000000100",
		"Name <a@b> 1700000000 +18446744073709551617",
		"Name <a@b> 1700000000 +2400",
		"Name <a@b> 1700000000 +99999999999",
		"Name <a@b> 253402300799 +0000",
		"Name <a@b> 253402300800 +0000",
		"Name <a@b> 4611686018427387903 +0000",
		"Name <a@b> 4611686018427387904 +0000",
		"Name <a@b> 9223372036854775807 +0000",
		"Name <a@b> 9223372036854775808 +0000",
		"Name <a@b> 100000000000000000000 +0000",
		"Name <a@b> 10413792000 +0000",
		"Name <a@b> 13569465600 +0000",
		"Name <a@b> 1 +0000",
		"Name <a@b> 4102444800 +0000",
		strings.Repeat("N", 300) + " <long@example.com> 1700000000 +0000",
		"Name\ttab <t@example.com> 1700000000 +0000",
	}
	var out []localScenario
	for i, line := range lines {
		line := line
		// Instants after the year 2262: Python writes the raw tick count of a year
		// the DateTime64 column does not document; the client the port writes
		// through wraps it, so the port refuses (named in the PR's RISK-NOTES).
		refuses := ""
		for _, epoch := range []string{" 99999999999 ", " 253402300799 ", " 10413792000 ", " 13569465600 "} {
			if strings.Contains(line, epoch) {
				refuses = "git"
			}
		}
		out = append(out, localScenario{name: fmt.Sprintf("gen identity %02d", i), goRefusesOn: refuses, targets: []string{"git", "prs"}, build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.git("add", "-A")
			f.rawCommit(nil, nil, []byte("identity\n"), line, line)
		}})
	}
	return out
}

// packedPullRefScenarios: pull refs that live only in packed-refs.
func packedPullRefScenarios() []localScenario {
	var out []localScenario
	for _, c := range []struct {
		name string
		body func(a, b string) string
	}{
		{"valid", func(a, b string) string {
			return "# pack-refs with: peeled fully-peeled sorted \n" + a + " refs/pull/3/head\n" + b + " refs/merge-requests/4/head\n"
		}},
		{"a missing object", func(a, b string) string { return strings.Repeat("ab", 20) + " refs/pull/3/head\n" }},
		{"a short sha", func(a, b string) string { return a[:10] + " refs/pull/3/head\n" }},
		{"an uppercase sha", func(a, b string) string { return strings.ToUpper(a) + " refs/pull/3/head\n" }},
		{"loose and packed for one ref", func(a, b string) string { return b + " refs/pull/1/head\n" }},
	} {
		c := c
		out = append(out, localScenario{name: "gen packed pull ref: " + c.name, targets: []string{"prs"}, build: func(f *fixture) {
			first, second := twoCommits(f)
			f.setRef("refs/pull/1/head", first)
			if err := os.WriteFile(filepath.Join(f.dir, ".git", "packed-refs"), []byte(c.body(first, second)), 0o644); err != nil {
				f.t.Fatal(err)
			}
		}})
	}
	return out
}

// packedRefsScenarios: hand-written packed-refs files, which GitPython reads as
// strict UTF-8 text and strips line by line.
func packedRefsScenarios() []localScenario {
	type packed struct {
		name  string
		lines func(head string) string
	}
	cases := []packed{
		{"control characters at the end of a name", func(h string) string {
			return "# pack-refs with: peeled fully-peeled sorted \n" + h + " refs/tags/ctl\x1f\n" + h + " refs/tags/\x1cfs\n"
		}},
		{"peeled lines and comments", func(h string) string {
			return "# pack-refs with: peeled fully-peeled sorted \n" + h + " refs/tags/annotated\n^" + h + "\n\n" + h + " refs/tags/plain\n"
		}},
		{"a line with no path", func(h string) string { return "# pack-refs with: peeled fully-peeled sorted \n" + h + "\n" }},
		{"a packing scheme it does not understand", func(h string) string { return "# pack-refs with: other\n" + h + " refs/tags/x\n" }},
		{"not valid UTF-8", func(h string) string {
			return "# pack-refs with: peeled fully-peeled sorted \n" + h + " refs/heads/bad\xffbranch\n" + h + " refs/tags/ok\n"
		}},
		{"a ref of another kind whose name starts like refs/tags", func(h string) string {
			return "# pack-refs with: peeled fully-peeled sorted \n" + h + " refs/tags-old/legacy\n" + h + " refs/tags/real\n"
		}},
		{"a tag line with two spaces", func(h string) string {
			return "# pack-refs with: peeled fully-peeled sorted \n" + h + "  refs/tags/double-space\n"
		}},
		{"windows line endings", func(h string) string {
			return "# pack-refs with: peeled fully-peeled sorted \r\n" + h + " refs/tags/crlf\r\n"
		}},
	}
	var out []localScenario
	for _, c := range cases {
		c := c
		out = append(out, localScenario{name: "gen packed-refs: " + c.name, targets: []string{"git", "prs"}, build: func(f *fixture) {
			first, _ := twoCommits(f)
			if err := os.WriteFile(filepath.Join(f.dir, ".git", "packed-refs"), []byte(c.lines(first)), 0o644); err != nil {
				f.t.Fatal(err)
			}
		}})
	}
	return out
}

// refValueScenarios: what a loose ref FILE can hold (a sha in some shape, a
// symbolic pointer, garbage), which GitPython's own reader interprets.
func refValueScenarios() []localScenario {
	type value struct {
		name  string
		build func(f *fixture, first, second string) string
	}
	values := []value{
		{"empty file", func(f *fixture, a, b string) string { return "" }},
		{"garbage", func(f *fixture, a, b string) string { return "not-a-sha" }},
		{"uppercase sha", func(f *fixture, a, b string) string { return strings.ToUpper(a) }},
		{"abbreviated sha", func(f *fixture, a, b string) string { return a[:7] }},
		{"sha with trailing spaces", func(f *fixture, a, b string) string { return a + "   " }},
		{"sha with CRLF", func(f *fixture, a, b string) string { return a + "\r" }},
		{"sha then extra words", func(f *fixture, a, b string) string { return a + " extra" }},
		{"symbolic to a branch", func(f *fixture, a, b string) string { return "ref: refs/heads/main" }},
		{"symbolic to a missing ref", func(f *fixture, a, b string) string { return "ref: refs/heads/nowhere" }},
		{"symbolic to another pull ref", func(f *fixture, a, b string) string { return "ref: refs/pull/1/head" }},
		// A symbolic ref loop is left out on purpose: GitPython's dereference_recursive never returns.
		{"symbolic without a space", func(f *fixture, a, b string) string { return "ref:refs/heads/main" }},
		{"whitespace only", func(f *fixture, a, b string) string { return "  \t \n" }},
		{"sha and a tab and words", func(f *fixture, a, b string) string { return a + "\tbranch 'main' of somewhere" }},
		{"sha with Unicode whitespace after it", func(f *fixture, a, b string) string { return a + "\u2003tail" }},
		{"leading Unicode whitespace", func(f *fixture, a, b string) string { return "\u2003" + a }},
		{"ref: alone", func(f *fixture, a, b string) string { return "ref:" }},
		{"symbolic with extra words", func(f *fixture, a, b string) string { return "ref: refs/heads/main extra words" }},
		{"symbolic to HEAD", func(f *fixture, a, b string) string { return "ref: HEAD" }},
		{"symbolic to an invalid name (space)", func(f *fixture, a, b string) string { return "ref: refs/heads/a b" }},
		{"symbolic to an invalid name (..)", func(f *fixture, a, b string) string { return "ref: refs/heads/../heads/main" }},
		{"symbolic to an absolute path", func(f *fixture, a, b string) string { return "ref: /refs/heads/main" }},
		{"symbolic to a .lock name", func(f *fixture, a, b string) string { return "ref: refs/heads/main.lock" }},
		{"symbolic to a tag ref", func(f *fixture, a, b string) string { f.setRef("refs/tags/target", a); return "ref: refs/tags/target" }},
		{"content that is not UTF-8", func(f *fixture, a, b string) string { return a + "\xff\xfe" }},
		{"NUL in the content", func(f *fixture, a, b string) string { return a + "\x00" }},
		{"CRLF between tokens", func(f *fixture, a, b string) string { return "ref:\r\nrefs/heads/main" }},
		{"all zeros", func(f *fixture, a, b string) string { return strings.Repeat("0", 40) }},
		{"a sha of 64 hex digits", func(f *fixture, a, b string) string { return a + a[:24] }},
	}
	var out []localScenario
	for _, v := range values {
		v := v
		out = append(out, localScenario{name: "gen ref value: " + v.name, targets: []string{"prs"}, build: func(f *fixture) {
			first, second := twoCommits(f)
			f.setRef("refs/pull/1/head", first)
			f.setRef("refs/pull/2/head", v.build(f, first, second))
		}})
	}
	return out
}

// richRepository is a repository with everything the sync reads: unicode paths (so
// core.quotepath matters), a merged pull request, a tag and an open pull request ref.
func richRepository(f *fixture, prNumber string) {
	f.write("seed.txt", "seed\n", 0o644)
	f.write("é.txt", "accent\n", 0o644)
	f.write("日本語/ファイル.txt", "cjk\n", 0o644)
	f.commit("seed\n")
	f.git("checkout", "-q", "-b", "topic")
	f.write("t.txt", "t\n", 0o644)
	topic := f.commit("topic\n")
	f.git("checkout", "-q", "main")
	f.merge("topic", "Merge pull request #"+prNumber+" from acme/topic\n\nA title\n")
	f.write("é.txt", "accent changed\n", 0o644)
	f.commit("change\n", commitAt{tz: "+0530"})
	f.git("tag", "release/1.0")
	f.setRef("refs/pull/5/head", topic)
}

// ambientGitEnvScenarios: the process environment reaches git. GitPython pins the
// repository's own git dir over an ambient GIT_DIR / GIT_COMMON_DIR (and an absolute
// GIT_OBJECT_DIRECTORY) and lets every other GIT_* variable through; each variable
// below names another repository, a path that does not exist, or a setting that
// changes git's output, and both planes run under it.
func ambientGitEnvScenarios() []localScenario {
	type envCase struct {
		name string
		vars func(self, other string) map[string]string
	}
	one := func(key, value string) func(self, other string) map[string]string {
		return func(self, other string) map[string]string { return map[string]string{key: value} }
	}
	config := func(pairs ...string) func(self, other string) map[string]string {
		return func(self, other string) map[string]string {
			env := map[string]string{"GIT_CONFIG_COUNT": fmt.Sprint(len(pairs) / 2)}
			for i := 0; i < len(pairs); i += 2 {
				env[fmt.Sprintf("GIT_CONFIG_KEY_%d", i/2)] = pairs[i]
				env[fmt.Sprintf("GIT_CONFIG_VALUE_%d", i/2)] = pairs[i+1]
			}
			return env
		}
	}
	cases := []envCase{
		{"GIT_DIR names another repository", func(self, other string) map[string]string { return map[string]string{"GIT_DIR": other + "/.git"} }},
		{"GIT_DIR names this repository", func(self, other string) map[string]string { return map[string]string{"GIT_DIR": self + "/.git"} }},
		{"GIT_DIR does not exist", one("GIT_DIR", "/nonexistent/.git")},
		{"GIT_DIR is relative", one("GIT_DIR", ".git")},
		{"GIT_DIR is empty", one("GIT_DIR", "")},
		{"GIT_COMMON_DIR names another repository", func(self, other string) map[string]string {
			return map[string]string{"GIT_COMMON_DIR": other + "/.git"}
		}},
		{"GIT_DIR and GIT_COMMON_DIR name another repository", func(self, other string) map[string]string {
			return map[string]string{"GIT_DIR": other + "/.git", "GIT_COMMON_DIR": other + "/.git"}
		}},
		{"GIT_OBJECT_DIRECTORY names another repository", func(self, other string) map[string]string {
			return map[string]string{"GIT_OBJECT_DIRECTORY": other + "/.git/objects"}
		}},
		{"GIT_OBJECT_DIRECTORY does not exist", one("GIT_OBJECT_DIRECTORY", "/nonexistent/objects")},
		{"GIT_OBJECT_DIRECTORY is relative", one("GIT_OBJECT_DIRECTORY", ".git/objects")},
		{"GIT_OBJECT_DIRECTORY is relative to the process directory", func(self, other string) map[string]string {
			cwd, err := os.Getwd()
			if err != nil {
				panic(err)
			}
			rel, err := filepath.Rel(cwd, other+"/.git/objects")
			if err != nil {
				panic(err)
			}
			return map[string]string{"GIT_OBJECT_DIRECTORY": rel}
		}},
		{"GIT_COMMON_DIR is empty", one("GIT_COMMON_DIR", "")},
		{"GIT_COMMON_DIR names this repository", func(self, other string) map[string]string {
			return map[string]string{"GIT_COMMON_DIR": self + "/.git"}
		}},
		{"GIT_ALTERNATE_OBJECT_DIRECTORIES names another repository", func(self, other string) map[string]string {
			return map[string]string{"GIT_ALTERNATE_OBJECT_DIRECTORIES": other + "/.git/objects"}
		}},
		{"GIT_WORK_TREE names another repository", func(self, other string) map[string]string { return map[string]string{"GIT_WORK_TREE": other} }},
		{"GIT_WORK_TREE does not exist", one("GIT_WORK_TREE", "/nonexistent")},
		{"GIT_INDEX_FILE does not exist", one("GIT_INDEX_FILE", "/nonexistent/index")},
		{"GIT_INDEX_FILE names another repository's index", func(self, other string) map[string]string {
			return map[string]string{"GIT_INDEX_FILE": other + "/.git/index"}
		}},
		{"GIT_NAMESPACE", one("GIT_NAMESPACE", "ns")},
		{"GIT_CEILING_DIRECTORIES", one("GIT_CEILING_DIRECTORIES", "/")},
		{"GIT_DISCOVERY_ACROSS_FILESYSTEM", one("GIT_DISCOVERY_ACROSS_FILESYSTEM", "1")},
		{"core.quotepath off", config("core.quotepath", "false")},
		{"diff.renames off", config("diff.renames", "false")},
		{"diff.noprefix", config("diff.noprefix", "true")},
		{"core.abbrev 4", config("core.abbrev", "4")},
		{"log.showSignature", config("log.showSignature", "true")},
		{"i18n.commitEncoding latin1", config("i18n.commitEncoding", "ISO-8859-1")},
		{"core.precomposeUnicode", config("core.precomposeUnicode", "true")},
		{"color.ui always", config("color.ui", "always")},
		{"diff.algorithm", config("diff.algorithm", "patience")},
		{"core.worktree another repository", func(self, other string) map[string]string {
			return config("core.worktree", other)(self, other)
		}},
		{"GIT_CONFIG_PARAMETERS", one("GIT_CONFIG_PARAMETERS", "'core.quotepath=false'")},
		{"GIT_DIFF_OPTS", one("GIT_DIFF_OPTS", "--unified=0")},
		{"GIT_EXTERNAL_DIFF", one("GIT_EXTERNAL_DIFF", "/bin/false")},
		{"GIT_PAGER", one("GIT_PAGER", "/bin/false")},
		{"PAGER", one("PAGER", "/bin/false")},
		{"GIT_TRACE", one("GIT_TRACE", "1")},
		{"GIT_TRACE2", one("GIT_TRACE2", "1")},
		{"GIT_OPTIONAL_LOCKS 0", one("GIT_OPTIONAL_LOCKS", "0")},
		{"GIT_LITERAL_PATHSPECS", one("GIT_LITERAL_PATHSPECS", "1")},
		{"GIT_NO_REPLACE_OBJECTS", one("GIT_NO_REPLACE_OBJECTS", "1")},
		{"GIT_GRAFT_FILE", one("GIT_GRAFT_FILE", "/nonexistent/graft")},
		{"GIT_SHALLOW_FILE", one("GIT_SHALLOW_FILE", "/nonexistent/shallow")},
		{"GIT_REPLACE_REF_BASE", one("GIT_REPLACE_REF_BASE", "refs/other/")},
		{"GIT_ASKPASS", one("GIT_ASKPASS", "/bin/false")},
		{"GIT_TERMINAL_PROMPT 0", one("GIT_TERMINAL_PROMPT", "0")},
		{"GIT_AUTHOR_DATE and GIT_COMMITTER_DATE", func(self, other string) map[string]string {
			return map[string]string{"GIT_AUTHOR_DATE": "1 +0000", "GIT_COMMITTER_DATE": "1 +0000"}
		}},
		{"LC_ALL", one("LC_ALL", "de_DE.UTF-8")},
		{"LANGUAGE", one("LANGUAGE", "fr")},
		{"LANG", one("LANG", "ja_JP.UTF-8")},
		{"TZ", one("TZ", "Pacific/Auckland")},
		{"GIT_EXEC_PATH does not exist", one("GIT_EXEC_PATH", "/nonexistent")},
		{"GIT_PROGRESS_DELAY", one("GIT_PROGRESS_DELAY", "0")},
	}
	var out []localScenario
	for _, c := range cases {
		c := c
		var other string
		out = append(out, localScenario{
			name: "gen env: " + c.name, targets: []string{"git", "prs"},
			build: func(f *fixture) {
				richRepository(f, "12")
				b := newFixture(f.t, "other-repository")
				richRepository(b, "99")
				// The other repository's index differs from this one's: the diff of a root
				// commit is taken against the working tree through the index of the git dir.
				b.git("rm", "-q", "--cached", "seed.txt")
				other = b.dir
			},
			envFn: func(f *fixture) map[string]string { return c.vars(f.dir, other) },
		})
	}
	return out
}

// tipTimeScenarios: the committer time of the commit an open PR ref points at, over the
// classes datetime.fromtimestamp treats in its own way (OK, past 9999, past 2**62, past 2**63).
func tipTimeScenarios() []localScenario {
	var out []localScenario
	for _, epoch := range []string{"1700000000", "253402300799", "253402300800", "67768036191676799", "67768036191676800", "4611686018427387903", "4611686018427387904", "9223372036854775807", "9223372036854775808", "18446744073709551617"} {
		epoch := epoch
		refuses := ""
		if epoch == "253402300799" {
			refuses = "prs" // year 9999: Python writes it, the ClickHouse client cannot
		}
		out = append(out, localScenario{name: "gen tip time: " + epoch, targets: []string{"prs"}, goRefusesOn: refuses, build: func(f *fixture) {
			f.write("a.txt", "a\n", 0o644)
			f.commit("main\n")
			tree := f.git("write-tree")
			line := "Name <a@b> " + epoch + " +0000"
			tip := f.object("commit", "tree "+tree+"\nauthor "+line+"\ncommitter "+line+"\n\ntip\n")
			f.setRef("refs/pull/5/head", tip)
		}})
	}
	return out
}

// linkedWorktreeScenarios: a linked worktree keeps its refs in the main repository's
// common dir; tags and pull refs live there.
func linkedWorktreeScenarios() []localScenario {
	var out []localScenario
	for _, packed := range []bool{false, true} {
		packed := packed
		out = append(out, localScenario{name: fmt.Sprintf("gen linked worktree with tags and refs packed=%v", packed), targets: []string{"git", "prs"}, build: func(f *fixture) {
			richRepository(f, "12")
			if packed {
				f.git("pack-refs", "--all")
			}
			linked := filepath.Join(filepath.Dir(f.dir), filepath.Base(f.dir)+"-linked")
			f.git("worktree", "add", "-q", "-b", "other", linked)
			f.dir = linked
		}})
	}
	return out
}

// blameTimeScenarios: the committer time of the commit that owns some lines of a file,
// over the classes datetime.fromtimestamp treats in its own way (fine, past the
// ClickHouse client's range, past year 9999, past a C int, past 2**63). The root commit
// is older than --since, so the walk never reads it and only `git blame` sees its time:
// Python's commit.committed_datetime raises inside fetch_blame's try and the file keeps
// the rows it had built, so the position of the failing lines in the file matters.
func blameTimeScenarios() []localScenario {
	var out []localScenario
	epochs := []string{"1700000000", "0", "10413792000", "253402300799", "253402300800", "67768036191676799", "67768036191676800", "4611686018427387904", "9223372036854775807", "9223372036854775808", "18446744073709551617"}
	for _, epoch := range epochs {
		for _, layout := range []string{"owner first", "owner last"} {
			epoch, layout := epoch, layout
			refuses := ""
			if epoch == "10413792000" || epoch == "253402300799" {
				refuses = "blame" // Python writes an instant the ClickHouse client cannot; the port refuses
			}
			out = append(out, localScenario{
				name: "gen blame time: " + epoch + " " + layout, targets: []string{"blame"}, goRefusesOn: refuses,
				args: []string{"--since", "2023-11-01"},
				build: func(f *fixture) {
					commit := func(parents string, when string, message string) string {
						f.t.Helper()
						f.git("add", "-A")
						tree := f.git("write-tree")
						line := "Name <a@b> " + when + " +0000"
						return f.object("commit", "tree "+tree+"\n"+parents+"author "+line+"\ncommitter "+line+"\n\n"+message+"\n")
					}
					f.write("a.txt", "1\n2\n3\n4\n", 0o644)
					root := commit("", epoch, "root")
					f.write("a.txt", "1\n2\n3\n4\n5\n", 0o644)
					second := commit("parent "+root+"\n", "1600000000", "second")
					if layout == "owner first" {
						f.write("a.txt", "1 changed\n2\n3\n4\n5\n", 0o644)
					} else {
						f.write("a.txt", "1\n2\n3\n4 changed\n5\n", 0o644)
					}
					third := commit("parent "+second+"\n", "1700000000", "third")
					f.setRef("refs/heads/main", third)
				},
			})
		}
	}
	return out
}

// blameBatchScenarios: process_files_and_blame writes in batches (chunks of 2000
// files, a batch at 1000 rows), so the rows a run that fails leaves behind are the
// batches before the failing one. A file name that is not valid UTF-8 makes the
// batch holding it raise. The port validates every batch first and writes nothing
// (named divergence, D2615): where Python left batches behind (where the name sits
// among the files, in the file system's os.walk order, decides it) the scenario
// asserts the port's all-or-nothing; where Python left none the tables compare.
// placeLastInWalkOrder writes a file alone in a directory that os.walk (the file
// system's own directory order, which is not sorted) reaches last, so it is the last
// file of the walk: the position that leaves every earlier batch to Python. Directory
// names are tried until one lists last.
func placeLastInWalkOrder(f *fixture, name string) {
	f.t.Helper()
	for k := 0; k < 500; k++ {
		dir := fmt.Sprintf("zz%03d", k)
		f.write(dir+"/"+name, "the last file\n", 0o644)
		handle, err := os.Open(f.dir)
		if err != nil {
			f.t.Fatal(err)
		}
		entries, err := handle.ReadDir(-1)
		_ = handle.Close()
		if err != nil {
			f.t.Fatal(err)
		}
		if entries[len(entries)-1].Name() == dir {
			return
		}
		if err := os.RemoveAll(filepath.Join(f.dir, dir)); err != nil {
			f.t.Fatal(err)
		}
	}
	f.t.Fatal("no directory name lists last")
}

func blameBatchScenarios() []localScenario {
	var out []localScenario
	for _, shape := range []struct {
		name  string
		files int
		since bool
	}{
		{"999 files", 999, false}, {"1000 files", 1000, false}, {"1500 files", 1500, false},
		{"2000 files", 2000, false}, {"2001 files", 2001, false}, {"2999 files", 2999, false},
		{"4001 files", 4001, false}, {"2500 files and a since window", 2500, true},
	} {
		shape := shape
		var args []string
		if shape.since {
			args = []string{"--since", "2023-11-01"}
		}
		out = append(out, localScenario{
			name: "gen blame batches: " + shape.name + " and a name that is not UTF-8", targets: []string{"blame"}, args: args, allOrNothing: true,
			build: func(f *fixture) {
				for i := 0; i < shape.files; i++ {
					f.write(fmt.Sprintf("d%d/f%04d.txt", i%7, i), fmt.Sprintf("line %d\n", i), 0o644)
				}
				placeLastInWalkOrder(f, "bad\xffname.txt")
				f.commit("many\n", commitAt{committerDate: 1_690_000_000})
				if shape.since {
					f.write("d0/f0000.txt", "changed\n", 0o644)
					f.commit("recent\n", commitAt{committerDate: 1_700_000_000})
				}
			},
		})
	}
	// The same shapes with every name valid: the batches are only a grain, the rows
	// that land are all of them.
	for _, files := range []int{1999, 2000, 2001, 2999, 3001} {
		files := files
		out = append(out, localScenario{
			name: fmt.Sprintf("gen blame batches: %d valid files", files), targets: []string{"blame"},
			build: func(f *fixture) {
				for i := 0; i < files; i++ {
					f.write(fmt.Sprintf("d%d/f%04d.txt", i%7, i), fmt.Sprintf("line %d\nsecond %d\n", i, i), 0o644)
				}
				f.commit("many\n")
			},
		})
	}
	return out
}
