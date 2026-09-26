package localgit

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// File is a GitFile row of processors/local.py _process_file_and_blame_sync.
type File struct {
	Path       string
	Executable bool
	// Contents is nil for a file of 1,000,000 bytes or more, or one that cannot be read.
	Contents *string
}

// BlameLine is a GitBlame row.
type BlameLine struct {
	Path        string
	LineNo      int
	AuthorEmail *string
	AuthorName  *string
	AuthorWhen  time.Time
	CommitHash  string
	Line        string
}

// skipExtensions and skipDirs are utils.py SKIP_EXTENSIONS and is_skippable's set.
var skipExtensions = map[string]bool{
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".ico": true, ".pdf": true, ".zip": true,
	".tar": true, ".gz": true, ".7z": true, ".exe": true, ".dll": true, ".so": true, ".dylib": true,
	".class": true, ".pyc": true, ".o": true, ".obj": true, ".bin": true, ".bak": true, ".tmp": true,
	".svg": true, ".eot": true, ".ttf": true, ".woff": true, ".woff2": true, ".mp4": true, ".mp3": true,
	".wav": true, ".mov": true, ".avi": true, ".mkv": true, ".webm": true, ".jar": true, ".war": true, ".ear": true,
}

var skipDirs = map[string]bool{
	"node_modules": true, "vendor": true, ".git": true, ".svn": true, ".hg": true, ".idea": true,
	".vscode": true, "__pycache__": true, "dist": true, "build": true, "target": true, "bin": true, "obj": true,
}

// pathSuffix is PurePath.suffix: the last dot of the name, when it is neither
// the first nor the last character.
func pathSuffix(path string) string {
	parts := pathParts(path)
	if len(parts) == 0 {
		return ""
	}
	name := parts[len(parts)-1]
	if i := strings.LastIndex(name, "."); i > 0 && i < len(name)-1 {
		return name[i:]
	}
	return ""
}

// isSkippable is utils.is_skippable.
func isSkippable(path string) bool {
	if skipExtensions[pythonparity.Lower(pathSuffix(path))] {
		return true
	}
	for _, part := range pathParts(path) {
		if skipDirs[part] {
			return true
		}
	}
	return false
}

// resolve is pathlib.Path.resolve() (non-strict): symlinks are followed as far
// as they exist, and a broken symlink resolves to its target.
func resolve(path string) string {
	for hops := 0; hops < 40; hops++ {
		if real, err := filepath.EvalSymlinks(path); err == nil {
			return real
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			// Missing (or not a link): resolve the parent and keep the name.
			dir, name := filepath.Split(filepath.Clean(path))
			if dir == "" || dir == path {
				return filepath.Clean(path)
			}
			return filepath.Join(resolve(filepath.Clean(dir)), name)
		}
		target, err := os.Readlink(path)
		if err != nil {
			return filepath.Clean(path)
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}
		path = target
	}
	return filepath.Clean(path)
}

// ChangedFiles is collect_changed_files over the commits: every path any
// commit's diff (against its first parent, or the working tree for a root
// commit, as CommitStats does) names, unless skippable, that exists now,
// resolved.
func (r Repo) ChangedFiles(ctx context.Context, commits []Commit) map[string]bool {
	paths := map[string]bool{}
	for _, commit := range commits {
		diffs, ok := r.commitDiffs(ctx, commit)
		if !ok {
			continue
		}
		for _, diff := range diffs {
			path := diff.BPath
			if path == "" {
				path = diff.APath
			}
			if path == "" || isSkippable(path) {
				continue
			}
			candidate := resolve(filepath.Join(r.Root, path))
			if _, err := os.Stat(candidate); err == nil {
				paths[candidate] = true
			}
		}
	}
	return paths
}

// AllFiles is the os.walk of process_local_blame: every non-directory entry
// (a symlink to a directory is a directory and is not entered), skipping files
// under a directory whose path has a `.git` component, each resolved.
func (r Repo) AllFiles() []string {
	var files []string
	var walk func(dir string)
	walk = func(dir string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		underGit := false
		for _, part := range strings.Split(dir, string(filepath.Separator)) {
			if part == ".git" {
				underGit = true
			}
		}
		for _, entry := range entries {
			full := filepath.Join(dir, entry.Name())
			isDir := entry.IsDir()
			if entry.Type()&os.ModeSymlink != 0 {
				if info, err := os.Stat(full); err == nil && info.IsDir() {
					isDir = true
					// os.walk lists it as a directory but never enters it.
					continue
				}
			}
			if isDir {
				walk(full)
				continue
			}
			if !underGit {
				files = append(files, resolve(full))
			}
		}
	}
	walk(r.Root)
	return files
}

// ReadFile is the GitFile half of _process_file_and_blame_sync: the path
// relative to the repository root, the effective execute permission, and the
// contents decoded like open(encoding="utf-8", errors="ignore") in text mode
// (universal newlines; invalid bytes dropped) for a file under 1,000,000 bytes.
func (r Repo) ReadFile(path string) File {
	rel, err := filepath.Rel(r.Root, path)
	if err != nil {
		rel = path
	}
	file := File{Path: rel, Executable: syscall.Access(path, 1) == nil}
	info, err := os.Stat(path)
	if err != nil || info.Size() >= 1_000_000 {
		return file
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return file
	}
	text := decodeUTF8Ignore(data)
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	file.Contents = &text
	return file
}

// decodeUTF8Ignore is bytes.decode("utf-8", "ignore"): every byte that is not
// part of a well-formed sequence is dropped.
func decodeUTF8Ignore(data []byte) string {
	if utf8.Valid(data) {
		return string(data)
	}
	var out bytes.Buffer
	for len(data) > 0 {
		r, size := utf8.DecodeRune(data)
		if r == utf8.RuneError && size == 1 {
			data = data[1:]
			continue
		}
		out.Write(data[:size])
		data = data[size:]
	}
	return out.String()
}

var (
	reHexsha           = regexp.MustCompile(`^[0-9A-Fa-f]{40}$`)
	reAuthorCommitterS = regexp.MustCompile(`^(author|committer)`)
)

// splitWhitespaceOnce is re.compile(r"\s+").split(text, 1) with Python's \s.
func splitWhitespaceOnce(text string) []string {
	start := -1
	for i, r := range text {
		if pythonparity.IsSpace(r) {
			start = i
			break
		}
	}
	if start < 0 {
		return []string{text}
	}
	end := start
	for i, r := range text[start:] {
		if !pythonparity.IsSpace(r) {
			end = start + i
			goto done
		}
		end = start + i + utf8.RuneLen(r)
	}
done:
	return []string{text[:start], text[end:]}
}

// bytesSplitLines is bytes.splitlines(keepends=True): \n, \r and \r\n.
func bytesSplitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' || data[i] == '\r' {
			end := i + 1
			if data[i] == '\r' && i+1 < len(data) && data[i+1] == '\n' {
				end = i + 2
				i++
			}
			lines = append(lines, data[start:end])
			start = end
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

// bytesRStrip is bytes.rstrip(): ASCII whitespace.
func bytesRStrip(b []byte) []byte {
	return bytes.TrimRight(b, " \t\n\r\x0b\x0c")
}

type blameCommit struct {
	author, authorEmail       string
	committer, committerEmail string
	committerDate             int64
	hasCommitterDate          bool
}

type blameGroup struct {
	commit *blameCommit
	hash   string
	lines  []string
}

// Blame is GitBlame.fetch_blame for one file: `git blame -p HEAD -- <path>`
// parsed like Repo.blame (GitPython 3.1.62), including its quirks: a content
// line is right-stripped, and any failure is no rows.
func (r Repo) Blame(ctx context.Context, relPath string) []BlameLine {
	data, err := r.run(ctx, "blame", "-p", "HEAD", "--", relPath)
	if err != nil {
		return nil
	}
	groups, ok := parseBlame(data)
	if !ok {
		return nil
	}
	return blameRows(groups, relPath)
}

// blameRows is the loop of GitBlame.fetch_blame over Repo.blame's groups: a
// group with no commit is skipped without advancing the line number, every line
// of the others is one row.
func blameRows(groups []blameGroup, relPath string) []BlameLine {
	var rows []BlameLine
	lineNo := 1
	for _, group := range groups {
		if group.commit == nil {
			continue
		}
		name, email := actorFromString(group.commit.author + " " + group.commit.authorEmail)
		for _, line := range group.lines {
			text := strings.TrimRight(line, "\n")
			rows = append(rows, BlameLine{
				Path: relPath, LineNo: lineNo, AuthorEmail: email, AuthorName: name,
				AuthorWhen: time.Unix(group.commit.committerDate, 0).UTC(), CommitHash: group.hash, Line: text,
			})
			lineNo++
		}
	}
	return rows
}

// blameInfo is the `info` dict of Repo.blame: only the keys it sets.
type blameInfo struct {
	id                        string
	hasID                     bool
	author, authorEmail       *string
	committer, committerEmail *string
	authorDate, committerDate *int64
}

// parseBlame is Repo.blame's `--porcelain` loop, statement by statement. false
// means Python would raise (a KeyError on a missing header, a ValueError in int()).
func parseBlame(data []byte) ([]blameGroup, bool) {
	commits := map[string]*blameCommit{}
	var blames []blameGroup
	info := blameInfo{}
	current := func() *blameGroup { return &blames[len(blames)-1] }
	for _, lineBytes := range bytesSplitLines(data) {
		stripped := bytesRStrip(lineBytes)
		var lineStr string
		var parts []string
		var firstpart string
		isBinary := !utf8.Valid(stripped)
		if !isBinary {
			lineStr = string(stripped)
			parts = splitWhitespaceOnce(lineStr)
			firstpart = parts[0]
		}
		last := ""
		if len(parts) > 0 {
			last = parts[len(parts)-1]
		}
		switch {
		case reHexsha.MatchString(firstpart):
			digits := strings.Split(last, " ")
			if len(digits) == 3 {
				info = blameInfo{id: firstpart, hasID: true}
				blames = append(blames, blameGroup{hash: firstpart})
			} else {
				if !info.hasID {
					return nil, false // info["id"]: KeyError
				}
				if info.id != firstpart {
					info = blameInfo{id: firstpart, hasID: true}
					blames = append(blames, blameGroup{commit: commits[firstpart], hash: firstpart})
				}
			}
		case reAuthorCommitterS.MatchString(firstpart):
			role := reAuthorCommitterS.FindString(firstpart)
			value := last
			switch {
			case strings.HasSuffix(firstpart, "-mail"):
				if role == "author" {
					info.authorEmail = &value
				} else {
					info.committerEmail = &value
				}
			case strings.HasSuffix(firstpart, "-time"):
				number, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
				if err != nil {
					return nil, false
				}
				if role == "author" {
					info.authorDate = &number
				} else {
					info.committerDate = &number
				}
			case role == firstpart:
				if role == "author" {
					info.author = &value
				} else {
					info.committer = &value
				}
			}
		case strings.HasPrefix(firstpart, "filename"), strings.HasPrefix(firstpart, "summary"):
			// recorded by Python, never read
		case firstpart == "":
			if info.hasID {
				sha := info.id
				commit := commits[sha]
				if commit == nil {
					if info.author == nil || info.authorEmail == nil || info.authorDate == nil ||
						info.committer == nil || info.committerEmail == nil || info.committerDate == nil {
						return nil, false // info[...]: KeyError
					}
					commit = &blameCommit{
						author: *info.author, authorEmail: *info.authorEmail,
						committer: *info.committer, committerEmail: *info.committerEmail,
						committerDate: *info.committerDate, hasCommitterDate: true,
					}
					commits[sha] = commit
				}
				if len(blames) == 0 {
					return nil, false // blames[-1]: IndexError
				}
				current().commit = commit
				current().hash = sha
				var line string
				if !isBinary {
					line = strings.TrimPrefix(lineStr, "\t")
				} else {
					line = pythonparity.DecodeUTF8Replace(string(lineBytes))
				}
				current().lines = append(current().lines, line)
				info = blameInfo{id: sha, hasID: true}
			}
		}
	}
	return blames, true
}
