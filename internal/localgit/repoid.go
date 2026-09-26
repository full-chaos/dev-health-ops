package localgit

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// RepoID is models/git.py get_repo_uuid(repo_path): REPO_UUID when set (an
// invalid value is Python's uncaught ValueError, returned as an error), else
// the SHA-256 of the first URL of the origin remote (or of the first remote in
// the repository config), else of the absolute path. UUID bytes are the first
// 16 bytes of the digest, with no version bits, like uuid.UUID(bytes=...).
func (r Repo) RepoID(ctx context.Context, lookup func(string) (string, bool)) (uuid.UUID, error) {
	if lookup != nil {
		if value, ok := lookup("REPO_UUID"); ok && value != "" {
			return pythonparity.ParseUUID(value)
		}
	}
	if url := r.remoteURL(ctx); url != "" {
		return digestUUID(url), nil
	}
	return digestUUID(r.Root), nil
}

func digestUUID(text string) uuid.UUID {
	sum := sha256.Sum256([]byte(text))
	var id uuid.UUID
	copy(id[:], sum[:16])
	return id
}

// remoteURL is the URL get_repo_uuid hashes, or "" for "use the path": any
// failure of the remote lookup falls back to the path in Python (a bare
// `except Exception`).
func (r Repo) remoteURL(ctx context.Context) string {
	names := r.remoteNames(ctx)
	if len(names) == 0 {
		return ""
	}
	name := names[0]
	for _, candidate := range names {
		if candidate == "origin" {
			name = "origin"
		}
	}
	out, err := r.run(ctx, "remote", "get-url", "--all", name)
	if err != nil {
		return ""
	}
	text := strings.TrimSuffix(string(out), "\n")
	first, _, _ := strings.Cut(text, "\n")
	return first
}

// remoteNames lists the remotes of the REPOSITORY config in file order, like
// Remote.list_items (sections named `remote "<name>"`).
func (r Repo) remoteNames(ctx context.Context) []string {
	out, err := r.run(ctx, "config", "--local", "-z", "--list", "--name-only")
	if err != nil {
		return nil
	}
	var names []string
	seen := map[string]bool{}
	for _, key := range strings.Split(string(out), "\x00") {
		rest, ok := strings.CutPrefix(key, "remote.")
		if !ok {
			continue
		}
		cut := strings.LastIndex(rest, ".")
		if cut <= 0 {
			continue // `[remote] var = ...`: no subsection, not `remote "<name>"`
		}
		if name := rest[:cut]; !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names
}

// TagsJSON is the repos.tags column: Repo(...).repo_tags is never set on the
// unflushed model, so build_repository_insert_row falls back to GitPython's
// Repo.tags, the tag references in sorted path order, each rendered by
// str() (Reference.name), and encoded by repository_json_or_none:
// json.dumps(ensure_ascii=False, separators=(",", ":")). A name that is not
// valid UTF-8 cannot be encoded for the insert: Python's UnicodeEncodeError
// (returned here as an error, so no row of the run is written).
func (r Repo) TagsJSON(ctx context.Context) (string, error) {
	paths, err := r.refPaths(ctx, "refs/tags")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, path := range paths {
		name := refName(path)
		if !utf8.ValidString(name) {
			return "", fmt.Errorf("tag name %q is not valid UTF-8 and cannot be written", name)
		}
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(pythonJSONString(name))
	}
	b.WriteByte(']')
	return b.String(), nil
}

// pythonJSONString is json.dumps(text, ensure_ascii=False).
func pythonJSONString(text string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range text {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\f':
			b.WriteString(`\f`)
		case r < 0x20:
			b.WriteString(fmt.Sprintf(`\u%04x`, r))
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
