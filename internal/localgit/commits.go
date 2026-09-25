package localgit

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Commit is a GitPython Commit as processors/local.py reads it.
type Commit struct {
	Hash           string
	Message        string
	AuthorName     *string
	AuthorEmail    *string
	CommitterName  *string
	CommitterEmail *string
	// CommittedAt is committed_datetime: the instant, in UTC.
	CommittedAt time.Time
	// TimeClass says whether Python can build committed_datetime from the
	// committer line: timeOK, timeValueError (an epoch whose year is past 9999:
	// ValueError) or timeOtherError (an epoch past 2**62: OSError or OverflowError).
	TimeClass int
	Parents   []string
}

// Classes of a committer timestamp (Commit.committed_datetime).
const (
	timeOK = iota
	timeValueError
	timeOtherError
)

// classifyEpoch mirrors datetime.fromtimestamp(epoch, utc) for a non-negative
// epoch: ValueError once the year passes 9999, OSError once it passes a C int
// (epoch 67768036191676800), OverflowError from 2**63.
func classifyEpoch(epoch int64, fits bool) int {
	switch {
	case !fits || epoch >= 67768036191676800:
		return timeOtherError // OSError (year past a C int) or OverflowError (past int64)
	case epoch > 253402300799:
		return timeValueError
	}
	return timeOK
}

// IterCommitsSince is list(iter_commits_since(repo, since)): `git rev-list HEAD
// --` (Repo.iter_commits with rev=None is head.commit), each commit parsed the
// way Commit._deserialize does, and the walk STOPS at the first commit whose
// committed time is before since (it is `break`, not a filter: a commit with a
// skewed date ends the list). An unborn HEAD is Python's uncaught ValueError.
func (r Repo) IterCommitsSince(ctx context.Context, since *time.Time) ([]Commit, error) {
	// Repo.iter_commits(): rev = self.head.commit, dereferenced by GitPython itself
	// (HEAD in the git dir, the branch in the common dir), then `git rev-list <sha> --`.
	// An unborn HEAD, a missing ref and a ref that is not a commit raise, uncaught.
	start, err := r.headCommit(ctx)
	if err != nil {
		return nil, err
	}
	out, err := r.run(ctx, "rev-list", start, "--")
	if err != nil {
		return nil, err
	}
	hashes := strings.Fields(string(out))
	commits, err := r.readCommits(ctx, hashes)
	if err != nil {
		return nil, err
	}
	var kept []Commit
	for _, commit := range commits {
		if commit.TimeClass != timeOK {
			// commit.committed_datetime raises inside iter_commits_since, uncaught.
			return nil, fmt.Errorf("commit %s has a committer time Python cannot represent", commit.Hash)
		}
		if since != nil && commit.CommittedAt.Before(*since) {
			break
		}
		kept = append(kept, commit)
	}
	return kept, nil
}

// readCommits parses every commit object with one `git cat-file --batch`.
func (r Repo) readCommits(ctx context.Context, hashes []string) ([]Commit, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	bin := r.Git
	if bin == "" {
		bin = "git"
	}
	command := exec.CommandContext(ctx, bin, "cat-file", "--batch")
	command.Dir = r.Root
	command.Env = r.env()
	command.Stdin = strings.NewReader(strings.Join(hashes, "\n") + "\n")
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(stdout, 1<<20)
	commits := make([]Commit, 0, len(hashes))
	for range hashes {
		header, err := reader.ReadString('\n')
		if err != nil {
			_ = command.Wait()
			return nil, fmt.Errorf("git cat-file: %w", err)
		}
		fields := strings.Fields(header)
		if len(fields) != 3 || fields[1] != "commit" {
			_ = command.Wait()
			return nil, fmt.Errorf("git cat-file: unexpected object header %q", strings.TrimSpace(header))
		}
		size, err := strconv.Atoi(fields[2])
		if err != nil {
			_ = command.Wait()
			return nil, err
		}
		body := make([]byte, size+1) // the object, then one newline
		if _, err := io.ReadFull(reader, body); err != nil {
			_ = command.Wait()
			return nil, err
		}
		commits = append(commits, parseCommit(fields[0], body[:size]))
	}
	if err := command.Wait(); err != nil {
		return nil, fmt.Errorf("git cat-file: %w", err)
	}
	return commits, nil
}

var (
	reActorEpoch = regexp.MustCompile(`^.+? (.*) (\p{Nd}+) ([+-]\p{Nd}+).*$`)
	reOnlyActor  = regexp.MustCompile(`^.+? (.*)$`)
)

// parseCommit is Commit._deserialize followed by parse_actor_and_date.
func parseCommit(hash string, raw []byte) Commit {
	commit := Commit{Hash: hash}
	head, message, _ := bytes.Cut(raw, []byte("\n\n"))
	lines := bytes.Split(head, []byte("\n"))
	var authorLine, committerLine []byte
	for _, line := range lines {
		switch {
		case bytes.HasPrefix(line, []byte("parent")):
			fields := bytes.Fields(line)
			commit.Parents = append(commit.Parents, string(fields[len(fields)-1]))
		case bytes.HasPrefix(line, []byte("author ")) && authorLine == nil:
			authorLine = line
		case bytes.HasPrefix(line, []byte("committer ")) && committerLine == nil:
			committerLine = line
		}
	}
	commit.Message = pythonparity.DecodeUTF8Replace(string(message))
	name, email, _ := parseActorAndDate(string(authorLine))
	commit.AuthorName, commit.AuthorEmail = name, email
	name, email, epoch := parseActorAndDate(string(committerLine))
	commit.CommitterName, commit.CommitterEmail = name, email
	commit.TimeClass = classifyEpoch(epoch.value, epoch.fits)
	if commit.TimeClass == timeOK && epoch.tzOverflow {
		commit.TimeClass = timeOtherError
	}
	if commit.TimeClass == timeOK {
		commit.CommittedAt = time.Unix(epoch.value, 0).UTC()
	}
	return commit
}

// parseActorAndDate is objects/util.py parse_actor_and_date + Actor.from_string.
// epochValue is int(epoch): the value when it fits an int64.
type epochValue struct {
	value int64
	fits  bool
	// tzOverflow: the timezone offset is too large for timedelta(seconds=...), which
	// tzoffset() builds when committed_datetime is read (OverflowError).
	tzOverflow bool
}

func parseActorAndDate(line string) (name, email *string, epoch epochValue) {
	line = pythonparity.DecodeUTF8Replace(line)
	epoch.fits = true
	actor := ""
	if match := reActorEpoch.FindStringSubmatch(line); match != nil {
		actor = match[1]
		epoch = unicodeDigitsToInt64(match[2])
		epoch.tzOverflow = tzOverflows(match[3])
	} else if match := reOnlyActor.FindStringSubmatch(line); match != nil {
		actor = match[1]
	} else {
		actor = line
	}
	first, _, _ := strings.Cut(actor, "\n")
	left := strings.Index(first, "<")
	right := -1
	if left >= 0 {
		if at := strings.Index(first[left+1:], ">"); at >= 0 {
			right = left + 1 + at
		}
	}
	if left >= 0 && right >= 0 {
		n, e := pythonparity.RStrip(first[:left]), first[left+1:right]
		return &n, &e, epoch
	}
	return &actor, nil, epoch
}

// unicodeDigitsToInt64 is int() of a run of Unicode decimal digits.
func unicodeDigitsToInt64(digits string) epochValue {
	var value int64
	for _, r := range digits {
		digit, ok := unicodeDigit(r)
		if !ok {
			return epochValue{}
		}
		if value > (math.MaxInt64-int64(digit))/10 {
			return epochValue{} // does not fit
		}
		value = value*10 + int64(digit)
	}
	return epochValue{value: value, fits: true}
}

// headCommit is `repo.head.commit`: HEAD is dereferenced through symbolic refs like
// GitPython does, an annotated tag is peeled once, and a target that is not a commit
// is an error.
func (r Repo) headCommit(ctx context.Context) (string, error) {
	commonDir, err := r.commonDir(ctx)
	if err != nil {
		return "", err
	}
	gitDir, _ := r.gitDir()
	hash, skip, err := dereferenceRef(gitDir, commonDir, "HEAD")
	if err != nil {
		return "", err
	}
	if skip {
		return "", fmt.Errorf("head has no commit (ValueError: the reference does not exist or cannot be parsed)")
	}
	kind, ok := r.objectType(ctx, hash)
	if !ok {
		return "", fmt.Errorf("head points at a missing object %s", hash)
	}
	tip, ok := r.peelToCommit(ctx, hash, kind)
	if !ok {
		return "", fmt.Errorf("head points at a %s, not a commit (TypeError)", kind)
	}
	return tip, nil
}

// tzOverflows is utctz_to_altz + timedelta(seconds=...): the offset "+HHMM" read as an
// integer gives (|n|//100)*3600 + (|n|%100)*60 seconds, and a timedelta holds at most
// 999999999 days.
func tzOverflows(offset string) bool {
	digits := strings.TrimLeft(offset, "+-")
	magnitude := unicodeDigitsToInt64(digits)
	if !magnitude.fits {
		return true
	}
	hundreds := magnitude.value / 100
	if hundreds > 24000000000 {
		return true
	}
	return hundreds*3600+(magnitude.value%100)*60 > 86399999999999
}
