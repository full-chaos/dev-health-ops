package run

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// Root is the run state on the prod node: <Root>/<target>/<RUN_TAG>/.
	Root = "/home/ubuntu/lane-prod-k8s/dho-deploy"
	// PreMaxAge is the kit PRE_MAX_AGE: deploy runs within 1800 s of pre.
	PreMaxAge = 1800 * time.Second
	// PostTries is the kit POST_TRIES: post, post-2 .. post-5.
	PostTries = 5

	inputsFile = "inputs.sha256"
	stateFile  = "state.txt"
)

// Markers (noclobber in the kit; O_EXCL here).
const (
	PrePass       = "pre.pass"
	DeployStarted = "deploy.started"
	DeployPass    = "deploy.pass"
	PostPass      = "post.pass"
)

var (
	runTagRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	targetRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	sha256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)
	labelRe  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
	numberRe = regexp.MustCompile(`^[0-9]+$`)
)

// stateKeys is the kit state_put table: one regex per key; any other key is a STOP.
var stateKeys = map[string]*regexp.Regexp{
	"LIVEREV":    numberRe,
	"RESTARTS0":  numberRe,
	"PREEND":     numberRe,
	"RESTARTMAP": regexp.MustCompile(`^[A-Za-z0-9.:,_-]+$`),
	"GROUPDIG":   regexp.MustCompile(`^[a-z0-9:,+-]+$`),
	"CFGHASH":    regexp.MustCompile(`^[a-z0-9-]+:[a-z0-9]+(,[a-z0-9-]+:[a-z0-9]+)*$`),
}

// State is the run dir of one RUN_TAG. Every file in it is created O_EXCL
// (0600, dirs 0700); state.txt is append-only, one line per key.
type State struct {
	Dir string
	now func() time.Time
}

// BeginPre makes the run dir of a NEW RUN_TAG and binds it to the inputs
// sha256. An existing dir is refused: a new pre phase needs a new RUN_TAG.
func BeginPre(root, target, runTag, inputsSHA string, now func() time.Time) (*State, error) {
	dir, err := runDir(root, target, runTag, inputsSHA)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return nil, Refuse("cannot create the dir of %s", dir)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, Refuse("%s exists: a new pre phase needs a new RUN_TAG", dir)
		}
		return nil, Refuse("cannot create %s", dir)
	}
	s := &State{Dir: dir, now: now}
	if err := s.create(inputsFile, []byte(inputsSHA+"\n")); err != nil {
		return nil, err
	}
	return s, nil
}

// Open takes the run dir of a RUN_TAG whose pre ran with the same inputs.
func Open(root, target, runTag, inputsSHA string, now func() time.Time) (*State, error) {
	dir, err := runDir(root, target, runTag, inputsSHA)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(dir, inputsFile))
	if err != nil {
		return nil, Refuse("no %s in %s: run the pre phase first", inputsFile, dir)
	}
	if string(b) != inputsSHA+"\n" {
		return nil, Refuse("the inputs differ from the ones the pre phase of RUN_TAG %s ran with", runTag)
	}
	return &State{Dir: dir, now: now}, nil
}

func runDir(root, target, runTag, inputsSHA string) (string, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", Refuse("run root is not a clean absolute path")
	}
	if !targetRe.MatchString(target) || !runTagRe.MatchString(runTag) {
		return "", Refuse("bad target or RUN_TAG")
	}
	if !sha256Re.MatchString(inputsSHA) {
		return "", Refuse("bad inputs sha256")
	}
	return filepath.Join(root, target, runTag), nil
}

// create writes a new file O_EXCL; an existing file is a STOP.
func (s *State) create(name string, data []byte) error {
	f, err := os.OpenFile(filepath.Join(s.Dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return Stop("%s exists in %s (records are append-only)", name, s.Dir)
		}
		return Stop("cannot create %s in %s", name, s.Dir)
	}
	_, werr := f.Write(data)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return Stop("cannot write %s in %s", name, s.Dir)
	}
	return nil
}

// Has says if a marker exists.
func (s *State) Has(marker string) bool {
	_, err := os.Lstat(filepath.Join(s.Dir, marker))
	return err == nil
}

// Mark makes a marker; an existing marker is a STOP.
func (s *State) Mark(marker string) error {
	return s.create(marker, nil)
}

// Put appends KEY=VALUE once. An unknown key, a bad value or a key that is
// already set is a STOP.
func (s *State) Put(key, value string) error {
	re, ok := stateKeys[key]
	if !ok {
		return Stop("STATE: unknown key %s", key)
	}
	if !re.MatchString(value) {
		return Stop("STATE %s has a bad value", key)
	}
	vals, err := s.values(key)
	if err != nil {
		return err
	}
	if len(vals) != 0 {
		return Stop("STATE %s is already set in %s", key, filepath.Join(s.Dir, stateFile))
	}
	f, err := os.OpenFile(filepath.Join(s.Dir, stateFile), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return Stop("cannot open %s", stateFile)
	}
	_, werr := fmt.Fprintf(f, "%s=%s\n", key, value)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return Stop("cannot write %s", stateFile)
	}
	return nil
}

// Get reads KEY: exactly one line, and the value matches the key regex.
func (s *State) Get(key string) (string, error) {
	re, ok := stateKeys[key]
	if !ok {
		return "", Stop("STATE: unknown key %s", key)
	}
	vals, err := s.values(key)
	if err != nil {
		return "", err
	}
	if len(vals) != 1 {
		return "", Refuse("state %s: %d lines in %s (want 1)", key, len(vals), stateFile)
	}
	if !re.MatchString(vals[0]) {
		return "", Refuse("state %s has a bad value in %s", key, stateFile)
	}
	return vals[0], nil
}

// GetInt is Get for a number key.
func (s *State) GetInt(key string) (int64, error) {
	v, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, Refuse("state %s is not a number", key)
	}
	return n, nil
}

func (s *State) values(key string) ([]string, error) {
	b, err := os.ReadFile(filepath.Join(s.Dir, stateFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, Stop("cannot read %s", stateFile)
	}
	var out []string
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), key+"="); ok {
			out = append(out, v)
		}
	}
	return out, nil
}

// EndPre records PREEND and the pre.pass marker.
func (s *State) EndPre() error {
	if err := s.Put("PREEND", strconv.FormatInt(s.now().Unix(), 10)); err != nil {
		return err
	}
	return s.Mark(PrePass)
}

// PreFresh refuses a pre that ended more than PreMaxAge ago.
func (s *State) PreFresh() (time.Duration, error) {
	t, err := s.GetInt("PREEND")
	if err != nil {
		return 0, err
	}
	age := s.now().Sub(time.Unix(t, 0))
	if age < 0 || age > PreMaxAge {
		return age, Refuse("the pre phase ended %d s ago (want 0..%d s): run a new pre phase (new RUN_TAG)", int64(age/time.Second), int64(PreMaxAge/time.Second))
	}
	return age, nil
}

// Out is the step output dir of one phase try.
type Out struct {
	Dir string
}

// Begin checks the dev-health phase order and makes the phase's output dir:
// pre (after BeginPre), deploy (once per RUN_TAG, within PreMaxAge of pre),
// wait (deploy started, not passed), post (deploy passed, at most PostTries).
func (s *State) Begin(phase string) (*Out, error) {
	switch phase {
	case "pre":
		if s.Has(PrePass) {
			return nil, Refuse("pre.pass exists: a new pre phase needs a new RUN_TAG")
		}
		return s.newOut("pre")
	case "deploy":
		if !s.Has(PrePass) {
			return nil, Refuse("no pre.pass in %s: run the pre phase first", s.Dir)
		}
		if s.Has(DeployStarted) {
			return nil, Refuse("deploy.started exists: the deploy runs once per RUN_TAG (use the wait phase)")
		}
		if _, err := s.PreFresh(); err != nil {
			return nil, err
		}
		o, err := s.newOut("deploy")
		if err != nil {
			return nil, err
		}
		if err := s.Mark(DeployStarted); err != nil {
			return nil, err
		}
		return o, nil
	case "wait":
		if !s.Has(DeployStarted) {
			return nil, Refuse("no deploy.started in %s", s.Dir)
		}
		if s.Has(DeployPass) {
			return nil, Refuse("deploy.pass exists: run the post phase")
		}
		return s.newOut("wait-" + s.now().UTC().Format("150405"))
	case "post":
		if !s.Has(DeployPass) {
			return nil, Refuse("no deploy.pass in %s", s.Dir)
		}
		if s.Has(PostPass) {
			return nil, Refuse("post.pass exists in %s", s.Dir)
		}
		n := 1
		for ; n <= PostTries; n++ {
			if _, err := os.Lstat(filepath.Join(s.Dir, postDir(n))); errors.Is(err, fs.ErrNotExist) {
				break
			}
		}
		if n > PostTries {
			return nil, Refuse("post: %d tries already in %s, max %d", PostTries, s.Dir, PostTries)
		}
		return s.newOut(postDir(n))
	}
	return nil, Refuse("unknown phase")
}

func postDir(n int) string {
	if n == 1 {
		return "post"
	}
	return "post-" + strconv.Itoa(n)
}

func (s *State) newOut(name string) (*Out, error) {
	dir := filepath.Join(s.Dir, name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, Stop("%s exists (records are append-only)", dir)
		}
		return nil, Stop("cannot create %s", dir)
	}
	return &Out{Dir: dir}, nil
}

// create makes <label>.out O_EXCL before the step runs; an existing file is a STOP.
func (o *Out) create(label string) (*os.File, error) {
	if !labelRe.MatchString(label) {
		return nil, Stop("bad step label")
	}
	p := filepath.Join(o.Dir, label+".out")
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, Stop("%s exists (records are append-only)", p)
		}
		return nil, Stop("cannot create %s", p)
	}
	return f, nil
}

// OpenLog makes the run log <RUN_TAG>/<verb>-<phase>-<HHMMSS>Z.log O_EXCL.
func (s *State) OpenLog(verb, phase string) (io.WriteCloser, error) {
	if !labelRe.MatchString(verb) || !labelRe.MatchString(phase) {
		return nil, Stop("bad log name")
	}
	name := fmt.Sprintf("%s-%s-%sZ.log", verb, phase, s.now().UTC().Format("150405"))
	f, err := os.OpenFile(filepath.Join(s.Dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, Stop("%s exists (records are append-only)", name)
		}
		return nil, Stop("cannot create %s", name)
	}
	return f, nil
}
