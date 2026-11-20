package run

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newClock() *clock { return &clock{t: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)} }

func root(t *testing.T) string {
	t.Helper()
	d, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(d, "dho-deploy")
}

func wantCode(t *testing.T, err error, code int, what string) {
	t.Helper()
	if Code(err) != code {
		t.Fatalf("%s: want code %d, got %v", what, code, err)
	}
}

// pre runs a pre phase to pre.pass and returns the state.
func pre(t *testing.T, r string, c *clock) *State {
	t.Helper()
	s, err := BeginPre(r, "dev-health", "s41-a", shaA, c.now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin("pre"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("LIVEREV", "449"); err != nil {
		t.Fatal(err)
	}
	if err := s.EndPre(); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestThePhasesRunInOrderOncePerRunTag(t *testing.T) {
	r, c := root(t), newClock()
	s, err := BeginPre(r, "dev-health", "s41-a", shaA, c.now)
	if err != nil {
		t.Fatal(err)
	}
	for _, ph := range []string{"deploy", "wait", "post"} {
		_, err := s.Begin(ph)
		wantCode(t, err, CodeRefused, ph+" before pre.pass")
	}
	if _, err := s.Begin("pre"); err != nil {
		t.Fatal(err)
	}
	if err := s.EndPre(); err != nil {
		t.Fatal(err)
	}
	_, err = s.Begin("pre")
	wantCode(t, err, CodeRefused, "a second pre on one RUN_TAG")
	_, err = BeginPre(r, "dev-health", "s41-a", shaA, c.now)
	wantCode(t, err, CodeRefused, "BeginPre on an existing RUN_TAG")
	_, err = s.Begin("wait")
	wantCode(t, err, CodeRefused, "wait before deploy.started")
	_, err = s.Begin("post")
	wantCode(t, err, CodeRefused, "post before deploy.pass")

	c.t = c.t.Add(PreMaxAge)
	if _, err := s.Begin("deploy"); err != nil {
		t.Fatalf("deploy at exactly PreMaxAge: %v", err)
	}
	if !s.Has(DeployStarted) {
		t.Fatal("deploy must mark deploy.started")
	}
	_, err = s.Begin("deploy")
	wantCode(t, err, CodeRefused, "a second deploy on one RUN_TAG")
	if _, err := s.Begin("wait"); err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(DeployPass); err != nil {
		t.Fatal(err)
	}
	_, err = s.Begin("wait")
	wantCode(t, err, CodeRefused, "wait after deploy.pass")
	_, err = s.Begin("upgrade")
	wantCode(t, err, CodeRefused, "an unknown phase")
}

func TestAStalePreIsRefused(t *testing.T) {
	r, c := root(t), newClock()
	s := pre(t, r, c)
	c.t = c.t.Add(PreMaxAge + time.Second)
	_, err := s.Begin("deploy")
	wantCode(t, err, CodeRefused, "deploy 1801 s after pre")
	if s.Has(DeployStarted) {
		t.Fatal("a refused deploy must not mark deploy.started")
	}
	c.t = c.t.Add(-2 * PreMaxAge)
	_, err = s.Begin("deploy")
	wantCode(t, err, CodeRefused, "a PREEND in the future")
}

func TestPostRunsAtMostFiveTries(t *testing.T) {
	r, c := root(t), newClock()
	s := pre(t, r, c)
	if _, err := s.Begin("deploy"); err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(DeployPass); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"post", "post-2", "post-3", "post-4", "post-5"} {
		o, err := s.Begin("post")
		if err != nil || filepath.Base(o.Dir) != want {
			t.Fatalf("try %d: %v %+v, want %s", i+1, err, o, want)
		}
	}
	_, err := s.Begin("post")
	wantCode(t, err, CodeRefused, "a 6th post try")
	if err := s.Mark(PostPass); err != nil {
		t.Fatal(err)
	}
	_, err = s.Begin("post")
	wantCode(t, err, CodeRefused, "post after post.pass")
}

func TestTheInputsBindEveryLaterPhase(t *testing.T) {
	r, c := root(t), newClock()
	pre(t, r, c)
	_, err := Open(r, "dev-health", "s41-a", shaB, c.now)
	wantCode(t, err, CodeRefused, "other inputs under the same RUN_TAG")
	_, err = Open(r, "dev-health", "s41-b", shaA, c.now)
	wantCode(t, err, CodeRefused, "a RUN_TAG with no pre")
	s, err := Open(r, "dev-health", "s41-a", shaA, c.now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin("deploy"); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][4]string{
		"relative root": {"rel/dho", "dev-health", "s41-a", shaA},
		"unclean root":  {r + "/../x", "dev-health", "s41-a", shaA},
		"bad RUN_TAG":   {r, "dev-health", "../x", shaA},
		"bad target":    {r, "Dev/health", "s41-a", shaA},
		"bad sha":       {r, "dev-health", "s41-c", "abc"},
	} {
		_, err := BeginPre(args[0], args[1], args[2], args[3], c.now)
		wantCode(t, err, CodeRefused, name)
	}
}

func TestStateKeysAreAppendOnlyWithOneRegexEach(t *testing.T) {
	r, c := root(t), newClock()
	s := pre(t, r, c)
	if v, err := s.GetInt("LIVEREV"); err != nil || v != 449 {
		t.Fatalf("LIVEREV = %d %v", v, err)
	}
	wantCode(t, s.Put("LIVEREV", "450"), CodeStop, "a key set twice")
	wantCode(t, s.Put("NEWKEY", "1"), CodeStop, "an unknown key")
	wantCode(t, s.Put("RESTARTS0", "1x"), CodeStop, "a bad number")
	wantCode(t, s.Put("GROUPDIG", "heavy:4bdf;ops"), CodeStop, "a bad GROUPDIG character")
	if err := s.Put("GROUPDIG", "heavy:4bdf329325b0,ops:fa9b7cd250de"); err != nil {
		t.Fatal(err)
	}
	wantCode(t, s.Mark(PrePass), CodeStop, "an existing marker")

	forge := func(line string) {
		f, err := os.OpenFile(filepath.Join(s.Dir, stateFile), os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err := f.WriteString(line); err != nil {
			t.Fatal(err)
		}
	}
	forge("LIVEREV=450\n")
	_, err := s.Get("LIVEREV")
	wantCode(t, err, CodeRefused, "a forged second LIVEREV line")
	forge("RESTARTS0=x\n")
	_, err = s.Get("RESTARTS0")
	wantCode(t, err, CodeRefused, "a forged bad value")
	_, err = s.Get("CFGHASH")
	wantCode(t, err, CodeRefused, "a missing key")
}

// G4: every file the run makes is 0600 and every dir 0700, with umask 0.
func TestRunFilesAre0600AndDirs0700(t *testing.T) {
	old := syscall.Umask(0)
	defer syscall.Umask(old)
	r, c := root(t), newClock()
	s := pre(t, r, c)
	o, err := s.Begin("deploy")
	if err != nil {
		t.Fatal(err)
	}
	f, err := o.create("I3d")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	l, err := s.OpenLog("roll", "deploy")
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	_, err = s.OpenLog("roll", "deploy")
	wantCode(t, err, CodeStop, "a second run log in the same second")
	n := 0
	err = filepath.WalkDir(filepath.Join(r, "dev-health"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fi, err := os.Lstat(p)
		if err != nil {
			return err
		}
		want := fs.FileMode(0o600)
		if d.IsDir() {
			want = fs.ModeDir | 0o700
		}
		if fi.Mode() != want {
			t.Errorf("%s: mode %v, want %v", strings.TrimPrefix(p, r), fi.Mode(), want)
		}
		n++
		return nil
	})
	if err != nil || n < 8 {
		t.Fatalf("walk: %v, %d entries", err, n)
	}
}

// A pre that recorded PREEND but did not mark pre.pass (it stopped between
// the two) does not open the deploy phase.
func TestDeployNeedsThePreMarkerNotOnlyPreEnd(t *testing.T) {
	r, c := root(t), newClock()
	s, err := BeginPre(r, "dev-health", "s41-a", shaA, c.now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin("pre"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("PREEND", "1795176000"); err != nil {
		t.Fatal(err)
	}
	_, err = s.Begin("deploy")
	wantCode(t, err, CodeRefused, "deploy with PREEND and no pre.pass")
	if s.Has(DeployStarted) {
		t.Fatal("a refused deploy must not mark deploy.started")
	}
}
