package run

import (
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	l, err := s.OpenLog("roll", o)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	_, err = s.OpenLog("roll", o)
	wantCode(t, err, CodeStop, "a second run log of one try")
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

// r1 P1: PREEND keeps the clock's full precision; a pre that ended at .9 s
// is fresh for exactly PreMaxAge.
func TestAFractionalPreEndKeepsItsFullAge(t *testing.T) {
	for i, c := range []struct {
		after time.Duration
		ok    bool
	}{{PreMaxAge - time.Millisecond, true}, {PreMaxAge, true}, {PreMaxAge + time.Millisecond, false}} {
		r, clk := root(t), newClock()
		clk.t = clk.t.Add(900 * time.Millisecond)
		s, err := BeginPre(r, "dev-health", "s41-"+strconv.Itoa(i), shaA, clk.now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Begin("pre"); err != nil {
			t.Fatal(err)
		}
		if err := s.EndPre(); err != nil {
			t.Fatal(err)
		}
		clk.t = clk.t.Add(c.after)
		_, err = s.Begin("deploy")
		if c.ok && err != nil || !c.ok && Code(err) != CodeRefused {
			t.Fatalf("deploy %v after a pre that ended at .9 s: %v (want ok=%v)", c.after, err, c.ok)
		}
	}
}

// race runs n calls at once and counts nil and each exit code.
func race(n int, call func(i int) error) (ok, stop, refused int) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			err := call(i)
			mu.Lock()
			defer mu.Unlock()
			switch Code(err) {
			case 0:
				ok++
			case CodeStop:
				stop++
			case CodeRefused:
				refused++
			}
		}(i)
	}
	close(start)
	wg.Wait()
	return
}

// r1 P1: racing phase calls, each on its own handle: one deploy wins and the
// others are refused (exit 3, not a STOP); post takes its 5 tries and refuses
// the rest; racing waits each get their own try dir and log.
func TestRacingPhaseCallsReserveOnceAndRefuseTheRest(t *testing.T) {
	r, c := root(t), newClock()
	pre(t, r, c)
	open := func() *State {
		s, err := Open(r, "dev-health", "s41-a", shaA, c.now)
		if err != nil {
			t.Error(err)
		}
		return s
	}
	if ok, stop, refused := race(24, func(int) error { _, err := open().Begin("deploy"); return err }); ok != 1 || stop != 0 || refused != 23 {
		t.Fatalf("24 racing deploys: ok %d, stop %d, refused %d; want 1, 0, 23", ok, stop, refused)
	}
	var mu sync.Mutex
	logs := map[string]bool{}
	ok, stop, refused := race(24, func(int) error {
		s := open()
		o, err := s.Begin("wait")
		if err != nil {
			return err
		}
		l, err := s.OpenLog("roll", o)
		if err != nil {
			return err
		}
		mu.Lock()
		logs[l.(*os.File).Name()] = true
		mu.Unlock()
		return l.Close()
	})
	if ok != 24 || stop != 0 || refused != 0 || len(logs) != 24 {
		t.Fatalf("24 racing waits in one second: ok %d, stop %d, refused %d, %d logs; want 24 each with its own log", ok, stop, refused, len(logs))
	}
	if err := open().Mark(DeployPass); err != nil {
		t.Fatal(err)
	}
	if ok, stop, refused := race(24, func(int) error { _, err := open().Begin("post"); return err }); ok != PostTries || stop != 0 || refused != 24-PostTries {
		t.Fatalf("24 racing posts: ok %d, stop %d, refused %d; want %d, 0, %d", ok, stop, refused, PostTries, 24-PostTries)
	}
}

// r1 P2: racing Puts of one key on their own handles set it once (50 rounds,
// 24 callers each: one round alone can miss the race).
func TestRacingPutsSetAKeyOnce(t *testing.T) {
	r, c := root(t), newClock()
	for round := 0; round < 50; round++ {
		tag := "p" + strconv.Itoa(round)
		if _, err := BeginPre(r, "dev-health", tag, shaA, c.now); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"RESTARTS0", "GROUPDIG"} {
			ok, stop, _ := race(24, func(i int) error {
				s, err := Open(r, "dev-health", tag, shaA, c.now)
				if err != nil {
					return err
				}
				return s.Put(key, strconv.Itoa(i))
			})
			if ok != 1 || stop != 23 {
				t.Fatalf("round %d %s: ok %d, stop %d; want 1, 23", round, key, ok, stop)
			}
			s, _ := Open(r, "dev-health", tag, shaA, c.now)
			if _, err := s.Get(key); err != nil {
				t.Fatalf("round %d %s after the race: %v", round, key, err)
			}
		}
	}
}

// r1 P2: a legal state line longer than 64 KiB hides no key from Get or Put.
func TestALongStateLineHidesNoKey(t *testing.T) {
	r, c := root(t), newClock()
	s, err := BeginPre(r, "dev-health", "s41-a", shaA, c.now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("RESTARTMAP", strings.Repeat("a:0,", 20000)+"a:0"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("LIVEREV", "449"); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get("LIVEREV"); err != nil || v != "449" {
		t.Fatalf("LIVEREV after a long line: %q %v", v, err)
	}
	wantCode(t, s.Put("RESTARTMAP", "b:1"), CodeStop, "a long key set twice")
	if v, err := s.Get("RESTARTMAP"); err != nil || len(v) < 80000 {
		t.Fatalf("RESTARTMAP: len %d %v", len(v), err)
	}
}
