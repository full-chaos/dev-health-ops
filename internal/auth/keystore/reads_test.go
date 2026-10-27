package keystore

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hungReads is a FileReads whose reads block until release is closed, then
// read the real file.
type hungReads struct {
	*FileReads
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func newHungReads() *hungReads {
	h := &hungReads{entered: make(chan struct{}, 64), release: make(chan struct{})}
	h.FileReads = NewFileReads(func(path string, limit int64) ([]byte, error) {
		h.calls.Add(1)
		h.entered <- struct{}{}
		<-h.release
		return readCustodiedFile(path, limit)
	})
	return h
}

// flightFor returns the in-flight read of path, or nil.
func (h *hungReads) flightFor(path string, limit int64) *readFlight {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.flights[readKey{path: path, limit: limit}]
}

func expiredBy(t *testing.T, err error, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
	if got := ReasonOf(err); got != string(ReasonUnreadable) {
		t.Fatalf("reason %q, want %q", got, ReasonUnreadable)
	}
}

func TestHungKeyReadReturnsAtTheCallersDeadline(t *testing.T) {
	path, _ := writeKeyFile(t, t.TempDir(), "k.pem")
	for name, call := range map[string]func(ctx context.Context, s FileSource) error{
		"describe": func(ctx context.Context, s FileSource) error { _, err := s.Describe(ctx); return err },
		"sign": func(ctx context.Context, s FileSource) error {
			_, err := s.Sign(ctx, "kid", []byte("m"))
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			hung := newHungReads()
			source := FileSource{Path: path, KeyID: "kid", Reads: hung.FileReads}
			ctx, cancel := context.WithCancel(context.Background())
			errc := make(chan error, 1)
			go func() { errc <- call(ctx, source) }()
			<-hung.entered
			flight := hung.flightFor(path, MaxKeyFileBytes)
			if flight == nil {
				t.Fatal("no read in flight")
			}
			cancel()
			expiredBy(t, <-errc, context.Canceled)

			select {
			case <-flight.done:
				t.Fatal("the read ended before it was released")
			default:
			}
			close(hung.release)
			<-flight.done
			if hung.flightFor(path, MaxKeyFileBytes) != nil {
				t.Fatal("a finished read stayed registered")
			}

			// The late result was not kept: the next call reads again.
			if err := call(context.Background(), source); err != nil {
				t.Fatalf("call after the hang: %v", err)
			}
			if n := hung.calls.Load(); n != 2 {
				t.Fatalf("%d reads, want 2", n)
			}
		})
	}
}

func TestConcurrentCallersOnAHungReadShareOneRead(t *testing.T) {
	path, _ := writeKeyFile(t, t.TempDir(), "k.pem")
	hung := newHungReads()
	source := FileSource{Path: path, KeyID: "kid", Reads: hung.FileReads}
	const callers = 16
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, errs[i] = source.Describe(ctx)
		})
	}
	wg.Wait()
	for _, err := range errs {
		expiredBy(t, err, context.DeadlineExceeded)
	}
	if n := hung.calls.Load(); n != 1 {
		t.Fatalf("%d reads for %d callers on one hung file, want 1", n, callers)
	}
	flight := hung.flightFor(path, MaxKeyFileBytes)
	close(hung.release)
	<-flight.done
}

func TestALateReadFailureIsNotKept(t *testing.T) {
	dir := t.TempDir()
	path, _ := writeKeyFile(t, dir, "k.pem")
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var calls atomic.Int64
	reads := NewFileReads(func(p string, limit int64) ([]byte, error) {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
			return nil, failure(ReasonUnreadable, errors.New("io"))
		}
		return readCustodiedFile(p, limit)
	})
	source := FileSource{Path: path, KeyID: "kid", Reads: reads}

	waiter := make(chan error, 1)
	go func() { _, err := source.Describe(context.Background()); waiter <- err }()
	<-entered
	reads.mu.Lock()
	flight := reads.flights[readKey{path: path, limit: MaxKeyFileBytes}]
	reads.mu.Unlock()
	close(release)
	<-flight.done
	if err := <-waiter; ReasonOf(err) != string(ReasonUnreadable) {
		t.Fatalf("live waiter got %v, want the read's failure", err)
	}
	if _, err := source.Describe(context.Background()); err != nil {
		t.Fatalf("next call after a late failure: %v", err)
	}
}

func TestHungManifestReadReturnsAtTheCallersDeadline(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "k1.pem")
	manifest := writeManifest(t, dir, `{"keys":[{"kid":"k1","state":"active","file":"k1.pem"}]}`)
	hung := newHungReads()
	custody := NewFileCustodyWithReads(manifest, nil, hung.FileReads)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := custody.Keys(ctx); errc <- err }()
	<-hung.entered
	flight := hung.flightFor(filepath.Clean(manifest), MaxManifestBytes)
	cancel()
	expiredBy(t, <-errc, context.Canceled)
	close(hung.release)
	<-flight.done
}
