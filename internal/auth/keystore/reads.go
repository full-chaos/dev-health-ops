package keystore

import (
	"context"
	"sync"
)

// FileReads bounds and shares key-file reads. A read of a hung filesystem
// cannot be interrupted, so each read runs on its own goroutine and a caller
// waits for it only until its own context ends. At most one read per file is
// in flight: callers that arrive meanwhile wait on that read instead of
// starting another, so a hung file holds one goroutine and one descriptor,
// not one per request. A finished read is never kept: the next call reads
// again, and a late failure answers only the callers still waiting.
//
// The zero value is ready to use. A nil *FileReads bounds each read but
// shares none.
type FileReads struct {
	mu      sync.Mutex
	flights map[readKey]*readFlight
	// read replaces readCustodiedFile; tests set it.
	read func(path string, limit int64) ([]byte, error)
}

// NewFileReads returns a FileReads whose reads go through read instead of the
// file contract. It exists so a test outside this package can make a read
// hang; production code uses the zero value.
func NewFileReads(read func(path string, limit int64) ([]byte, error)) *FileReads {
	return &FileReads{read: read}
}

type readKey struct {
	path  string
	limit int64
}

type readFlight struct {
	done chan struct{}
	data []byte
	err  error
}

func (r *FileReads) readFile(ctx context.Context, path string, limit int64) ([]byte, error) {
	if r == nil {
		r = &FileReads{}
	}
	key := readKey{path: path, limit: limit}
	r.mu.Lock()
	flight, joined := r.flights[key]
	if !joined {
		flight = &readFlight{done: make(chan struct{})}
		if r.flights == nil {
			r.flights = map[readKey]*readFlight{}
		}
		r.flights[key] = flight
		read := r.read
		if read == nil {
			read = readCustodiedFile
		}
		go func() {
			data, err := read(path, limit)
			r.mu.Lock()
			delete(r.flights, key)
			r.mu.Unlock()
			flight.data, flight.err = data, err
			close(flight.done)
		}()
	}
	r.mu.Unlock()

	select {
	case <-flight.done:
		if flight.err != nil {
			return nil, flight.err
		}
		return append([]byte(nil), flight.data...), nil
	case <-ctx.Done():
		return nil, failure(ReasonUnreadable, ctx.Err())
	}
}
