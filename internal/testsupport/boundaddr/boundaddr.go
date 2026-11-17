// Package boundaddr lets a test start a service on port 0 and read the address its listener really bound.
// A port picked, closed and bound again later can be taken by another test in between; a port bound once by the service cannot.
package boundaddr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

const listeningMessage = "runtime component listening"

// Logs is a log sink the service writes from its own goroutines while the test reads.
type Logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *Logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *Logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// Find returns the address attribute of the first "runtime component listening" JSON line of component. A partial or
// non-JSON line is skipped.
func Find(logs string, component string) (string, bool) {
	for _, line := range strings.Split(logs, "\n") {
		var record struct {
			Msg       string `json:"msg"`
			Component string `json:"component"`
			Address   string `json:"address"`
		}
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record.Msg == listeningMessage && record.Component == component && record.Address != "" {
			return record.Address, true
		}
	}
	return "", false
}

// Await waits for the listening line of component and returns its address. exited reports the service's exit code: an
// early exit fails the test at once. No line within the guard fails the test; there is no fallback address.
func Await(t testing.TB, logs *Logs, component string, exited <-chan int) string {
	t.Helper()
	address, err := await(logs, component, exited, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func await(logs *Logs, component string, exited <-chan int, guard time.Duration) (string, error) {
	deadline := time.Now().Add(guard)
	for {
		if address, ok := Find(logs.String(), component); ok {
			return address, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("no listening line for %s within %v; logs=%s", component, guard, logs.String())
		}
		select {
		case code := <-exited:
			return "", fmt.Errorf("exited %d before a listening line for %s; logs=%s", code, component, logs.String())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
