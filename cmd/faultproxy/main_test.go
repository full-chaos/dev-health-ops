package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestCLIUsageErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"no flags":          {},
		"no upstream":       {"--listen", "127.0.0.1:0"},
		"bad schedule":      {"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--schedule", "5s=explode"},
		"unsorted":          {"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--schedule", "5s=pass,1s=cut"},
		"upstream host":     {"--listen", "127.0.0.1:0", "--upstream", "db.example.com:5432"},
		"listen host":       {"--listen", "0.0.0.0:0", "--upstream", "127.0.0.1:1"},
		"zero conns":        {"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--max-conns", "0"},
		"control public":    {"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--control", "0.0.0.0:0"},
		"unknown flag":      {"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--nope"},
		"events unwritable": {"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--events", "/nonexistent-dir/x.jsonl"},
	} {
		var o, e bytes.Buffer
		if code := run(context.Background(), args, &o, &e); code != 2 {
			t.Fatalf("%s: code=%d err=%s", name, code, e.String())
		}
	}
}

func TestCLIRunsUntilCancelled(t *testing.T) {
	var o, e safeBuf
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--control", "127.0.0.1:0", "--schedule", "10s=blackhole"}, &o, &e)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(o.String(), "control ") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(o.String(), "listening 127.0.0.1:") || !strings.Contains(o.String(), "control 127.0.0.1:") {
		t.Fatalf("out=%q err=%q", o.String(), e.String())
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code=%d err=%s", code, e.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop on cancel")
	}
}

func TestCLIEventsWriteFailureExitsNonZero(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("no /dev/full")
	}
	var o, e safeBuf
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--events", "/dev/full"}, &o, &e)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(o.String(), "listening") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 || !strings.Contains(e.String(), "events") {
			t.Fatalf("code=%d err=%q", code, e.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop")
	}
}

func TestCLIEventsFileIsWrittenAndClosedCleanly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ev.jsonl")
	var o, e safeBuf
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"--listen", "127.0.0.1:0", "--upstream", "127.0.0.1:1", "--events", path}, &o, &e)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(o.String(), "listening") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("code=%d err=%q", code, e.String())
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), `"pass"`) {
		t.Fatalf("%q %v", data, err)
	}
}
