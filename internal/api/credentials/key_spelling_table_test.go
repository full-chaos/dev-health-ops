//go:build unix

package credentials

import (
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// openCounter is a named pipe that serves a key to every reader and counts each open, so a cell can prove that no
// file is opened at all, not only that no request followed.
type openCounter struct {
	path  string
	opens atomic.Int32
	done  chan struct{}
}

func newOpenCounter(t *testing.T, content string) *openCounter {
	t.Helper()
	c := &openCounter{path: filepath.Join(t.TempDir(), "key.pem"), done: make(chan struct{})}
	if err := syscall.Mkfifo(c.path, 0o600); err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		for {
			f, err := os.OpenFile(c.path, os.O_WRONLY, 0)
			if err != nil {
				return
			}
			select {
			case <-c.done:
				f.Close()
				return
			default:
			}
			c.opens.Add(1)
			_, _ = f.WriteString(content)
			f.Close()
			// A reader that still holds the pipe would pair with the next writer open at once (Linux) and be served
			// again: give it time to read EOF and close. A slow reader is then counted twice (a false red), and a new
			// reader waits in its own open until the next writer open, so no open is missed.
			select {
			case <-c.done:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() {
		close(c.done)
		// The server may not have reached its blocking open yet: keep opening a reader until it has seen done.
		for deadline := time.Now().Add(10 * time.Second); ; {
			if f, err := os.OpenFile(c.path, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
				f.Close()
			}
			select {
			case <-served:
				return
			case <-time.After(10 * time.Millisecond):
			}
			if time.Now().After(deadline) {
				t.Error("the open counter did not stop")
				return
			}
		}
	})
	return c
}

// keySpellingValues are the value classes of a key-path field: a usable path, false, blank and a non-string.
func keySpellingValues(path string) map[string]string {
	return map[string]string{"truthy": strconv.Quote(path), "falsy": "false", "blank": `""`, "wrongtype": "7"}
}

type keySpellingRow struct {
	name   string
	fields string // the two key-path members, in order, as JSON object members
}

func keySpellingRows(path string) []keySpellingRow {
	values := keySpellingValues(path)
	var rows []keySpellingRow
	for _, order := range [][2]string{{"private_key_path", "privateKeyPath"}, {"privateKeyPath", "private_key_path"}} {
		for _, first := range []string{"truthy", "falsy", "blank", "wrongtype"} {
			for _, second := range []string{"truthy", "falsy", "blank", "wrongtype"} {
				rows = append(rows, keySpellingRow{
					name:   order[0] + "=" + first + "," + order[1] + "=" + second,
					fields: strconv.Quote(order[0]) + ":" + values[first] + "," + strconv.Quote(order[1]) + ":" + values[second],
				})
			}
		}
	}
	return rows
}

func storedPathFields(t *testing.T, stored *pyjson.Object) []string {
	t.Helper()
	var named []string
	for _, key := range stored.Keys() {
		if key == "private_key_path" || key == "privateKeyPath" {
			named = append(named, key)
		}
	}
	return named
}

// Every pair of key-path spellings, in both orders, over truthy / false / blank / non-string values: each route either
// answers the fixed refusal, or goes on with no file opened, no provider request, and (for a write) no key-path field in
// the secret it stores.
func TestCredentialRoutesKeySpellingTable(t *testing.T) {
	counter := newOpenCounter(t, testPrivateKey(t))
	rows := keySpellingRows(counter.path)
	for _, surface := range []string{"create", "patch", "test"} {
		t.Run(surface, func(t *testing.T) {
			refused, accepted := 0, 0
			for _, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					harness := newInputsHarness(t)
					credentials := `{"app_id":"1","installation_id":"2",` + row.fields + `,"base_url":"` + inputsStubBase + `"}`
					var recorder *httptest.ResponseRecorder
					var reachedStore any
					opensBefore := counter.opens.Load()
					switch surface {
					case "create":
						recorder, reachedStore = harness.serve(t, handlers.create, http.MethodPost, `{"provider":"github","name":"app","credentials":`+credentials+`}`, nil)
					case "patch":
						recorder, reachedStore = harness.serve(t, handlers.update, http.MethodPatch, `{"credentials":`+credentials+`}`, map[string]string{"provider": "github", "name": "app"})
					case "test":
						recorder, reachedStore = harness.serve(t, handlers.testConnection, http.MethodPost, `{"provider":"github","credentials":`+credentials+`}`, nil)
					}
					if reachedStore == nil && recorder.Code == http.StatusBadRequest && strings.TrimSpace(recorder.Body.String()) == wantKeyPathRefusal {
						refused++
					} else {
						accepted++
						if surface != "test" && reachedStore == nil {
							t.Errorf("accepted without reaching the store: status %d body %s", recorder.Code, recorder.Body.String())
						}
						if surface != "test" {
							// The secret a write seals is exactly normalizeCredentialKeys(provider, credentials) (setIn).
							if named := storedPathFields(t, normalizeCredentialKeys("github", decodeObject(t, credentials))); len(named) != 0 {
								t.Errorf("an accepted write stores the key-path field(s) %v", named)
							}
						}
					}
					if opened := counter.opens.Load() - opensBefore; opened != 0 {
						t.Errorf("%d file open(s); want 0", opened)
					}
					if sent := harness.hits.Load(); sent != 0 {
						t.Errorf("%d provider request(s); want 0", sent)
					}
				})
			}
			t.Logf("%s: %d refused with the fixed text, %d accepted", surface, refused, accepted)
		})
	}
}

// A stored row holding both spellings, in both orders, through the repository listing: whatever it answers, it opens no
// file and sends no provider request.
func TestRepositoryListingStoredKeySpellingTable(t *testing.T) {
	counter := newOpenCounter(t, testPrivateKey(t))
	for _, row := range keySpellingRows(counter.path) {
		t.Run(row.name, func(t *testing.T) {
			harness := newInputsHarness(t)
			decrypted := decodeObject(t, `{"app_id":"1","installation_id":"2",`+row.fields+`,"base_url":"`+inputsStubBase+`"}`)
			opensBefore := counter.opens.Load()
			recorder := httptest.NewRecorder()
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("the listing panicked: %v", r)
					}
				}()
				harness.h.listGitHubRepos(recorder, httptest.NewRequest(http.MethodGet, "/credentials/x/repos", nil),
					&repoCredential{provider: "github", decrypted: decrypted}, "acme", "", big.NewInt(100))
			}()
			if opened := counter.opens.Load() - opensBefore; opened != 0 {
				t.Errorf("%d file open(s); want 0", opened)
			}
			if sent := harness.hits.Load(); sent != 0 {
				t.Errorf("%d provider request(s); want 0 (status %d)", sent, recorder.Code)
			}
		})
	}
}

// The open counter itself: a direct read of its path is one open and yields the key, so a zero above is not a dead seam.
func TestKeySpellingOpenCounterSeesARead(t *testing.T) {
	key := testPrivateKey(t)
	counter := newOpenCounter(t, key)
	content, err := os.ReadFile(counter.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if got := counter.opens.Load(); got != 1 || string(content) != key {
		t.Errorf("a direct read counted %d open(s) and read %d bytes; want 1 open and the key", got, len(content))
	}
}
