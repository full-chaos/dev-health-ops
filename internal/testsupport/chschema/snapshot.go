package chschema

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The migration chain is expensive: the Python runner executes ~110 migrations
// one DDL statement at a time, which costs 15-30 s per fresh container on a
// hosted runner. A package whose tests each start their own container paid
// that price once per test, and internal/jobs/metrics/remaining alone spent
// ~17 of a 25-minute CI job budget on it.
//
// The chain is deterministic for a given checkout AND a given process
// environment (OPERATIONAL_ORDERING_CONTRACT, for one, selects which table
// shape the chain builds), so it runs ONCE per distinct environment in a test
// process, against the first container. Its end state is then read back from
// that container (system.tables plus the rows of every table that holds any)
// and replayed into every later container of the same environment over the
// same HTTP interface, in parallel. The key is the whole child environment,
// never a hand-listed subset of variables, so a variable the chain starts to
// read later cannot be missed; an unrelated change only costs one more run. The schema is still produced only by the real chain; nothing here
// authors DDL. TestReplayedSchemaEqualsMigratedSchema compares the two
// containers object by object, column by column and row by row.

// replayWorkers bounds the concurrent DDL statements of one replay.
const replayWorkers = 8

// replayHTTPTimeout bounds one statement. Loaded runners answer DDL slowly.
const replayHTTPTimeout = 2 * time.Minute

// schemaObject is one table or view of the migrated database.
type schemaObject struct {
	Name   string `json:"name"`
	Engine string `json:"engine"`
	Query  string `json:"create_table_query"`
}

func (o schemaObject) isView() bool {
	return strings.HasSuffix(o.Engine, "View")
}

// schemaSnapshot is the migrated database as read back from a container.
type schemaSnapshot struct {
	database string
	objects  []schemaObject
	// rows holds JSONEachRow bodies keyed by table name. Migrations seed rows
	// (schema_migrations, and any future seed migration) and a schema guard
	// reads them, so they are part of the schema.
	rows map[string][]byte
}

var (
	snapshotMu sync.Mutex
	// cachedSnapshots maps chainInputKey() to the end state captured for it.
	cachedSnapshots = map[string]*schemaSnapshot{}
	// replayCount counts replays that ran, so a test can prove the fast path
	// was taken rather than the Python runner.
	replayCount atomic.Int64
)

// chainInputKey identifies everything the migration runner inherits: the
// process environment it is started with.
func chainInputKey() string {
	environment := os.Environ()
	sort.Strings(environment)
	sum := sha256.Sum256([]byte(strings.Join(environment, "\x00")))
	return hex.EncodeToString(sum[:])
}

// clickHouseHTTP speaks the ClickHouse HTTP interface using a clickhouse:// DSN
// whose port is the HTTP one (see containers.ClickHouseHTTPDSN).
type clickHouseHTTP struct {
	base     string
	user     string
	password string
	database string
	client   *http.Client
}

func newClickHouseHTTP(dsn string) (*clickHouseHTTP, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse ClickHouse HTTP DSN: %w", err)
	}
	password, _ := parsed.User.Password()
	return &clickHouseHTTP{
		base:     "http://" + parsed.Host + "/",
		user:     parsed.User.Username(),
		password: password,
		database: strings.TrimPrefix(parsed.Path, "/"),
		client:   &http.Client{Timeout: replayHTTPTimeout},
	}, nil
}

// do sends query, with body as the request payload when non-nil, and returns
// the response body. Server errors carry the server's own message.
func (c *clickHouseHTTP) do(ctx context.Context, query string, body []byte) ([]byte, error) {
	values := url.Values{}
	values.Set("database", c.database)
	var payload io.Reader
	if body != nil {
		values.Set("query", query)
		payload = bytes.NewReader(body)
	} else {
		payload = strings.NewReader(query)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"?"+values.Encode(), payload)
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(c.user, c.password)
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// captureSnapshot reads the migrated database back from the container behind c.
func captureSnapshot(ctx context.Context, c *clickHouseHTTP) (*schemaSnapshot, error) {
	// `.inner` tables are created by their materialized view's own CREATE, so
	// replaying them separately would collide with it.
	listing, err := c.do(ctx, "SELECT name, engine, create_table_query, total_rows FROM system.tables "+
		"WHERE database = "+quoteString(c.database)+" AND name NOT LIKE '.inner%' ORDER BY name FORMAT JSONEachRow", nil)
	if err != nil {
		return nil, fmt.Errorf("list migrated objects: %w", err)
	}
	snapshot := &schemaSnapshot{database: c.database, rows: map[string][]byte{}}
	for _, line := range bytes.Split(bytes.TrimSpace(listing), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry struct {
			schemaObject
			// total_rows is Nullable: engines that cannot count report NULL.
			TotalRows *json.Number `json:"total_rows"`
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("decode migrated object %q: %w", line, err)
		}
		snapshot.objects = append(snapshot.objects, entry.schemaObject)
		if entry.isView() {
			continue
		}
		if entry.TotalRows == nil {
			return nil, fmt.Errorf("table %s (%s) does not report a row count, so its seed rows cannot be captured",
				entry.Name, entry.Engine)
		}
		if entry.TotalRows.String() == "0" {
			continue
		}
		data, err := c.do(ctx, "SELECT * FROM "+quoteIdentifier(c.database)+"."+quoteIdentifier(entry.Name)+
			" FORMAT JSONEachRow", nil)
		if err != nil {
			return nil, fmt.Errorf("read seed rows of %s: %w", entry.Name, err)
		}
		snapshot.rows[entry.Name] = data
	}
	if len(snapshot.objects) == 0 {
		return nil, fmt.Errorf("database %q holds no tables after the migration chain", c.database)
	}
	return snapshot, nil
}

func quoteString(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}

// replay creates every captured object in the (empty) database behind c.
//
// Order: tables, then their seed rows, then views. A materialized view
// created before the rows land would re-emit them into its target, so the
// rows go in while no view exists. Within a phase a statement that fails is
// retried after the others (a view over a view needs its source first); a
// round that makes no progress is an error, never a silent partial schema.
func replay(ctx context.Context, c *clickHouseHTTP, snapshot *schemaSnapshot) error {
	var tables, views []schemaObject
	for _, object := range snapshot.objects {
		if object.isView() {
			views = append(views, object)
		} else {
			tables = append(tables, object)
		}
	}
	if err := createAll(ctx, c, tables); err != nil {
		return err
	}
	names := make([]string, 0, len(snapshot.rows))
	for name := range snapshot.rows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, err := c.do(ctx, "INSERT INTO "+quoteIdentifier(snapshot.database)+"."+quoteIdentifier(name)+
			" FORMAT JSONEachRow", snapshot.rows[name]); err != nil {
			return fmt.Errorf("restore seed rows of %s: %w", name, err)
		}
	}
	return createAll(ctx, c, views)
}

func createAll(ctx context.Context, c *clickHouseHTTP, objects []schemaObject) error {
	pending := objects
	for len(pending) > 0 {
		failures := make([]error, len(pending))
		work := make(chan int)
		var wg sync.WaitGroup
		for worker := 0; worker < replayWorkers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for index := range work {
					_, failures[index] = c.do(ctx, pending[index].Query, nil)
				}
			}()
		}
		for index := range pending {
			work <- index
		}
		close(work)
		wg.Wait()

		var retry []schemaObject
		var first error
		for index, err := range failures {
			if err != nil {
				retry = append(retry, pending[index])
				if first == nil {
					first = fmt.Errorf("create %s: %w", pending[index].Name, err)
				}
			}
		}
		if len(retry) == len(pending) {
			return first
		}
		pending = retry
	}
	return nil
}

// applyReplayed runs the real migration chain via migrate the first time a
// given environment is seen on an empty database and replays that captured end
// state on every later empty database of the same environment. Only throwaway
// containers use it; a remote ClickHouse is always migrated by the runner
// itself (see Apply).
func applyReplayed(ctx context.Context, t *testing.T, dsn string, migrate func()) {
	t.Helper()
	client, err := newClickHouseHTTP(dsn)
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	// Only a database with no objects takes the fast path. Applying the chain
	// to an already-migrated database is a real operation (moving a table to a
	// newer ordering contract in place, re-running a converging migration), and
	// a replay of CREATE statements into it would fail or, worse, be skipped.
	// Such a call always runs the chain itself and neither reads nor feeds the
	// cache: its end state is not the end state of a fresh container.
	listing, err := client.do(ctx, "SELECT count() FROM system.tables WHERE database = "+
		quoteString(client.database), nil)
	if err != nil {
		t.Fatalf("chschema: count existing objects: %v", err)
	}
	if strings.TrimSpace(string(listing)) != "0" {
		migrate()
		return
	}

	key := chainInputKey()
	snapshotMu.Lock()
	snapshot := cachedSnapshots[key]
	if snapshot == nil {
		func() {
			defer snapshotMu.Unlock()
			migrate()
			captured, err := captureSnapshot(ctx, client)
			if err != nil {
				t.Fatalf("chschema: capture the migrated schema: %v", err)
			}
			cachedSnapshots[key] = captured
			t.Logf("chschema: applied the real migration chain and captured %d objects, %d seeded tables",
				len(captured.objects), len(captured.rows))
		}()
		return
	}
	snapshotMu.Unlock()

	if snapshot.database != client.database {
		t.Fatalf("chschema: captured database %q, replay target %q", snapshot.database, client.database)
	}
	started := time.Now()
	if err := replay(ctx, client, snapshot); err != nil {
		t.Fatalf("chschema: replay the captured migrated schema: %v", err)
	}
	replayCount.Add(1)
	t.Logf("chschema: replayed the captured migrated schema (%d objects, %d seeded tables) in %s",
		len(snapshot.objects), len(snapshot.rows), time.Since(started).Round(time.Millisecond))
}
