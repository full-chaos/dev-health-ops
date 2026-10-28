//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// alembicVersions reads the recorded revision(s), so the test can prove a downgrade moved them and a refusal left
// them untouched.
func alembicVersions(t *testing.T, uri string) []string {
	t.Helper()
	conn := connect(t, uri)
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), "SELECT version_num FROM alembic_version ORDER BY version_num")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			t.Fatal(err)
		}
		out = append(out, version)
	}
	return out
}

// goDowngrade runs `dho migrate postgres downgrade <args...>` against uri.
func goDowngrade(t *testing.T, uri string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == "downgrade" {
			run = child.Run
		}
	}
	var out, errs bytes.Buffer
	got := run(context.Background(), cli.Env{Args: args, Lookup: func(string) (string, bool) { return "", false }, Stdout: &out, Stderr: &errs})
	if strings.Contains(out.String()+errs.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return got, out.String(), errs.String()
}

// downInstance is one PostgreSQL server with scratch databases.
type downInstance struct {
	instance *containers.Instance
	admin    *pgx.Conn
}

func newDownInstance(t *testing.T) downInstance {
	t.Helper()
	instance := sharedInstance(t)
	return downInstance{instance: instance, admin: connect(t, instance.URI)}
}

// newIsolatedDownInstance is for a test that changes server-wide state (a role default).
func newIsolatedDownInstance(t *testing.T) downInstance {
	t.Helper()
	instance := isolatedInstance(t)
	return downInstance{instance: instance, admin: connect(t, instance.URI)}
}

// at is a scratch database at chain revision k of the chain (k = 0 is the baseline,
// len(chain) the head): the baseline plus the first k chain files, built by dho's own
// upgrade.
func (d downInstance) at(t *testing.T, k int) string {
	t.Helper()
	uri := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), connect(t, uri), baseline, chain[:k]); err != nil {
		t.Fatalf("upgrade to chain position %d: %v", k, err)
	}
	return uri
}

// schemaShape is the public schema as the catalogs describe it -- columns (type,
// nullability, default), constraints, indexes, sequences and the recorded revisions --
// the state a downgrade must reach. Not a mock: read from the database.
func schemaShape(t *testing.T, uri string) string {
	t.Helper()
	conn := connect(t, uri)
	queries := []string{
		"SELECT 'col ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod) || ' notnull=' || a.attnotnull || ' default=' || coalesce(pg_get_expr(d.adbin, d.adrelid), '') FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND a.attnum > 0 AND NOT a.attisdropped",
		"SELECT 'con ' || c.relname || ' ' || con.conname || ' ' || pg_get_constraintdef(con.oid) FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'",
		"SELECT 'idx ' || indexdef FROM pg_indexes WHERE schemaname = 'public'",
		"SELECT 'rel ' || c.relkind::text || ' ' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'",
		"SELECT 'ver ' || version_num FROM alembic_version",
	}
	var lines []string
	for _, query := range queries {
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			t.Fatalf("read the schema shape: %v", err)
		}
		got, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, got...)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// plantedWalk is four revisions planted above the baseline (testdata/postfreeze/walk), each with its down file; the
// down file of 0164 refuses while planted_walk_b holds a row.
func plantedWalk(t *testing.T) {
	t.Helper()
	pgmigrate.UsePlantedChain(t, os.DirFS("testdata/postfreeze/walk"))
}

// floor is the baseline application head: the lowest revision a downgrade reverts to.
func floor(t *testing.T) string {
	t.Helper()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	for _, head := range baseline.Heads {
		if head != "0066" {
			return head
		}
	}
	t.Fatal("the baseline has no application head")
	return ""
}

func previousName(t *testing.T, chain []pgmigrate.ChainFile, k int) string {
	t.Helper()
	if k == 0 {
		return floor(t)
	}
	return chain[k-1].Revision
}

func shapeDiff(want, got string) string {
	inWant := map[string]bool{}
	for _, line := range strings.Split(want, "\n") {
		inWant[line] = true
	}
	inGot := map[string]bool{}
	var out []string
	for _, line := range strings.Split(got, "\n") {
		inGot[line] = true
		if !inWant[line] {
			out = append(out, "  extra:   "+line)
		}
	}
	for _, line := range strings.Split(want, "\n") {
		if !inGot[line] {
			out = append(out, "  missing: "+line)
		}
	}
	return strings.Join(out, "\n")
}

// On a single-head database at the head, -N (N the chain's length) reaches the floor revision and gives the same
// schema as the explicit floor target, and -(N+1) is refused with the database left alone.
func TestDowngradeFloorCasesOnARealSingleHeadDatabase(t *testing.T) {
	plantedWalk(t)
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	singleHead := func() string {
		uri := d.at(t, len(chain))
		if _, err := connect(t, uri).Exec(context.Background(), "DELETE FROM alembic_version WHERE version_num = '0066'"); err != nil {
			t.Fatal(err)
		}
		return uri
	}
	all, beyond := fmt.Sprintf("-%d", len(chain)), fmt.Sprintf("-%d", len(chain)+1)
	refused := singleHead()
	before := schemaShape(t, refused)
	code, stdout, stderr := goDowngrade(t, refused, beyond)
	if code != cli.ExitFailure || stdout != "" || !strings.Contains(stderr, `"code":"below_baseline_floor"`) {
		t.Errorf("downgrade %s: exit %d stdout %q stderr %q, want a below_baseline_floor refusal", beyond, code, stdout, stderr)
	}
	if got := schemaShape(t, refused); got != before {
		t.Fatalf("downgrade %s changed the database:\n%s", beyond, shapeDiff(before, got))
	}
	relative := singleHead()
	if code, _, stderr := goDowngrade(t, relative, all); code != cli.ExitOK {
		t.Fatalf("downgrade %s: exit %d %s", all, code, stderr)
	}
	explicit := singleHead()
	if code, _, stderr := goDowngrade(t, explicit, floor(t)); code != cli.ExitOK {
		t.Fatalf("downgrade %s: exit %d %s", floor(t), code, stderr)
	}
	if got := alembicVersions(t, relative); !reflect.DeepEqual(got, []string{floor(t)}) {
		t.Fatalf("downgrade %s left alembic_version %v, want %s", all, got, floor(t))
	}
	if got, want := schemaShape(t, relative), schemaShape(t, explicit); got != want {
		t.Errorf("downgrade %s and downgrade %s disagree:\n%s", all, floor(t), shapeDiff(want, got))
	}
}

// A database at the production shape (0066 and the application head both recorded):
// an explicit target reverts only the application branch and leaves 0066; the refusal
// cells leave the database byte-for-byte as it was. Enumerated by cell.
func TestDowngradeRefusalsLeaveTheDatabaseAlone(t *testing.T) {
	plantedWalk(t)
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	head := d.at(t, len(chain))
	shape := schemaShape(t, head)
	cells := []struct {
		args []string
		code int
		name string
	}{
		{[]string{"-1"}, cli.ExitFailure, "below_baseline_floor"},
		{[]string{"-2"}, cli.ExitFailure, "below_baseline_floor"},
		{[]string{"base"}, cli.ExitRefused, "unsupported_target"},
		{[]string{"0066"}, cli.ExitRefused, "below_baseline_floor"},
		{[]string{"0137"}, cli.ExitRefused, "below_baseline_floor"},
		{[]string{"0161"}, cli.ExitRefused, "below_baseline_floor"},
		{[]string{"0999"}, cli.ExitRefused, "unknown_revision"},
		{[]string{"014"}, cli.ExitRefused, "unknown_revision"},
	}
	for _, cell := range cells {
		code, stdout, stderr := goDowngrade(t, head, cell.args...)
		if code != cell.code || stdout != "" || !strings.Contains(stderr, `"code":"`+cell.name+`"`) || !strings.Contains(stderr, `"outcome":"refused"`) {
			t.Errorf("downgrade %v: exit %d stdout %q stderr %q, want exit %d and %s", cell.args, code, stdout, stderr, cell.code, cell.name)
		}
		if got := schemaShape(t, head); got != shape {
			t.Fatalf("downgrade %v changed the database:\n%s", cell.args, shapeDiff(shape, got))
		}
	}
	// at the target already: a no-op success.
	top := chain[len(chain)-1].Revision
	code, stdout, stderr := goDowngrade(t, head, top)
	if code != cli.ExitOK || !strings.Contains(stdout, `"action":"at_target"`) || !strings.Contains(stderr, `"outcome":"noop"`) || schemaShape(t, head) != shape {
		t.Errorf("downgrade %s (the current revision): exit %d stdout %q stderr %q", top, code, stdout, stderr)
	}
	// above current: Go refuses, database untouched.
	low := d.at(t, 2)
	lowShape := schemaShape(t, low)
	if code, _, stderr := goDowngrade(t, low, top); code != cli.ExitFailure || !strings.Contains(stderr, "target_not_below_current") || schemaShape(t, low) != lowShape {
		t.Errorf("downgrade %s from chain position 2: exit %d stderr %s", top, code, stderr)
	}
	// An explicit target leaves 0066 alone on a two-head database.
	if code, _, stderr := goDowngrade(t, head, "0164"); code != cli.ExitOK {
		t.Fatalf("downgrade 0164: exit %d %s", code, stderr)
	}
	if got := alembicVersions(t, head); !reflect.DeepEqual(got, []string{"0066", "0164"}) {
		t.Fatalf("downgrade 0164 left alembic_version %v, want 0066 and 0164", got)
	}
	// the state refusals on a database that is not a chain database.
	empty := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	if code, _, stderr := goDowngrade(t, empty, "0163"); code != cli.ExitFailure || !strings.Contains(stderr, "no_revision_recorded") {
		t.Errorf("empty database: exit %d %s", code, stderr)
	}
	ahead := d.at(t, len(chain))
	if _, err := connect(t, ahead).Exec(context.Background(), "UPDATE alembic_version SET version_num = '9999' WHERE version_num <> '0066'"); err != nil {
		t.Fatal(err)
	}
	aheadShape := schemaShape(t, ahead)
	if code, _, stderr := goDowngrade(t, ahead, "0163"); code != cli.ExitFailure || !strings.Contains(stderr, "ahead_of_build") || schemaShape(t, ahead) != aheadShape {
		t.Errorf("ahead-of-build database: exit %d %s", code, stderr)
	}
}

// Alembic runs the whole downgrade walk in one transaction (env.py wraps it in
// context.begin_transaction()), so a step that fails rolls back every step before it;
// dho does the same. A row in planted_walk_b makes 0164's down refuse after 0166 and 0165
// were reverted: nothing may change, and the error says so.
func TestDowngradeFailureMidWalkRollsBackTheWholeWalk(t *testing.T) {
	plantedWalk(t)
	d := newDownInstance(t)
	chain, _ := pgmigrate.LoadChain()
	uri := d.at(t, len(chain))
	if _, err := connect(t, uri).Exec(context.Background(), "INSERT INTO planted_walk_b VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	before := schemaShape(t, uri)
	head := chain[len(chain)-1].Revision // the head of the shipped chain, never a literal: the next revision must not break this test
	code, _, stderr := goDowngrade(t, uri, "0163")
	if code != cli.ExitFailure || !strings.Contains(stderr, "downgrade_failed") || !strings.Contains(stderr, "down 0164") || !strings.Contains(stderr, "verified by re-read: alembic_version still holds [0066 "+head+"]") || !strings.Contains(stderr, `"observed":["0066","`+head+`"],"outcome":"rolled_back"`) {
		t.Fatalf("exit %d stderr %s, want downgrade_failed naming 0164 and the verified unchanged state", code, stderr)
	}
	if got := schemaShape(t, uri); got != before {
		t.Fatalf("the failed walk changed the database:\n%s", shapeDiff(before, got))
	}
	if got := alembicVersions(t, uri); !reflect.DeepEqual(got, []string{"0066", head}) {
		t.Fatalf("alembic_version %v after the failed walk, want 0066 and %s", got, head)
	}
}

// ackLossProxy is a PostgreSQL wire proxy that forwards the first simple-query COMMIT
// to the server, then drops the server's response and closes the client's connection:
// the server commits, the client never learns. With refuseAfter the listener is closed
// at that point too, so a reconnect to read the state back fails.
type ackLossProxy struct {
	uri         string
	dropped     atomic.Bool
	refuseAfter bool
	listener    net.Listener
}

func newAckLossProxy(t *testing.T, serverURI string, refuseAfter bool) *ackLossProxy {
	t.Helper()
	parsed, err := url.Parse(serverURI)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &ackLossProxy{refuseAfter: refuseAfter, listener: listener}
	clientURL := *parsed
	clientURL.Host = listener.Addr().String()
	query := clientURL.Query()
	query.Set("sslmode", "disable")
	clientURL.RawQuery = query.Encode()
	proxy.uri = clientURL.String()
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go proxy.serve(client, parsed.Host)
		}
	}()
	return proxy
}

func (p *ackLossProxy) serve(client net.Conn, serverAddr string) {
	server, err := net.Dial("tcp", serverAddr)
	if err != nil {
		_ = client.Close()
		return
	}
	var armed atomic.Bool
	go func() {
		buffer := make([]byte, 64<<10)
		for {
			n, err := server.Read(buffer)
			if n > 0 {
				if armed.Load() {
					// the server has the COMMIT and answered; the client never hears it.
					// Refuse first, then hang up: the client reconnects the instant it sees
					// the close, so the listener must already be gone or the read-back
					// can be accepted and the "outcome unknown" path never runs.
					if p.refuseAfter {
						_ = p.listener.Close()
					}
					_ = client.Close()
					_ = server.Close()
					return
				}
				if _, werr := client.Write(buffer[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				_ = client.Close()
				return
			}
		}
	}()
	// startup message: length-prefixed, no type byte; then typed messages.
	header := make([]byte, 4)
	if _, err := io.ReadFull(client, header); err != nil {
		return
	}
	startup := make([]byte, binary.BigEndian.Uint32(header)-4)
	if _, err := io.ReadFull(client, startup); err != nil {
		return
	}
	_, _ = server.Write(append(header, startup...))
	for {
		kind := make([]byte, 1)
		if _, err := io.ReadFull(client, kind); err != nil {
			_ = server.Close()
			return
		}
		if _, err := io.ReadFull(client, header); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(header)-4)
		if _, err := io.ReadFull(client, body); err != nil {
			return
		}
		if kind[0] == 'Q' && strings.EqualFold(strings.TrimRight(string(body), "\x00"), "commit") && p.dropped.CompareAndSwap(false, true) {
			armed.Store(true)
		}
		if _, err := server.Write(append(append(kind, header...), body...)); err != nil {
			return
		}
	}
}

// A COMMIT whose acknowledgement is lost may have committed. The verb must report only
// what it reads back: here the server committed the whole downgrade, so the message
// says the database CHANGED and names the revision it now holds, never "rolled back";
// and when the read-back connection is refused, it says the outcome is unknown.
func TestDowngradeReportsOnlyVerifiedStateAfterALostCommitAcknowledgement(t *testing.T) {
	plantedWalk(t)
	d := newDownInstance(t)
	low := floor(t)
	chain, _ := pgmigrate.LoadChain()
	t.Run("read-back succeeds", func(t *testing.T) {
		uri := d.at(t, len(chain))
		proxy := newAckLossProxy(t, uri, false)
		code, stdout, stderr := goDowngrade(t, proxy.uri, low)
		if !proxy.dropped.Load() {
			t.Fatal("the proxy never saw the COMMIT: the fault was not injected")
		}
		if got := alembicVersions(t, uri); !reflect.DeepEqual(got, []string{"0066", low}) {
			t.Fatalf("the server holds %v, want the committed 0066 %s", got, low)
		}
		if code != cli.ExitFailure || stdout != "" || strings.Contains(stderr, "rolled back") ||
			!strings.Contains(stderr, "verified by re-read: the database CHANGED") || !strings.Contains(stderr, "[0066 "+low+"]") || !strings.Contains(stderr, `"msg":"migrate outcome","direction":"down","from":["0066","`+chain[len(chain)-1].Revision+`"],"requested":"`+low+`","observed":["0066","`+low+`"],"outcome":"committed"`) {
			t.Fatalf("exit %d stdout %q stderr %q, want a failure that reports the verified committed state", code, stdout, stderr)
		}
	})
	t.Run("read-back refused", func(t *testing.T) {
		uri := d.at(t, len(chain))
		proxy := newAckLossProxy(t, uri, true)
		code, _, stderr := goDowngrade(t, proxy.uri, low)
		if !proxy.dropped.Load() {
			t.Fatal("the proxy never saw the COMMIT: the fault was not injected")
		}
		if code != cli.ExitFailure || strings.Contains(stderr, "rolled back") || !strings.Contains(stderr, "outcome unknown") || !strings.Contains(stderr, `"direction":"down"`) || !strings.Contains(stderr, `"outcome":"unknown"`) {
			t.Fatalf("exit %d stderr %q, want a failure that says the outcome is unknown", code, stderr)
		}
	})
}

// The upgrade verb ends with the same final outcome line: a run that failed at 0163 is one
// rolled-back transaction (CHAOS-7291), so it says "rolled_back" with the revisions it started
// from, never "committed" and no longer "partial".
func TestUpgradeVerbLogsTheVerifiedOutcome(t *testing.T) {
	plantedWalk(t)
	d := newDownInstance(t)
	uri := d.at(t, 0)
	if _, err := connect(t, uri).Exec(context.Background(), "CREATE TABLE planted_walk_a (conflict integer)"); err != nil {
		t.Fatal(err)
	}
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == "upgrade" {
			run = child.Run
		}
	}
	var out, errs bytes.Buffer
	code := run(context.Background(), cli.Env{Lookup: func(key string) (string, bool) {
		if key == pgmigrate.CutoverEnv {
			return "1", true
		}
		return "", false
	}, Stdout: &out, Stderr: &errs})
	want := `"msg":"migrate outcome","direction":"up","from":["0066","` + floor(t) + `"],"requested":"head","observed":["0066","` + floor(t) + `"],"outcome":"rolled_back"`
	if code != cli.ExitFailure || !strings.Contains(errs.String(), want) {
		t.Fatalf("exit %d stderr %q, want a failure with the line %s", code, errs.String(), want)
	}
}
