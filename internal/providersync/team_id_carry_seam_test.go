package providersync

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type carrySeamConn struct {
	queries []string
	batches int
	err     error
}

func (conn *carrySeamConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	return nil, conn.err
}

func (conn *carrySeamConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.batches++
	return nil, errors.New("no batch")
}

type carrySeamCollector struct{ ran *bool }

func (collector carrySeamCollector) CollectTeamCatalog(context.Context, TeamCatalogReference, providerfoundation.Credential,
	*providerfoundation.HTTPClient, TeamCatalogSelections, time.Time,
) (TeamCatalogResult, error) {
	*collector.ran = true
	return TeamCatalogResult{TeamsWritten: 1}, nil
}

// A failed carry stops the collector before it runs; every registry entry
// is wrapped.
func TestTheCarryRunsBeforeTheCollectorAndAFailureStopsIt(t *testing.T) {
	failed := errors.New("count read failed")
	conn := &carrySeamConn{err: failed}
	ran := false
	registry := CarryFirstTeamCatalogCollectors(conn, map[string]TeamCatalogCollector{"linear": carrySeamCollector{ran: &ran}})
	if len(registry) != 1 {
		t.Fatalf("registry = %d entries, want 1", len(registry))
	}
	_, err := registry["linear"].CollectTeamCatalog(context.Background(), TeamCatalogReference{OrgID: "org-1"},
		providerfoundation.Credential{}, nil, TeamCatalogSelections{Teams: true}, time.Now())
	if !errors.Is(err, failed) || ran || conn.batches != 0 || len(conn.queries) != 1 || conn.queries[0] != teamIDCarryCountQuery {
		t.Fatalf("err = %v, collector ran = %v, batches = %d, queries = %d; want the count read only, its error, and no collector run",
			err, ran, conn.batches, len(conn.queries))
	}
	if _, err := (CarryFirstTeamCatalogCollector{Collector: carrySeamCollector{ran: &ran}}).CollectTeamCatalog(context.Background(),
		TeamCatalogReference{OrgID: "org-1"}, providerfoundation.Credential{}, nil, TeamCatalogSelections{}, time.Now()); !errors.Is(err, ErrInvalidConfiguration) || ran {
		t.Fatalf("no connection: err = %v, ran = %v", err, ran)
	}
}

// teamIDWriteSites is every production line that runs a team catalog
// collector, builds one, or writes Atlassian team ids, per file. Each is
// behind the carry: the registries wrap every collector they build
// (newNativeTeamCatalogCollectors, buildCatalogCollector; their own tests
// run each entry), the dispatchers run only registry entries, the combined
// Jira collector's legs run inside its wrapped entry, and the route
// handlers only fetch.
var teamIDWriteSites = map[string]int{
	"internal/providersync/team_id_carry_seam.go":                     2,
	"internal/providersync/gitlab_team_catalog_route.go":              1,
	"internal/providersync/jira_team_catalog_route.go":                1,
	"internal/syncdispatchruntime/team_catalog_discovery_executor.go": 1,
	"internal/workerservice/team_catalog_clients.go":                  1,
	"internal/workerservice/jira_atlassian_teams_collector.go":        2,
	"internal/workerservice/sync_dispatch.go":                         5,
	"internal/synccli/teamscatalog.go":                                5,
	"internal/synccli/synccli.go":                                     1,
}

var teamIDWriteSitePattern = regexp.MustCompile(`\.CollectTeamCatalog\(|\b(?:Linear|GitHub|GitLab|Jira|CarryFirst)TeamCatalogCollector\{|\bjiraCombinedTeamCatalogCollector\{|\batlassianteams\.Write\(`)

// TestEveryTeamIDWriteSiteRunsBehindTheCarryCensus fails when a new line
// runs or builds a team catalog collector, or writes Atlassian team ids,
// outside the sites above: such a path could read or write a team id
// before the carry.
func TestEveryTeamIDWriteSiteRunsBehindTheCarryCensus(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]int{}
	sources := map[string]string{}
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			for _, line := range strings.Split(string(data), "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "func (") || strings.HasPrefix(trimmed, "var _ ") {
					continue
				}
				found[rel] += len(teamIDWriteSitePattern.FindAllString(line, -1))
			}
			if found[rel] == 0 {
				delete(found, rel)
			} else {
				sources[rel] = string(data)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(found) == 0 {
		t.Fatal("no team id write site found: the scan read nothing")
	}
	for file, count := range found {
		if want, ok := teamIDWriteSites[file]; !ok || want != count {
			t.Errorf("%s: %d team id write sites, want %d: put a new path behind the carry (CarryFirstTeamCatalogCollector, or CarryTeamIDsBeforeWrite at its entry), then list it here", file, count, want)
		}
	}
	for file, want := range teamIDWriteSites {
		if found[file] != want {
			t.Errorf("%s: %d team id write sites, want %d", file, found[file], want)
		}
	}
	// The registries build every collector inside the function that wraps it.
	for file, builder := range map[string]string{
		"internal/workerservice/sync_dispatch.go": "func newNativeTeamCatalogCollectors(",
		"internal/synccli/teamscatalog.go":        "func buildProviderCatalogCollector(",
	} {
		source := sources[file]
		start := strings.Index(source, builder)
		if start < 0 {
			t.Errorf("%s: %s not found", file, builder)
			continue
		}
		body := source[start:]
		if end := strings.Index(body[1:], "\nfunc "); end >= 0 {
			body = body[:end+1]
		}
		inside := len(regexp.MustCompile(`\b(?:Linear|GitHub|GitLab|Jira)TeamCatalogCollector\{|\bjiraCombinedTeamCatalogCollector\{`).FindAllString(body, -1))
		total := len(regexp.MustCompile(`\b(?:Linear|GitHub|GitLab|Jira)TeamCatalogCollector\{|\bjiraCombinedTeamCatalogCollector\{`).FindAllString(source, -1))
		if inside == 0 || inside != total {
			t.Errorf("%s: %d of %d collectors are built inside %s", file, inside, total, builder)
		}
	}
	// The Atlassian teams CLI verb writes without a collector: it carries
	// first, in the same function.
	source := sources["internal/synccli/synccli.go"]
	write := strings.Index(source, "atlassianteams.Write(")
	if write < 0 {
		t.Fatal("synccli.go: atlassianteams.Write not found")
	}
	enclosing := source[strings.LastIndex(source[:write], "\nfunc "):write]
	if !strings.Contains(enclosing, "providersync.CarryTeamIDsBeforeWrite(") {
		t.Error("synccli.go: the Atlassian teams write is not preceded by CarryTeamIDsBeforeWrite in its function")
	}
}

func TestRequireCarriedRefusesEveryUncarriedShape(t *testing.T) {
	ran := false
	inner := carrySeamCollector{ran: &ran}
	conn := &carrySeamConn{}
	for name, collector := range map[string]TeamCatalogCollector{
		"bare collector":      inner,
		"nil collector":       nil,
		"carry without conn":  CarryFirstTeamCatalogCollector{Collector: inner},
		"carry without inner": CarryFirstTeamCatalogCollector{Conn: conn},
	} {
		if err := RequireCarried(collector); !errors.Is(err, ErrTeamCatalogCollectorNotCarried) {
			t.Errorf("%s: error = %v, want %v", name, err, ErrTeamCatalogCollectorNotCarried)
		}
	}
	if err := RequireCarried(CarryFirstTeamCatalogCollector{Conn: conn, Writer: "test", Collector: inner}); err != nil {
		t.Errorf("a carried collector: error = %v", err)
	}
	if ran || len(conn.queries) != 0 {
		t.Errorf("RequireCarried ran the collector or read the store: ran=%v queries=%d", ran, len(conn.queries))
	}
}
