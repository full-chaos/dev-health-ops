//go:build integration

package chmigrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// chainRecordUpdate rewrites testdata/python_chain_contract2.json from the real Python chain: the
// recorder of that file (CHAOS-7471). Until then it was written by hand with the whole shell
// environment of the day; this test runs the chain in a CLOSED environment (pyoracle.ClosedEnv: the
// DSN and OPERATIONAL_ORDERING_CONTRACT=2 on purpose, nothing inherited), on a fresh database,
// from the Python root DHO_PYTHON_ROOT names (a clean checkout of pythonGoldenBuild), and writes
// what capture reads back.
//
//	DHO_PYTHON_ROOT=<worktree at pythonGoldenBuild> DHO_PYTHON_CHAIN_GOLDEN_UPDATE=1 \
//	  go test -tags=integration -run '^TestRecordPythonChainContract2$' ./internal/chmigrate
//
// then update the digest of the file in pythonGoldens (goldens_test.go).
const chainRecordUpdate = "DHO_PYTHON_CHAIN_GOLDEN_UPDATE"

func TestRecordPythonChainContract2(t *testing.T) {
	if os.Getenv(chainRecordUpdate) != "1" {
		t.Skip("records testdata/python_chain_contract2.json only with " + chainRecordUpdate + "=1 (see the recipe on this test)")
	}
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin := openDatabase(t, instance.URI, "")
	database := scratchDatabase(t, admin)

	root := pyoracle.Root(t)
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink\nClickHouseMetricsSink(sys.argv[1]).ensure_schema(force=True)\n"
	command := exec.Command(python, "-c", program, httpDSN(t, ctx, instance, database))
	command.Env = pyoracle.ClosedEnv(root, "CLICKHOUSE_URI="+httpDSN(t, ctx, instance, database), "OPERATIONAL_ORDERING_CONTRACT=2")
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		t.Fatalf("the Python chain failed: %v", pyoracle.RunError(python, err, output.Bytes()))
	}
	captured := capture(t, ctx, openDatabase(t, instance.URI, database), database, productionContract)
	raw, err := json.MarshalIndent(captured, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("testdata", "python_chain_contract2.json"), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
