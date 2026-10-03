// routerconf writes the nginx configuration of the self-hosted compose router
// from the route-plane contract. Run it from the repository root:
//
//	go run ./internal/ingressplanes/cmd/routerconf          # write the file
//	go run ./internal/ingressplanes/cmd/routerconf -check   # exit 1 when the file is stale
//
// It is a development tool: the file it writes is checked in, and nothing runs
// it at run time (contracts/ingress/v1/README.md).
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/ingressplanes"
)

const (
	exitOK    = 0
	exitStale = 1
	exitUsage = 2
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("routerconf", flag.ContinueOnError)
	flags.SetOutput(stderr)
	contractPath := flags.String("contract", ingressplanes.ContractPath, "the route-plane contract to read")
	outPath := flags.String("out", ingressplanes.RouterConfigPath, "the nginx configuration to write")
	check := flags.Bool("check", false, "write nothing; exit 1 when the file on disk is not what the contract generates")
	if err := flags.Parse(args); err != nil {
		return exitUsage
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "routerconf: unexpected argument %q\n", flags.Arg(0))
		return exitUsage
	}
	contract, err := ingressplanes.Load(*contractPath)
	if err != nil {
		fmt.Fprintf(stderr, "routerconf: %v\n", err)
		return exitStale
	}
	generated, err := ingressplanes.Render(contract, ingressplanes.DefaultOptions())
	if err != nil {
		fmt.Fprintf(stderr, "routerconf: %v\n", err)
		return exitStale
	}
	if *check {
		onDisk, err := os.ReadFile(*outPath)
		if err != nil {
			fmt.Fprintf(stderr, "routerconf: %v\n", err)
			return exitStale
		}
		if !bytes.Equal(onDisk, generated) {
			fmt.Fprintf(stderr, "routerconf: %s is not what %s generates; run `go run ./internal/ingressplanes/cmd/routerconf`\n", *outPath, *contractPath)
			return exitStale
		}
		fmt.Fprintf(stdout, "%s is fresh: %d rules\n", *outPath, len(contract.Rules))
		return exitOK
	}
	if err := os.WriteFile(*outPath, generated, 0o644); err != nil {
		fmt.Fprintf(stderr, "routerconf: %v\n", err)
		return exitStale
	}
	fmt.Fprintf(stdout, "wrote %s: %d rules\n", *outPath, len(contract.Rules))
	return exitOK
}
