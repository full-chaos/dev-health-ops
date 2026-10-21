# Recorded Python/Go parity pairs

`parity_test.go` (`TestCLIOutputParityRecordedPair`) replays each pair under `parity/` with the Go subcommand and
compares exit code, stdout, stderr and every file the tool wrote, byte for byte. The Python side was recorded once by
running the four scripts as they were at ops main before their deletion:

- ops main: `e557a21dc4ab232af9c2790ecc7de047467961e2` (the scripts were extracted from that commit with
  `git archive e557a21dc4 ci/bigboy`)
- Python 3.12.3, PyYAML 6.0.1, git 2.43
- inputs (synthetic, no deployment data): `parity/values.prod.yaml`, `parity/compose.bigboy.yml`,
  `parity/web-env-names.txt`, `parity/route-statuses.json`

Each pair directory holds `args` (the Go arguments, one per line; `{...}` is filled by the test), `rc`, `stdout`,
`stderr` and the written file when there is one. The commands, run in `parity/`:

| pair | Python command |
| --- | --- |
| `plane-split-router-pinned` | `python3 ci/bigboy/generate-plane-split-router.py --deploy-repo $W/deploy --deploy-sha 66f8e3b6 --expect-ops-sha 0123456789AB --values-out $W/values-out.yaml --format dynamic` |
| `plane-split-router-labels` | `python3 ci/bigboy/generate-plane-split-router.py values.prod.yaml` |
| `query-api-flags` | `python3 ci/bigboy/generate-query-api-enabled-flags.py values.prod.yaml --out query-api-flags/query-api-flags.out` |
| `route-coverage` | `python3 ci/bigboy/check-route-coverage.py values.prod.yaml --go-api-url http://127.0.0.1:$GP --query-api-url http://127.0.0.1:$QP` |
| `web-env-parity` | `python3 ci/bigboy/check-web-env-parity.py values.prod.yaml compose.bigboy.yml --live-names web-env-names.txt` |

`$W/deploy` is a git repository holding `values.prod.yaml` and a `vendor/dev-health-ops` gitlink at
`0123456789abcdef0123456789abcdef01234567`, committed with the fixed author, committer and dates of `gitEnv` in
`pinned_test.go`, so its HEAD is `66f8e3b6fb34ea33c2b031d319ffa6e60ef1f428` wherever it is built; the replay rebuilds it
and fails if the sha differs. `$GP` and `$QP` are two local HTTP servers that answer `ROUTEPROBE` with the status
`route-statuses.json` gives the path (405 for any other path); the replay serves the same table from `httptest`
servers. No port appears in the output.
