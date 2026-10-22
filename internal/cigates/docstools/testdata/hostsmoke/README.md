# Recorded Python/Go parity cases, host-smoke (CHAOS-10434)

One directory per case. The Python side was recorded once, locally, with Python 3.14.7 and `scripts/check_docs_cloudflare_host.py`
as at ops `256f1ea0a` (merge base), by `../refusals/record_host_smoke.py.frozen` (kept as text, never run in CI; the script it
drives is frozen beside it). The recorder starts one raw-bytes TLS server per case (a self-made certificate, trusted
through `SSL_CERT_FILE`), runs the script against it and stores what it did.

| file | meaning |
| --- | --- |
| `routes.json` | request path -> raw response bytes (`*` = default; `HANG` / `RESET` = special); `{ORIGIN}` and `{HOSTPORT}` are filled in with the server address |
| `redirects.tsv` | the redirect manifest the run reads |
| `args` | the arguments (default `--origin {ORIGIN} --redirects redirects.tsv`) |
| `stdout`, `stderr`, `rc` | what Python printed and its exit code |
| `requests` | the ordered request log: method, path, User-Agent |
| `untrusted` | the run has no `SSL_CERT_FILE` (system roots only) |
| `alpn-h2` | the server offers only h2 in ALPN (the client offers none) |
| `hang` | the server never answers `/use/`; Python waited 20 s; replayed in-process by `TestHostSmokeRequestTimeout` |
| `go/` | the Go answer where it differs from Python (rc, stdout, stderr, requests); every such case is listed in `NAMED.txt` and in `../refusals/host-smoke.tsv` |

`TestHostSmokeCasesRunLiveThroughTheCLI` replays every case (row count pinned) through the built cigate binary. For rc 2 the
usage text differs by design (argparse vs the Go verb); the message after `error: ` must equal.

Not recorded, by decision: the retry loop (6 attempts, 10 s apart) stays in the workflow; the proxy behaviour is
`TestHostSmokeHonoursHTTPSProxyAndNoProxy` (Python 3.14.7 was checked by hand to send `CONNECT host:port` to `HTTPS_PROXY`,
as Go does). Header values are read from the raw bytes with the http.client / email rules (trailing space and tab kept), so there is no leniency. Known deviations (all stricter or text only, named in `../refusals/host-smoke.tsv`): a followed Location that urllib would re-quote is refused ("location needs re-quote"); net/http refuses a header line with no colon and a "From " line; Go's error text differs from urllib's; usage text differs. Proxy variables follow urllib (lower-case name wins, two passes; `no_proxy` suffix rules; plain CONNECT whatever the proxy scheme says). For the PR body (RISK-NOTES): call sites 1 (deploy smoke) and 2 (rollback smoke) in docs-cloudflare.yml are deleted or re-homed by the follow-up that removes the GitHub wrangler steps, and whatever remains must keep calling `cigate docs host-smoke`.
