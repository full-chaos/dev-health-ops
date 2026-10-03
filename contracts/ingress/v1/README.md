# Route planes of an api host v1

`planes.json` is the rule table of a host that serves the api: for each path,
the plane that answers it. There are two planes, `go-api` (REST) and `query-api`
(`/graphql` and the query REST paths). In prod the table is the set of Ingress
paths that ingress-nginx reads. The self-hosted compose stack has no Ingress, so
`compose-router.nginx.conf` is generated from the same table and an nginx
container routes with it on host port 8000 (CHAOS-8363).

This directory is the one source of truth for that table:

| File | What it is |
| --- | --- |
| `planes.json` | The contract. Edit this file to change a route. |
| `planes.schema.json` | Its Draft 2020-12 shape. |
| `compose-router.nginx.conf` | GENERATED from the contract. Never edit it. |

The Go package is `internal/ingressplanes`.

## The contract

```json
{
  "schema_version": 1,
  "regex_mode": true,
  "rules": [
    {"path": "/", "path_type": "Prefix", "plane": "go-api"},
    {"path": "/graphql$", "path_type": "ImplementationSpecific", "plane": "query-api"}
  ],
  "public_host_rules": [
    {"path": "/metrics", "path_type": "Exact", "plane": "go-api"}
  ]
}
```

- One rule is one Ingress path as the Ingress object holds it AFTER the charts
  rendered it: `path`, `path_type` (the Ingress `pathType`), and `plane` (the
  backend Service: the Go api or the query-api).
- `rules` are the paths that EVERY api host carries.
- `public_host_rules` are the paths that only the PUBLIC api host carries
  beside `rules`. In prod they are the four Exact paths of the block object
  (`/metrics`, `/docs`, `/redoc`, `/openapi.json`, to the Go api). The key must
  be there; the list can be empty.
- `regex_mode` says that an Ingress of the host carries
  `nginx.ingress.kubernetes.io/use-regex: "true"`. ingress-nginx then reads
  every path of the host as a regex.
- The order of the rules has no meaning. The generator sorts them.
- A key is read only in the spelling of the schema (`path`, not `Path`).

The table is the RENDERED rules, not the lists of the values file. The values
file holds three lists that two charts change before ingress-nginx sees them (a
`{name}` token becomes `[^/]+`, a `$` is added, an Exact path is quoted). A
contract of the lists would make the generator a second reading of the values
beside the charts. A contract of the rendered rules has no such second reading:
the charts are compared with it by rendering them.

### First content

The first content is what prod routes at deploy commit
`c72844763bb1ec5c7c886c4b0d3b85eaa34437cd` (vendored ops
`98a8577112a297b20c50ac6863c4437e8980d616`). It was rendered with helm v4.2.4
from the prod values (the kiac values, then the prod values) through the four
templates that make these rules, and not written by hand:

- the deploy chart's `go-api-ingress.yaml` (171 anchored rules to the Go api);
- the deploy chart's `query-api-ingress.yaml` (24 anchored rules to query-api);
- the vendored ops chart's `templates/ingress.yaml` (the default rule `/` to the
  Go api, and `/graphql$` to query-api);
- the deploy chart's `api-public-internal-path-block.yaml` (the four Exact
  paths, on the public host only).

The in-cluster host (the one web's server-side proxy calls) renders the 197
rules of `rules`. The public host (`api.fullchaos.dev`) renders 201: the same
197 and the four of `public_host_rules`. Both hosts are in regex mode. A second
render at deploy commit `947a68c8021ee432fee73b315535f772c463546a` (the values
file and the templates did not change) gave the same rows.

Not in the contract, by design: the billing host (`ingress.goApiHosts`). It is
another host and another listener.

Limit of the first content: the four templates were rendered one by one, not
the whole deploy chart (that render needs the secrets values, which were not
opened). The deploy-side test below renders the whole chart.

### The four paths of the public host

The compose router is one host. It stands for both prod hosts only when a path
that the public host alone lists gets the plane there that the other host gives
it. The validator holds that: a public host rule must be `Exact`, must name the
default plane, and must not be able to take a request from a rule of another
plane. The generator then writes NO location for it: the default rule gives its
paths the same plane.

In regex mode ingress-nginx reads the Exact path `/docs` as the regex `^/docs`:
no end anchor, any case, and a `.` is any character. The validator uses that
widest reading, which holds the exact one, so the result does not depend on
which reading a running ingress-nginx applies.

What the four paths answer: the Go api's own 404 (`{"detail":"Not Found"}`,
`x-dev-health-plane: go`), in prod on both hosts and in compose through the
router. The Go api's public listener serves none of them;
`TestConfigureRegistersTheListenerReadinessCheck` in `internal/apiservice`
holds that for `/docs`, `/redoc`, `/openapi.json` and `/metrics`. So the router
does not have to answer 404 itself, and it has no such location.

### What the validator refuses

`internal/ingressplanes` refuses a table that it cannot write with the match
result ingress-nginx gives. A refusal is by design: a shape that would have to
be guessed is not read.

- `regex_mode: false`. Only a host in regex mode is modelled.
- More or fewer than one default rule (`/`, `Prefix`).
- A rule of `rules` that is not the default and is `Exact` or `Prefix`. In
  regex mode ingress-nginx writes such a path as a regex with no end anchor, so
  `/docs` also matches `/docsx`. Write the rule anchored.
- A path that is not anchored: segments of letters, digits, `_`, `-` and `\.`,
  or the whole-segment token `[^/]+`, then one `$`. Every other character is
  refused (`;`, a space, `{`, `}`, `#`, a quote, a newline, a `$` inside the
  path): the path is written into an nginx directive.
- A `.` or `..` segment, a plane that is not `go-api` or `query-api`, the same
  path twice (compared without case).
- Two rules that can match one request path. With such rules the answer would
  depend on the order of the locations.
- A public host rule that is not a literal `Exact` path, that names another
  plane than the default, or that can take a request from a rule of another
  plane.
- A document with a missing key, an unknown key or a key in another case.

The schema holds the shape of each row. The rules across rows (one path one
rule, a public host path on the default plane) are the validator's alone.

## The generated router configuration

```bash
go run ./internal/ingressplanes/cmd/routerconf          # write the file
go run ./internal/ingressplanes/cmd/routerconf -check   # exit 1 when it is stale
```

Run it from the repository root. Nothing runs it at run time and there is no
Python in it: the compose stack mounts the checked-in file.

Each rule of `rules` becomes one location, in the form and the order
ingress-nginx uses for a host in regex mode:

- Form: `location ~* "^<path>"` for every rule, the default rule too
  (`~* "^/"`). So an anchored path stays anchored and the match ignores case
  (`/GRAPHQL` reaches query-api, as in prod).
- Order: the longest path first, then the greater path first. nginx takes the
  first regex location that matches, so the default rule (one character) is
  last.
- A location proxies with no URI part, so the plane receives the request target
  as the client sent it.

### The plane a location proxies to

A location proxies to `http://$plane_go_api` or `http://$plane_query_api`. Each
variable is one constant: `set $plane_go_api "go-api:8000"` and
`set $plane_query_api "query-api:8090"`, once, in the server block. No request
can change where a location proxies to: nothing else in the file gives a
variable a value, and the value holds no variable.

They are variables for one reason: with a variable in `proxy_pass`, nginx
resolves the plane's name when a request comes (`resolver 127.0.0.11`, Docker's
embedded DNS), not when it starts. So the router starts while a plane is not
there yet, answers 502 for that plane, and finds the plane when it comes up or
was recreated with a new address. With the name written in `proxy_pass` (or in
an `upstream` block), nginx resolves it once at start and does not start when
the name does not resolve: `[emerg] host not found in upstream "go-api"`
(measured, nginx 1.24.0, on a host where the name does not resolve).

`TestPlaneUpstreamsAreConstants` holds the constants on the checked-in file.
The real-nginx test starts the router BEFORE its planes, and sends requests
that name the other plane in the Host, in headers and in the query string.

Another container runtime needs another resolver address; the generator has no
option for that yet.

### The headers a plane receives

The router owns the forwarded headers, with the values ingress-nginx sends for
a direct client with its default configuration (`use-forwarded-headers` and
`compute-full-forwarded-for` off; `nginx.tmpl` of `controller-v1.14.5`):

| Header | ingress-nginx (defaults) | Router |
| --- | --- | --- |
| `Host` | `$best_http_host` = `$http_host` (else `$host`) | `$http_host` |
| `X-Real-IP` | `$remote_addr` | `$remote_addr` |
| `X-Forwarded-For` | `$remote_addr` (replaced) | `$remote_addr` |
| `X-Forwarded-Host` | `$best_http_host` = `$http_host` (else `$host`) | `$http_host` |
| `X-Forwarded-Port` | `$pass_port` = `$server_port` | `$server_port` |
| `X-Forwarded-Proto` | `$pass_access_scheme` = `$scheme` | `$scheme` |
| `X-Forwarded-Scheme` | `$pass_access_scheme` = `$scheme` | `$scheme` |
| `X-Scheme` | `$pass_access_scheme` = `$scheme` | `$scheme` |
| `X-Original-Forwarded-For` | `$http_x_forwarded_for` | `$http_x_forwarded_for` |
| `X-Original-Forwarded-Host` | `$http_x_forwarded_host` | `$http_x_forwarded_host` |
| `X-Request-ID` | the client's, or a new one | not set (passes as sent) |
| `Upgrade`, `Connection` | websocket upgrade | not set (no plane serves a websocket) |
| `Proxy` | removed | not set (passes as sent) |

So a value that a client sends for `X-Real-IP` or an `X-Forwarded-*` header
never reaches a plane: the plane gets the router's value. The client's
`X-Forwarded-For` and `X-Forwarded-Host` are kept only under the
`X-Original-Forwarded-*` names, as ingress-nginx keeps them.

Two differences stay. For a request with no `Host` header ingress-nginx sends
its server name (`$best_http_host` falls back to `$host`); the router sends no
`Host` then. And prod's ingress is not at the defaults: per
`internal/api/clientip` it runs `use-forwarded-headers` behind its own trusted
proxy, so there the scheme, the port and the host come from that proxy's
`X-Forwarded-*` headers and `$remote_addr` is the client address the realip
module set. The router is the first hop of a compose stack, so it takes the
values of a direct client.

Notes for the package that wires the router into compose:

- A plane sees the router as its TCP peer. `internal/api/clientip` takes
  `X-Forwarded-For` and `X-Real-IP` only from a peer in `TRUSTED_PROXIES`.
  Unset, the client of every request is the router's address (one rate-limit
  and audit key for all). Set to the router's address, the client is the
  router's peer.
- `internal/auth/httpapi/forwarded.go` takes `X-Forwarded-Proto` only from a
  peer in `FORWARDED_ALLOW_IPS` (default `127.0.0.1`, so the router is not
  trusted and the header is not read). Its one use is the scheme of the
  trailing-slash redirect. If the router is trusted there, the scheme is the
  router's own (`http`).

### Other parts of the file that do not come from the contract

Constants of the generator; no test compares them with prod:

- Listen port 8000; the planes `go-api:8000` and `query-api:8090` (the compose
  service names and the public listener ports).
- `client_max_body_size 50m`, read and send timeouts of 60 s: the values of
  prod's Ingress annotations at the deploy commit above. `proxy_http_version
  1.1` and `proxy_buffering off`: the defaults of ingress-nginx.
- The pid file and every temporary path are under `/tmp`, so the router runs as
  any user id.

## The router adds no credential

Web's server-side proxy is the only place that turns the session cookie into an
`Authorization` bearer. The router keeps that property in three ways:

1. It never sets, reads or checks a credential. It has no directive that names
   `Authorization` or a cookie, and no auth module. A request passes with the
   credential headers it came with.
2. It proxies to the two planes only. It has no route to web, so it is never
   the origin of the web app: a browser request for a page path cannot reach a
   plane through it in place of web's proxy.
3. A request that comes to it with a cookie and no bearer reaches the plane
   with a cookie and no bearer, and the plane refuses it.

This is not the rule of the bigboy router (plane rules on the internal host
name only). That router is one listener for the public web host and for the
planes, so it must tell the two apart by host. The compose router serves the
planes only, on its own port, and API clients call it with any host name, so it
has no host rule.

`TestRouterConfigAddsNoCredential` holds points 1 and 2 on the checked-in file
(a closed list of directives and of upstreams).
`TestRealNginxRoutesEveryPathToItsPlane` measures point 3 on a real nginx.

## Tests in this repository

All in `internal/ingressplanes`.

| Test | What fails it |
| --- | --- |
| `TestCheckedInRouterConfigIsWhatTheContractGenerates` | The file is not the bytes the contract generates (a hand edit, or a contract change with no generate). |
| `TestCheckedInRouterConfigAgreesWithTheContract` | The file, read by the test's own nginx parser, does not hold one location per rule, to the rule's plane, in the order above. It does not call the generator. |
| `TestCheckedInRouterConfigRoutesEveryPathToItsPlane` | A request path does not reach its plane, by nginx's location rule over the locations of the file. |
| `TestRouterOwnsTheForwardedHeaders` | The headers the file sets are not the table above. |
| `TestPlaneUpstreamsAreConstants` | A plane variable is not one literal set once in the server block, or a `proxy_pass` is not one of the two. |
| `TestRealNginxRoutesEveryPathToItsPlane` (build tag `integration`) | The same rows through a real nginx: the checked-in file in a container of `ingressplanes.RouterImage`, started before the stub planes, which then come up on the compose names. Forged forwarded headers, requests that name the other plane, and credentials are sent through it. |
| `TestSchemaAndValidatorAgree`, `TestValidateRefusesWhatItCannotRoute`, `TestParseRefusesWhatItDoesNotRead` | The schema, the validator or the reader accepts a planted defect. |

The rows are: for each rule its own path, the path in upper case, with a
trailing slash and with one more character; four rows for each public host
rule; and hand-written rows (the `/graphql` cases, near misses, the four public
host paths, an unknown path). The real-nginx test adds percent-escapes, dot
segments, doubled slashes and query strings.

The test without nginx does NOT prove: that nginx accepts the file, nginx's
regex engine, the normalisation nginx applies to a path before it chooses a
location, and what the plane receives. Only the real-nginx test proves those.

`DEV_HEALTH_ROUTER_NGINX_BIN=<nginx binary>` makes the real-nginx test start no
container: it runs that binary on the generator's output with stub planes in
the test process. It is for a host where a lane may not start a container. It
does not prove the checked-in bytes or the name lookup.

No test in this repository compares the contract with the prod render: that is
the deploy repository's test.

## The test the deploy repository must have

The deploy repository vendors this repository. Its test reads
`vendor/dev-health-ops/contracts/ingress/v1/planes.json` and compares it with
the RENDER of its chart with the prod values. Nothing of the render is left
out:

1. Render the chart with the prod values, as its other tests do.
2. From every Ingress object take each rule: host, `path`, `pathType`, the
   backend Service name, and if the object has the annotation
   `nginx.ingress.kubernetes.io/use-regex: "true"`.
3. The api hosts are the hosts with a rule `/` whose backend is the release's
   Go api Service. No api host is a failure: nothing was measured.
4. For each api host, take its rules from ALL Ingress objects. Write each as
   `{path, path_type, plane}`: `path_type` is the `pathType`; `plane` is
   `go-api` for the Go api Service and `query-api` for the query-api Service;
   any other Service is a failure.
5. The public hosts are the hosts that a block object names
   (`ingress.blockedInternalPaths.host`). For a public host the rows must equal
   `rules` plus `public_host_rules`. For every other api host they must equal
   `rules`. Equal means: the same rows, each one time, in any order. When
   `public_host_rules` is not empty and no api host is a public host, the test
   fails.
6. `regex_mode` must be true exactly when an Ingress object that holds the
   host has the use-regex annotation.

A path that is added, removed, moved to the other plane or rendered in another
form on one side only then fails the deploy test, and the ops tests above fail
when the router file does not follow the contract.

## Read from source, not measured

These points were read from the ingress-nginx source at tag
`controller-v1.14.5` (the version prod runs) or from this repository. They were
not measured against a running ingress-nginx.

- The location form `~* "^<path>"` for every path of a server that has a
  use-regex location (`buildLocation` and `enforceRegexModifier` in
  `internal/ingress/controller/template/template.go`). For an anchored path
  the ops chart's test records a live observation on v1.14.5. For the default
  rule (`~* "^/"`) and for the Exact paths of the public host there is none.
- The location order (`internal/ingress/controller/controller.go`: sort by
  path, descending, then stable by path length, descending). The contract
  refuses two rules that match one path, so the only fact a request depends on
  is that the default rule is last.
- The forwarded headers and their default values (`nginx.tmpl`,
  `rootfs/etc/nginx/lua/lua_ingress.lua` and
  `internal/ingress/controller/config/config.go`).
- The path nginx chooses on (percent-escapes decoded, dot segments resolved,
  slashes merged): measured on stock nginx (1.30.5 in the router image), taken
  as equal in ingress-nginx's nginx build (1.27.1 at that tag).
- The regex engine: the path grammar the contract accepts (literal characters,
  `\.`, `[^/]+`, `$`) has one meaning in every PCRE version; the engine of
  ingress-nginx's build was not compared with the router image's.
- `Exact` and `Prefix` rules of `rules` in regex mode, and a host without regex
  mode, are not modelled. They are refused.
