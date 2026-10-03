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
  ]
}
```

- One rule is one Ingress path as the Ingress object holds it AFTER the charts
  rendered it: `path`, `path_type` (the Ingress `pathType`), and `plane` (the
  backend Service: the Go api or the query-api).
- `regex_mode` says that an Ingress of the host carries
  `nginx.ingress.kubernetes.io/use-regex: "true"`. ingress-nginx then reads
  every path of the host as a regex.
- The order of the rules has no meaning. The generator sorts them.

The table is the RENDERED rules, not the lists of the values file. The values
file holds three lists that two charts change before ingress-nginx sees them (a
`{name}` token becomes `[^/]+`, a `$` is added, an Exact path is quoted). A
contract of the lists would make the generator a second reading of the values
beside the charts. A contract of the rendered rules has no such second reading:
the charts are compared with it by rendering them.

### First content

The first content is what prod routes at deploy commit
`c72844763bb1ec5c7c886c4b0d3b85eaa34437cd` (vendored ops
`98a8577112a297b20c50ac6863c4437e8980d616`): 197 rules. It was rendered with
helm v4.2.4 from the prod values (the kiac values, then the prod values) through
the three templates that make these rules, and not written by hand:

- the deploy chart's `go-api-ingress.yaml` (171 anchored rules to the Go api);
- the deploy chart's `query-api-ingress.yaml` (24 anchored rules to query-api);
- the vendored ops chart's `templates/ingress.yaml` (the default rule `/` to the
  Go api, and `/graphql$` to query-api).

The two api hosts (`api.fullchaos.dev` and the in-cluster host that web's
server-side proxy calls) render the same 197 rules, and both are in regex mode.

Not in the contract, by design:

- The public host's block object (`/metrics`, `/docs`, `/redoc`,
  `/openapi.json`, Exact, to the Go api). It is on the public host only and it
  gives those paths the plane the default rule gives them.
- The billing host (`ingress.goApiHosts`). It is another host and another
  listener.

Limit of the first content: the three templates were rendered one by one, not
the whole deploy chart (that render needs the secrets values, which were not
opened). The deploy-side test below renders the whole chart.

### What the validator refuses

`internal/ingressplanes` refuses a table that it cannot write with the match
result ingress-nginx gives. A refusal is by design: a shape that would have to
be guessed is not read.

- `regex_mode: false`. Only a host in regex mode is modelled.
- More or fewer than one default rule (`/`, `Prefix`).
- A rule that is not the default and is `Exact` or `Prefix`. In regex mode
  ingress-nginx writes such a path as a regex with no end anchor, so `/docs`
  also matches `/docsx`. Write the rule anchored.
- A path that is not anchored: segments of letters, digits, `_`, `-` and `\.`,
  or the whole-segment token `[^/]+`, then one `$`.
- A `.` or `..` segment, a plane that is not `go-api` or `query-api`, the same
  path twice (compared without case).
- Two rules that can match one request path. With such rules the answer would
  depend on the order of the locations.

## The generated router configuration

```bash
go run ./internal/ingressplanes/cmd/routerconf          # write the file
go run ./internal/ingressplanes/cmd/routerconf -check   # exit 1 when it is stale
```

Run it from the repository root. Nothing runs it at run time and there is no
Python in it: the compose stack mounts the checked-in file.

Each rule becomes one location, in the form and the order ingress-nginx uses for
a host in regex mode:

- Form: `location ~* "^<path>"` for every rule, the default rule too
  (`~* "^/"`). So an anchored path stays anchored and the match ignores case
  (`/GRAPHQL` reaches query-api, as in prod).
- Order: the longest path first, then the greater path first. nginx takes the
  first regex location that matches, so the default rule (one character) is
  last.
- A location proxies with no URI part, so the plane receives the request target
  as the client sent it.

Parts of the file that do NOT come from the contract (constants of the
generator; no test compares them with prod):

- Listen port 8000; the planes `go-api:8000` and `query-api:8090` (the compose
  service names and the public listener ports).
- `resolver 127.0.0.11`: Docker's embedded DNS. A plane is named through a
  variable, so nginx resolves the name at request time. The router starts
  before the planes and finds a plane again after it was recreated. Another
  container runtime needs another resolver address; the generator has no
  option for that yet.
- `client_max_body_size 50m`, read and send timeouts of 60 s: the values of
  prod's Ingress annotations at the deploy commit above. `proxy_http_version
  1.1` and `proxy_buffering off`: the defaults of ingress-nginx.
- Headers: `Host` is the Host the client sent. `X-Real-IP` is the router's
  peer. `X-Forwarded-For` is the client's value with the router's peer added
  (the "append" form that `internal/api/clientip` accepts from a trusted
  proxy). The router sets no other header.
- The pid file and every temporary path are under `/tmp`, so the router runs as
  any user id.

## The router adds no credential

Web's server-side proxy is the only place that turns the session cookie into an
`Authorization` bearer. The router keeps that property in three ways:

1. It never sets, reads or checks a credential. It has no directive that names
   `Authorization` or a cookie, and no auth module. A request passes with the
   headers it came with.
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
(a closed list of directives, of headers and of upstreams).
`TestRealNginxRoutesEveryPathToItsPlane` measures point 3 on a real nginx.

## Tests in this repository

All in `internal/ingressplanes`.

| Test | What fails it |
| --- | --- |
| `TestCheckedInRouterConfigIsWhatTheContractGenerates` | The file is not the bytes the contract generates (a hand edit, or a contract change with no generate). |
| `TestCheckedInRouterConfigAgreesWithTheContract` | The file, read by the test's own nginx parser, does not hold one location per rule, to the rule's plane, in the order above. It does not call the generator. |
| `TestCheckedInRouterConfigRoutesEveryPathToItsPlane` | A request path does not reach its plane, by nginx's location rule over the locations of the file. |
| `TestRealNginxRoutesEveryPathToItsPlane` (build tag `integration`) | The same rows through a real nginx: the checked-in file in a container of `ingressplanes.RouterImage`, with stub planes on the compose names. |
| `TestSchemaAndValidatorAgree`, `TestValidateRefusesWhatItCannotRoute` | The schema or the validator accepts a planted defect. |

The rows are: for each rule its own path, the path in upper case, with a
trailing slash and with one more character; and hand-written rows (the
`/graphql` cases, near misses, an unknown path). The real-nginx test adds
percent-escapes, dot segments, doubled slashes and query strings.

The test without nginx does NOT prove: that nginx accepts the file, nginx's
regex engine, the normalisation nginx applies to a path before it chooses a
location, and what the plane receives. Only the real-nginx test proves those.

`DEV_HEALTH_ROUTER_NGINX_BIN=<nginx binary>` makes the real-nginx test start no
container: it runs that binary on the generator's output with stub planes in
the test process. It is for a host where a lane may not start a container. It
does not prove the checked-in bytes or the name lookup.

## The test the deploy repository must have

The deploy repository vendors this repository. Its test reads
`vendor/dev-health-ops/contracts/ingress/v1/planes.json` and compares it with
the RENDER of its chart with the prod values:

1. Render the chart with the prod values, as its other tests do.
2. From every Ingress object take each rule: host, `path`, `pathType`, the
   backend Service name, and if the object has the annotation
   `nginx.ingress.kubernetes.io/use-regex: "true"`.
3. The api hosts are the hosts with a rule `/` whose backend is the release's
   Go api Service. No api host is a failure: nothing was measured.
4. For each api host, take its rules from ALL Ingress objects, without the
   rules of the block object (name `<release>-ops-blocked-internal-paths-<host>`,
   label `app.kubernetes.io/component: internal-path-block`).
   Write each as `{path, path_type, plane}`: `path_type` is the `pathType`;
   `plane` is `go-api` for the Go api Service and `query-api` for the query-api
   Service; any other Service is a failure.
5. That set must equal the `rules` of the contract: the same rows, each one
   time, in any order.
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
  rule (`~* "^/"`) there is none.
- The location order (`internal/ingress/controller/controller.go`: sort by
  path, descending, then stable by path length, descending). The contract
  refuses two rules that match one path, so the only fact a request depends on
  is that the default rule is last.
- The path nginx chooses on (percent-escapes decoded, dot segments resolved,
  slashes merged): measured on stock nginx (1.30.5 in the router image), taken
  as equal in ingress-nginx's nginx build (1.27.1 at that tag).
- The regex engine: the path grammar the contract accepts (literal characters,
  `\.`, `[^/]+`, `$`) has one meaning in every PCRE version; the engine of
  ingress-nginx's build was not compared with the router image's.
- `Exact` and `Prefix` rules in regex mode, and a host without regex mode, are
  not modelled. They are refused.
