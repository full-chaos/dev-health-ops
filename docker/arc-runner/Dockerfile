# ARC (actions-runner-controller) runner image for the oci-arc-runners scale
# set, adding tools the upstream image doesn't carry. Diagnosed 2026-09-03:
# CF's first job picked up on oci-arc-runners failed with `make: command not
# found`; the pod also lacked psql/pg_isready (no postgresql-client) and node.
# CF's second pickup then failed with `-race requires cgo; CGO_ENABLED=1` --
# no C toolchain either. node is still not added -- not requested, and this
# image should stay a minimal, targeted addition over upstream rather than
# growing into a general toolbox; add it in a follow-up if a job actually
# needs it.
#
# TWO MORE GAPS, diagnosed 2026-09-04/05 by go-quality's first live runs on
# this pool (ops PR #2242, which routes go-quality here for the first time --
# these gaps were always latent, just never exercised before, since
# go-quality's prior `runs-on` named a hosted label this enterprise doesn't
# actually offer):
#   - xz-utils (CHAOS-5130): `tar -xJf` on a pinned shellcheck .tar.xz release failed
#     `xz: Cannot exec: No such file or directory`. GitHub-hosted
#     ubuntu-latest ships xz-utils by default; this image never needed it.
#     Worked around in the PR by switching that download to .tar.gz (gzip is
#     an apt dependency, so it needs no image change) -- installed here too
#     so a future job that genuinely needs `tar -xJf`/`xz` directly is not
#     surprised a second time.
#   - docker-compose-plugin (CHAOS-5134): `ci/check_river_compat_static.sh`
#     runs `docker compose` (v2 CLI plugin syntax) directly against the
#     River compatibility harness's compose file -- a real dependency, not
#     a format choice like shellcheck's tarball, so there is no
#     script-level workaround. Installing just the plugin package, not
#     `docker-ce`/`containerd.io`, is deliberate: this runner talks to a
#     Docker daemon over a DinD SIDECAR, not a locally-managed engine, so
#     only the CLI-side plugin is needed.
#
#     MEASURED, not assumed (2026-09-05, PR #2245's own first build):
#     `docker-ce-cli` already works in every job routed to this pool
#     (`docker build`/`docker run`/`docker buildx imagetools`, including
#     this Dockerfile's own build job) -- the ORIGINAL hypothesis here was
#     that this meant Docker's official apt repo was already registered in
#     this base layer, so `apt-get install docker-compose-plugin` would
#     just resolve. WRONG: that first attempt failed `E: Unable to locate
#     package docker-compose-plugin` -- this base image's apt sources are
#     stock Ubuntu noble only (ports.ubuntu.com + a git-core PPA, no
#     download.docker.com entry), so `docker-ce-cli`'s presence must come
#     from somewhere else in the upstream actions-runner image (baked in
#     directly, or reachable only via the DinD sidecar's own binary) --
#     its presence proves nothing about apt sources. Docker's official apt
#     repository is added explicitly below, per their own install docs
#     (docs.docker.com/engine/install/ubuntu/, "Install using the apt
#     repository") -- `arch=$(dpkg --print-architecture)` so this resolves
#     correctly on both the native-arm64 self-hosted leg and the x64+QEMU
#     fallback leg.
#
# gcc + libc6-dev, not build-essential: measured directly against this exact
# base image (2026-09-03, `apt-get install -y <set>` on a fresh container
# each), not assumed -- gcc+libc6-dev pulls 59 packages/59.3MB download/199MB
# installed; build-essential pulls 86 packages/76.0MB/261MB (g++, dpkg-dev,
# and friends this image has no use for). gcc+libc6-dev is the smaller,
# sufficient set for CGO compilation.
#
# Pinned by digest, not the `latest` tag: the scale set's own values.yaml
# already floats on `latest` for the base image (see the AutoscalingRunnerSet
# spec), so pinning here is what makes a rebuild of THIS image reproducible
# against a known-good base rather than whatever `latest` happens to resolve
# to on build day. Re-derive the digest (`docker buildx imagetools inspect
# ghcr.io/actions/actions-runner:latest --format '{{json .Manifest}}'`) and
# bump it deliberately, not automatically -- the upstream project's own
# release notes should be read first, since this image runs privileged
# (actions-runner + dind) in-cluster.
FROM ghcr.io/actions/actions-runner@sha256:e5496277be5d09bc968b3d64911b74e219ac4a3f2edce956a3ecf9271bea1ef4

USER root

# Docker's official apt repository -- NOT already present in this base
# image (see the header comment's docker-compose-plugin entry for the
# measured proof). Exact recipe from docs.docker.com/engine/install/ubuntu/
# ("Install using the apt repository"), the only change being
# `--no-install-recommends` to match this file's existing convention.
# ca-certificates/curl/gnupg are the repo-setup tool dependencies (fetching
# and verifying the GPG key over https); harmless if the base image already
# has any of them.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${VERSION_CODENAME}") stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update

# make: needed by any job that shells out to the repo's own Makefile targets.
# postgresql-client: provides both psql and pg_isready, needed by any job
# that waits on or inspects a Postgres instance directly rather than only
# through the Go/Python client libraries already baked into the checkout.
# gcc + libc6-dev: a C toolchain, needed for CGO_ENABLED=1 -- required by
# `go test -race` and anything else that needs cgo. Go itself is NOT baked
# into this image; the job's own actions/setup-go step provides it, same as
# on any other runner.
# xz-utils: `tar -xJf` (.tar.xz decompression) -- see the header comment.
# docker-compose-plugin: `docker compose` (v2 CLI syntax) -- see the header
# comment. Deliberately NOT `docker-ce`/`docker-ce-cli`/`containerd.io`:
# this base image already carries those (it talks to a DinD sidecar), so
# installing them again would be redundant at best -- the repo above is
# added ONLY to make the plugin package resolve.
RUN apt-get install -y --no-install-recommends \
        make \
        postgresql-client \
        gcc \
        libc6-dev \
        xz-utils \
        docker-compose-plugin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER runner
