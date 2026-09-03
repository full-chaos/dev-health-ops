# ARC (actions-runner-controller) runner image for the oci-arc-runners scale
# set, adding tools the upstream image doesn't carry. Diagnosed 2026-09-03:
# CF's first job picked up on oci-arc-runners failed with `make: command not
# found`; the pod also lacked psql/pg_isready (no postgresql-client) and node.
# node is not added here -- not requested, and this image should stay a
# minimal, targeted addition over upstream rather than growing into a general
# toolbox; add it in a follow-up if a job actually needs it.
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

# make: needed by any job that shells out to the repo's own Makefile targets.
# postgresql-client: provides both psql and pg_isready, needed by any job
# that waits on or inspects a Postgres instance directly rather than only
# through the Go/Python client libraries already baked into the checkout.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        make \
        postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER runner
