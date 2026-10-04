# GitLab Runner (Docker) template

This folder provides a simple Docker-based GitLab Runner setup suitable for running `.gitlab-ci.yml` (including `services` like Postgres, ClickHouse, Valkey and `docker:27-dind`).

## Start the runner container

```bash
docker compose -f gitlab-runner/docker-compose.yml up -d
```

## Register the runner

Get a registration token from your GitLab project/group, then run:

```bash
docker exec -it gitlab-runner gitlab-runner register
```

Recommended answers:

- **Executor**: `docker`
- **Default Docker image**: `python:3.14`
- **Privileged**: yes (the Go, container and live-e2e jobs start a `docker:27-dind` service)
- **Tags**: `docker` (optional; only needed if your project requires tagged runners)

## Notes

- This mounts `/var/run/docker.sock` into the runner container so it can start job + service containers (common approach, but treat the runner host as trusted).
