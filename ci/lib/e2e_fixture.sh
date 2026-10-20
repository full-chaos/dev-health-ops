# shellcheck shell=bash
# Fixture helpers the live backend e2e and the metrics executed-proof runners share. SOURCED, never executed.
#
# Reads (set by the caller): BIN_DIR (cigate and dho built into it), POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SUPERUSER,
# POSTGRES_SUPERUSER_PASSWORD, POSTGRES_DB, POSTGRES_URI, E2E_ORG_ID, SETTINGS_ENCRYPTION_KEY.

# psql against the job's throwaway Postgres (superuser, like ci/lib/go_worker_fixture.sh).
e2e_psql() {
  PGPASSWORD="${POSTGRES_SUPERUSER_PASSWORD}" psql \
    --host="${POSTGRES_HOST}" --port="${POSTGRES_PORT}" --username="${POSTGRES_SUPERUSER}" --dbname="${POSTGRES_DB}" \
    --set=ON_ERROR_STOP=1 --quiet --tuples-only --no-align "$@"
}

# The dho admin verbs against the job's Postgres: the migration DSN is the superuser here (the verbs prefer
# MIGRATION_DATABASE_URI), and they print their results on stdout.
e2e_dho_admin() {
  MIGRATION_DATABASE_URI="${POSTGRES_URI}" "${BIN_DIR}/dho" admin "$@"
}

# Seeds the e2e organization, user and admin membership and prints the access token the api's /health check sends.
# The user and the membership go through the dho verbs, so the writer's invariants apply. The organization row is
# the one raw write: no dho verb exists for creating an organization with a chosen id, and E2E_ORG_ID is the fixed
# uuid the ClickHouse fixtures and the envelope tokens already name. It is a fixed uuid of this script, safe in SQL.
generate_auth_token() {
  local created user_id password
  e2e_psql -c "
    INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at)
      VALUES ('${E2E_ORG_ID}', 'e2e-org', 'E2E Org', '{}', 'enterprise', true, now(), now())
      ON CONFLICT (id) DO NOTHING;" >/dev/null
  password="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
  created="$(printf '%s\n' "${password}" | e2e_dho_admin users create --email e2e@test.local --password-stdin)" || {
    echo "ERROR: dho admin users create failed: ${created}" >&2
    return 1
  }
  user_id="$(printf '%s\n' "${created}" | sed -n 's/^Created user: .* (id: \([0-9a-fA-F-]*\))$/\1/p' | head -n 1)"
  if [ -z "${user_id}" ]; then
    echo "ERROR: could not read the user id from: ${created}" >&2
    return 1
  fi
  e2e_dho_admin users update --id "${user_id}" --org "${E2E_ORG_ID}" --role admin >/dev/null || {
    echo "ERROR: dho admin users update could not add the e2e user to the e2e organization" >&2
    return 1
  }
  E2E_USER_ID="${user_id}" E2E_ORG_ID="${E2E_ORG_ID}" SETTINGS_ENCRYPTION_KEY="${SETTINGS_ENCRYPTION_KEY}" "${BIN_DIR}/cigate" live-e2e mint-token
}

# run_start_stamp prints the UTC ISO8601 stamp a run records before it seeds: microseconds where `date` supports them
# (GNU), whole seconds elsewhere.
run_start_stamp() {
  local stamp
  stamp="$(date -u +%Y-%m-%dT%H:%M:%S.%6N+00:00)"
  case "${stamp}" in
    *N*) stamp="$(date -u +%Y-%m-%dT%H:%M:%S+00:00)" ;;
  esac
  printf '%s\n' "${stamp}"
}
