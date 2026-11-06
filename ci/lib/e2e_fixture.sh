# shellcheck shell=bash
# Fixture helpers the live backend e2e and the metrics executed-proof runners share. SOURCED, never executed.
#
# Reads (set by the caller): BIN_DIR (cigate and dho built into it), POSTGRES_HOST, POSTGRES_PORT, POSTGRES_SUPERUSER,
# POSTGRES_SUPERUSER_PASSWORD, POSTGRES_DB, POSTGRES_URI, E2E_ORG_ID, E2E_ISSUER_URL and E2E_ISSUER_KEY_FILE (the CI issuer
# stub the api verifies against, ci/run_live_backend_e2e.sh).

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
# The user goes through the dho verb, so the writer's invariants apply. The organization row, the membership and
# the user's session are the raw writes: no dho verb exists for creating an organization with a chosen id (E2E_ORG_ID
# is the fixed uuid the ClickHouse fixtures and the envelope tokens already name), and an access token is accepted
# only for a live session of its user, pinned to the user's token_version, which auth-service writes at login (no
# login runs here). The session row is the test double of that login; the token is minted by the CI issuer stub's key.
generate_auth_token() {
  local created user_id password session_id
  e2e_psql -c "
    INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at)
      VALUES ('${E2E_ORG_ID}', 'e2e-org', 'E2E Org', '{}', 'enterprise', true, now(), now())
      ON CONFLICT (id) DO NOTHING;" >/dev/null
  # A rerun against the same database reuses the user (a read), the way the fixed-id seed did.
  user_id="$(e2e_psql -c "SELECT id FROM users WHERE email = 'e2e@test.local'" | head -n 1)"
  if [ -z "${user_id}" ]; then
    # "Aa1-" keeps a letter and a digit in the password whatever the random hex is (shared password policy, 12+ chars).
    password="Aa1-$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
    created="$(printf '%s\n' "${password}" | e2e_dho_admin users create --email e2e@test.local --password-stdin)" || {
      echo "ERROR: dho admin users create failed: ${created}" >&2
      return 1
    }
    user_id="$(printf '%s\n' "${created}" | sed -n 's/^Created user: .* (id: \([0-9a-fA-F-]*\))$/\1/p' | head -n 1)"
    if [ -z "${user_id}" ]; then
      echo "ERROR: could not read the user id from: ${created}" >&2
      return 1
    fi
  fi
  case "${user_id}" in
    *[!0-9a-fA-F-]* | "")
      echo "ERROR: the e2e user id is not a uuid" >&2
      return 1
      ;;
  esac
  # The membership is a raw write like the organization row: the dho membership verbs send the change to auth-service
  # (CHAOS-11799), and this job runs no auth-service.
  e2e_psql -c "
    UPDATE memberships SET role = 'admin', updated_at = now() WHERE org_id = '${E2E_ORG_ID}' AND user_id = '${user_id}';
    INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
      SELECT gen_random_uuid(), '${E2E_ORG_ID}', '${user_id}', 'admin', now(), now(), now()
      WHERE NOT EXISTS (SELECT 1 FROM memberships WHERE org_id = '${E2E_ORG_ID}' AND user_id = '${user_id}');" >/dev/null || {
    echo "ERROR: could not add the e2e user to the e2e organization" >&2
    return 1
  }
  session_id="$(e2e_psql -c "
    INSERT INTO auth.principals (id, kind, display_name) VALUES ('${user_id}', 'user', 'e2e') ON CONFLICT (id) DO NOTHING;
    INSERT INTO auth.sessions (principal_id, token_hash, expires_at, legacy_token_version)
      SELECT id, sha256(convert_to(gen_random_uuid()::text, 'UTF8')), now() + interval '2 hours', token_version
        FROM public.users WHERE id = '${user_id}'
      RETURNING id;" | tail -n 1)"
  case "${session_id}" in
    *[!0-9a-fA-F-]* | "")
      echo "ERROR: could not read the e2e session id" >&2
      return 1
      ;;
  esac
  E2E_USER_ID="${user_id}" E2E_ORG_ID="${E2E_ORG_ID}" E2E_SESSION_ID="${session_id}" "${BIN_DIR}/cigate" live-e2e mint-token
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
