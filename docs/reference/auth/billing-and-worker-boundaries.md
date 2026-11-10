---
page_id: ref-auth-billing-worker-boundaries
summary: Signature-only billing and provider intake, worker operator authority, queue credential guards, and database role isolation.
content_type: reference
owner: platform-operations
applicability: current
lifecycle: active
---

# Billing and worker boundaries

## Billing webhook and workload authentication

These signature-only routes skip organization-scope and impersonation middleware. The handler owns authentication. The shared exemption set is `internal/apiservice/scopeexempt.go`.

| Method | Raw path | Credential class |
| --- | --- | --- |
| POST | `/api/v1/billing/webhooks/stripe` | `stripe_webhook_signature` |
| POST | `/api/v1/webhooks/github` | `github_webhook_signature` |
| POST | `/api/v1/webhooks/pagerduty/{binding_id}` | `pagerduty_webhook_signature` |

The method and raw escaped path must match exactly. PagerDuty uses the literal prefix plus one canonical UUID segment. Encoded segments, extra segments, and trailing slashes do not qualify. The [endpoint profiles](endpoint-profiles.md) in `contracts/auth/v1/endpoint-profiles.go-api.json` list only the matching signature class and no scope validators for these routes.

Enforced by: `internal/apiservice/scopeexempt_providers_test.go`, `TestSignatureOnlyRouteSkipsScopeOnlyForExactRoute`, and `TestEveryAPIListenerRowNamesTheScopeMiddlewaresAsReachableValidators` in `internal/apiservice/endpoint_profile_rows_test.go`.

Missing configuration never disables the signature check. When the Stripe signing secret or the GitHub webhook secret is unset, the handler checks against a random stand-in key, so no delivery verifies, including one signed with an empty key. An unset Stripe secret answers like a wrong secret (HTTP 400, `{"detail":"Invalid Stripe signature"}`). An unset GitHub secret answers HTTP 401. Startup logs one warning for an unset GitHub secret, never the secret itself.

Enforced by: `TestUnverifiableWebhookAnswersLikeAWrongSecret` and `TestUnverifiableWebhookRefusesADeliverySignedWithTheEmptyKey` in `internal/api/billing/webhook_unverifiable_test.go`; `TestUnverifiableProviderRefusesDeliveriesSignedWithAnEmptyKey`, `TestStandInKeyIs32NonZeroRandomBytes`, and `TestLogUnconfiguredSecretsWritesOneLinePerMissingProvider` in `internal/api/webhookintake/unverifiable_intake_test.go`. Implementation: `internal/api/billing/webhooksig.go` and `internal/api/webhookintake/secrets.go`.

GitLab and Jira intake routes are absent. They are not signature-only exemptions in this build.

Enforced by: `TestRemovedSharedSecretIntakeRoutesAnswerNotFound` in `internal/apiservice/removed_intake_routes_test.go` and `TestRoutesAreExactlyGitHubPagerDutyAndHealth` in `internal/api/webhookintake/removed_providers_test.go`.

The provider handler checks its signature before accepting a delivery. PagerDuty loads and decrypts the addressed binding's secret to check it. This requires a database read. Provider delivery and refusal proofs run in hosted integration CI.

Enforced by: `internal/api/webhookintake/webhookintake_integration_test.go`, including `TestGitHubWebhookInvalidSignatureRejectedNoRowWritten` and `TestPagerDutyWebhookInvalidSignatureRejectedNoStreamEntry`. Handler source: `internal/api/webhookintake/pagerduty.go`.

The billing-edge listener has no organization-scope or impersonation middleware. Without a valid Stripe signature, other registry credential classes, legacy credentials, and requests with no credential receive the same signature refusal: HTTP 400 with `{"detail":"Invalid Stripe signature"}`. The matrix measures zero database connections and checks that responses and logs do not expose credentials. The signature check runs before any database access. A valid signature authenticates the delivery even if another credential accompanies it.

Enforced by: `TestTheScopeMiddlewaresAreWrappedAroundTheAPIListener` and `internal/apiservice/billing_edge_wrong_class_matrix_test.go`, including `TestBillingEdgeWrongClassMatrix`, `TestBillingEdgeLegacyKeysAreRefused`, `TestBillingEdgeNoCredentialIsRefused`, and `TestBillingEdgeSignatureCheckedBeforeAnyDB`.

Stripe signs the raw body bytes. The timestamp tolerance is 300 seconds and one-sided: older signatures fail; future timestamps pass. Replay after the tolerance fails. Replay within it still reaches the handler; the listener does not reject every duplicate.

Enforced by: `TestBillingEdgeRawBodyIsPinned`, `TestBillingEdgeTimestampTolerance`, and `TestBillingEdgeStripeReplay` in `internal/apiservice/billing_edge_wrong_class_matrix_test.go`. Implementation: `internal/api/billing/webhooksig.go`.

Known residual: an unsigned POST to the Stripe route with a non-UTF-8 body returns a bare HTTP 500 before the signature decides. `utf8.Valid` runs before header parsing. This path makes no database connection and does not expose credentials.

Enforced by: `TestBillingEdgeUnsignedNonUTF8Body` in `internal/apiservice/billing_edge_wrong_class_matrix_test.go`.

Billing-edge and workers have no workload principal until a credentialed caller exists. Billing-edge makes no credentialed call to an internal service. Database access does not establish a workload principal. See [Workload registry](workload-registry.md) for the built registry.

Enforced by: `TestDefaultRegistryIsTheFiveSeededWorkloads` in `internal/auth/workload/registry_test.go`. Billing handler source: `internal/api/billing/webhook.go`.

## Worker and queue authorization

Job payloads and diagnostics must never carry reusable credentials. This includes bearer tokens, refresh tokens, service secrets, database connection strings, and API keys. The queue credential gate scans contract schemas and examples under `contracts/jobs/v1`, Go payload and queue argument types, and operator JSON in `internal/workersctl`. It probes string schemas with credential shapes. The Go quality test legs run the gate.

Enforced by: `internal/cigates/queuecreds/scan.go`, `internal/cigates/queuecreds/gate_test.go`, and `internal/cigates/queuecreds/scan_test.go`. Test execution: `.github/workflows/go-quality.yml`.

The static scan is not the only check. `jobcontract.Decode` also rejects credential-shaped strings and object keys at runtime. This includes `correlation_id`, `idempotency_key`, and `domain.type`. Their JSON schemas accept these shapes, but decoding refuses them. No free-text identifier exemption remains. Refusal text names the safe path and rule, never the submitted value. Shape checks cannot prove that arbitrary text is not a secret.

Enforced by: `TestCredentialShapedStringsAreAdmittedOnlyWhereAnIdIsFreeText`, `TestEveryRuntimeBoundStringRefusesCredentialShapedValues`, and `TestDomainTypeRefusesCredentialShapedValues` in `internal/cigates/queuecreds/gate_test.go`; `TestDecodeRefusesACredentialShapedValueInEveryEnvelopeString` and `TestRefusalPathNeverCarriesAFreeTextKey` in `internal/jobcontract/guard_test.go`. Runtime implementation: `internal/jobcontract/guard.go`.

`dho workers` uses no bearer token. Its operator boundary is access to execute the command with its database connection credentials. Startup requires domain, queue-control, and coordinator connections. It checks each role's posture with its own pool before dispatch. The command acts as the fixed operator principal: type `operator`, id `dho-workers`. This is not a workload principal. It does not check `workers:read` or `workers:operate` scopes.

Enforced by: `TestStartupPairsEachRolePostureWithItsOwnPool` in `internal/workersctl/operator_authority_inventory_test.go` and `TestOperatorAuthorizerAllowsOnlyTheOperatorPrincipal` in `internal/joboperator/auth_test.go`. Implementation: `internal/workersctl/main.go` and `internal/joboperator/auth.go`.

The authority inventory covers every dispatched verb. Service verbs use the operator authorizer, including reads such as `status`, `streams status`, `contracts`, `jobs list`, and `jobs inspect`. Direct writes use the same principal check. The four direct reads also check it: `metrics daily-blocked`, `metrics list-ambiguous-executions`, `workgraph list-ambiguous`, and `workgraph list-undelivered`. Delegating commands pass to their subcommand checks. The `capacity` and `recommendations` arguments select a backstop operation. The inventory has no `gateNone` rows.

Enforced by: `TestEveryDispatchedVerbHasAnAuthorityRow`, `TestDirectWriteRowsCoverEveryDirectWriteVerb`, `TestServiceVerbsRefuseEveryPrincipalButTheOperator`, `TestDirectWriteVerbsRefuseEveryPrincipalButTheOperator`, and `TestReadVerbsRefuseEveryPrincipalButTheOperator` in `internal/workersctl/operator_authority_inventory_test.go`. For commands and required audit inputs, see [Worker operator CLI](../cli/index.md#worker-operator-cli).

## Database role defense in depth

Domain, queue-control, and coordinator logins have separate privilege sets. Each role's posture check rejects elevated role attributes: `SUPERUSER`, `CREATEROLE`, `CREATEDB`, `BYPASSRLS`, and `REPLICATION`. It also rejects cross-role membership and grants outside its declared access. The isolation matrix changes one defect at a time, checks refusal through the affected login, checks the other logins, and checks recovery after undo. Shared grants affect all three logins. This proof runs in hosted integration CI.

Enforced by: `TestRuntimeLoginIsolationMatrix` in `internal/storage/postgres/role_isolation_matrix_integration_test.go`.

The deployment manifest validates each process class's connection wiring. River processes require queue-control connections and no coordinator connections. Control processes require queue-control and coordinator connections. Stream processes have neither. Long-running processes require a domain connection and cannot receive the migration connection string. The operator requires all three runtime connections; the migration job receives its dedicated connection string.

Enforced by: `TestEveryWorkerClassRejectsItsBrokenDatabaseWiring` and `TestOperatorAndMigrationJobRejectBrokenDatabaseWiring` in `internal/deploymentcontract/role_isolation_test.go`. Validators: `internal/deploymentcontract/manifest.go`. For deployment details, see [Go worker runtime](../../contribute/architecture/go-worker-runtime.md).

## Retired worker service credentials

The `svc_worker_` credential class is retired. No endpoint profile accepts a retired class. The entire `dho service-credentials` command group is absent. Create, rotate, list, and revoke all return an unknown-command error with exit 2. This build does not provide the earlier create/rotate JSON refusal or stored-row list/revoke operations.

Enforced by: `TestRetiredVerbGroupsAreUnknownCommands` and `TestRetiredVerbGroupsAreNotInTheTree` in `cmd/dho/retired_verbs_test.go`; `TestNoEndpointProfileRowAcceptsARetiredClass` in `internal/auth/contracts/credential_class_retired_test.go`. See [Credential classes](credential-classes.md).

The command removal provides no row deletion or migration. Stored service credential rows do not grant worker operator authority. The operator authorizer accepts only its fixed operator principal.

Enforced by: `cmd/dho/retired_verbs_test.go` and `internal/joboperator/auth_test.go`.
