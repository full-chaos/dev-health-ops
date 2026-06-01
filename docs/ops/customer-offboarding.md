# Customer Offboarding and Data Deletion

This document outlines the operational procedures, technical architecture, and data deletion flows executed when a customer offboards from the Full Chaos Dev Health platform, or when a trial workspace expires.

The primary objective of the offboarding flow is to ensure the complete, secure, and verifiable removal of all customer-owned metadata, credentials, and derived metrics from active systems, while maintaining strict tenant isolation and auditability.

---

## 1. Offboarding Triggers

The offboarding and data deletion process is initiated under two primary scenarios:
1. **Customer Request**: An authorized administrator requests organization deletion via the platform UI (Danger Zone) or through a support request.
2. **Trial Expiration**: A trial period expires without conversion to a paid subscription, triggering automated workspace decommissioning.

---

## 2. Centralized Deletion Architecture

All deletion operations are orchestrated by the centralized `OrganizationDeletionService` (or `OrganizationService.delete`). Rather than relying on implicit database cascades (which can leave orphaned rows or bypass non-relational stores), the service executes an explicit, multi-phase purge across all active storage layers.

```
[Trigger: Request/Expiry]
         │
         ▼
┌─────────────────────────────────┐
│  OrganizationDeletionService    │
└────────┬───────────────┬────────┘
         │               │
         ▼               ▼
┌────────────────┐┌───────────────┐
│  Postgres DB   ││  ClickHouse   │
│  (Semantic)    ││  (Analytics)  │
└────────────────┘└───────────────┘
```

### 2.1 Dry-Run Mode (Deletion Preview)
Before any destructive action is taken, the service supports a **dry-run mode** (`dry_run=True`). 
- **Behavior**: Computes the exact counts of records, credentials, and scheduled jobs that will be affected across all Postgres and ClickHouse tables.
- **Safety**: Performs zero mutations, opens no write transactions, and issues no ClickHouse `DELETE` statements.
- **Output**: Returns a structured `DeletionResult` plan that is rendered in the user interface to provide a transparent preview of the deletion scope.

### 2.2 Audit-Safe DeletionResult
Both dry-run previews and real deletions return a standardized, serializable `DeletionResult` object. This object provides a verifiable audit trail of the deletion scope:
- **Organization ID**: The sanitized unique identifier of the organization.
- **Deleted Record Counts**: Broken down by table and category for both Postgres and ClickHouse.
- **Disabled Job Counts**: The number of background sync and metrics jobs deactivated.
- **Credential Deletion Count**: The number of encrypted integration credentials destroyed.
- **Dry-Run Flag**: Indicates whether the operation was a preview or a real deletion.
- **Timestamp**: The UTC timestamp of the operation.
- **Warnings**: Surfaced for any tables or resources whose counts could not be verified.

### 2.3 Sanitized Logging and Security
To prevent credential leakage, the deletion path enforces strict logging sanitization:
- **No Secrets**: Plaintext credentials, encrypted credentials, tokens, API keys, repository secrets, or customer-provided free text are **never** written to logs.
- **Sanitized Identifiers**: Log lines use only sanitized identifiers (such as the UUID `org_id`) to trace the deletion progress.

---

## 3. Active System Deletion Flow

When a real deletion is executed, the `OrganizationDeletionService` performs the following steps in a strict, foreign-key-safe sequence:

### Phase 1: Disable Scheduled Syncs and Queued State
To prevent background processes from resurrecting or writing new data during or after the deletion window:
1. **Deactivate Scheduled Jobs**: All `ScheduledJob` rows and beat-driven schedules associated with the organization are disabled or deleted.
2. **Cancel Queued Sync State**: Any active or queued Celery sync tasks for the organization are canceled.
3. **Dispatcher Guards**: Platform dispatchers (`sync_scheduler`, `metrics_daily`, `report_scheduler`) explicitly skip and short-circuit any operations targeting the deleted organization.

### Phase 2: Delete Integration and GitHub Credentials
All authentication secrets are permanently destroyed:
- **Integration Credentials**: Stored `IntegrationCredential.credentials_encrypted` records are deleted.
- **Encrypted Settings**: Encrypted `Setting.value` records are purged.
- **SSO Secrets**: Encrypted `SSOProvider.encrypted_secrets` are destroyed.
- **Scope**: Secrets are overwritten and removed from active memory and storage, ensuring they can never be recovered or reused.

### Phase 3: Purge Workspace-Owned Metadata (Postgres)
All relational metadata scoped to the organization is explicitly deleted from the Postgres semantic database. The deletion respects foreign key constraints by purging tables in the following order:
1. **Job and Sync State**: `JobRun` (linked to `ScheduledJob`), `BackfillJob` (linked to `SyncConfiguration`), `SyncWatermark`.
2. **Schedules and Configurations**: `ScheduledJob` (linked to `SyncConfiguration`), `SyncConfiguration`.
3. **Reports**: `ReportRun` (linked to `SavedReport`), `SavedReport`.
4. **Billing and Subscriptions**: `Refund` (linked to `Invoice`/`Subscription`), `InvoiceLineItem` (linked to `Invoice`), `Invoice`, `SubscriptionEvent` (linked to `Subscription`), `Subscription`.
5. **Access and Identity**: `Membership`, `OrgInvite`, `RefreshToken`, `ImpersonationSession` (target_org_id), `IdentityMapping`, `TeamMapping`.
6. **Core Organization**: `Organization`, `Setting`, `OrgRetentionPolicy`, `MetricCheckpoint`, `Team`, `AuditLog`, `BillingAuditLog`, `SSOProvider`, `OrgIPAllowlist`, `OrgFeatureOverride`, `OrgLicense`.

*Note: Certain billing and audit logs may be subject to regulatory retention requirements and are handled in accordance with the platform's compliance policies.*

### Phase 4: Purge Analytics and Derived Metrics (ClickHouse)
All analytics data, raw logs, and derived metrics are purged from the ClickHouse analytics store. The platform utilizes ClickHouse `ALTER TABLE ... DELETE WHERE org_id = {org_id}` (or `DROP PARTITION` where applicable) to prune the following tables:
- **Daily Metrics**: `repo_metrics_daily`, `user_metrics_daily`, `team_metrics_daily`, `work_item_metrics_daily`, `work_item_user_metrics_daily`, `work_item_state_durations_daily`, `commit_metrics`, `file_metrics_daily`, `ic_landscape_rolling_30d`, `review_edges_daily`, `cicd_metrics_daily`, `deploy_metrics_daily`, `incident_metrics_daily`, `dora_metrics_daily`, `issue_type_metrics_daily`.
- **Investment & Work Graph**: `investment_classifications_daily`, `investment_metrics_daily`, `work_unit_investments`, `work_unit_investment_quotes`, `investment_explanations`, and all `work_graph` cached analysis tables.
- **AI & Governance**: `ai_attribution`, `ai_impact_metrics_daily`, `ai_policy_events`, `ai_governance_coverage_daily`, `recommendations_daily`.
- **Raw Logs & Security**: `security_alerts`, `backfill_log`, `teams`, `repos`.

### Phase 5: Invalidate Access
All active sessions, tokens, and membership records are invalidated. Stale browser sessions or API clients are immediately blocked by the platform's central middleware and auth guards, redirecting users to the sign-in page.

---

## 4. Backup Expiry Policy

!!! warning "Important Backup Caveat"
    Executing an organization deletion removes all data from **active systems** immediately. However, this action does **not** perform an immediate, permanent deletion of data from historical system backups.

- **Backup Expiry**: Retained data in system backups is not manually deleted at the time of offboarding. Instead, these records are left to **expire naturally** under the platform's standard backup-retention policy.
- **Contractual Timelines**: If a customer contract specifies a stricter, legally binding data-deletion timeline (e.g., "complete deletion from all media within 30 days"), dedicated operational procedures are triggered to cycle or overwrite backup media in compliance with those terms.

---

## 5. Customer-Controlled Revocation

Customers do not need to wait for platform administrators to secure their repositories. Access to GitHub can be revoked directly and immediately from the customer's GitHub account:
1. **Uninstall the GitHub App**: Navigate to your GitHub Organization Settings -> Installed GitHub Apps, and uninstall the **Full Chaos Dev Health** app.
2. **Revoke Token Authorization**: If a Personal Access Token (PAT) was used, revoke the token or its authorization directly within your GitHub Developer Settings.

Once access is revoked, all further API collection and scheduled syncs fail immediately and safely at the provider boundary.
