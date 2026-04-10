# TestOps Entity Resolution and Ownership Mapping

This document defines how TestOps entities link to the platform's core entities. It covers the resolution logic for ownership and the hierarchy of test data.

## Entity Hierarchy

The platform organizes test data through a strict hierarchy. This structure ensures that every test result or pipeline event traces back to a specific team and service.

### Execution Hierarchy
The execution flow follows this path:
org, team, service, repo, branch, PR, commit, pipeline_run, job_run, test_suite, test_case.

### Coverage Hierarchy
Coverage data links directly to the pipeline execution:
pipeline_run, coverage_snapshot, file-level coverage.

## Resolution Logic

Entity linkage happens at ingestion time. The processor layer resolves these relationships before data reaches the analytics sinks.

### Ingestion-Time Resolution
The system uses provider-native references to establish initial links. For example, GitHub Actions provides the repository, branch, commit hash, and pull request number directly in the event payload.

### Service Resolution
The service_id is resolved using one of two methods:
1. Explicit mapping in CODEOWNERS files.
2. Path-prefix conventions defined in the repository configuration.

### Team Resolution
The team_id is looked up from the semantic database (Postgres). The system checks existing team-to-repo or team-to-service mappings to assign the correct owner.

### Org ID
The org_id serves as a partition and tenant key. It's present on every entity to ensure data isolation and efficient partitioning in ClickHouse.

## Test Ownership Fallbacks
When explicit ownership isn't provided, the system follows this priority:
1. Explicit CODEOWNERS entries for the test file or directory.
2. Directory-to-service mapping based on the project structure.
3. Historical authorship of the test file as a final fallback.

## Linkage Reference Table

The following fields provide the anchor for linkage across TestOps entities.

| Entity | repo_id | run_id | commit_hash | branch | pr_number | team_id | service_id |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| Pipeline Run | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Job Run | Yes | FK | No | No | No | No | No |
| Test Suite | Yes | FK | No | No | No | Yes | Yes |
| Test Case | Yes | FK | No | No | No | No | No |
| Coverage Snapshot | Yes | FK | Yes | Yes | Yes | Yes | Yes |

Note: FK indicates a foreign key reference to the parent pipeline run.

## Schema Reference
For detailed field definitions and types, see metrics/testops_schemas.py.
