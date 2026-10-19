package providersync

// The names stored in integration_datasets.state_changed_by: one per code path
// that creates a dataset row or switches is_enabled. The writer census
// (internal/api/syncadmin/dataset_writer_census_test.go) keeps this list and
// the writers in step.
const (
	DatasetWriterConfigSave      = "api:config_save"
	DatasetWriterConfigCreate    = "api:config_create"
	DatasetWriterDatasetEndpoint = "api:dataset_endpoint"
	DatasetWriterSecurityRow     = "scheduler:security_row"
	DatasetWriterPagerDutyRepair = "scheduler:pagerduty_repair"
	DatasetWriterFixturesSeed    = "fixtures:seed"
)
