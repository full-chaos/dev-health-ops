import uuid
from datetime import datetime, timezone
from typing import Optional
from typing_extensions import NotRequired, TypedDict

import pytest

from dev_health_ops.metrics.loaders.validation import (
    ValidationError,
    validate_rows,
    validate_typed_dict,
    validate_or_raise,
)
from dev_health_ops.metrics.schemas import CommitStatRow, PullRequestRow


class SimpleTypedDict(TypedDict):
    name: str
    count: int


class WithOptional(TypedDict):
    required_field: str
    optional_field: Optional[int]


class WithNotRequired(TypedDict):
    required_field: str
    not_required_field: NotRequired[int]


class TestValidationError:
    def test_str_without_index(self):
        err = ValidationError("name", "Missing required field")
        assert str(err) == "Field 'name': Missing required field"

    def test_str_with_index(self):
        err = ValidationError("count", "Expected int, got str", row_index=5)
        assert str(err) == "Row 5, field 'count': Expected int, got str"

    def test_repr(self):
        err = ValidationError("field", "message", row_index=1)
        assert "ValidationError" in repr(err)
        assert "'field'" in repr(err)


class TestValidateTypedDict:
    def test_valid_simple_dict(self):
        data = {"name": "test", "count": 42}
        errors = validate_typed_dict(data, SimpleTypedDict)
        assert errors == []

    def test_missing_required_field(self):
        data = {"name": "test"}
        errors = validate_typed_dict(data, SimpleTypedDict)
        assert len(errors) == 1
        assert errors[0].field == "count"
        assert "Missing required" in errors[0].message

    def test_wrong_type(self):
        data = {"name": "test", "count": "not an int"}
        errors = validate_typed_dict(data, SimpleTypedDict)
        assert len(errors) == 1
        assert errors[0].field == "count"
        assert "Expected int" in errors[0].message

    def test_optional_field_with_none(self):
        data = {"required_field": "test", "optional_field": None}
        errors = validate_typed_dict(data, WithOptional)
        assert errors == []

    def test_optional_field_with_value(self):
        data = {"required_field": "test", "optional_field": 42}
        errors = validate_typed_dict(data, WithOptional)
        assert errors == []

    def test_optional_field_missing_is_error(self):
        # In TypedDict, Optional[T] means value can be None, but field is still required
        data = {"required_field": "test"}
        errors = validate_typed_dict(data, WithOptional)
        assert len(errors) == 1
        assert errors[0].field == "optional_field"

    def test_not_required_field_missing(self):
        data = {"required_field": "test"}
        errors = validate_typed_dict(data, WithNotRequired)
        assert errors == []

    def test_not_required_field_present(self):
        data = {"required_field": "test", "not_required_field": 42}
        errors = validate_typed_dict(data, WithNotRequired)
        assert errors == []

    def test_bool_not_accepted_as_int(self):
        data = {"name": "test", "count": True}
        errors = validate_typed_dict(data, SimpleTypedDict)
        assert len(errors) == 1
        assert errors[0].field == "count"

    def test_extra_field_allowed(self):
        data = {"name": "test", "count": 42, "extra": "ignored"}
        errors = validate_typed_dict(data, SimpleTypedDict)
        assert errors == []

    def test_row_index_propagated(self):
        data = {"name": 123}
        errors = validate_typed_dict(data, SimpleTypedDict, row_index=7)
        assert errors[0].row_index == 7


class TestValidateTypedDictWithSchemas:
    def test_valid_commit_stat_row(self):
        data: CommitStatRow = {
            "repo_id": uuid.uuid4(),
            "commit_hash": "abc123",
            "author_email": "test@example.com",
            "author_name": "Test User",
            "committer_when": datetime.now(timezone.utc),
            "file_path": "src/main.py",
            "additions": 10,
            "deletions": 5,
        }
        errors = validate_typed_dict(data, CommitStatRow)
        assert errors == []

    def test_commit_stat_missing_required(self):
        data = {
            "repo_id": uuid.uuid4(),
            "commit_hash": "abc123",
        }
        errors = validate_typed_dict(data, CommitStatRow)
        assert len(errors) > 0
        missing_fields = {e.field for e in errors}
        assert "committer_when" in missing_fields

    def test_pull_request_row_missing_not_required_reports_no_error(self):
        data: PullRequestRow = {
            "repo_id": uuid.uuid4(),
            "number": 42,
            "author_email": "test@example.com",
            "author_name": "Test User",
            "created_at": datetime.now(timezone.utc),
            "merged_at": None,
        }
        errors = validate_typed_dict(data, PullRequestRow)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_pull_request_with_all_not_required_fields(self):
        data: PullRequestRow = {
            "repo_id": uuid.uuid4(),
            "number": 42,
            "author_email": "test@example.com",
            "author_name": "Test User",
            "created_at": datetime.now(timezone.utc),
            "merged_at": datetime.now(timezone.utc),
            "first_review_at": datetime.now(timezone.utc),
            "first_comment_at": datetime.now(timezone.utc),
            "reviews_count": 3,
            "changes_requested_count": 1,
            "comments_count": 5,
            "additions": 100,
            "deletions": 20,
            "changed_files": 3,
        }
        errors = validate_typed_dict(data, PullRequestRow)
        assert errors == [], f"Unexpected errors: {errors}"


class TestValidateRows:
    def test_empty_list(self):
        errors = validate_rows([], SimpleTypedDict, validate=True)
        assert errors == []

    def test_valid_rows(self):
        rows = [
            {"name": "one", "count": 1},
            {"name": "two", "count": 2},
        ]
        errors = validate_rows(rows, SimpleTypedDict, validate=True)
        assert errors == []

    def test_invalid_row_reports_index(self):
        rows = [
            {"name": "valid", "count": 1},
            {"name": "invalid"},
        ]
        errors = validate_rows(rows, SimpleTypedDict, validate=True)
        assert len(errors) == 1
        assert errors[0].row_index == 1

    def test_max_errors_limit(self):
        rows = [{"name": f"row{i}"} for i in range(20)]
        errors = validate_rows(rows, SimpleTypedDict, max_errors=5, validate=True)
        assert len(errors) <= 5

    def test_validation_disabled_by_default(self):
        rows = [{"invalid": "data"}]
        errors = validate_rows(rows, SimpleTypedDict)
        assert errors == []

    def test_validation_enabled_via_flag(self):
        rows = [{"invalid": "data"}]
        errors = validate_rows(rows, SimpleTypedDict, validate=True)
        assert len(errors) > 0


class TestValidateOrRaise:
    def test_valid_data_no_exception(self):
        rows = [{"name": "test", "count": 42}]
        validate_or_raise(rows, SimpleTypedDict, validate=True)

    def test_invalid_data_raises(self):
        rows = [{"name": "test"}]
        with pytest.raises(ValueError) as exc_info:
            validate_or_raise(rows, SimpleTypedDict, validate=True)
        assert "Schema validation failed" in str(exc_info.value)
        assert "SimpleTypedDict" in str(exc_info.value)

    def test_context_in_error_message(self):
        rows = [{"name": "test"}]
        with pytest.raises(ValueError) as exc_info:
            validate_or_raise(rows, SimpleTypedDict, context="load_test", validate=True)
        assert "load_test" in str(exc_info.value)

    def test_disabled_validation_no_exception(self):
        rows = [{"invalid": "data"}]
        validate_or_raise(rows, SimpleTypedDict)
