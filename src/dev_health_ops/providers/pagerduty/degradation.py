from dev_health_ops.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
    PaginationException,
)

DATASET_FETCH_ERRORS = (
    APIException,
    AuthenticationException,
    NotFoundException,
    PaginationException,
)


class PagerDutyDatasetDegradedError(APIException):
    def __init__(self, dataset_key: str, cause: Exception) -> None:
        super().__init__(f"PagerDuty dataset {dataset_key} degraded: {cause}")
