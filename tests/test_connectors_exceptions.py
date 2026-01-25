"""
Tests for connector exceptions.
"""

import pytest

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    ConnectorException,
    NotFoundException,
    PaginationException,
    RateLimitException,
)


class TestExceptions:
    """Test exception classes."""

    def test_connector_exception(self):
        """Test base ConnectorException."""
        with pytest.raises(ConnectorException) as exc_info:
            raise ConnectorException("Test error")
        # Assertions outside with block to inspect captured exception
        assert "Test error" in str(exc_info.value)

    def test_rate_limit_exception(self):
        """Test RateLimitException."""
        with pytest.raises(RateLimitException) as exc_info:
            raise RateLimitException("Rate limit exceeded")
        # Assertions outside with block to inspect captured exception
        assert "Rate limit exceeded" in str(exc_info.value)
        assert isinstance(exc_info.value, ConnectorException)

    def test_authentication_exception(self):
        """Test AuthenticationException."""
        with pytest.raises(AuthenticationException) as exc_info:
            raise AuthenticationException("Auth failed")
        # Assertions outside with block to inspect captured exception
        assert "Auth failed" in str(exc_info.value)
        assert isinstance(exc_info.value, ConnectorException)

    def test_not_found_exception(self):
        """Test NotFoundException."""
        with pytest.raises(NotFoundException) as exc_info:
            raise NotFoundException("Resource not found")
        # Assertions outside with block to inspect captured exception
        assert "Resource not found" in str(exc_info.value)
        assert isinstance(exc_info.value, ConnectorException)

    def test_pagination_exception(self):
        """Test PaginationException."""
        with pytest.raises(PaginationException) as exc_info:
            raise PaginationException("Pagination error")
        # Assertions outside with block to inspect captured exception
        assert "Pagination error" in str(exc_info.value)
        assert isinstance(exc_info.value, ConnectorException)

    def test_api_exception(self):
        """Test APIException."""
        with pytest.raises(APIException) as exc_info:
            raise APIException("API error")
        # Assertions outside with block to inspect captured exception
        assert "API error" in str(exc_info.value)
        assert isinstance(exc_info.value, ConnectorException)

    def test_exception_hierarchy(self):
        """Test exception hierarchy."""
        assert issubclass(RateLimitException, ConnectorException)
        assert issubclass(AuthenticationException, ConnectorException)
        assert issubclass(NotFoundException, ConnectorException)
        assert issubclass(PaginationException, ConnectorException)
        assert issubclass(APIException, ConnectorException)

    def test_catching_base_exception(self):
        """Test catching derived exceptions with base class."""
        try:
            raise RateLimitException("Rate limit")
        except ConnectorException as e:
            assert isinstance(e, RateLimitException)

        try:
            raise AuthenticationException("Auth error")
        except ConnectorException as e:
            assert isinstance(e, AuthenticationException)
