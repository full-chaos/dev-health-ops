"""
Pagination utilities for handling paginated API responses.
"""

from typing import Callable, Generator, List, Optional, TypeVar

T = TypeVar("T")


class PaginationHandler:
    """
    Generic pagination handler for API responses.

    Supports different pagination styles:
    - Link header based (GitHub)
    - Page/per_page parameters (GitLab)
    - Cursor based
    """

    def __init__(
        self,
        per_page: int = 100,
        max_pages: Optional[int] = None,
        max_items: Optional[int] = None,
    ):
        """
        Initialize pagination handler.

        :param per_page: Number of items per page.
        :param max_pages: Maximum number of pages to fetch (None = unlimited).
        :param max_items: Maximum number of items to fetch (None = unlimited).
        """
        self.per_page = per_page
        self.max_pages = max_pages
        self.max_items = max_items
        self.current_page = 0
        self.total_items = 0

    def should_continue(self) -> bool:
        """Check if pagination should continue."""
        if self.max_pages and self.current_page >= self.max_pages:
            return False
        if self.max_items and self.total_items >= self.max_items:
            return False
        return True

    def paginate(
        self,
        fetch_func: Callable[[int, int], List[T]],
        start_page: int = 1,
    ) -> Generator[T, None, None]:
        """
        Paginate through results using a fetch function.

        :param fetch_func: Function that takes (page, per_page) and returns a list of items.
        :param start_page: Starting page number (default 1).
        :yield: Individual items from paginated results.
        """
        page = start_page
        self.current_page = 0

        while self.should_continue():
            items = fetch_func(page, self.per_page)

            if not items:
                break

            for item in items:
                if self.max_items and self.total_items >= self.max_items:
                    return

                yield item
                self.total_items += 1

            self.current_page += 1
            page += 1

            # Stop if we got fewer items than requested (last page)
            if len(items) < self.per_page:
                break

    def paginate_all(
        self,
        fetch_func: Callable[[int, int], List[T]],
        start_page: int = 1,
    ) -> List[T]:
        """
        Fetch all paginated results and return as a list.

        :param fetch_func: Function that takes (page, per_page) and returns a list of items.
        :param start_page: Starting page number (default 1).
        :return: List of all items from all pages.
        """
        return list(self.paginate(fetch_func, start_page))


class AsyncPaginationHandler:
    """
    Async pagination handler for API responses.

    Similar to PaginationHandler but for async operations.
    """

    def __init__(
        self,
        per_page: int = 100,
        max_pages: Optional[int] = None,
        max_items: Optional[int] = None,
    ):
        """
        Initialize async pagination handler.

        :param per_page: Number of items per page.
        :param max_pages: Maximum number of pages to fetch (None = unlimited).
        :param max_items: Maximum number of items to fetch (None = unlimited).
        """
        self.per_page = per_page
        self.max_pages = max_pages
        self.max_items = max_items
        self.current_page = 0
        self.total_items = 0

    def should_continue(self) -> bool:
        """Check if pagination should continue."""
        if self.max_pages and self.current_page >= self.max_pages:
            return False
        if self.max_items and self.total_items >= self.max_items:
            return False
        return True

    async def paginate(
        self,
        fetch_func: Callable,
        start_page: int = 1,
    ):
        """
        Async paginate through results using a fetch function.

        :param fetch_func: Async function that takes (page, per_page) and returns a list of items.
        :param start_page: Starting page number (default 1).
        :yield: Individual items from paginated results.
        """
        page = start_page
        self.current_page = 0

        while self.should_continue():
            items = await fetch_func(page, self.per_page)

            if not items:
                break

            for item in items:
                if self.max_items and self.total_items >= self.max_items:
                    return

                yield item
                self.total_items += 1

            self.current_page += 1
            page += 1

            # Stop if we got fewer items than requested (last page)
            if len(items) < self.per_page:
                break

    async def paginate_all(
        self,
        fetch_func: Callable,
        start_page: int = 1,
    ) -> List[T]:
        """
        Async fetch all paginated results and return as a list.

        :param fetch_func: Async function that takes (page, per_page) and returns a list of items.
        :param start_page: Starting page number (default 1).
        :return: List of all items from all pages.
        """
        results = []
        async for item in self.paginate(fetch_func, start_page):
            results.append(item)
        return results
