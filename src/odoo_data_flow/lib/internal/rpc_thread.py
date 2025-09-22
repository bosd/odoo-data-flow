"""RPC Threads.

This module provides a robust, thread-safe mechanism for executing
RPC calls to Odoo in parallel with proper connection pool management.
"""

import concurrent.futures
from typing import Any, Callable, Optional

from ...logging_config import log


class RpcThread:
    """A wrapper around ThreadPoolExecutor to manage parallel RPC calls to Odoo.

    This class simplifies running multiple functions concurrently while limiting
    the number of simultaneous connections to the server and managing connection pools.
    """

    def __init__(self, max_connection: int) -> None:
        """Initializes the thread pool.

        Args:
            max_connection: The maximum number of threads to run in parallel.
        """
        if not isinstance(max_connection, int) or max_connection < 1:
            raise ValueError("max_connection must be a positive integer.")

        # Limit the actual number of connections to prevent pool exhaustion
        # This is especially important for Odoo which has connection pool limits
        effective_max_connections = min(max_connection, 4)  # Cap at 4 connections

        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=effective_max_connections
        )
        self.futures: list[concurrent.futures.Future[Any]] = []
        self.max_connection = max_connection
        self.effective_max_connections = effective_max_connections

        log.debug(
            f"Initialized RPC thread pool with requested {max_connection} "
            f"connections, effectively using {effective_max_connections} "
            f"to prevent connection pool exhaustion"
        )

    def spawn_thread(
        self,
        fun: Callable[..., Any],
        args: list[Any],
        kwargs: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Submits a function to be executed by a worker thread in the pool.

        Args:
            fun: The function to execute.
            args: A list of positional arguments to pass to the function.
            kwargs: A dictionary of keyword arguments to pass to the function.
        """
        if kwargs is None:
            kwargs = {}

        future = self.executor.submit(fun, *args, **kwargs)
        self.futures.append(future)
        return future

    def wait(self) -> None:
        """Waits for all submitted tasks to complete.

        This method will block until every task has finished. If any task
        raised an exception during its execution, that exception will be logged.
        """
        log.info(f"Waiting for {len(self.futures)} tasks to complete...")

        # Use as_completed to process results as they finish,
        # which is memory efficient.
        for future in concurrent.futures.as_completed(self.futures):
            try:
                # Calling .result() will re-raise any exception that occurred
                # in the worker thread. We catch it to log it.
                future.result()
            except Exception as e:
                # Log the exception from the failed thread.
                log.error(f"A task in a worker thread failed: {e}", exc_info=True)

        # Shutdown the executor gracefully.
        self.executor.shutdown(wait=True)
        log.info("All tasks have completed.")

    def thread_number(self) -> int:
        """Returns the total number of tasks submitted to the pool."""
        return len(self.futures)
