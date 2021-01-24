"""Utilities"""

import asyncio
from asyncio import Event
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Optional,
    TypeVar,
    TYPE_CHECKING
)
if TYPE_CHECKING:
    # pylint: disable=ungrouped-imports
    from asyncio import Future
    from typing import Any, Set

T = TypeVar('T')


async def cancellable_aiter(
        async_iterator: AsyncIterator[T],
        cancellation_event: Event
) -> AsyncIterator[T]:
    result_iter = async_iterator.__aiter__()
    cancellation_task = asyncio.create_task(cancellation_event.wait())
    while not cancellation_event.is_set():
        iter_task = asyncio.create_task(result_iter.__anext__())
        pending: "Set[Future[Any]]" = {
            cancellation_task,
            iter_task
        }
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED
        )
        for done_task in done:
            if done_task == cancellation_task:
                for pending_task in pending:
                    await pending_task
                break
            else:
                yield done_task.result()


async def aiter_adapter(
    awaitable: Callable[[], Awaitable[T]],
    cancellation_event: Event,
    *,
    timeout: Optional[float] = None
) -> AsyncIterator[T]:
    cancellation_task = asyncio.create_task(
        cancellation_event.wait(),
        name='cancel'
    )
    awaitable_task = asyncio.create_task(
        awaitable(),
        name='awaitable'
    )
    pending: "Set[Future[Any]]" = {cancellation_task, awaitable_task}
    while not cancellation_event.is_set():
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED,
            timeout=timeout
        )

        if not done:
            # The wait has timed out.
            break

        if awaitable_task in done:
            yield awaitable_task.result()
            awaitable_task = asyncio.create_task(
                awaitable(),
                name='awaitable'
            )
            pending.add(awaitable_task)

    for task in pending:
        try:
            task.cancel()
        except asyncio.CancelledError:
            pass
