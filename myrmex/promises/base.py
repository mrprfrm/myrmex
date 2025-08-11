import asyncio
import inspect
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    ParamSpec,
    Protocol,
    Self,
    TypeVar,
    cast,
)

P = ParamSpec("P")
R = TypeVar("R")
U = TypeVar("U")
T = TypeVar("T", bound="Promise")


class PromiseLike(Protocol[P, R]):
    def __init__(self, fn: Callable[P, Awaitable[R]]) -> None:
        """Initialize the promise with a callable and its arguments."""
        ...

    def __await__(self) -> Generator[None, None, R]:
        """Resolve the promise and return a result."""
        ...

    def execute(self, *args: P.args, **kwargs: P.kwargs) -> "PromiseLike[P, R]":
        """Resolve promise immediately and return a result."""
        ...

    def then(self, fn: Callable[[R], Awaitable[U]]) -> "PromiseLike[[R], U]":
        """Chain a callback to be executed when the promise resolves."""
        ...


class Promise(PromiseLike[P, R]):
    def __init__(
        self,
        fn: Callable[P, Awaitable[R] | R],
    ) -> None:
        self._fn = fn
        self._tail: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        self._head: Coroutine[Any, Any, None] | None = None
        self._processed: bool = False

    def __await__(self) -> Generator[None, None, R]:
        if self._processed:
            raise RuntimeError("Promise has already been awaited.")

        if self._head is None:
            raise RuntimeError(
                "Promise must be executed before awaiting. Call execute() first."
            )

        self._processed = True
        asyncio.create_task(self._head)
        return self._tail.__await__()

    async def __process(self, *args: P.args, **kwargs: P.kwargs):
        try:
            result = self._fn(*args, **kwargs)
            if inspect.isawaitable(result):
                result = cast(R, await result)
            if not self._tail.done():
                self._tail.set_result(result)
        except asyncio.CancelledError:
            if not self._tail.done():
                self._tail.cancel()
            raise
        except Exception as exc:
            if not self._tail.done():
                self._tail.set_exception(exc)

    def execute(self, *args: P.args, **kwargs: P.kwargs) -> Self:
        if self._head is not None:
            raise RuntimeError("Promise has already been executed.")

        self._head = self.__process(*args, **kwargs)
        return self

    def then(self, fn: Callable[[R], Awaitable[U] | U]) -> "PromiseLike[[R], U]":
        nxt = Promise[[R], U](fn)
        nxt._head = self._head

        def callback(fut: asyncio.Future[R]) -> None:
            try:
                if fut.cancelled():
                    if not nxt._tail.done():
                        nxt._tail.cancel()
                elif exc := fut.exception():
                    if not nxt._tail.done():
                        nxt._tail.set_exception(exc)
                else:
                    asyncio.create_task(nxt.__process(fut.result()))
            except Exception as exc:
                if not nxt._tail.done():
                    nxt._tail.set_exception(exc)

        self._tail.add_done_callback(callback)
        return nxt

    def chain(self, promise: T) -> T:
        promise._head = self._head

        def callback(fut: asyncio.Future[R]) -> None:
            try:
                if fut.cancelled():
                    if not promise._tail.done():
                        promise._tail.cancel()
                elif exc := fut.exception():
                    if not promise._tail.done():
                        promise._tail.set_exception(exc)
                else:
                    asyncio.create_task(promise.__process(fut.result()))
            except Exception as exc:
                if not promise._tail.done():
                    promise._tail.set_exception(exc)

        self._tail.add_done_callback(callback)
        return promise
