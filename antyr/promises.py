import copy
import functools
import inspect
from typing import (
    Any,
    Callable,
    Coroutine,
    Generator,
    Generic,
    List,
    ParamSpec,
    TypeVar,
    cast,
)

import trio
from typing_extensions import Self

# Parameter specification for promise functions
P = ParamSpec("P")
# Parameter specification for parent promise functions
V = ParamSpec("V")

# Promise result type
R = TypeVar("R")
# Parent promise result type
U = TypeVar("U")
# Next promise result type
K = TypeVar("K")


class PromiseNode(Generic[P, R]):
    def __init__(
        self,
        fn: Callable[P, Coroutine[None, None, R]],
        *,
        parent: "PromiseNode[V, U] | None" = None,
        scope: trio.CancelScope | None = None,
    ) -> None:
        self._fn = fn
        self._parent: "PromiseNode[Any, Any] | None" = parent
        self._head: Coroutine[None, None, R] | None = None
        self._scope = scope or trio.CancelScope()

    def __clone(self, *, parent: "PromiseNode[V, U] | None") -> "PromiseNode[P, R]":
        new = copy.copy(self)
        new._parent = parent
        new._head = None
        new._scope = self._scope
        return new

    def __await__(self) -> Generator[None, None, R]:
        current = self
        promises: List[PromiseNode[P, R]] = []
        while current is not None:
            promises.append(current)
            current = current._parent

        async def execute() -> R:
            result = None
            with self._scope:
                async with trio.open_nursery():
                    while promises:
                        promise = promises.pop()
                        if promise._head is not None:
                            result = await promise._head
                            continue
                        signature = inspect.signature(promise._fn)
                        if result is None:
                            bound = signature.bind()
                        else:
                            bound = signature.bind(result)
                        result = await promise._fn(*bound.args, **bound.kwargs)
            return cast(R, result)

        return execute().__await__()

    def then(
        self,
        nxt: Callable[[R], Coroutine[None, None, K]] | "PromiseNode[[R], K]",
        **kwargs: Any,
    ) -> "PromiseNode[[R], K]":
        if self._parent is None and self._head is None:
            raise RuntimeError("Cannot chain a root promise that is not initialized.")

        if isinstance(nxt, PromiseNode):
            return nxt.__clone(parent=self)

        nxt = functools.partial(nxt, **kwargs)
        nxt = PromiseNode[[R], K](nxt, parent=self)
        return nxt


class Promise(PromiseNode[P, R]):
    def __init__(
        self,
        fn: Callable[P, Coroutine[None, None, R]],
        *,
        scope: trio.CancelScope | None = None,
    ) -> None:
        super().__init__(fn, parent=None, scope=scope)
        self._head: Coroutine[None, None, R] | None = None

    def init(self, *args: P.args, **kwargs: P.kwargs) -> Self:
        if self._parent is not None:
            raise RuntimeError("Cannot initialize a chained promise directly.")
        self._head = self._fn(*args, **kwargs)
        return self


__all__ = ["Promise"]
