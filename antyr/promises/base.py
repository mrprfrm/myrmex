import inspect
from typing import (
    Any,
    Callable,
    Coroutine,
    Generator,
    Generic,
    List,
    Optional,
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
        parent: Optional["PromiseNode[V, U]"] = None,
    ) -> None:
        self._fn = fn
        self.__parent: Optional["PromiseNode[Any, Any]"] = parent

    def __copy(self, *, parent: Optional["PromiseNode[V, U]"]) -> "PromiseNode[P, R]":
        return PromiseNode(self._fn, parent=parent)

    def __await__(self) -> Generator[None, None, R]:
        current = self
        promises: List[PromiseNode[P, R]] = []
        while current is not None:
            promises.append(current)
            current = current.__parent

        async def execute() -> R:
            result = None
            async with trio.open_nursery():
                while promises:
                    handler = promises.pop()
                    signature = inspect.signature(handler._fn)
                    if result is None:
                        bound = signature.bind()
                    else:
                        bound = signature.bind(result)
                    result = await handler._fn(*bound.args, **bound.kwargs)
            return cast(R, result)

        return execute().__await__()

    def then(
        self, nxt: Callable[[R], Coroutine[None, None, K]] | "PromiseNode[[R], K]"
    ) -> "PromiseNode[[R], K]":
        if self.__parent is not None:
            raise RuntimeError("Cannot chain a root promise that is not initialized.")

        if isinstance(nxt, PromiseNode):
            return nxt.__copy(parent=self)
        nxt = PromiseNode[[R], K](nxt, parent=self)
        return nxt


class Promise(PromiseNode[P, R]):
    def __init__(self, fn: Callable[P, Coroutine[None, None, R]]) -> None:
        super().__init__(fn, parent=None)
        self._head: Optional[Coroutine[None, None, R]] = None

    def init(self, *args: P.args, **kwargs: P.kwargs) -> Self:
        if self.__parent is not None:
            raise RuntimeError("Cannot initialize a chained promise directly.")
        self._head = self._fn(*args, **kwargs)
        return self
