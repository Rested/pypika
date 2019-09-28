# coding: utf-8
from typing import Optional, Callable, Any, List, TypeVar, Union, Tuple, Dict, overload, Protocol
from mypy_extensions import VarArg, KwArg, Arg
# from typing_extensions import Protocol

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class QueryException(Exception):
    pass


class GroupingException(Exception):
    pass


class CaseException(Exception):
    pass


class JoinException(Exception):
    pass


class UnionException(Exception):
    pass


class RollupException(Exception):
    pass


class DialectNotSupported(Exception):
    pass

QueryLike = TypeVar("QueryLike")

class Combiner(Protocol[QueryLike]):
    def __call__(x, self: QueryLike, *args, **kwargs) -> Optional[QueryLike]: ...


class CombNonOpt(Protocol[QueryLike]):
    def __call__(x, self: QueryLike, *args: Any, **kwargs: Any) -> QueryLike: ...
class Combiner2(Protocol[QueryLike]):
    def __call__(x, self: QueryLike, __arg) -> Optional[QueryLike]: ...

# BuilderFuncSignature = Callable[[Arg(QueryLike, 'self'), VarArg(), KwArg()], Optional[QueryLike]]
# BuilderFuncNoArgSignature = Callable[[Arg(QueryLike, 'self')], Optional[QueryLike]]
# BuilderFuncSingleArgSignature = Callable[[Arg(QueryLike, 'self'), Arg(Any)], Optional[QueryLike]]
# BuilderFuncTwoArgSignature = Callable[[Arg(QueryLike, 'self'), Arg(Any), Arg(Any)], Optional[QueryLike]]
BuilderDecSignature = Callable[[Arg(QueryLike, 'self'), VarArg(), KwArg()], QueryLike]

def builder(func: Union[Combiner[QueryLike], Combiner2[QueryLike]]) -> Callable[..., QueryLike]:
    """
    Decorator for wrapper "builder" functions.  These are functions on the Query class or other classes used for
    building queries which mutate the query and return self.  To make the build functions immutable, this decorator is
    used which will deepcopy the current instance.  This decorator will return the return value of the inner function
    or the new copy of the instance.  The inner function does not need to return self.
    """
    import copy

    def _copy(self, *args: Any, **kwargs: Any) -> QueryLike:
        reveal_type(self)
        self_copy: QueryLike = copy.copy(self)
        reveal_type(self_copy)
        #
        result = func(self_copy, *args, **kwargs) # type: ignore
        reveal_type(result)
        # Return self if the inner function returns None.  This way the inner function can return something
        # different (for example when creating joins, a different builder is returned).
        if result is None:
            return self_copy

        return result

    return _copy


def ignore_copy(func: Callable[..., Any]) -> Callable[[Any, str], Any]:
    """
    Decorator for wrapping the __getattr__ function for classes that are copied via deepcopy.  This prevents infinite
    recursion caused by deepcopy looking for magic functions in the class. Any class implementing __getattr__ that is
    meant to be deepcopy'd should use this decorator.

    deepcopy is used by pypika in builder functions (decorated by @builder) to make the results immutable.  Any data
    model type class (stored in the Query instance) is copied.
    """

    def _getattr(self, name: str) -> Any:
        if name in ['__copy__', '__deepcopy__', '__getstate__', '__setstate__', '__getnewargs__']:
            raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name__, name))

        return func(self, name)

    return _getattr


def resolve_is_aggregate(values: List[Optional[bool]]) -> Optional[bool]:
    """
    Resolves the is_aggregate flag for an expression that contains multiple terms.  This works like a voter system,
    each term votes True or False or abstains with None.

    :param values: A list of booleans (or None) for each term in the expression
    :return: If all values are True or None, True is returned.  If all values are None, None is returned. Otherwise,
        False is returned.
    """
    result = [x
              for x in values
              if x is not None]
    if result:
        return all(result)
    return None


def format_quotes(value: str, quote_char: Optional[str]) -> str:
    return '{quote}{value}{quote}' \
        .format(value=value, quote=quote_char or '')


def format_alias_sql(sql: str, alias: Optional[str], quote_char: Optional[str]=None, alias_quote_char: Optional[str]=None, **kwargs) -> str:
    if alias is None:
        return sql
    return '{sql} {alias}'.format(sql=sql, alias=format_quotes(alias, alias_quote_char or quote_char))


def validate(*args: Any, exc: Optional[Exception]=None, type: Optional[type]=None) -> None:
    if type is not None:
        for arg in args:
            if not isinstance(arg, type):
                if exc is None:
                    exc = TypeError("Argument received not matching specified type {specified_type}".format(specified_type=type))
                raise exc
