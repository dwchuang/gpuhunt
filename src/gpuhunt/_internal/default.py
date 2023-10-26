import functools
import importlib
import logging
from typing import Callable, TypeVar

from typing_extensions import Concatenate, ParamSpec

from gpuhunt._internal.catalog import Catalog

logger = logging.getLogger(__name__)


@functools.lru_cache()
def default_catalog() -> Catalog:
    # TODO refresh every 4 hours
    catalog = Catalog()
    catalog.load()
    for module, provider in [("gpuhunt.providers.tensordock", "TensorDockProvider")]:
        try:
            module = importlib.import_module(module)
            provider = getattr(module, provider)()
            catalog.add_provider(provider)
        except ImportError:
            logger.warning("Failed to import provider %s", provider)
    return catalog


P = ParamSpec("P")
R = TypeVar("R")
Method = Callable[P, R]
CatalogMethod = Callable[Concatenate[Catalog, P], R]


def with_signature(method: CatalogMethod) -> Callable[[Method], Method]:
    """
    Returns:
        decorator to add the signature of the Catalog method to the decorated method
    """

    def decorator(func: Method) -> Method:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return func(*args, **kwargs)

        return wrapper

    return decorator


@with_signature(Catalog.query)
def query(*args: P.args, **kwargs: P.kwargs) -> R:
    return default_catalog().query(*args, **kwargs)
