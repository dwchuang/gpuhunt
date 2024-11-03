import functools
import importlib
import logging
from typing import Callable, TypeVar
from typing_extensions import Concatenate, ParamSpec
from gpuhunt._internal.catalog import Catalog

logger = logging.getLogger(__name__)

@functools.lru_cache
def default_catalog() -> Catalog:
    """
    Returns:
        the latest catalog with all available providers loaded
    """
    catalog = Catalog()
    catalog.load()
    
    # Define providers to load - including Oracle, CoreWeave, Crusoe, and Hyperstack
    providers = [
        ("gpuhunt.providers.oracle", "OracleCloudProvider"),
        ("gpuhunt.providers.coreweave", "CoreWeaveProvider"),
        ("gpuhunt.providers.crusoe", "CrusoeCloudProvider"),
        ("gpuhunt.providers.hyperstack", "HyperstackProvider"),
        ("gpuhunt.providers.tensordock", "TensorDockProvider"),
        ("gpuhunt.providers.vastai", "VastAIProvider"),
        ("gpuhunt.providers.cudo", "CudoProvider"),
    ]
    
    # Try to load each provider
    for module_path, provider_class in providers:
        try:
            module = importlib.import_module(module_path)
            provider = getattr(module, provider_class)()
            catalog.add_provider(provider)
        except ImportError:
            logger.warning(f"Failed to import provider {provider_class}")
        except Exception as e:
            logger.warning(f"Failed to initialize provider {provider_class}: {e}")

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
    """
    Query the `default_catalog`.
    See `Catalog.query` for more details on parameters

    Returns:
        (List[CatalogItem]): the result of the query
    """
    return default_catalog().query(*args, **kwargs)