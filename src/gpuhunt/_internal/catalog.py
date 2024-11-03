import csv
import dataclasses
import heapq
import io
import logging
import time
import urllib.request
import zipfile
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Optional, Union

import gpuhunt._internal.constraints as constraints
from gpuhunt._internal.models import AcceleratorVendor, CatalogItem, QueryFilter
from gpuhunt._internal.utils import parse_compute_capability
from gpuhunt.providers import AbstractProvider

logger = logging.getLogger(__name__)
version_url = "https://dstack-gpu-pricing.s3.eu-west-1.amazonaws.com/v1/version"
catalog_url = "https://dstack-gpu-pricing.s3.eu-west-1.amazonaws.com/v1/{version}/catalog.zip"

OFFLINE_PROVIDERS = ["aws", "azure", "datacrunch", "gcp", "lambdalabs", "oci", "runpod"]
# Add hyperstack to ONLINE_PROVIDERS since it's also scraped in real-time
ONLINE_PROVIDERS = [
    "cudo", 
    "tensordock", 
    "vastai", 
    "oracle", 
    "coreweave", 
    "crusoe",
    "hyperstack"
]
RELOAD_INTERVAL = 15 * 60  # 15 minutes


class Catalog:
    def __init__(self, balance_resources: bool = True, auto_reload: bool = True):
        """
        Args:
            balance_resources: increase min resources to better match the chosen GPU
            auto_reload: if `True`, the catalog will be automatically loaded from the S3 bucket every 4 hours
        """
        self.catalog = None
        self.loaded_at = None
        self.providers: list[AbstractProvider] = []
        self.balance_resources = balance_resources
        self.auto_reload = auto_reload
        self._online_only = False

    def query(
        self,
        *,
        provider: Optional[Union[str, list[str]]] = None,
        min_cpu: Optional[int] = None,
        max_cpu: Optional[int] = None,
        min_memory: Optional[float] = None,
        max_memory: Optional[float] = None,
        min_gpu_count: Optional[int] = None,
        max_gpu_count: Optional[int] = None,
        gpu_vendor: Optional[Union[AcceleratorVendor, str]] = None,
        gpu_name: Optional[Union[str, list[str]]] = None,
        min_gpu_memory: Optional[float] = None,
        max_gpu_memory: Optional[float] = None,
        min_total_gpu_memory: Optional[float] = None,
        max_total_gpu_memory: Optional[float] = None,
        min_disk_size: Optional[int] = None,
        max_disk_size: Optional[int] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        min_compute_capability: Optional[Union[str, tuple[int, int]]] = None,
        max_compute_capability: Optional[Union[str, tuple[int, int]]] = None,
        spot: Optional[bool] = None,
    ) -> list[CatalogItem]:
        """Query the catalog for matching offers."""
        # Check if we need to load or reload the catalog for offline providers
        if not self._online_only and self.auto_reload and (
            self.loaded_at is None or time.monotonic() - self.loaded_at > RELOAD_INTERVAL
        ):
            try:
                self.get_latest_version()
                self.load()
            except (AttributeError, urllib.error.URLError):
                self._online_only = True
                logger.debug("Running in online-only mode")

        query_filter = QueryFilter(
            provider=[provider] if isinstance(provider, str) else provider,
            min_cpu=min_cpu,
            max_cpu=max_cpu,
            min_memory=min_memory,
            max_memory=max_memory,
            min_gpu_count=min_gpu_count,
            max_gpu_count=max_gpu_count,
            gpu_vendor=AcceleratorVendor.cast(gpu_vendor) if gpu_vendor else None,
            gpu_name=[gpu_name] if isinstance(gpu_name, str) else gpu_name,
            min_gpu_memory=min_gpu_memory,
            max_gpu_memory=max_gpu_memory,
            min_total_gpu_memory=min_total_gpu_memory,
            max_total_gpu_memory=max_total_gpu_memory,
            min_disk_size=min_disk_size,
            max_disk_size=max_disk_size,
            min_price=min_price,
            max_price=max_price,
            min_compute_capability=parse_compute_capability(min_compute_capability),
            max_compute_capability=parse_compute_capability(max_compute_capability),
            spot=spot,
        )

        if query_filter.provider is not None:
            # validate providers
            valid_providers = set(p.lower() for p in OFFLINE_PROVIDERS + ONLINE_PROVIDERS)
            for p in query_filter.provider:
                if p.lower() not in valid_providers:
                    raise ValueError(f"Unknown provider: {p}")
        else:
            query_filter.provider = OFFLINE_PROVIDERS + list(
                set(p.NAME for p in self.providers if p.NAME in ONLINE_PROVIDERS)
            )

        # fetch providers
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []

            # Handle online providers including Hyperstack
            for provider_name in ONLINE_PROVIDERS:
                if provider_name in map(str.lower, query_filter.provider):
                    futures.append(
                        executor.submit(
                            self._get_online_provider_items,
                            provider_name,
                            query_filter,
                        )
                    )

            # Handle offline providers if we have catalog access
            if not self._online_only:
                for provider_name in OFFLINE_PROVIDERS:
                    if provider_name in map(str.lower, query_filter.provider):
                        futures.append(
                            executor.submit(
                                self._get_offline_provider_items,
                                provider_name,
                                query_filter,
                            )
                        )

            completed, _ = wait(futures)
            # Merge preserving provider-specific order but sorting by price
            items = list(heapq.merge(*[f.result() for f in completed], key=lambda i: i.price))
        return items

    def load(self, version: Optional[str] = None):
        """
        Fetch the catalog from the S3 bucket

        Args:
            version: specific version of the catalog to download. If not specified, the latest version will be used
        """
        if version is None:
            version = self.get_latest_version()
        logger.debug("Downloading catalog %s...", version)
        with urllib.request.urlopen(catalog_url.format(version=version)) as f:
            self.loaded_at = time.monotonic()
            self.catalog = io.BytesIO(f.read())

    @staticmethod
    def get_latest_version() -> str:
        """Get the latest version of the catalog from the S3 bucket."""
        with urllib.request.urlopen(version_url) as f:
            return f.read().decode("utf-8").strip()

    def add_provider(self, provider: AbstractProvider):
        """Add provider for querying offers."""
        self.providers.append(provider)

    def _get_offline_provider_items(
        self, provider_name: str, query_filter: QueryFilter
    ) -> list[CatalogItem]:
        """Get items from offline catalog."""
        logger.debug("Loading items for offline provider %s", provider_name)

        items = []

        if self.catalog is None:
            logger.warning("Catalog not loaded")
            return items

        with zipfile.ZipFile(self.catalog) as zip_file:
            with zip_file.open(f"{provider_name}.csv", "r") as csv_file:
                reader: Iterable[dict[str, str]] = csv.DictReader(
                    io.TextIOWrapper(csv_file, "utf-8")
                )
                for row in reader:
                    item = CatalogItem.from_dict(row, provider=provider_name)
                    if constraints.matches(item, query_filter):
                        items.append(item)
        return items

    def _get_online_provider_items(
        self, provider_name: str, query_filter: QueryFilter
    ) -> list[CatalogItem]:
        """Get items from online providers including Hyperstack."""
        logger.debug("Loading items for online provider %s", provider_name)
        items = []
        found = False
        for provider in self.providers:
            if provider.NAME != provider_name:
                continue
            found = True
            for i in provider.get(
                query_filter=query_filter, balance_resources=self.balance_resources
            ):
                item = CatalogItem(provider=provider_name, **dataclasses.asdict(i))
                if constraints.matches(item, query_filter):
                    items.append(item)
        if not found:
            raise ValueError(f"Provider is not loaded: {provider_name}")
        return items