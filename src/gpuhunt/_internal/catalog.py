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
ONLINE_PROVIDERS = ["cudo", "tensordock", "vastai"]

# Add Crusoe's provider name constants
PROVIDER_CRUSOE = "crusoe"
ONLINE_PROVIDERS.append(PROVIDER_CRUSOE)

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
        if self.auto_reload and (
            self.loaded_at is None or time.monotonic() - self.loaded_at > RELOAD_INTERVAL
        ):
            self.load()

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

            for provider_name in ONLINE_PROVIDERS:
                if provider_name in map(str.lower, query_filter.provider):
                    futures.append(
                        executor.submit(
                            self._get_online_provider_items,
                            provider_name,
                            query_filter,
                        )
                    )

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
            items = list(heapq.merge(*[f.result() for f in completed], key=lambda i: i.price))
        return items

    # ... rest of the Catalog class implementation remains the same ...