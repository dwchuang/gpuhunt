#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 14:33:28 2024

@author: dwchuang_mbp2
"""

import logging
import re
from typing import Optional, List
import requests
from bs4 import BeautifulSoup

from gpuhunt import QueryFilter, RawCatalogItem
from gpuhunt.providers import AbstractProvider

logger = logging.getLogger(__name__)

HYPERSTACK_CLOUD_URL = "https://www.hyperstack.cloud/gpu-pricing"

class HyperstackProvider(AbstractProvider):
    """Provider class for interacting with Hyperstack pricing."""
    
    NAME = "hyperstack"
    
    def __init__(self):
        self.session = requests.Session()

    def get(self, query_filter: Optional[QueryFilter] = None, balance_resources: bool = True) -> List[RawCatalogItem]:
        """Get current GPU pricing from Hyperstack.
        
        Args:
            query_filter: Optional filter criteria
            balance_resources: Whether to balance resources for GPU configurations
            
        Returns:
            List[RawCatalogItem]: List of GPU configurations and their pricing
        """
        try:
            html_content = self._fetch_page()
            return self._parse_gpu_offerings(html_content)
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Hyperstack pricing: {e}")
            return []

    def _fetch_page(self) -> str:
        """Fetch the Hyperstack pricing page."""
        logger.debug("Fetching Hyperstack pricing page")
        response = self.session.get(HYPERSTACK_CLOUD_URL)
        response.raise_for_status()
        return response.text

    def _parse_gpu_offerings(self, html_content: str) -> List[RawCatalogItem]:
        """Parse GPU offerings from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        items = []

        # Find the pricing table
        price_table = soup.find('table', {'class': 'sort-test-jquery'})
        if not price_table:
            logger.warning("Could not find GPU pricing table")
            return items

        # Process each row in the table body
        rows = price_table.find('tbody').find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if not cells or len(cells) < 5:
                continue

            try:
                # Parse GPU model and specs
                gpu_name = cells[0].get_text(strip=True)
                vram = float(cells[1].get_text(strip=True))
                max_cpus = int(cells[2].get_text(strip=True))
                max_ram = float(cells[3].get_text(strip=True))
                
                # Parse price
                price_text = cells[4].get_text(strip=True)
                price_match = re.search(r'\$\s*([\d.]+)', price_text)
                if not price_match:
                    continue
                price = float(price_match.group(1))

                # Extract form factor if present in GPU name
                form_factor = "Unknown"
                if "SXM" in gpu_name:
                    form_factor = "SXM"
                elif "PCIe" in gpu_name:
                    form_factor = "PCIe"
                
                # Create RawCatalogItem
                instance = RawCatalogItem(
                    instance_name=gpu_name,
                    location="EU",  # Hyperstack has DCs in Europe
                    price=price,
                    cpu=max_cpus,
                    memory=max_ram,
                    gpu_name=gpu_name.split()[1],  # Get the GPU model number (H100, A100, etc)
                    gpu_count=1,  # Per GPU pricing
                    gpu_memory=vram,
                    gpu_vendor="NVIDIA",  # All GPUs are NVIDIA
                    spot=False,  # On-demand pricing
                    disk_size=None  # Storage priced separately
                )
                items.append(instance)

            except (ValueError, AttributeError, IndexError) as e:
                logger.warning(f"Failed to parse GPU row: {e}")
                continue

        return items

    def _normalize_price(self, price_str: str) -> Optional[float]:
        """Helper method to normalize price strings to float values."""
        try:
            # Remove currency symbol and 'per Hour' text, convert to float
            price_text = re.search(r'\$\s*([\d.]+)', price_str)
            if price_text:
                return float(price_text.group(1))
            return None
        except (ValueError, AttributeError):
            return None

    def _extract_gpu_specs(self, gpu_name: str) -> dict:
        """Extract detailed GPU specifications from name if available."""
        specs = {
            "H100": {
                "arch": "Hopper",
                "memory": 80,
            },
            "A100": {
                "arch": "Ampere", 
                "memory": 80,
            },
            "L40": {
                "arch": "Ada Lovelace",
                "memory": 48,
            },
            "A6000": {
                "arch": "Ampere",
                "memory": 48,
            },
            "A5000": {
                "arch": "Ampere",
                "memory": 24,
            },
            "A4000": {
                "arch": "Ampere",
                "memory": 16,
            },
        }
        
        for model, info in specs.items():
            if model in gpu_name:
                return info
                
        return {"arch": None, "memory": None}