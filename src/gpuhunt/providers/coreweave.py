#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  2 22:57:43 2024

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

COREWEAVE_CLOUD_URL = "https://www.coreweave.com/gpu-cloud-pricing"

class CoreWeaveProvider(AbstractProvider):
    """Provider class for interacting with CoreWeave pricing."""
    
    NAME = "coreweave"
    
    def __init__(self):
        self.session = requests.Session()

    def get(self, query_filter: Optional[QueryFilter] = None, balance_resources: bool = True) -> List[RawCatalogItem]:
        """Get current GPU pricing from CoreWeave.
        
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
            logger.error(f"Failed to fetch CoreWeave pricing: {e}")
            return []

    def _fetch_page(self) -> str:
        """Fetch the CoreWeave pricing page."""
        logger.debug("Fetching CoreWeave pricing page")
        response = self.session.get(COREWEAVE_CLOUD_URL)
        response.raise_for_status()
        return response.text

    def _parse_gpu_offerings(self, html_content: str) -> List[RawCatalogItem]:
        """Parse GPU offerings from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        items = []

        # Find the GPU pricing table section
        gpu_section = soup.find('div', {'class': 'table-body'})
        if not gpu_section:
            logger.warning("Could not find GPU pricing section")
            return items

        # Process each GPU row
        rows = gpu_section.find_all('div', {'class': 'table-body-row'})
        for row in rows:
            try:
                # Extract GPU name
                gpu_name_elem = row.find('div', {'class': 'table-body-left'})
                if not gpu_name_elem:
                    continue
                gpu_name = gpu_name_elem.get_text(strip=True)

                # Extract memory size
                memory_cell = row.find_all('div', {'class': 'w-col-2'})[0]
                if not memory_cell:
                    continue
                memory_text = memory_cell.get_text(strip=True)
                memory_size = float(re.search(r'(\d+)', memory_text).group(1))

                # Extract CPU cores
                cpu_cell = row.find_all('div', {'class': 'w-col-2'})[1]
                if not cpu_cell:
                    continue
                cpu_cores = int(re.search(r'(\d+)', cpu_cell.get_text(strip=True)).group(1))

                # Extract RAM
                ram_cell = row.find_all('div', {'class': 'w-col-2'})[2]
                if not ram_cell:
                    continue
                ram_size = float(re.search(r'(\d+)', ram_cell.get_text(strip=True)).group(1))

                # Extract pricing
                price_cell = row.find_all('div', {'class': 'w-col-2'})[-1]
                if not price_cell:
                    continue
                
                price_text = price_cell.get_text(strip=True)
                if price_text == "Contact Us":
                    continue
                    
                price_match = re.search(r'\$?([\d.]+)', price_text)
                if not price_match:
                    continue
                    
                price = float(price_match.group(1))

                items.append(RawCatalogItem(
                    instance_name=gpu_name,
                    location="US",  # CoreWeave locations
                    price=price,
                    cpu=cpu_cores,
                    memory=ram_size,
                    gpu_name=gpu_name.split()[0],  # First word is usually GPU model
                    gpu_count=1,  # Per GPU pricing
                    gpu_memory=memory_size,
                    gpu_vendor="NVIDIA",  # CoreWeave uses NVIDIA GPUs
                    spot=False,  # On-demand pricing
                    disk_size=None  # Storage priced separately
                ))

            except (AttributeError, ValueError, IndexError) as e:
                logger.warning(f"Error parsing GPU row: {e}")
                continue

        return items

    def _extract_gpu_specs(self, gpu_name: str) -> dict:
        """Extract detailed GPU specifications from modal data if available."""
        specs = {
            "H100": {
                "memory": 80,
                "form_factor": "SXM",
            },
            "A100": {
                "memory": 80,
                "form_factor": "SXM",
            },
            "A40": {
                "memory": 48,
                "form_factor": "PCIe",
            },
            "A6000": {
                "memory": 48,
                "form_factor": "PCIe",
            },
        }
        
        for model, info in specs.items():
            if model in gpu_name:
                return info
                
        return {"memory": None, "form_factor": None}