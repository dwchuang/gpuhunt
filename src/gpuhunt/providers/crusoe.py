#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  2 13:28:09 2024

@author: dwchuang_mbp2
"""

import logging
import re
from typing import Optional, List
import requests
from bs4 import BeautifulSoup

from gpuhunt import QueryFilter, RawCatalogItem

logger = logging.getLogger(__name__)
CRUSOE_CLOUD_URL = "https://crusoe.ai/cloud/"

class CrusoeCloudProvider:
    """Provider class for interacting with Crusoe Cloud pricing."""
    
    NAME = "crusoe"
    
    def __init__(self):
        self.session = requests.Session()

    def get(self, query_filter: Optional[QueryFilter] = None, balance_resources: bool = True) -> List[RawCatalogItem]:
        """Get current GPU pricing from Crusoe Cloud.
        
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
            logger.error(f"Failed to fetch Crusoe Cloud pricing: {e}")
            return []

    def _fetch_page(self) -> str:
        """Fetch the Crusoe Cloud pricing page."""
        logger.debug("Fetching Crusoe Cloud pricing page")
        response = self.session.get(CRUSOE_CLOUD_URL)
        response.raise_for_status()
        return response.text

    def _parse_gpu_offerings(self, html_content: str) -> List[RawCatalogItem]:
        """Parse GPU offerings from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        items = []
        
        # Find the GPU pricing section
        gpu_section = soup.find(id="gpu-pricing")
        if not gpu_section:
            logger.warning("Could not find GPU pricing section")
            return items

        # Process each GPU row
        rows = gpu_section.find_all('tr')
        for row in rows[1:]:  # Skip header row
            cells = row.find_all('td')
            if not cells:
                continue

            # Parse GPU info from first cell
            info_cell = cells[0]
            gpu_info = info_cell.get_text(strip=True, separator=' ').split()
            
            # Extract memory size
            memory_text = info_cell.find(class_="bg-main-green/25")
            if not memory_text:
                continue
            memory_size = float(memory_text.get_text(strip=True).replace('GB', ''))
            
            # Get form factor (SXM, PCIe, OAM)
            form_factor_elem = info_cell.find(string=re.compile(r'(SXM|PCIE|OAM)'))
            form_factor = form_factor_elem.strip() if form_factor_elem else "Unknown"
            
            # Extract GPU name
            gpu_name = ' '.join([part for part in gpu_info 
                               if not part.endswith('GB') 
                               and part not in ['SXM', 'PCIE', 'OAM']])

            # Parse on-demand price
            price_text = cells[1].get_text(strip=True)
            if price_text == "Contact Us":
                continue
            
            price_match = re.search(r'\$?([\d.]+)', price_text)
            if not price_match:
                continue
                
            price = float(price_match.group(1))

            # Create RawCatalogItem
            items.append(RawCatalogItem(
                instance_name=f"{gpu_name}-{form_factor}",
                location="US",  # Default location
                price=price,
                cpu=None,  # CPU info not directly available
                memory=None,  # Memory info not directly available
                gpu_name=gpu_name,
                gpu_count=1,  # Default to 1 GPU per instance
                gpu_memory=memory_size,
                gpu_vendor="NVIDIA",  # All GPUs are NVIDIA
                spot=False,  # Crusoe doesn't offer spot instances
                disk_size=None  # Disk size info not directly available
            ))

        return items