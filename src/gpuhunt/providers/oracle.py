#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import re
from typing import Optional, List
from bs4 import BeautifulSoup
import requests
import os
from datetime import datetime

from gpuhunt import QueryFilter, RawCatalogItem
from gpuhunt.providers import AbstractProvider

logger = logging.getLogger(__name__)

ORACLE_CLOUD_URL = "https://www.oracle.com/cloud/compute/pricing/"
SNAPSHOT_DIR = "/Users/dwchuang_mbp2/Downloads/gpuhunt-main"

class OracleCloudProvider(AbstractProvider):
    """Provider class for interacting with Oracle Cloud pricing."""
    
    NAME = "oracle"
    
    def __init__(self):
        self.session = requests.Session()

    def get(self, query_filter: Optional[QueryFilter] = None, balance_resources: bool = True) -> List[RawCatalogItem]:
        """Get current GPU pricing from Oracle Cloud."""
        try:
            # Try to read from existing snapshot first
            snapshot_files = [f for f in os.listdir(SNAPSHOT_DIR) if f.endswith('_rendered.html')]
            if snapshot_files:
                latest_snapshot = sorted(snapshot_files)[-1]
                with open(os.path.join(SNAPSHOT_DIR, latest_snapshot), 'r', encoding='utf-8') as f:
                    html_content = f.read()
                logger.info(f"Using existing snapshot: {latest_snapshot}")
                return self._parse_gpu_offerings(html_content)
            else:
                logger.warning("No snapshot file found")
                return []
        except Exception as e:
            logger.error(f"Failed to fetch Oracle Cloud pricing: {e}")
            return []

    def _parse_memory_size(self, memory_text: str) -> float:
        """Parse memory size from text with GB suffix."""
        try:
            # Extract numeric value before 'GB'
            match = re.search(r'([\d,]+)\s*GB', memory_text)
            if match:
                # Remove commas and convert to float
                return float(match.group(1).replace(',', ''))
            raise ValueError(f"Could not parse memory size from: {memory_text}")
        except (ValueError, AttributeError) as e:
            logger.warning(f"Error parsing memory size: {e}")
            raise

    def _parse_storage_size(self, storage_text: str) -> Optional[float]:
        """Parse storage size from text and convert to GB."""
        if storage_text == "Block storage":
            return None
            
        try:
            match = re.search(r'(\d+)x\s*([\d.]+)TB', storage_text)
            if match:
                count = int(match.group(1))
                size = float(match.group(2))
                return count * size * 1024  # Convert TB to GB
            return None
        except (ValueError, AttributeError) as e:
            logger.warning(f"Error parsing storage size: {e}")
            return None

    def _parse_gpu_offerings(self, html_content: str) -> List[RawCatalogItem]:
        """Parse GPU offerings from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        items = []

        try:
            gpu_table = soup.find('table', attrs={'aria-labelledby': 'compute-gpu'})
            if not gpu_table:
                logger.error("Could not find GPU pricing table")
                return items

            for row in gpu_table.find_all('tr'):
                if (row.find('th', class_='bctxt') or 
                    row.find_parent('thead') or 
                    len(row.find_all(['th', 'td'])) < 10):
                    continue

                try:
                    cells = row.find_all(['th', 'td'])
                    
                    # Get instance shape
                    shape = cells[0].get_text(strip=True)
                    
                    # Parse GPU info
                    gpu_text = cells[1].get_text(strip=True)
                    gpu_match = re.search(r'(\d+)x\s+(NVIDIA|AMD)\s+([A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)', gpu_text)
                    if not gpu_match:
                        continue
                        
                    gpu_count = int(gpu_match.group(1))
                    gpu_vendor = gpu_match.group(2)
                    gpu_name = gpu_match.group(3).split(' Tensor Core')[0].split(' Matrix Core')[0]
                    
                    # Get GPU memory (e.g., "640 GB")
                    gpu_memory = self._parse_memory_size(cells[4].get_text(strip=True))
                    
                    # Get CPU cores and system memory
                    cpu_cores = int(cells[5].get_text(strip=True))
                    memory = self._parse_memory_size(cells[6].get_text(strip=True))
                    
                    # Get storage if available
                    storage_text = cells[7].get_text(strip=True)
                    disk_size = self._parse_storage_size(storage_text)
                    
                    # Get price
                    price_text = cells[9].get_text(strip=True)
                    price = float(price_text.replace('$', ''))

                    items.append(RawCatalogItem(
                        instance_name=shape,
                        location="",  # Oracle has multiple regions
                        price=price,
                        cpu=cpu_cores,
                        memory=memory,
                        gpu_name=gpu_name,
                        gpu_count=gpu_count,
                        gpu_memory=gpu_memory,
                        gpu_vendor=gpu_vendor,
                        spot=False,
                        disk_size=disk_size
                    ))

                    logger.debug(f"Successfully parsed instance: {shape}")

                except (AttributeError, ValueError, IndexError) as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error parsing GPU offerings: {str(e)}")
            return []

        logger.info(f"Successfully parsed {len(items)} GPU instances")
        return items
