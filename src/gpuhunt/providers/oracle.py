import logging
import re
from typing import Optional, List
from bs4 import BeautifulSoup
import requests

from gpuhunt import QueryFilter, RawCatalogItem
from gpuhunt.providers import AbstractProvider

logger = logging.getLogger(__name__)

ORACLE_CLOUD_URL = "https://www.oracle.com/cloud/compute/pricing/"

class OracleCloudProvider(AbstractProvider):
    """Provider class for interacting with Oracle Cloud pricing."""
    
    NAME = "oracle"
    
    def __init__(self):
        self.session = requests.Session()

    def get(self, query_filter: Optional[QueryFilter] = None, balance_resources: bool = True) -> List[RawCatalogItem]:
        """Get current GPU pricing from Oracle Cloud.
        
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
            logger.error(f"Failed to fetch Oracle Cloud pricing: {e}")
            return []

    def _fetch_page(self) -> str:
        """Fetch the Oracle Cloud pricing page."""
        logger.debug("Fetching Oracle Cloud pricing page")
        response = self.session.get(ORACLE_CLOUD_URL)
        response.raise_for_status()
        return response.text

    def _parse_gpu_offerings(self, html_content: str) -> List[RawCatalogItem]:
        """Parse GPU offerings from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        items = []
        
        # Find the GPU pricing section - look for the table with GPU instances
        gpu_section = soup.find('div', id='compute-gpu')
        if not gpu_section:
            logger.warning("Could not find GPU pricing section")
            return items

        # Process each GPU row
        rows = gpu_section.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if not cells or len(cells) < 9:  # We need at least shape, GPUs, memory, CPU info
                continue

            try:
                # Extract shape name and details
                shape_cell = cells[0].get_text(strip=True)
                if not shape_cell.startswith(('VM.GPU', 'BM.GPU')):
                    continue
                
                # Extract GPU info
                gpu_info = cells[1].get_text(strip=True)
                gpu_count = int(re.search(r'(\d+)x', gpu_info).group(1))
                gpu_name = re.search(r'NVIDIA\s+([A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)', gpu_info).group(1)
                
                # Extract memory
                gpu_memory = float(re.search(r'(\d+)\s*GB', cells[4].get_text(strip=True)).group(1))
                
                # Extract CPU cores and memory
                cpu_cores = int(cells[5].get_text(strip=True))
                cpu_memory = float(re.search(r'(\d+(?:,\d+)?)', cells[6].get_text(strip=True).replace(',', '')).group(1))
                
                # Extract pricing - this might need adjustment based on the actual HTML structure
                price_cell = cells[9].get_text(strip=True)
                if price_cell == "Contact Us":
                    continue
                
                price_match = re.search(r'\$?([\d.]+)', price_cell)
                if not price_match:
                    continue
                
                price = float(price_match.group(1))

                items.append(RawCatalogItem(
                    instance_name=shape_cell,
                    location="",  # Oracle has multiple regions, would need separate logic to specify
                    price=price,
                    cpu=cpu_cores,
                    memory=cpu_memory,
                    gpu_name=gpu_name,
                    gpu_count=gpu_count,
                    gpu_memory=gpu_memory,
                    gpu_vendor="NVIDIA",  # All GPUs listed are NVIDIA
                    spot=False,  # Default to on-demand pricing
                    disk_size=None  # Storage is configurable separately
                ))

            except (AttributeError, ValueError, IndexError) as e:
                logger.warning(f"Error parsing GPU row: {e}")
                continue

        return items