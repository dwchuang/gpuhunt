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
        
        # Find the GPU pricing section - now uses h4 with id='compute-gpu'
        gpu_section = None
        for h4 in soup.find_all('h4'):
            if h4.get('id') == 'compute-gpu':
                gpu_section = h4.find_parent('div').find_parent('div')
                break
                
        if not gpu_section:
            logger.warning("Could not find GPU pricing section")
            return items

        # Get the table that follows the GPU section header
        table = gpu_section.find('table')
        if not table:
            logger.warning("Could not find GPU pricing table")
            return items

        # Process each GPU row, skipping header and section rows
        rows = table.find_all('tr')
        for row in rows:
            # Skip header or section title rows
            if row.find('th', class_='bctxt rw-neutral-20bg') or row.find('th', scope='col'):
                continue
                
            cells = row.find_all(['th', 'td'])
            if not cells or len(cells) < 10:  # Need all columns including price
                continue

            try:
                # Extract shape name
                shape_cell = cells[0].get_text(strip=True)
                if not shape_cell.startswith(('VM.GPU', 'BM.GPU')):
                    continue
                
                # Extract GPU info
                gpu_info = cells[1].get_text(strip=True)
                gpu_count = int(re.search(r'(\d+)x', gpu_info).group(1))
                gpu_model_match = re.search(r'NVIDIA\s+([A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)', gpu_info)
                if not gpu_model_match:
                    continue
                gpu_name = gpu_model_match.group(1)
                
                # Extract memory from dedicated GPU memory column
                gpu_memory_text = cells[4].get_text(strip=True)
                gpu_memory = float(re.search(r'(\d+)\s*GB', gpu_memory_text).group(1))
                
                # Extract CPU cores and memory
                cpu_cores = int(cells[5].get_text(strip=True))
                cpu_memory_text = cells[6].get_text(strip=True)
                cpu_memory = float(re.search(r'(\d+(?:,\d+)?)', cpu_memory_text.replace(',', '')).group(1))
                
                # Extract pricing from last column
                price_cell = cells[9].get_text(strip=True)
                if price_cell == "Contact Us" or not price_cell:
                    continue
                
                price_match = re.search(r'\$?([\d.]+)', price_cell)
                if not price_match:
                    continue
                
                price = float(price_match.group(1))

                # Get storage info if available
                storage_text = cells[7].get_text(strip=True)
                disk_size = None
                if storage_text != "Block storage":
                    storage_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:TB|GB)', storage_text)
                    if storage_match:
                        size = float(storage_match.group(1))
                        # Convert TB to GB if needed
                        if 'TB' in storage_text:
                            size *= 1024
                        disk_size = size

                items.append(RawCatalogItem(
                    instance_name=shape_cell,
                    location="",  # Oracle has multiple regions
                    price=price,
                    cpu=cpu_cores,
                    memory=cpu_memory,
                    gpu_name=gpu_name,
                    gpu_count=gpu_count,
                    gpu_memory=gpu_memory,
                    gpu_vendor="NVIDIA",
                    spot=False,
                    disk_size=disk_size
                ))

            except (AttributeError, ValueError, IndexError) as e:
                logger.warning(f"Error parsing GPU row: {e}")
                continue

        return items