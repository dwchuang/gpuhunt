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
        """Get current GPU pricing from Oracle Cloud."""
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

    def _parse_storage_size(self, storage_text: str) -> Optional[float]:
        """Parse storage size from text and convert to GB."""
        if storage_text == "Block storage":
            return None
            
        # Match patterns like "2x 3.84TB NVMe" or "4x 6.8TB NVMe"
        match = re.search(r'(\d+)x\s*([\d.]+)TB', storage_text)
        if match:
            count = int(match.group(1))
            size = float(match.group(2))
            return count * size * 1024  # Convert TB to GB
        return None

    def _parse_gpu_info(self, gpu_text: str) -> tuple[int, str, str]:
        """Parse GPU count, name and vendor from GPU text."""
        count_match = re.search(r'(\d+)x\s+(\w+)\s+([A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)*)', gpu_text)
        if not count_match:
            raise ValueError(f"Could not parse GPU info from: {gpu_text}")
            
        count = int(count_match.group(1))
        vendor = count_match.group(2)
        model = count_match.group(3)
        
        return count, model, vendor

    def _parse_gpu_offerings(self, html_content: str) -> List[RawCatalogItem]:
        """Parse GPU offerings from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        items = []
        
        # Find table with GPU offerings
        table = soup.find('table', attrs={'aria-labelledby': 'compute-gpu'})
        if not table:
            logger.warning("Could not find GPU pricing table")
            return items

        # Process each row
        rows = table.find('tbody').find_all('tr')
        for row in rows:
            # Skip section header rows
            if row.find('th', class_='bctxt'):
                continue

            try:
                cells = row.find_all(['th', 'td'])
                if len(cells) < 10:
                    continue

                # Extract instance shape
                shape = cells[0].find('div').get_text(strip=True)
                
                # Parse GPU information
                gpu_text = cells[1].find('div').get_text(strip=True)
                gpu_count, gpu_name, gpu_vendor = self._parse_gpu_info(gpu_text)
                
                # Extract other specifications
                gpu_memory = float(re.search(r'(\d+)\s*GB', cells[4].find('div').get_text(strip=True)).group(1))
                cpu_cores = int(cells[5].find('div').get_text(strip=True))
                cpu_memory = float(cells[6].find('div').get_text(strip=True).replace(',', ''))
                
                # Parse storage
                storage_text = cells[7].find('div').get_text(strip=True)
                disk_size = self._parse_storage_size(storage_text)
                
                # Extract price
                price_elem = cells[9].find('div')
                if not price_elem:
                    continue
                    
                price_text = price_elem.get_text(strip=True)
                price = float(price_text.replace('$', ''))

                items.append(RawCatalogItem(
                    instance_name=shape,
                    location="",  # Oracle has multiple regions
                    price=price,
                    cpu=cpu_cores,
                    memory=cpu_memory,
                    gpu_name=gpu_name,
                    gpu_count=gpu_count,
                    gpu_memory=gpu_memory,
                    gpu_vendor=gpu_vendor,
                    spot=False,
                    disk_size=disk_size
                ))

            except (AttributeError, ValueError, IndexError) as e:
                logger.warning(f"Error parsing GPU row: {str(e)}")
                continue

        return items