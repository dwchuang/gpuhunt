import logging
import re
import requests
from bs4 import BeautifulSoup
from typing import Optional

from gpuhunt._internal.models import QueryFilter, RawCatalogItem
from gpuhunt.providers import AbstractProvider

logger = logging.getLogger(__name__)

class CrusoeProvider(AbstractProvider):
    """
    CrusoeProvider scrapes GPU pricing information from Crusoe Cloud website.
    Required dependencies: requests, beautifulsoup4
    """

    NAME = "crusoe"
    BASE_URL = "https://crusoe.ai/cloud/"

    def __init__(self):
        pass

    def _parse_gpu_memory(self, memory_str: str) -> float:
        """Extract GPU memory size in GB from string like '80GB'"""
        match = re.match(r'(\d+)GB', memory_str.strip())
        if match:
            return float(match.group(1))
        return 0.0

    def _parse_price(self, price_str: str) -> float:
        """Extract price from string like '$ 4.29'"""
        if 'Contact Us' in price_str:
            return 0.0
        match = re.search(r'\$\s*(\d+\.\d+)', price_str)
        if match:
            return float(match.group(1))
        return 0.0

    def _extract_cpu_count(self, gpu_name: str) -> int:
        """
        Map GPU types to their CPU counts.
        This is an approximation based on common configurations.
        """
        cpu_mapping = {
            'H200': 96,
            'H100': 96,
            'MI300x': 96,
            'A100': 48,
            'L40S': 48,
            'A40': 24
        }
        return cpu_mapping.get(gpu_name, 8)  # default to 8 if unknown

    def _extract_memory(self, gpu_name: str) -> float:
        """
        Map GPU types to their system memory in GB.
        This is an approximation based on common configurations.
        """
        memory_mapping = {
            'H200': 2048,
            'H100': 2048,
            'MI300x': 2048,
            'A100': 1024,
            'L40S': 512,
            'A40': 256
        }
        return float(memory_mapping.get(gpu_name, 128))  # default to 128 if unknown

    def get(
        self, query_filter: Optional[QueryFilter] = None, balance_resources: bool = True
    ) -> list[RawCatalogItem]:
        """
        Fetch and parse GPU instances from Crusoe Cloud website.
        
        Args:
            query_filter: Optional filtering criteria
            balance_resources: Whether to balance resources (not used in this provider)
            
        Returns:
            List of RawCatalogItem objects containing instance information
        """
        try:
            response = requests.get(self.BASE_URL, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            offers = []
            
            # Find the GPU pricing section
            gpu_section = soup.find('section', {'id': 'gpu-pricing'})
            if not gpu_section:
                logger.error("Could not find GPU pricing section")
                return []

            # Parse pricing table rows
            pricing_rows = gpu_section.find_all('tr')[1:]  # Skip header row
            
            for row in pricing_rows:
                cells = row.find_all(['td'])
                if not cells or len(cells) < 5:
                    continue

                # Extract GPU info
                memory_div = cells[0].find('div', class_='bg-main-green/25')
                if not memory_div:
                    continue

                gpu_name = cells[0].find('div', class_='w-[4ch]').get_text(strip=True)
                memory = memory_div.get_text(strip=True)
                gpu_memory = self._parse_gpu_memory(memory)

                # Extract prices for different commitment levels
                on_demand_price = self._parse_price(cells[1].get_text())
                if on_demand_price == 0:  # Skip if price is "Contact Us"
                    continue

                # Create instance for on-demand pricing
                instance = RawCatalogItem(
                    instance_name=f"crusoe-{gpu_name.lower()}",
                    location="us-central",  # Default location
                    price=on_demand_price,
                    cpu=self._extract_cpu_count(gpu_name),
                    memory=self._extract_memory(gpu_name),
                    gpu_name=gpu_name,
                    gpu_count=1,
                    gpu_memory=gpu_memory,
                    spot=False,
                    gpu_vendor="NVIDIA" if gpu_name != "MI300x" else "AMD",
                    disk_size=None  # Crusoe doesn't specify default disk sizes
                )
                offers.append(instance)

                # Add reserved instance options
                reserved_prices = [
                    (cells[2], "6month"),
                    (cells[3], "1year"),
                    (cells[4], "3year")
                ]

                for price_cell, term in reserved_prices:
                    price = self._parse_price(price_cell.get_text())
                    if price > 0:
                        reserved_instance = RawCatalogItem(
                            instance_name=f"crusoe-{gpu_name.lower()}-reserved-{term}",
                            location="us-central",
                            price=price,
                            cpu=self._extract_cpu_count(gpu_name),
                            memory=self._extract_memory(gpu_name),
                            gpu_name=gpu_name,
                            gpu_count=1,
                            gpu_memory=gpu_memory,
                            spot=False,
                            gpu_vendor="NVIDIA" if gpu_name != "MI300x" else "AMD",
                            disk_size=None
                        )
                        offers.append(reserved_instance)

            return sorted(offers, key=lambda x: x.price)

        except requests.RequestException as e:
            logger.error(f"Error fetching data from {self.BASE_URL}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error parsing GPU instances: {e}")
            return []

    @classmethod
    def filter(cls, offers: list[RawCatalogItem]) -> list[RawCatalogItem]:
        """Filter offers based on GPU types"""
        return [
            i for i in offers
            if any(gpu in i.instance_name for gpu in [
                'h200', 'h100', 'a100', 'l40s', 'a40', 'mi300x'
            ])
        ]