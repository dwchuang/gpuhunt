#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  2 13:28:09 2024

@author: dwchuang_mbp2
"""

import re
import logging
from typing import TypedDict, List, Optional
from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Constants
CRUSOE_CLOUD_URL = "https://crusoe.ai/cloud/"

@dataclass
class GPUPreset:
    gpu_name: str
    memory_size: int  # GB
    form_factor: str  # SXM, PCIe, OAM
    prices: dict[str, float]  # pricing tier -> price/hour

class CrusoeCloudProvider:
    """Provider class for interacting with Crusoe Cloud pricing."""
    
    NAME = "crusoe"
    
    def __init__(self):
        self.session = requests.Session()
    
    def fetch_page(self) -> str:
        """Fetch the Crusoe Cloud pricing page.
        
        Returns:
            str: HTML content of the pricing page
            
        Raises:
            requests.RequestException: If the page cannot be fetched
        """
        logger.debug("Fetching Crusoe Cloud pricing page")
        response = self.session.get(CRUSOE_CLOUD_URL)
        response.raise_for_status()
        return response.text
    
    def get_gpu_pricing(self) -> List[GPUPreset]:
        """Get current GPU pricing from Crusoe Cloud.
        
        Returns:
            List[GPUPreset]: List of GPU configurations and their pricing
        """
        try:
            html_content = self.fetch_page()
            return self.parse_gpu_platforms(html_content)
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Crusoe Cloud pricing: {e}")
            return []

    @staticmethod
    def parse_gpu_platforms(html_content: str) -> List[GPUPreset]:
        """Parse GPU pricing information from Crusoe Cloud webpage.
        
        Args:
            html_content (str): Raw HTML content of the pricing page
            
        Returns:
            List[GPUPreset]: List of GPU configurations and their pricing
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the GPU pricing section
        gpu_section = soup.find(id="gpu-pricing")
        if not gpu_section:
            logger.warning("Could not find GPU pricing section")
            return []

        platforms = []
        
        # Process each GPU row
        rows = gpu_section.find_all('tr')
        for row in rows[1:]:  # Skip header row
            cells = row.find_all('td')
            if not cells:
                continue
                
            # Parse GPU info from first cell
            info_cell = cells[0]
            gpu_info = info_cell.get_text(strip=True, separator=' ').split()
            
            # Extract memory size and form factor
            memory_text = info_cell.find(class_="bg-main-green/25")
            if not memory_text:
                continue
            memory_size = int(memory_text.get_text(strip=True).replace('GB', ''))
            
            # Get form factor (SXM, PCIe, OAM)
            form_factor_elem = info_cell.find(string=re.compile(r'(SXM|PCIE|OAM)'))
            form_factor = form_factor_elem.strip() if form_factor_elem else "Unknown"
            
            # Extract GPU name
            gpu_name = ' '.join([part for part in gpu_info if not part.endswith('GB') and part not in ['SXM', 'PCIE', 'OAM']])
            
            # Parse pricing tiers
            prices = {}
            price_types = ['On-Demand', '6-month reserved', '1-year reserved', '3-year reserved']
            
            for i, price_type in enumerate(price_types, 1):
                if i < len(cells):
                    price_text = cells[i].get_text(strip=True)
                    if price_text == "Contact Us":
                        prices[price_type] = None
                    else:
                        # Extract numeric price value
                        price_match = re.search(r'\$?([\d.]+)', price_text)
                        if price_match:
                            prices[price_type] = float(price_match.group(1))
            
            platforms.append(GPUPreset(
                gpu_name=gpu_name,
                memory_size=memory_size,
                form_factor=form_factor,
                prices=prices
            ))
        
        return platforms

    @staticmethod
    def format_gpu_pricing(platforms: List[GPUPreset]) -> str:
        """Format GPU pricing information into a readable string.
        
        Args:
            platforms (List[GPUPreset]): List of GPU configurations
            
        Returns:
            str: Formatted pricing information
        """
        output = []
        output.append(f"Crusoe Cloud GPU Pricing ({CRUSOE_CLOUD_URL})")
        output.append("-" * 50)
        
        for p in platforms:
            output.append(f"{p.gpu_name} ({p.memory_size}GB {p.form_factor})")
            for price_type, price in p.prices.items():
                if price is None:
                    output.append(f"  {price_type}: Contact Us")
                else:
                    output.append(f"  {price_type}: ${price:.2f}/hr")
            output.append("")
        
        return "\n".join(output)

def main():
    """Example usage of the CrusoeCloudProvider."""
    logging.basicConfig(level=logging.INFO)
    
    provider = CrusoeCloudProvider()
    platforms = provider.get_gpu_pricing()
    
    if platforms:
        print(provider.format_gpu_pricing(platforms))
    else:
        print("Failed to fetch GPU pricing information")

if __name__ == "__main__":
    main()