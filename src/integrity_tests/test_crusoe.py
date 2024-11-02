#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  2 13:53:46 2024

@author: dwchuang_mbp2
"""

from pathlib import Path
import pytest
import csv
from io import StringIO

@pytest.fixture
def data(catalog_dir: Path) -> str:
    """Read the Crusoe catalog CSV file."""
    return (catalog_dir / "crusoe.csv").read_text()

class TestCrusoeCatalog:
    def test_gpus_presented(self, data: str):
        """Test that all expected GPU models are present in the catalog."""
        expected_gpus = [
            "H200",
            "H100",
            "A100",
            "MI300x",
            "L40S",
            "A40"
        ]
        assert all(f",{gpu}," in data for gpu in expected_gpus)

    def test_gpu_memory_configs(self, data: str):
        """Test that expected GPU memory configurations are present."""
        memory_configs = {
            "H200": "80GB",
            "H100": "80GB",
            "A100": ["40GB", "80GB"],
            "MI300x": "192GB",
            "L40S": "80GB",
            "A40": "48GB"
        }
        
        for gpu, memory in memory_configs.items():
            if isinstance(memory, list):
                assert any(f",{gpu},{mem}," in data for mem in memory)
            else:
                assert f",{gpu},{memory}," in data

    def test_form_factors(self, data: str):
        """Test that expected form factors are present."""
        form_factors = [
            "SXM",
            "PCIe",
            "OAM"
        ]
        assert all(f",{ff}," in data for ff in form_factors)

    def test_pricing_tiers(self, data: str):
        """Test that all pricing tiers are present."""
        pricing_tiers = [
            "On-Demand",
            "6-month reserved",
            "1-year reserved",
            "3-year reserved"
        ]
        
        csv_data = csv.DictReader(StringIO(data))
        for row in csv_data:
            assert all(tier in row for tier in pricing_tiers)

    def test_h100_configurations(self, data: str):
        """Test specific H100 configurations."""
        h100_configs = [
            # format: memory, form_factor
            ("80GB", "SXM"),
        ]
        
        for memory, form_factor in h100_configs:
            assert f",H100,{memory},{form_factor}," in data

    def test_valid_prices(self, data: str):
        """Test that prices are either valid floats or 'Contact Us'."""
        csv_data = csv.DictReader(StringIO(data))
        price_columns = [
            "On-Demand",
            "6-month reserved",
            "1-year reserved",
            "3-year reserved"
        ]
        
        for row in csv_data:
            for col in price_columns:
                price = row[col]
                if price != "Contact Us":
                    try:
                        float(price)
                    except ValueError:
                        pytest.fail(f"Invalid price format: {price}")

    def test_mi300x_contact_us(self, data: str):
        """Test that MI300x shows 'Contact Us' for pricing."""
        csv_data = csv.DictReader(StringIO(data))
        for row in csv_data:
            if "MI300x" in row["gpu_name"]:
                assert all(row[tier] == "Contact Us" for tier in [
                    "On-Demand",
                    "6-month reserved",
                    "1-year reserved",
                    "3-year reserved"
                ])

    def test_memory_sizes(self, data: str):
        """Test that memory sizes are valid integers followed by GB."""
        csv_data = csv.DictReader(StringIO(data))
        for row in csv_data:
            memory = row["memory"]
            assert memory.endswith("GB")
            memory_size = memory[:-2]
            assert memory_size.isdigit()
            assert int(memory_size) > 0

    def test_expected_csv_structure(self, data: str):
        """Test that CSV has the expected column structure."""
        expected_headers = {
            "gpu_name",
            "memory",
            "form_factor",
            "On-Demand",
            "6-month reserved",
            "1-year reserved",
            "3-year reserved"
        }
        
        csv_data = csv.reader(StringIO(data))
        headers = set(next(csv_data))
        assert headers == expected_headers