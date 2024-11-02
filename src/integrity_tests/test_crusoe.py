#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  2 12:56:44 2024

@author: dwchuang_mbp2
"""

from pathlib import Path

import pytest


@pytest.fixture
def data(catalog_dir: Path) -> str:
    return (catalog_dir / "crusoe.csv").read_text()


class TestCrusoeCatalog:
    def test_location_presented(self, data: str):
        """Test that the default location is present"""
        assert ",us-central," in data  # Crusoe's primary location

    def test_gpu_presented(self, data: str):
        """Test that all GPU types are present"""
        gpus = [
            "H200",
            "H100",
            "MI300X",
            "A100",
            "L40S",
            "A40",
        ]
        assert all(f",{i}," in data for i in gpus)

    def test_gpu_variants(self, data: str):
        """Test specific GPU variants are present"""
        variants = [
            # A100 variants
            "A100,80,",  # 80GB SXM
            "A100,40,",  # 40GB PCIe
            # H100 variants
            "H100,80,",  # 80GB SXM
            # H200 variants
            "H200,80,",  # 80GB
        ]
        assert all(v in data for v in variants)

    def test_pricing_tiers(self, data: str):
        """Test that different pricing tiers appear for instances"""
        # Check that we have both on-demand and reserved instances
        assert "crusoe-h100" in data  # On-demand
        assert "crusoe-h100-reserved-6month" in data  # 6-month reserved
        assert "crusoe-h100-reserved-1year" in data  # 1-year reserved
        assert "crusoe-h100-reserved-3year" in data  # 3-year reserved

    def test_gpu_memory_correct(self, data: str):
        """Test that GPU memory is correctly specified"""
        memory_configs = [
            ("H200", "80"),
            ("H100", "80"),
            ("MI300X", "192"),
            ("A100", "80"),
            ("A100", "40"),
            ("L40S", "80"),
            ("A40", "48"),
        ]
        for gpu, memory in memory_configs:
            assert f",{gpu},{memory}," in data

    def test_no_spots(self, data: str):
        """Test that no spot instances are present (Crusoe doesn't offer spot)"""
        assert ",True\n" not in data

    def test_gpu_counts(self, data: str):
        """Test that GPU counts are correct"""
        # Crusoe offers single GPU instances
        lines = data.split("\n")
        for line in lines:
            if line and not line.startswith("instance_name"):
                parts = line.split(",")
                if len(parts) > 5:  # Make sure line has enough fields
                    gpu_count_index = 5  # Adjust based on your CSV structure
                    assert parts[gpu_count_index] == "1"  # All instances have 1 GPU

    def test_required_fields(self, data: str):
        """Test that all required fields are present in the CSV"""
        required_headers = [
            "instance_name",
            "location",
            "price",
            "cpu",
            "memory",
            "gpu_count",
            "gpu_name",
            "gpu_memory",
            "spot"
        ]
        header_line = data.split("\n")[0]
        for field in required_headers:
            assert field in header_line

    def test_valid_prices(self, data: str):
        """Test that prices are valid numbers and in expected ranges"""
        lines = data.split("\n")[1:]  # Skip header
        for line in lines:
            if line:
                price = float(line.split(",")[2])  # Price column index
                assert 0.5 <= price <= 5.0  # Crusoe's price range as of 2024

    def test_instance_naming(self, data: str):
        """Test that instance names follow the correct format"""
        lines = data.split("\n")[1:]  # Skip header
        for line in lines:
            if line:
                instance_name = line.split(",")[0]
                assert instance_name.startswith("crusoe-")
                # Check reserved instance naming
                if "reserved" in instance_name:
                    assert any(term in instance_name for term in [
                        "reserved-6month",
                        "reserved-1year",
                        "reserved-3year"
                    ])

    def test_cpu_memory_ratios(self, data: str):
        """Test that CPU and memory configurations are consistent"""
        expected_ratios = {
            "H200": (96, 2048),  # (CPU cores, Memory GB)
            "H100": (96, 2048),
            "MI300X": (96, 2048),
            "A100": (48, 1024),
            "L40S": (48, 512),
            "A40": (24, 256),
        }
        
        lines = data.split("\n")[1:]  # Skip header
        for line in lines:
            if line:
                parts = line.split(",")
                gpu_name = parts[6]  # GPU name column
                cpu_count = int(parts[3])  # CPU count column
                memory_gb = float(parts[4])  # Memory column
                
                if gpu_name in expected_ratios:
                    expected_cpu, expected_memory = expected_ratios[gpu_name]
                    assert cpu_count == expected_cpu
                    assert memory_gb == expected_memory