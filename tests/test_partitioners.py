#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for partitioner functions.
"""

import pytest

from confluent_kafka import consistent, fnv1a, murmur2


class TestPartitioners:

    def test_deterministic(self):
        """Test that deterministic partitioners produce the same output for the same input."""
        key = b"test_key"
        partition_count = 10

        # Same input should always produce same output
        assert murmur2(key, partition_count) == murmur2(key, partition_count)
        assert consistent(key, partition_count) == consistent(key, partition_count)
        assert fnv1a(key, partition_count) == fnv1a(key, partition_count)

    def test_input_validation(self):
        """Test input validation logic."""
        # Invalid partition_count
        with pytest.raises(ValueError, match="partition_count must be > 0"):
            murmur2(b"key", 0)

        # Invalid key type
        with pytest.raises(TypeError):
            murmur2("string_key", 10)

        # Invalid partition_count type
        with pytest.raises(TypeError):
            murmur2(b"key", "10")
