"""Tests for the analytics API layer."""
import os
import sys
import json
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.analytics import _fmt


class TestFormatting:
    def test_fmt_float(self):
        assert _fmt(3.14159, 2) == 3.14

    def test_fmt_int(self):
        assert _fmt(42) == 42

    def test_fmt_numpy(self):
        import numpy as np
        assert _fmt(np.float64(2.5), 1) == 2.5
        assert _fmt(np.int64(100)) == 100

    def test_fmt_string_passthrough(self):
        assert _fmt("hello") == "hello"
