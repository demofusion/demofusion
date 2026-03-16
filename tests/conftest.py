"""Pytest configuration for demofusion tests."""

import os
import pytest


DEMO_PATH = os.environ.get(
    "DEMOFUSION_TEST_DEMO", "/home/opencode/shared/66499852.dem"
)


@pytest.fixture
def demo_path():
    """Path to test demo file. Skips test if not available."""
    if not os.path.isfile(DEMO_PATH):
        pytest.skip(f"Demo file not found: {DEMO_PATH}")
    return DEMO_PATH


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    import asyncio

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
